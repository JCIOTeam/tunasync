//! Command provider — runs an arbitrary shell command to perform a sync.
//!
//! Mirrors Go's `cmdProvider` / `cmd_provider.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use regex::Regex;

use crate::hooks::DockerConfig;
use crate::provider::MirrorProvider;
use crate::runner;

/// Configuration for a command provider instance.
pub struct CmdProvider {
    pub name: String,
    pub upstream: String,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Shared log path — set by LogLimitHook::preExec, read in run().
    /// Falls back to `log_dir/latest.log` when PreExec hasn't run yet.
    pub log_path_shared: Arc<Mutex<PathBuf>>,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    pub env: HashMap<String, String>,
    pub command: Vec<String>,
    pub fail_on_match: Option<Regex>,
    pub size_pattern: Option<Regex>,
    pub success_exit_codes: Vec<i32>,
    /// Minimum free-space bytes required before starting a sync. 0 = no check.
    pub disk_quota_bytes: u64,
    /// Whether to probe upstream reachability before syncing.
    pub check_upstream: bool,
    /// Fallback upstream URLs (rsync:// or http(s)://) for the probe.
    /// Used only by `probe_upstream`; the actual sync command is unchanged.
    pub upstream_fallback: Vec<String>,
    /// Whether to swap staging↔publish atomically via renameat2 after sync.
    pub atomic_publish_enabled: bool,
    /// Resolved staging directory for atomic publish (per mirror / global /
    /// fallback chain — see `MirrorConfig::effective_staging_dir`).
    pub atomic_staging_path: PathBuf,
    data_size: Mutex<String>,
    /// PID of the currently running child process (set before wait, cleared after).
    current_pid: Arc<Mutex<Option<u32>>>,
    /// Docker container name, set when DockerHook wraps the command.
    docker_container_name: Option<String>,
    /// Docker wrapping config — set by `build_providers()` when Docker is active.
    docker_config: Option<DockerConfig>,
    /// CgroupHook reference — set on Linux when cgroup is active.
    #[cfg(target_os = "linux")]
    cgroup_hook: Option<std::sync::Arc<crate::hooks::CgroupHook>>,
    /// Per-mirror live-log broadcast sender (powers the streaming log API).
    log_publisher: Option<crate::log_stream::LogPublisher>,
}

impl CmdProvider {
    /// Build from a `MirrorConfig` + global config.
    pub fn from_config(
        mc: &crate::config::MirrorConfig,
        global: &crate::config::GlobalConfig,
    ) -> Result<Self> {
        let command_str = &mc.command;
        let command = shell_words::split(command_str)
            .with_context(|| format!("parse command for mirror {:?}", mc.name))?;
        if command.is_empty() {
            anyhow::bail!("mirror {:?}: command is empty", mc.name);
        }

        let fail_on_match =
            if mc.fail_on_match.is_empty() {
                None
            } else {
                Some(Regex::new(&mc.fail_on_match).with_context(|| {
                    format!("mirror {:?}: invalid fail_on_match regex", mc.name)
                })?)
            };

        let size_pattern = if mc.size_pattern.is_empty() {
            None
        } else {
            Some(
                Regex::new(&mc.size_pattern)
                    .with_context(|| format!("mirror {:?}: invalid size_pattern regex", mc.name))?,
            )
        };

        let working_dir = mc.effective_mirror_dir(global);
        let log_dir = if mc.log_dir.is_empty() {
            PathBuf::from(&global.log_dir)
        } else {
            PathBuf::from(&mc.log_dir)
        };
        let log_path_shared = Arc::new(Mutex::new(log_dir.join("latest.log")));

        // Merge success exit codes exactly as Go's newMirrorProvider does:
        // global codes first, then mirror-specific codes.
        // Note: rsync_success_exit_codes are intentionally NOT included for
        // command providers (Go warns and ignores them for non-rsync providers).
        let mut success_exit_codes = mc.success_exit_codes.clone();
        success_exit_codes.extend(global.dangerous_global_success_exit_codes.iter().copied());
        if !mc.rsync_success_exit_codes.is_empty() {
            tracing::warn!(
                mirror = %mc.name,
                "rsync_success_exit_codes is ignored for command provider"
            );
        }

        let disk_quota_bytes = if mc.disk_quota.is_empty() {
            0
        } else {
            tunasync_common::util::parse_size_bytes(&mc.disk_quota).unwrap_or(0)
        };

        Ok(Self {
            name: mc.name.clone(),
            upstream: mc.upstream.clone(),
            working_dir,
            log_dir,
            log_path_shared,
            interval: mc.effective_interval(global),
            retry: mc.effective_retry(global),
            timeout: mc.effective_timeout(global).unwrap_or(Duration::ZERO),
            is_master: mc.is_master(),
            env: mc.env.clone(),
            command,
            fail_on_match,
            size_pattern,
            success_exit_codes,
            disk_quota_bytes,
            check_upstream: mc.check_upstream,
            upstream_fallback: mc.upstream_fallback.clone(),
            atomic_publish_enabled: mc.atomic_publish,
            atomic_staging_path: mc.effective_staging_dir(global),
            data_size: Mutex::new(String::new()),
            current_pid: Arc::new(Mutex::new(None)),
            docker_container_name: None,
            docker_config: None,
            #[cfg(target_os = "linux")]
            cgroup_hook: None,
            log_publisher: None,
        })
    }

    /// Build environment for the user's sync command.
    ///
    /// `working_dir_override` lets the caller substitute a staging directory
    /// for atomic-publish — the user's script sees that as its destination
    /// via `TUNASYNC_WORKING_DIR`. Pass `None` to use `self.working_dir`.
    fn tunasync_env(
        &self,
        working_dir_override: Option<&std::path::Path>,
    ) -> HashMap<String, String> {
        let log_file = self.log_path_shared.lock().unwrap().clone();
        let wd = working_dir_override.unwrap_or(&self.working_dir);
        let mut env = HashMap::new();
        env.insert("TUNASYNC_MIRROR_NAME".into(), self.name.clone());
        env.insert("TUNASYNC_WORKING_DIR".into(), wd.to_string_lossy().into());
        env.insert("TUNASYNC_UPSTREAM_URL".into(), self.upstream.clone());
        env.insert(
            "TUNASYNC_LOG_DIR".into(),
            self.log_dir.to_string_lossy().into(),
        );
        env.insert(
            "TUNASYNC_LOG_FILE".into(),
            log_file.to_string_lossy().into(),
        );
        // User-defined env overrides.
        env.extend(self.env.iter().map(|(k, v)| (k.clone(), v.clone())));
        env
    }
}

#[async_trait]
impl MirrorProvider for CmdProvider {
    fn name(&self) -> &str {
        &self.name
    }
    fn upstream(&self) -> &str {
        &self.upstream
    }
    fn is_master(&self) -> bool {
        self.is_master
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    fn retry(&self) -> u32 {
        self.retry
    }
    fn timeout(&self) -> Duration {
        self.timeout
    }

    async fn run(&self) -> Result<()> {
        *self.data_size.lock().unwrap() = String::new();

        // Determine sync destination (staging or publish dir).
        let publish_dir = self.working_dir.clone();
        let sync_target = if self.atomic_publish_enabled {
            self.atomic_staging_path.clone()
        } else {
            publish_dir.clone()
        };
        if self.atomic_publish_enabled {
            super::rsync_provider::ensure_atomic_publish_dirs(&sync_target, &publish_dir)
                .with_context(|| format!("atomic publish setup for {}", self.name))?;
        }

        let wd_override = if self.atomic_publish_enabled {
            Some(sync_target.as_path())
        } else {
            None
        };
        let env = self.tunasync_env(wd_override);
        // When Docker wrapping is active, the argv is wrapped with `docker run …`
        // and env vars go through `-e` flags (inside the container). The host
        // `docker run` process doesn't need those env overrides.
        //
        // CRITICAL: when atomic_publish is on, the container must see the
        // staging dir (not the publish dir) as its working directory and
        // TUNASYNC_WORKING_DIR. Otherwise a user-supplied mirror script
        // running inside the container would write directly to the publish
        // path and completely bypass the atomic-swap mechanism.
        let (argv, spawn_env) = if let Some(docker) = &self.docker_config {
            (
                docker.wrap_argv_for(&self.command, wd_override),
                HashMap::new(),
            )
        } else {
            (self.command.clone(), env)
        };

        let log_file = self.log_path_shared.lock().unwrap().clone();
        let log_path = if log_file.to_string_lossy() == "/dev/null" {
            None
        } else {
            Some(log_file.as_path())
        };

        let proc = runner::spawn(
            &argv,
            &sync_target,
            &spawn_env,
            log_path,
            self.log_publisher.clone(),
        )
        .await?;

        // Store PID so terminate() can send SIGTERM.
        if let Some(pid) = proc.pid() {
            *self.current_pid.lock().unwrap() = Some(pid);
        }
        // Place the child PID into the cgroup (Linux only).
        #[cfg(target_os = "linux")]
        if let Some(ref hook) = self.cgroup_hook {
            if let Err(e) = hook.add_pid_stopped(&proc) {
                tracing::warn!(mirror = %self.name, error = %e, "failed to add PID to cgroup");
            }
        }
        let wait_result = proc.wait(&self.success_exit_codes).await;
        *self.current_pid.lock().unwrap() = None;
        wait_result?;

        // Atomic publish swap.
        if self.atomic_publish_enabled {
            super::rsync_provider::atomic_publish_swap(&sync_target, &publish_dir).with_context(
                || {
                    format!(
                        "atomic publish: swap {} ↔ {}",
                        sync_target.display(),
                        publish_dir.display()
                    )
                },
            )?;
            tracing::info!(mirror = %self.name, dest = %publish_dir.display(), "atomic publish complete");
        }

        // Check fail_on_match regex in the log file.
        if let Some(re) = &self.fail_on_match {
            if log_file.exists() {
                let content = tokio::fs::read_to_string(&log_file)
                    .await
                    .unwrap_or_default();
                let matches: Vec<_> = re.find_iter(&content).collect();
                if !matches.is_empty() {
                    anyhow::bail!("fail_on_match regex found {} matches in log", matches.len());
                }
            }
        }

        // Extract size from log — matches Go's ExtractSizeFromLog.
        // Go uses FindAllSubmatch and takes the first capture group of the LAST match.
        // Our re.find_iter gives full matches; use find_iter + captures to get groups.
        if let Some(re) = &self.size_pattern {
            if log_file.exists() {
                let content = tokio::fs::read_to_string(&log_file)
                    .await
                    .unwrap_or_default();
                let all_captures: Vec<_> = re.captures_iter(&content).collect();
                if let Some(last_cap) = all_captures.last() {
                    // Capture group 1 if present, else full match (group 0).
                    let size = last_cap
                        .get(1)
                        .or(last_cap.get(0))
                        .map(|m| m.as_str())
                        .unwrap_or_default();
                    *self.data_size.lock().unwrap() = size.to_owned();
                }
            }
        }

        Ok(())
    }

    async fn terminate(&self) -> Result<()> {
        #[cfg(unix)]
        {
            if let Some(ref name) = self.docker_container_name {
                let out = tokio::process::Command::new("docker")
                    .args(["stop", "-t", "2", name])
                    .output()
                    .await;
                match out {
                    Ok(o) if o.status.success() => {
                        tracing::debug!(container = %name, "docker stop succeeded");
                    }
                    Ok(o) => {
                        tracing::warn!(container = %name, status = %o.status, "docker stop failed — falling back to SIGTERM");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, container = %name, "docker stop failed — falling back to SIGTERM");
                    }
                }
            }
            // Extract PID before awaiting so the MutexGuard is dropped (not Send).
            let pid = *self.current_pid.lock().unwrap();
            if let Some(pid) = pid {
                runner::terminate_process_group(pid).await;
            }
        }
        Ok(())
    }

    fn data_size(&self) -> String {
        self.data_size.lock().unwrap().clone()
    }

    fn working_dir(&self) -> &std::path::Path {
        &self.working_dir
    }

    fn disk_quota_bytes(&self) -> u64 {
        self.disk_quota_bytes
    }

    fn atomic_publish(&self) -> bool {
        self.atomic_publish_enabled
    }

    /// Probe upstream reachability before syncing.
    ///
    /// For cmd providers the upstream URL might be HTTP(S), rsync://, ftp,
    /// or anything the user's script supports — there is no single probe
    /// command that works universally. We default to attempting an rsync
    /// probe if the URL begins with `rsync://`, an HTTP HEAD otherwise.
    /// On a 5xx or network error we treat the URL as unreachable.
    /// Each probe is wrapped in a 15-second tokio::time::timeout, and the
    /// primary + fallbacks run concurrently — see RsyncProvider's
    /// probe_upstream for the rationale.
    async fn probe_upstream(&self) -> anyhow::Result<()> {
        if !self.check_upstream {
            return Ok(());
        }
        let mut urls: Vec<&str> = vec![self.upstream.as_str()];
        urls.extend(self.upstream_fallback.iter().map(String::as_str));

        use futures::stream::{FuturesUnordered, StreamExt};
        let mut futures: FuturesUnordered<_> = urls
            .iter()
            .map(|&url| async move {
                let res =
                    tokio::time::timeout(std::time::Duration::from_secs(15), probe_url(url)).await;
                (url, res)
            })
            .collect();

        let mut last_err: Option<String> = None;
        while let Some((url, res)) = futures.next().await {
            match res {
                Ok(Ok(())) => {
                    if url != self.upstream.as_str() {
                        tracing::info!(
                            mirror = %self.name,
                            primary = %self.upstream,
                            reachable = %url,
                            "primary upstream unreachable; fallback responded"
                        );
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_err = Some(format!("{url}: {e}"));
                }
                Err(_) => {
                    last_err = Some(format!("{url}: probe timed out after 15s"));
                }
            }
        }
        anyhow::bail!(
            "all {} upstream(s) unreachable; last: {}",
            urls.len(),
            last_err.as_deref().unwrap_or("no probes attempted")
        )
    }

    fn set_docker_config(&mut self, config: DockerConfig) {
        self.docker_container_name = Some(config.container_name());
        self.docker_config = Some(config);
    }

    fn set_log_path_shared(&mut self, path: Arc<Mutex<PathBuf>>) {
        self.log_path_shared = path;
    }

    fn set_log_publisher(&mut self, p: crate::log_stream::LogPublisher) {
        self.log_publisher = Some(p);
    }

    #[cfg(target_os = "linux")]
    fn set_cgroup_hook(&mut self, hook: std::sync::Arc<crate::hooks::CgroupHook>) {
        self.cgroup_hook = Some(hook);
    }
}

impl CmdProvider {
    /// Set the docker container name when DockerHook wraps the command.
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}

/// Probe a single URL for reachability — used by `CmdProvider::probe_upstream`.
///
/// For `rsync://` URLs we delegate to the rsync probe in
/// `rsync_provider::probe_rsync_url` (already wrapped with --contimeout and
/// kill-on-drop). For other schemes (http, https, ftp, file, custom) we do
/// an HTTP HEAD via reqwest; on any 2xx-or-3xx response or a redirect chain
/// we consider it reachable. ftp/file/etc. are accepted optimistically
/// (we return Ok without probing) — these are typically used with
/// user scripts that don't lend themselves to a generic health check.
async fn probe_url(url: &str) -> anyhow::Result<()> {
    if url.starts_with("rsync://") {
        return super::rsync_provider::probe_rsync_url(url).await;
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        // Share a single reqwest::Client across all probe calls. Each Client
        // maintains its own connection pool; rebuilding it on every probe
        // throws away DNS cache and TLS sessions unnecessarily, adding latency
        // on each call. once_cell::sync::Lazy gives us a zero-cost static
        // initialiser that is safe to call from async context.
        use once_cell::sync::Lazy;
        static HTTP_PROBE_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::limited(5))
                .build()
                .expect("build static http probe client")
        });
        let resp = HTTP_PROBE_CLIENT
            .head(url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("http HEAD failed: {e}"))?;
        let status = resp.status();
        if status.is_success() || status.is_redirection() {
            return Ok(());
        }
        anyhow::bail!("http HEAD returned status {status}");
    }
    // ftp://, file://, custom schemes: optimistic — assume reachable.
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    //! Unit tests for CmdProvider's new extension fields.
    //!
    //! These were added alongside the same fields on RsyncProvider but had
    //! no direct test coverage. The integration paths through
    //! atomic_publish_swap and probe_url are tested via rsync_provider.rs;
    //! here we verify that CmdProvider correctly reads its config fields,
    //! that the MirrorProvider trait getters reflect those values, and that
    //! the no-op probe path returns Ok when check_upstream is false.

    use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
    use crate::provider::MirrorProvider;

    fn base_global() -> GlobalConfig {
        let mut g = GlobalConfig::default();
        g.log_dir = "/tmp/tunasync-cmd-test-log".into();
        g.mirror_dir = "/tmp/tunasync-cmd-test-mirror".into();
        g
    }

    fn base_mirror() -> MirrorConfig {
        let mut mc = MirrorConfig::default();
        mc.name = "cmd-test".into();
        mc.provider = ProviderKind::Command;
        mc.upstream = "https://example.com/data/".into();
        mc.command = "/bin/true".into();
        mc
    }

    /// atomic_publish defaults to false; the trait getter must reflect this.
    #[test]
    fn atomic_publish_defaults_to_false() {
        let global = base_global();
        let mc = base_mirror();
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert!(
            !p.atomic_publish(),
            "atomic_publish must default to false on CmdProvider"
        );
    }

    /// Setting atomic_publish = true in the config must propagate to the
    /// trait getter.
    #[test]
    fn atomic_publish_flows_through_from_config() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.atomic_publish = true;
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert!(
            p.atomic_publish(),
            "atomic_publish must be true after config"
        );
    }

    /// When check_upstream is false (default), probe_upstream must be a
    /// no-op — it must not try to hit example.com / spawn rsync / DNS-resolve.
    #[tokio::test]
    async fn probe_upstream_noop_when_check_disabled() {
        let global = base_global();
        let mc = base_mirror();
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        // Even with a bogus upstream, this must return Ok in <100ms.
        let start = std::time::Instant::now();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(200), p.probe_upstream()).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "noop probe should be instant, took {elapsed:?}"
        );
        assert!(
            matches!(result, Ok(Ok(()))),
            "noop probe must return Ok, got {result:?}"
        );
    }

    /// upstream_fallback flows through unchanged from MirrorConfig.
    #[test]
    fn upstream_fallback_flows_through() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.upstream_fallback = vec![
            "https://fallback1.example.com/".into(),
            "https://fallback2.example.com/".into(),
        ];
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        assert_eq!(p.upstream_fallback.len(), 2);
        assert_eq!(p.upstream_fallback[0], "https://fallback1.example.com/");
    }

    /// disk_quota = "100M" parses to 100 * 1024 * 1024 bytes via the helper.
    #[test]
    fn disk_quota_parsed_into_bytes() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.disk_quota = "100M".into();
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert_eq!(p.disk_quota_bytes(), 100 * 1024 * 1024);
    }

    /// Empty disk_quota means no check (0 bytes).
    #[test]
    fn disk_quota_empty_is_zero() {
        let global = base_global();
        let mc = base_mirror();
        let p = super::CmdProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert_eq!(p.disk_quota_bytes(), 0);
    }
}
