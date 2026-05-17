//! rsync provider — mirrors Go's `rsyncProvider` / `rsync_provider.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::hooks::DockerConfig;
use crate::provider::MirrorProvider;
use crate::runner;

/// Rsync provider.
pub struct RsyncProvider {
    pub name: String,
    pub upstream: String,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Shared log path — set by LogLimitHook::preExec, read in run().
    pub log_path_shared: Arc<Mutex<PathBuf>>,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    options: Vec<String>,
    rsync_cmd: String,
    rsync_env: HashMap<String, String>,
    /// Minimum free-space bytes required before starting a sync.
    /// Parsed from `MirrorConfig::disk_quota` via `parse_size_bytes`.
    /// 0 = no quota check.
    pub disk_quota_bytes: u64,
    /// Whether to probe upstream reachability before syncing.
    /// Maps to `MirrorConfig::check_upstream`.
    pub check_upstream: bool,
    /// Fallback upstream URLs tried when the primary is unreachable.
    /// These are only used for the pre-sync probe — the actual rsync
    /// data source never changes.
    pub upstream_fallback: Vec<String>,
    pub success_exit_codes: Vec<i32>,
    data_size: Mutex<String>,
    transferred_bytes: Mutex<u64>,
    current_pid: Arc<Mutex<Option<u32>>>,
    /// Docker container name, set when DockerHook wraps the command.
    docker_container_name: Option<String>,
    /// Docker wrapping config — set by `build_providers()` when Docker is active.
    docker_config: Option<DockerConfig>,
    /// CgroupHook reference — set by `build_providers()` on Linux when cgroup is
    /// active. The provider calls `add_pid_stopped` after spawn so the child
    /// process is placed inside the cgroup before execution begins.
    #[cfg(target_os = "linux")]
    cgroup_hook: Option<std::sync::Arc<crate::hooks::CgroupHook>>,
}

impl RsyncProvider {
    /// Build from mirror + global config.
    pub fn from_config(
        mc: &crate::config::MirrorConfig,
        global: &crate::config::GlobalConfig,
    ) -> Result<Self> {
        if !mc.upstream.ends_with('/') {
            anyhow::bail!("rsync upstream URL must end with '/': {:?}", mc.upstream);
        }

        let working_dir = mc.effective_mirror_dir(global);
        let log_dir = if mc.log_dir.is_empty() {
            PathBuf::from(&global.log_dir)
        } else {
            PathBuf::from(&mc.log_dir)
        };
        let log_path_shared = Arc::new(Mutex::new(log_dir.join("latest.log")));

        let rsync_cmd = if mc.command.is_empty() {
            "rsync".to_string()
        } else {
            mc.command.clone()
        };

        // Validate: rsync_override_only requires rsync_override to be non-empty.
        if mc.rsync_override_only && mc.rsync_override.is_empty() {
            anyhow::bail!("rsync_override_only is set but no rsync_override provided");
        }

        // Build rsync options — matches Go's newRsyncProvider exactly.
        let mut options: Vec<String> = if !mc.rsync_override.is_empty() {
            mc.rsync_override.clone()
        } else {
            vec![
                "-aHvh".into(),
                "--no-o".into(),
                "--no-g".into(),
                "--stats".into(),
                "--filter".into(),
                "risk .~tmp~/".into(),
                "--exclude".into(),
                ".~tmp~/".into(),
                "--delete".into(),
                "--delete-after".into(),
                "--delay-updates".into(),
                "--safe-links".into(),
            ]
        };

        if !mc.rsync_override_only {
            if !mc.rsync_no_timeout {
                let timeo = if mc.rsync_timeout > 0 {
                    mc.rsync_timeout
                } else {
                    120
                };
                options.push(format!("--timeout={timeo}"));
            }
            if mc.use_ipv6 {
                options.push("-6".into());
            } else if mc.use_ipv4 {
                options.push("-4".into());
            }
            if !mc.exclude_file.is_empty() {
                options.extend(["--exclude-from".into(), mc.exclude_file.clone()]);
            }
            // global rsync options
            options.extend(global.rsync_options.iter().cloned());
            // mirror-specific rsync options
            options.extend(mc.rsync_options.iter().cloned());
        }

        // Environment.
        let mut rsync_env = HashMap::new();
        if !mc.username.is_empty() {
            rsync_env.insert("USER".into(), mc.username.clone());
        }
        if !mc.password.is_empty() {
            rsync_env.insert("RSYNC_PASSWORD".into(), mc.password.clone());
        }

        // Merge global success exit codes.
        let mut success_exit_codes = mc.success_exit_codes.clone();
        success_exit_codes.extend(global.dangerous_global_success_exit_codes.iter());
        success_exit_codes.extend(global.dangerous_global_rsync_success_exit_codes.iter());
        success_exit_codes.extend(mc.rsync_success_exit_codes.iter());

        // Parse disk quota — empty or unparseable = 0 = no check.
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
            options,
            rsync_cmd,
            rsync_env,
            success_exit_codes,
            disk_quota_bytes,
            check_upstream: mc.check_upstream,
            upstream_fallback: mc.upstream_fallback.clone(),
            data_size: Mutex::new(String::new()),
            transferred_bytes: Mutex::new(0),
            current_pid: Arc::new(Mutex::new(None)),
            docker_container_name: None,
            docker_config: None,
            #[cfg(target_os = "linux")]
            cgroup_hook: None,
        })
    }

    fn build_argv(&self) -> Vec<String> {
        let mut argv = vec![self.rsync_cmd.clone()];
        argv.extend(self.options.iter().cloned());
        argv.push(self.upstream.clone());
        argv.push(self.working_dir.to_string_lossy().into());
        argv
    }
}

#[async_trait]
impl MirrorProvider for RsyncProvider {
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
        let argv = self.build_argv();
        // When Docker wrapping is active, the argv is wrapped with `docker run …`
        // and env vars go through `-e` flags. The host process doesn't need them.
        let (argv, spawn_env) = if let Some(docker) = &self.docker_config {
            (docker.wrap_argv(&argv), HashMap::new())
        } else {
            (argv, self.rsync_env.clone())
        };

        let log_file = self.log_path_shared.lock().unwrap().clone();
        let log_path = if log_file.to_string_lossy() == "/dev/null" {
            None
        } else {
            Some(log_file.as_path())
        };

        let proc = runner::spawn(&argv, &self.working_dir, &spawn_env, log_path)
            .await
            .with_context(|| format!("spawn rsync for {}", self.name))?;

        if let Some(pid) = proc.pid() {
            *self.current_pid.lock().unwrap() = Some(pid);
        }
        // Place the child PID into the cgroup (Linux only). Must happen between
        // spawn() and wait() so the process is in the cgroup before it executes.
        #[cfg(target_os = "linux")]
        if let Some(ref hook) = self.cgroup_hook {
            if let Err(e) = hook.add_pid_stopped(&proc) {
                tracing::warn!(mirror = %self.name, error = %e, "failed to add PID to cgroup");
            }
        }
        let wait_result = proc.wait(&self.success_exit_codes).await;
        *self.current_pid.lock().unwrap() = None;
        wait_result?;

        // Extract size and transferred bytes from log after successful run.
        if log_file.exists() {
            let content = tokio::fs::read_to_string(&log_file)
                .await
                .unwrap_or_default();
            let size = tunasync_common::util::extract_size_from_rsync_log(&content);
            if !size.is_empty() {
                *self.data_size.lock().unwrap() = size;
            }
            let transferred = tunasync_common::util::extract_transferred_bytes_from_rsync_log(&content);
            if transferred > 0 {
                *self.transferred_bytes.lock().unwrap() = transferred;
            }
        }
        Ok(())
    }

    async fn terminate(&self) -> Result<()> {
        #[cfg(unix)]
        {
            // If Docker is wrapping this command, call `docker stop` instead of raw SIGTERM.
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

    fn transferred_bytes(&self) -> u64 {
        *self.transferred_bytes.lock().unwrap()
    }

    fn working_dir(&self) -> &std::path::Path {
        &self.working_dir
    }

    fn disk_quota_bytes(&self) -> u64 {
        self.disk_quota_bytes
    }

    /// Probe upstream reachability before syncing.
    ///
    /// When `check_upstream` is true, tries the primary upstream and then each
    /// fallback in order. Returns `Ok(())` as soon as one URL is reachable
    /// (`rsync --list-only --timeout=10` exits 0). Returns `Err` only if every
    /// URL fails — the sync is then skipped, not retried.
    ///
    /// When `check_upstream` is false (default) this is a no-op.
    async fn probe_upstream(&self) -> anyhow::Result<()> {
        if !self.check_upstream {
            return Ok(());
        }
        let mut urls: Vec<&str> = vec![self.upstream.as_str()];
        urls.extend(self.upstream_fallback.iter().map(String::as_str));
        for url in &urls {
            if probe_rsync_url(url).await.is_ok() {
                if *url != self.upstream.as_str() {
                    tracing::info!(
                        mirror = %self.name,
                        primary = %self.upstream,
                        reachable = %url,
                        "primary upstream unreachable; fallback responded"
                    );
                }
                return Ok(());
            }
        }
        anyhow::bail!(
            "all upstreams unreachable ({} URL(s) probed)",
            urls.len()
        )
    }

    fn set_docker_config(&mut self, config: DockerConfig) {
        self.docker_container_name = Some(config.container_name());
        self.docker_config = Some(config);
    }

    fn set_log_path_shared(&mut self, path: Arc<Mutex<PathBuf>>) {
        self.log_path_shared = path;
    }

    #[cfg(target_os = "linux")]
    fn set_cgroup_hook(&mut self, hook: std::sync::Arc<crate::hooks::CgroupHook>) {
        self.cgroup_hook = Some(hook);
    }
}

/// Probe a single rsync URL for reachability.
///
/// Runs `rsync --list-only --timeout=10 <url>` and returns `Ok(())` if the
/// exit code is 0. This is intentionally lightweight — we don't care about
/// the listing contents, only whether the daemon is accepting connections.
async fn probe_rsync_url(url: &str) -> anyhow::Result<()> {
    let out = tokio::process::Command::new("rsync")
        .args(["--list-only", "--timeout=10", url])
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("failed to spawn rsync probe: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        let code = out.status.code().unwrap_or(-1);
        anyhow::bail!("rsync probe exited {code} for {url}")
    }
}

impl RsyncProvider {
    /// Set the docker container name when DockerHook wraps the command.
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}

#[cfg(test)]
mod upstream_probe_tests {
    //! Tests for probe_upstream / probe_rsync_url.
    //!
    //! The integration test (`probe_unreachable_url_fails_fast`) requires the
    //! `rsync` binary to be present. It is marked `#[ignore]` so it doesn't
    //! block CI environments that lack rsync; run it with `cargo test -- --ignored`.

    use super::probe_rsync_url;

    /// Probing a port that is not listening must return an error quickly.
    /// Uses 127.0.0.1:1 — port 1 is privileged and virtually never open,
    /// so rsync exits non-zero (usually with "connection refused").
    #[tokio::test]
    #[ignore = "requires rsync binary in PATH"]
    async fn probe_unreachable_url_fails_fast() {
        let result = probe_rsync_url("rsync://127.0.0.1:1/nonexistent").await;
        assert!(
            result.is_err(),
            "expected probe to fail for unreachable URL, got Ok"
        );
    }

    /// When check_upstream is false (default), probe_upstream must be a no-op
    /// and always succeed — even with a bogus upstream URL.
    #[tokio::test]
    async fn probe_upstream_noop_when_disabled() {
        use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
        use crate::provider::MirrorProvider;

        // Build a minimal config with check_upstream = false (the default).
        let global = GlobalConfig::default();
        let mut mc = MirrorConfig::default();
        mc.name = "probe-noop-test".into();
        mc.provider = ProviderKind::Rsync;
        mc.upstream = "rsync://localhost/will-not-be-called/".into();
        // check_upstream defaults to false — leave it unset.

        let provider = super::RsyncProvider::from_config(&mc, &global)
            .expect("from_config should succeed");

        // Must return Ok without spawning rsync.
        // Cast to the trait to invoke the overridden probe_upstream().
        let provider: &dyn MirrorProvider = &provider;
        provider.probe_upstream().await.expect("no-op probe should always succeed");
    }
}
