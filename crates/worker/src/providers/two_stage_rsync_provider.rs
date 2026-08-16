//! Two-stage rsync provider — mirrors Go's `twoStageRsyncProvider`.
//!
//! Stage 1 syncs files (no delete) using a distro-specific profile.
//! Stage 2 syncs the full tree with `--delete`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::hooks::DockerConfig;
use crate::provider::LaunchPlanSpec;
use crate::provider::MirrorProvider;
use crate::provider::ProbeError;
use crate::runner;

/// Stage-1 profiles — matches Go's `rsyncStage1Profiles`.
/// Returns profile-specific rsync options, or Err for unknown profiles.
/// Matches Go's `stage1ProfileOptions()`.
fn stage1_profile_options(profile: &str) -> anyhow::Result<Vec<String>> {
    match profile {
        "debian" => Ok(vec![
            "--include=*.diff/".into(),
            "--include=by-hash/".into(),
            "--exclude=*.diff/Index".into(),
            "--exclude=Contents*".into(),
            "--exclude=Packages*".into(),
            "--exclude=Sources*".into(),
            "--exclude=Release*".into(),
            "--exclude=InRelease".into(),
            "--exclude=i18n/*".into(),
            "--exclude=dep11/*".into(),
            "--exclude=installer-*/current".into(),
            "--exclude=ls-lR*".into(),
        ]),
        "debian-oldstyle" => Ok(vec![
            "--exclude=Packages*".into(),
            "--exclude=Sources*".into(),
            "--exclude=Release*".into(),
            "--exclude=InRelease".into(),
            "--exclude=i18n/*".into(),
            "--exclude=ls-lR*".into(),
            "--exclude=dep11/*".into(),
        ]),
        _ => anyhow::bail!("invalid Stage 1 Profile: {profile}"),
    }
}

/// Two-stage rsync provider.
pub struct TwoStageRsyncProvider {
    pub name: String,
    pub upstream: String,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Shared log path — set by LogLimitHook::preExec, read in run().
    /// Both stages write to the same rotated log file (matching Go where
    /// each cmdJob invocation truncates the same log file).
    pub log_path_shared: Arc<Mutex<PathBuf>>,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    stage1_options: Vec<String>,
    stage2_options: Vec<String>,
    rsync_cmd: String,
    rsync_env: HashMap<String, String>,
    pub success_exit_codes: Vec<i32>,
    /// Minimum free-space bytes required before starting a sync. 0 = no check.
    pub disk_quota_bytes: u64,
    /// Whether to probe upstream reachability before syncing.
    pub check_upstream: bool,
    /// Fallback upstream URLs.
    pub upstream_fallback: Vec<String>,
    /// Whether to swap staging↔publish atomically via renameat2 after sync.
    pub atomic_publish_enabled: bool,
    /// Resolved staging directory for atomic publish.
    pub atomic_staging_path: PathBuf,
    data_size: Mutex<String>,
    current_pid: Arc<Mutex<Option<crate::runner::ProcessHandle>>>,
    docker_container_name: Option<String>,
    /// Docker wrapping config — set by `build_providers()` when Docker is active.
    docker_config: Option<DockerConfig>,
    /// CgroupHook reference — set on Linux when cgroup is active.
    #[cfg(target_os = "linux")]
    cgroup_hook: Option<std::sync::Arc<crate::hooks::CgroupHook>>,
    /// Per-mirror live-log broadcast sender (powers the streaming log API).
    log_publisher: Option<crate::log_stream::LogPublisher>,
    broker_config: Option<crate::runner::BrokerConfig>,
}

impl TwoStageRsyncProvider {
    pub fn from_config(
        mc: &crate::config::MirrorConfig,
        global: &crate::config::GlobalConfig,
    ) -> Result<Self> {
        if !mc.upstream.ends_with('/') {
            anyhow::bail!(
                "two-stage-rsync upstream URL must end with '/': {:?}",
                crate::redact_url_diagnostic(&mc.upstream)
            );
        }

        let working_dir = mc.effective_mirror_dir(global);
        let log_dir = if mc.log_dir.is_empty() {
            PathBuf::from(&global.log_dir)
        } else {
            PathBuf::from(&mc.log_dir)
        };
        let log_path_shared = Arc::new(Mutex::new(log_dir.join("latest.log")));

        let base_opts_s1: Vec<String> = vec![
            "-aHvh".into(),
            "--no-o".into(),
            "--no-g".into(),
            "--stats".into(),
            "--filter".into(),
            "risk .~tmp~/".into(),
            "--exclude".into(),
            ".~tmp~/".into(),
            "--safe-links".into(),
        ];
        let base_opts_s2: Vec<String> = vec![
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
        ];

        // Build stage2 options: base + timeout + IPv4/6 + exclude-file + global/mirror rsync options.
        let append_common_for_stage2 = |mut opts: Vec<String>| -> Vec<String> {
            if !mc.rsync_no_timeout {
                let timeo = if mc.rsync_timeout > 0 {
                    mc.rsync_timeout
                } else {
                    120
                };
                opts.push(format!("--timeout={timeo}"));
            }
            if mc.use_ipv6 {
                opts.push("-6".into());
            } else if mc.use_ipv4 {
                opts.push("-4".into());
            }
            if !mc.exclude_file.is_empty() {
                opts.extend(["--exclude-from".into(), mc.exclude_file.clone()]);
            }
            opts.extend(global.rsync_options.iter().cloned());
            opts.extend(mc.rsync_options.iter().cloned());
            opts
        };

        // Stage1: base options + timeout + IPv4/6 + exclude-file. NO global/mirror rsync options (per Go).
        let mut stage1_options = base_opts_s1.clone();
        if !mc.rsync_no_timeout {
            let timeo = if mc.rsync_timeout > 0 {
                mc.rsync_timeout
            } else {
                120
            };
            stage1_options.push(format!("--timeout={timeo}"));
        }
        if mc.use_ipv6 {
            stage1_options.push("-6".into());
        } else if mc.use_ipv4 {
            stage1_options.push("-4".into());
        }
        if !mc.exclude_file.is_empty() {
            stage1_options.extend(["--exclude-from".into(), mc.exclude_file.clone()]);
        }
        stage1_options.extend(stage1_profile_options(&mc.stage1_profile)?);

        // Stage2: base options + timeout + IPv4/6 + exclude-file + global/mirror rsync options.
        let stage2_options = append_common_for_stage2(base_opts_s2);

        let mut rsync_env = HashMap::new();
        if !mc.username.is_empty() {
            rsync_env.insert("USER".into(), mc.username.clone());
        }
        if !mc.password.is_empty() {
            rsync_env.insert("RSYNC_PASSWORD".into(), mc.password.clone());
        }

        let mut success_exit_codes = mc.success_exit_codes.clone();
        success_exit_codes.extend(global.dangerous_global_success_exit_codes.iter());
        success_exit_codes.extend(global.dangerous_global_rsync_success_exit_codes.iter());
        success_exit_codes.extend(mc.rsync_success_exit_codes.iter());

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
            stage1_options,
            stage2_options,
            rsync_cmd: if mc.command.is_empty() {
                "rsync".to_string()
            } else {
                mc.command.clone()
            },
            rsync_env,
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
            broker_config: None,
        })
    }

    fn build_argv(&self, opts: &[String], dest: &std::path::Path) -> Vec<String> {
        let executable = if self.broker_config.is_some() && self.rsync_cmd == "rsync" {
            "/usr/bin/rsync".to_string()
        } else {
            self.rsync_cmd.clone()
        };
        let mut argv = vec![executable];
        argv.extend(opts.iter().cloned());
        argv.push(self.upstream.clone());
        argv.push(dest.to_string_lossy().into());
        argv
    }

    async fn run_stage(&self, stage: u8, dest: &std::path::Path) -> Result<()> {
        let opts = if stage == 1 {
            &self.stage1_options
        } else {
            &self.stage2_options
        };
        let argv = self.build_argv(opts, dest);
        // When Docker wrapping is active, wrap argv and use empty env.
        //
        // Under atomic_publish, `dest` is the staging directory (see the
        // caller in run() below). Pass it through as the Docker working_dir
        // override so the container's $PWD, -v mount, and TUNASYNC_WORKING_DIR
        // all match where rsync is writing — preventing user post-exec
        // scripts from bypassing the atomic swap.
        let wd_override = if self.atomic_publish_enabled {
            Some(dest)
        } else {
            None
        };
        let (argv, spawn_env) = if let Some(docker) = &self.docker_config {
            (docker.wrap_argv_for(&argv, wd_override), HashMap::new())
        } else {
            (argv, self.rsync_env.clone())
        };

        // Both stages write to the same shared log path (the rotated file
        // from LogLimitHook). tee_to_log truncates on open, so stage2
        // output overwrites stage1 — matching Go's cmdJob behaviour.
        let log_file = self.log_path_shared.lock().unwrap().clone();
        let lp = if log_file.to_string_lossy() == "/dev/null" {
            None
        } else {
            Some(log_file.as_path())
        };
        let operation = if stage == 1 { "stage1" } else { "stage2" };
        let placement = self
            .broker_config
            .as_ref()
            .map(|broker| broker.placement(operation))
            .unwrap_or_default();
        let proc = runner::spawn_placed(
            &argv,
            dest,
            &spawn_env,
            lp,
            self.log_publisher.clone(),
            &placement,
        )
        .await
        .with_context(|| format!("spawn rsync stage {stage} for {}", self.name))?;

        if let Some(handle) = proc.handle() {
            *self.current_pid.lock().unwrap() = Some(handle);
        }
        // Place the child PID into the cgroup (Linux only). Both stages are
        // placed individually since each stage is a separate spawn/wait cycle.
        #[cfg(target_os = "linux")]
        if let Some(ref hook) = self.cgroup_hook {
            if let Err(e) = hook.add_pid_stopped(&proc) {
                tracing::warn!(mirror = %self.name, stage, error = %e, "failed to add PID to cgroup");
            }
        }
        let result = proc
            .wait(&self.success_exit_codes)
            .await
            .with_context(|| format!("rsync stage {stage} for {} failed", self.name));
        *self.current_pid.lock().unwrap() = None;
        result
    }
}

#[async_trait]
impl MirrorProvider for TwoStageRsyncProvider {
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

        // Determine destination (staging or publish dir).
        let publish_dir = self.working_dir.clone();
        let dest = if self.atomic_publish_enabled {
            self.atomic_staging_path.clone()
        } else {
            publish_dir.clone()
        };
        if self.atomic_publish_enabled {
            super::rsync_provider::ensure_atomic_publish_dirs(&dest, &publish_dir)
                .with_context(|| format!("atomic publish setup for {}", self.name))?;
        }

        self.run_stage(1, &dest).await?;
        self.run_stage(2, &dest).await?;

        // Extract size from the shared log after successful run.
        // Stage2 overwrites stage1 in the same file (tee_to_log truncates),
        // so the log contains only stage2 output — which has the size stats.
        let log_file = self.log_path_shared.lock().unwrap().clone();
        if log_file.exists() {
            let content = tokio::fs::read_to_string(&log_file)
                .await
                .unwrap_or_default();
            let size = tunasync_common::util::extract_size_from_rsync_log(&content);
            if !size.is_empty() {
                *self.data_size.lock().unwrap() = size;
            }
        }

        // Atomic publish swap.
        if self.atomic_publish_enabled {
            super::rsync_provider::atomic_publish_swap(&dest, &publish_dir).with_context(|| {
                format!(
                    "atomic publish: swap {} ↔ {}",
                    dest.display(),
                    publish_dir.display()
                )
            })?;
            tracing::info!(mirror = %self.name, dest = %publish_dir.display(), "atomic publish complete");
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
            let handle = self.current_pid.lock().unwrap().clone();
            if let Some(handle) = handle {
                runner::terminate_process(handle).await?;
            }
        }
        Ok(())
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

    fn set_broker_config(&mut self, config: crate::runner::BrokerConfig) {
        self.broker_config = Some(config);
    }

    fn launch_plan_specs(&self) -> Vec<LaunchPlanSpec> {
        let dest = if self.atomic_publish_enabled {
            self.atomic_staging_path.clone()
        } else {
            self.working_dir.clone()
        };
        let mut specs = vec![
            LaunchPlanSpec {
                operation: "stage1".into(),
                argv: self.build_argv(&self.stage1_options, &dest),
                cwd: dest.clone(),
                env: self.rsync_env.clone(),
            },
            LaunchPlanSpec {
                operation: "stage2".into(),
                argv: self.build_argv(&self.stage2_options, &dest),
                cwd: dest,
                env: self.rsync_env.clone(),
            },
        ];
        if self.check_upstream {
            for url in std::iter::once(&self.upstream).chain(&self.upstream_fallback) {
                specs.push(super::rsync_provider::probe_plan_spec(
                    url,
                    &self.working_dir,
                ));
            }
        }
        specs
    }

    #[cfg(target_os = "linux")]
    fn set_cgroup_hook(&mut self, hook: std::sync::Arc<crate::hooks::CgroupHook>) {
        self.cgroup_hook = Some(hook);
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

    /// Probe upstream reachability before syncing. See `RsyncProvider::probe_upstream`
    /// for the rationale (per-URL 15s timeout, concurrent probing).
    async fn probe_upstream(&self) -> Result<(), ProbeError> {
        if !self.check_upstream {
            return Ok(());
        }
        let mut urls: Vec<&str> = vec![self.upstream.as_str()];
        urls.extend(self.upstream_fallback.iter().map(String::as_str));

        use futures::stream::{FuturesUnordered, StreamExt};
        let mut futures: FuturesUnordered<_> = urls
            .iter()
            .map(|&url| async move {
                let res = tokio::time::timeout(
                    std::time::Duration::from_secs(15),
                    super::rsync_provider::probe_configured_url(
                        url,
                        self.broker_config.as_ref(),
                        &self.working_dir,
                    ),
                )
                .await;
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
                            primary = %crate::redact_url_diagnostic(&self.upstream),
                            reachable = %crate::redact_url_diagnostic(url),
                            "primary upstream unreachable; fallback responded"
                        );
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    if e.downcast_ref::<runner::IsolationError>().is_some() {
                        return Err(ProbeError::Infrastructure(e.to_string()));
                    }
                    last_err = Some(format!("{}: {e}", crate::redact_url_diagnostic(url)));
                }
                Err(_) => {
                    last_err = Some(format!(
                        "{}: probe timed out after 15s",
                        crate::redact_url_diagnostic(url)
                    ))
                }
            }
        }
        Err(ProbeError::Unreachable(format!(
            "all {} upstream(s) unreachable; last: {}",
            urls.len(),
            last_err.as_deref().unwrap_or("no probes attempted")
        )))
    }
}

impl TwoStageRsyncProvider {
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    //! Unit tests for TwoStageRsyncProvider's new extension fields.

    use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
    use crate::provider::MirrorProvider;

    fn base_global() -> GlobalConfig {
        let mut g = GlobalConfig::default();
        g.log_dir = "/tmp/tunasync-2srsync-test-log".into();
        g.mirror_dir = "/tmp/tunasync-2srsync-test-mirror".into();
        g
    }

    fn base_mirror() -> MirrorConfig {
        let mut mc = MirrorConfig::default();
        mc.name = "2srsync-test".into();
        mc.provider = ProviderKind::TwoStageRsync;
        mc.upstream = "rsync://example.com/data/".into();
        mc.stage1_profile = "debian".into();
        mc
    }

    /// atomic_publish defaults to false; trait getter reflects it.
    #[test]
    fn atomic_publish_defaults_to_false() {
        let global = base_global();
        let mc = base_mirror();
        let p = super::TwoStageRsyncProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert!(!p.atomic_publish());
    }

    /// Setting atomic_publish = true propagates.
    #[test]
    fn atomic_publish_flows_through() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.atomic_publish = true;
        let p = super::TwoStageRsyncProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert!(p.atomic_publish());
    }

    /// check_upstream = false → probe_upstream is instant-Ok.
    #[tokio::test]
    async fn probe_upstream_noop_when_check_disabled() {
        let global = base_global();
        let mc = base_mirror();
        let p = super::TwoStageRsyncProvider::from_config(&mc, &global).expect("from_config");
        let start = std::time::Instant::now();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(200), p.probe_upstream()).await;
        let elapsed = start.elapsed();
        assert!(elapsed < std::time::Duration::from_millis(100));
        assert!(matches!(result, Ok(Ok(()))));
    }

    /// upstream_fallback flows through.
    #[test]
    fn upstream_fallback_flows_through() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.upstream_fallback = vec!["rsync://fallback.example.com/data/".into()];
        let p = super::TwoStageRsyncProvider::from_config(&mc, &global).expect("from_config");
        assert_eq!(p.upstream_fallback.len(), 1);
    }

    #[test]
    fn constructor_error_redacts_upstream_credentials() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.upstream = "rsync://user:top-secret@example.com/data".into();
        let error = super::TwoStageRsyncProvider::from_config(&mc, &global)
            .err()
            .expect("invalid upstream must fail")
            .to_string();
        assert!(!error.contains("user"));
        assert!(!error.contains("top-secret"));
        assert!(error.contains("rsync://example.com/data"));
    }

    /// disk_quota = "500M" parses correctly.
    #[test]
    fn disk_quota_parsed_into_bytes() {
        let global = base_global();
        let mut mc = base_mirror();
        mc.disk_quota = "500M".into();
        let p = super::TwoStageRsyncProvider::from_config(&mc, &global).expect("from_config");
        let p: &dyn MirrorProvider = &p;
        assert_eq!(p.disk_quota_bytes(), 500 * 1024 * 1024);
    }
}
