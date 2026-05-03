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

use crate::provider::MirrorProvider;
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
            "--exclude=Packages*".into(), "--exclude=Sources*".into(),
            "--exclude=Release*".into(), "--exclude=InRelease".into(),
            "--exclude=i18n/*".into(), "--exclude=ls-lR*".into(),
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
    pub log_file: PathBuf,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    stage1_options: Vec<String>,
    stage2_options: Vec<String>,
    rsync_cmd: String,
    rsync_env: HashMap<String, String>,
    pub success_exit_codes: Vec<i32>,
    current_pid: Arc<Mutex<Option<u32>>>,
    docker_container_name: Option<String>,
}

impl TwoStageRsyncProvider {
    pub fn from_config(
        mc: &crate::config::MirrorConfig,
        global: &crate::config::GlobalConfig,
    ) -> Result<Self> {
        if !mc.upstream.ends_with('/') {
            anyhow::bail!(
                "two-stage-rsync upstream URL must end with '/': {:?}",
                mc.upstream
            );
        }

        let working_dir = mc.effective_mirror_dir(global);
        let log_dir = if mc.log_dir.is_empty() {
            PathBuf::from(&global.log_dir)
        } else {
            PathBuf::from(&mc.log_dir)
        };
        let log_file = log_dir.join(format!("{}.log", mc.name));

        let base_opts_s1: Vec<String> = vec![
            "-aHvh".into(), "--no-o".into(), "--no-g".into(), "--stats".into(),
            "--filter".into(), "risk .~tmp~/".into(),
            "--exclude".into(), ".~tmp~/".into(),
            "--safe-links".into(),
        ];
        let base_opts_s2: Vec<String> = vec![
            "-aHvh".into(), "--no-o".into(), "--no-g".into(), "--stats".into(),
            "--filter".into(), "risk .~tmp~/".into(),
            "--exclude".into(), ".~tmp~/".into(),
            "--delete".into(), "--delete-after".into(), "--delay-updates".into(),
            "--safe-links".into(),
        ];

        // Build stage2 options: base + timeout + IPv4/6 + exclude-file + global/mirror rsync options.
        let append_common_for_stage2 = |mut opts: Vec<String>| -> Vec<String> {
            if !mc.rsync_no_timeout {
                let timeo = if mc.rsync_timeout > 0 { mc.rsync_timeout } else { 120 };
                opts.push(format!("--timeout={timeo}"));
            }
            if mc.use_ipv6 { opts.push("-6".into()); }
            else if mc.use_ipv4 { opts.push("-4".into()); }
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
            let timeo = if mc.rsync_timeout > 0 { mc.rsync_timeout } else { 120 };
            stage1_options.push(format!("--timeout={timeo}"));
        }
        if mc.use_ipv6 { stage1_options.push("-6".into()); }
        else if mc.use_ipv4 { stage1_options.push("-4".into()); }
        if !mc.exclude_file.is_empty() {
            stage1_options.extend(["--exclude-from".into(), mc.exclude_file.clone()]);
        }
        stage1_options.extend(stage1_profile_options(&mc.stage1_profile)?);

        // Stage2: base options + timeout + IPv4/6 + exclude-file + global/mirror rsync options.
        let stage2_options = append_common_for_stage2(base_opts_s2);

        let mut rsync_env = HashMap::new();
        if !mc.username.is_empty() { rsync_env.insert("USER".into(), mc.username.clone()); }
        if !mc.password.is_empty() { rsync_env.insert("RSYNC_PASSWORD".into(), mc.password.clone()); }

        let mut success_exit_codes = mc.success_exit_codes.clone();
        success_exit_codes.extend(global.dangerous_global_success_exit_codes.iter());
        success_exit_codes.extend(global.dangerous_global_rsync_success_exit_codes.iter());
        success_exit_codes.extend(mc.rsync_success_exit_codes.iter());

        Ok(Self {
            name: mc.name.clone(),
            upstream: mc.upstream.clone(),
            working_dir,
            log_dir,
            log_file,
            interval: mc.effective_interval(global),
            retry: mc.effective_retry(global),
            timeout: mc.effective_timeout(global).unwrap_or(Duration::ZERO),
            is_master: mc.is_master(),
            stage1_options,
            stage2_options,
            rsync_cmd: "rsync".into(),
            rsync_env,
            success_exit_codes,
            current_pid: Arc::new(Mutex::new(None)),
            docker_container_name: None,
        })
    }

    fn build_argv(&self, opts: &[String]) -> Vec<String> {
        let mut argv = vec![self.rsync_cmd.clone()];
        argv.extend(opts.iter().cloned());
        argv.push(self.upstream.clone());
        argv.push(self.working_dir.to_string_lossy().into());
        argv
    }

    async fn run_stage(&self, stage: u8) -> Result<()> {
        let opts = if stage == 1 { &self.stage1_options } else { &self.stage2_options };
        let argv = self.build_argv(opts);
        let log_suffix = if stage == 1 { "stage1.log" } else { "stage2.log" };
        let log_path = self.log_dir.join(format!("{}.{log_suffix}", self.name));

        let lp = if log_path.to_string_lossy() == "/dev/null" { None } else { Some(log_path.as_path()) };
        let proc = runner::spawn(&argv, &self.working_dir, &self.rsync_env, lp)
            .await
            .with_context(|| format!("spawn rsync stage {stage} for {}", self.name))?;

        if let Some(pid) = proc.pid() {
            *self.current_pid.lock().unwrap() = Some(pid);
        }
        let result = proc.wait(&self.success_exit_codes)
            .await
            .with_context(|| format!("rsync stage {stage} for {} failed", self.name));
        *self.current_pid.lock().unwrap() = None;
        result
    }
}

#[async_trait]
impl MirrorProvider for TwoStageRsyncProvider {
    fn name(&self) -> &str { &self.name }
    fn upstream(&self) -> &str { &self.upstream }
    fn is_master(&self) -> bool { self.is_master }
    fn interval(&self) -> Duration { self.interval }
    fn retry(&self) -> u32 { self.retry }
    fn timeout(&self) -> Duration { self.timeout }

    async fn run(&self) -> Result<()> {
        self.run_stage(1).await?;
        self.run_stage(2).await
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
            if let Some(pid) = *self.current_pid.lock().unwrap() {
                use nix::sys::signal::{Signal, kill};
                use nix::unistd::Pid;
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
            }
        }
        Ok(())
    }
}

impl TwoStageRsyncProvider {
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}
