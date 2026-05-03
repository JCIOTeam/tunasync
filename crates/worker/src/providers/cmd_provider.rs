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

use crate::provider::MirrorProvider;
use crate::runner;

/// Configuration for a command provider instance.
pub struct CmdProvider {
    pub name: String,
    pub upstream: String,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    pub log_file: PathBuf,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    pub env: HashMap<String, String>,
    pub command: Vec<String>,
    pub fail_on_match: Option<Regex>,
    pub size_pattern: Option<Regex>,
    pub success_exit_codes: Vec<i32>,
    data_size: Mutex<String>,
    /// PID of the currently running child process (set before wait, cleared after).
    current_pid: Arc<Mutex<Option<u32>>>,
    /// Docker container name, set when DockerHook wraps the command.
    docker_container_name: Option<String>,
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
        let log_file = log_dir.join("latest.log");

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
            env: mc.env.clone(),
            command,
            fail_on_match,
            size_pattern,
            success_exit_codes: mc.success_exit_codes.clone(),
            data_size: Mutex::new(String::new()),
            current_pid: Arc::new(Mutex::new(None)),
            docker_container_name: None,
        })
    }

    fn tunasync_env(&self) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("TUNASYNC_MIRROR_NAME".into(), self.name.clone());
        env.insert(
            "TUNASYNC_WORKING_DIR".into(),
            self.working_dir.to_string_lossy().into(),
        );
        env.insert("TUNASYNC_UPSTREAM_URL".into(), self.upstream.clone());
        env.insert(
            "TUNASYNC_LOG_DIR".into(),
            self.log_dir.to_string_lossy().into(),
        );
        env.insert(
            "TUNASYNC_LOG_FILE".into(),
            self.log_file.to_string_lossy().into(),
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

        let env = self.tunasync_env();
        let log_path = if self.log_file.to_string_lossy() == "/dev/null" {
            None
        } else {
            Some(self.log_file.as_path())
        };

        let proc = runner::spawn(&self.command, &self.working_dir, &env, log_path).await?;

        // Store PID so terminate() can send SIGTERM.
        if let Some(pid) = proc.pid() {
            *self.current_pid.lock().unwrap() = Some(pid);
        }
        let wait_result = proc.wait(&self.success_exit_codes).await;
        *self.current_pid.lock().unwrap() = None;
        wait_result?;

        // Check fail_on_match regex in the log file.
        if let Some(re) = &self.fail_on_match {
            if self.log_file.exists() {
                let content = tokio::fs::read_to_string(&self.log_file)
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
            if self.log_file.exists() {
                let content = tokio::fs::read_to_string(&self.log_file)
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
            if let Some(pid) = *self.current_pid.lock().unwrap() {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
            }
        }
        Ok(())
    }

    fn data_size(&self) -> String {
        self.data_size.lock().unwrap().clone()
    }
}

impl CmdProvider {
    /// Set the docker container name when DockerHook wraps the command.
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}
