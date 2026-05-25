//! exec_post hook — runs a shell command after a sync completes.
//!
//! Mirrors Go's `execPostHook` / `exec_post_hook.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::hooks::{HookPhase, JobHook};
use crate::runner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecOn {
    Success,
    Failure,
}

/// Hook that executes a shell command on success or failure.
pub struct ExecPostHook {
    /// Command + args (already shell-split).
    command: Vec<String>,
    exec_on: ExecOn,
    /// Provider context for TUNASYNC_* env vars.
    mirror_name: String,
    working_dir: PathBuf,
    upstream: String,
    log_dir: PathBuf,
    /// Shared log path — read dynamically so TUNASYNC_LOG_FILE reflects
    /// the rotated timestamped file set by LogLimitHook::preExec.
    log_file: Arc<Mutex<PathBuf>>,
}

impl ExecPostHook {
    pub fn new(
        command_str: &str,
        exec_on: ExecOn,
        mirror_name: String,
        working_dir: PathBuf,
        upstream: String,
        log_dir: PathBuf,
        log_file: Arc<Mutex<PathBuf>>,
    ) -> Result<Self> {
        let command = shell_words::split(command_str)
            .with_context(|| format!("parse exec_post command for mirror {mirror_name:?}"))?;
        if command.is_empty() {
            anyhow::bail!("exec_post command for {mirror_name:?} is empty");
        }
        Ok(Self {
            command,
            exec_on,
            mirror_name,
            working_dir,
            upstream,
            log_dir,
            log_file,
        })
    }

    async fn do_exec(&self) -> Result<()> {
        let exit_status = match self.exec_on {
            ExecOn::Success => "success",
            ExecOn::Failure => "failure",
        };
        // Read log path dynamically — LogLimitHook::preExec sets the
        // timestamped path before this hook runs.
        let log_file = self.log_file.lock().unwrap().clone();
        let mut env = HashMap::new();
        env.insert("TUNASYNC_MIRROR_NAME".into(), self.mirror_name.clone());
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
            log_file.to_string_lossy().into(),
        );
        env.insert("TUNASYNC_JOB_EXIT_STATUS".into(), exit_status.into());

        let proc = runner::spawn(&self.command, &self.working_dir, &env, None, None).await?;
        proc.wait(&[]).await
    }
}

#[async_trait]
impl JobHook for ExecPostHook {
    fn name(&self) -> &str {
        "exec_post"
    }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match (phase, self.exec_on) {
            (HookPhase::PostSuccess, ExecOn::Success) => self.do_exec().await,
            (HookPhase::PostFail, ExecOn::Failure) => self.do_exec().await,
            _ => Ok(()),
        }
    }
}
