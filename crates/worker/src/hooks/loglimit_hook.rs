//! Log-limit hook — rotates per-mirror log files, keeping at most 9.
//!
//! Mirrors Go's `logLimiter` / `loglimit_hook.go`.
//!
//! On `PreExec`:
//!   1. Read log directory, delete files > 9 (oldest first).
//!   2. Compute new log file name with timestamp.
//!   3. Update `latest` symlink.
//!
//! On `PostSuccess` / `PostFail`:
//!   - Rename log file to add `.fail` suffix on failure.
//!   - Update `latest` symlink.

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Local;

use crate::hooks::{HookPhase, JobHook};

pub struct LogLimitHook {
    mirror_name: String,
    log_dir: PathBuf,
    /// The log file path set during `preExec`, used in post phases.
    current_log: Mutex<PathBuf>,
}

impl LogLimitHook {
    pub fn new(mirror_name: String, log_dir: PathBuf) -> Self {
        Self {
            current_log: Mutex::new(log_dir.join(format!("{mirror_name}.log"))),
            mirror_name,
            log_dir,
        }
    }

    async fn pre_exec(&self) -> Result<()> {
        let log_dir = &self.log_dir;
        let name = &self.mirror_name;

        // Ensure log dir exists.
        tokio::fs::create_dir_all(log_dir).await.ok();

        // List and sort files by modification time (oldest first for pruning).
        let mut matched: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
        let mut entries = tokio::fs::read_dir(log_dir).await?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let fname = entry.file_name().to_string_lossy().into_owned();
            if fname.starts_with(name.as_str()) {
                if let Ok(meta) = entry.metadata().await {
                    if let Ok(mtime) = meta.modified() {
                        matched.push((mtime, entry.path()));
                    }
                }
            }
        }

        // Sort oldest first.
        matched.sort_by_key(|(t, _)| *t);
        // Keep at most 9 (delete the oldest ones beyond that).
        let total = matched.len();
        if total > 9 {
            for (_, path) in &matched[..total - 9] {
                let _ = tokio::fs::remove_file(path).await;
            }
        }

        // New log file name with timestamp.
        let ts = Local::now().format("%Y-%m-%d_%H_%M").to_string();
        let log_filename = format!("{name}_{ts}.log");
        let log_path = log_dir.join(&log_filename);

        // Update `latest` symlink.
        let link = log_dir.join("latest");
        let _ = tokio::fs::remove_file(&link).await;
        #[cfg(unix)]
        tokio::fs::symlink(&log_filename, &link).await.ok();

        *self.current_log.lock().unwrap() = log_path;
        Ok(())
    }

    async fn post_success(&self) -> Result<()> {
        Ok(())
    }

    async fn post_fail(&self) -> Result<()> {
        let log_file = self.current_log.lock().unwrap().clone();
        let fail_file = {
            let mut p = log_file.clone();
            let name = format!(
                "{}.fail",
                p.file_name().unwrap_or_default().to_string_lossy()
            );
            p.set_file_name(name);
            p
        };

        if log_file.exists() {
            tokio::fs::rename(&log_file, &fail_file).await.ok();
        }

        // Update `latest` to the .fail file.
        let link = self.log_dir.join("latest");
        let _ = tokio::fs::remove_file(&link).await;
        #[cfg(unix)]
        if let Some(fname) = fail_file.file_name() {
            tokio::fs::symlink(fname, &link).await.ok();
        }

        Ok(())
    }

    /// The log file path set by the most recent `preExec` call.
    pub fn current_log_file(&self) -> PathBuf {
        self.current_log.lock().unwrap().clone()
    }
}

#[async_trait]
impl JobHook for LogLimitHook {
    fn name(&self) -> &str {
        "loglimit"
    }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match phase {
            HookPhase::PreExec => self.pre_exec().await,
            HookPhase::PostSuccess => self.post_success().await,
            HookPhase::PostFail => self.post_fail().await,
            _ => Ok(()),
        }
    }
}
