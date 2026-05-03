//! cgroup hook — Linux cgroup v1/v2 resource isolation for sync jobs.
//!
//! Mirrors Go's `cgroupHook` / `cgroup.go`.
//!
//! Lifecycle:
//!   `PreExec`  → detect v1/v2, create per-job sub-cgroup, apply memory limit
//!   `PostExec` → SIGKILL all pids in the cgroup, delete the cgroup dir
//!
//! # PID placement
//!
//! After spawning the child process, call `CgroupHook::add_pid(pid)` to register
//! the process. This is done by the provider's `run()` between `spawn()` and
//! `wait()`. See `runner.rs` for the `RunningProcess::pid()` method.

#![cfg(target_os = "linux")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::hooks::{HookPhase, JobHook};

// ---------------------------------------------------------------------------
// cgroup version detection
// ---------------------------------------------------------------------------

fn is_cgroup_v2() -> bool {
    Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
}

// ---------------------------------------------------------------------------
// Inner mutable state — extracted so we can move it into closures
// ---------------------------------------------------------------------------

struct CgroupInner {
    job_path: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// CgroupHook
// ---------------------------------------------------------------------------

pub struct CgroupHook {
    mirror_name: String,
    parent_path: PathBuf,
    memory_limit_bytes: i64,
    is_v2: bool,
    inner: Mutex<CgroupInner>,
}

impl CgroupHook {
    /// Create a new cgroup hook.
    ///
    /// `base_path`: cgroup mount root, e.g. `/sys/fs/cgroup` (empty → default).
    /// `group`: sub-group to use as the parent, e.g. `tunasync`.
    /// `memory_limit_bytes`: 0 = no limit.
    pub fn new(
        mirror_name: String,
        base_path: &str,
        group: &str,
        memory_limit_bytes: i64,
    ) -> Self {
        let is_v2 = is_cgroup_v2();
        let base = if base_path.is_empty() {
            "/sys/fs/cgroup"
        } else {
            base_path
        };
        let parent_path = if is_v2 {
            PathBuf::from(base).join(group)
        } else {
            // Default to the `memory` subsystem for v1.
            PathBuf::from(base).join("memory").join(group)
        };
        Self {
            mirror_name,
            parent_path,
            memory_limit_bytes,
            is_v2,
            inner: Mutex::new(CgroupInner { job_path: None }),
        }
    }

    /// Path to the procs file for this job's cgroup.
    /// Returns `None` until `preExec` has run.
    pub fn procs_file(&self) -> Option<PathBuf> {
        let guard = self.inner.lock().unwrap();
        guard.job_path.as_ref().map(|p| {
            if self.is_v2 { p.join("cgroup.procs") } else { p.join("tasks") }
        })
    }

    /// Write `pid` directly to the cgroup procs file.
    pub fn add_pid(&self, pid: u32) -> Result<()> {
        if let Some(procs) = self.procs_file() {
            fs::write(&procs, pid.to_string())
                .with_context(|| format!("add pid {pid} to {}", procs.display()))?;
        }
        Ok(())
    }

    /// Register a child PID into the job cgroup using SIGSTOP → write → SIGCONT.
    /// Called by providers after `runner::spawn` and before `wait()`.
    pub fn add_pid_stopped(&self, proc: &crate::runner::RunningProcess) -> Result<()> {
        if let Some(pid) = proc.pid() {
            proc.stop_for_cgroup();
            let result = self.add_pid(pid);
            proc.cont_after_cgroup();
            result
        } else {
            Ok(())
        }
    }

    // ------------------------------------------------------------------
    // Blocking helpers (called via block_in_place)
    // ------------------------------------------------------------------

    fn create_cgroup(&self) -> Result<()> {
        let job_path = self.parent_path.join(&self.mirror_name);
        tracing::debug!(path = %job_path.display(), v2 = self.is_v2, "creating per-job cgroup");
        fs::create_dir_all(&job_path)
            .with_context(|| format!("mkdir cgroup {}", job_path.display()))?;

        if self.memory_limit_bytes != 0 {
            let limit = self.memory_limit_bytes.to_string();
            let limit_file = if self.is_v2 {
                job_path.join("memory.max")
            } else {
                job_path.join("memory.limit_in_bytes")
            };
            fs::write(&limit_file, &limit)
                .with_context(|| format!("set memory limit in {}", limit_file.display()))?;
        }

        self.inner.lock().unwrap().job_path = Some(job_path);
        Ok(())
    }

    fn kill_and_delete_cgroup(&self) -> Result<()> {
        let job_path = {
            let guard = self.inner.lock().unwrap();
            match guard.job_path.clone() {
                Some(p) => p,
                None => return Ok(()),
            }
        };
        self.kill_all(&job_path)?;
        if let Err(e) = fs::remove_dir(&job_path) {
            tracing::warn!(path = %job_path.display(), error = %e, "failed to remove cgroup dir");
        }
        self.inner.lock().unwrap().job_path = None;
        Ok(())
    }

    fn kill_all(&self, job_path: &Path) -> Result<()> {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let procs_file = if self.is_v2 {
            job_path.join("cgroup.procs")
        } else {
            job_path.join("tasks")
        };

        for attempt in 0..4u32 {
            if attempt == 3 {
                anyhow::bail!("failed to empty cgroup after 3 kill rounds");
            }
            let content = match fs::read_to_string(&procs_file) {
                Ok(c) => c,
                Err(_) => return Ok(()),
            };
            let pids: Vec<i32> = content.lines()
                .filter_map(|l| l.trim().parse().ok())
                .collect();
            if pids.is_empty() { return Ok(()); }
            for pid in &pids {
                tracing::debug!(pid, "SIGKILL cgroup process");
                let _ = kill(Pid::from_raw(*pid), Signal::SIGKILL);
            }
            let sleep_ms = if attempt == 0 { 10 } else { attempt as u64 * 1000 };
            std::thread::sleep(Duration::from_millis(sleep_ms));
        }
        Ok(())
    }
}

#[async_trait]
impl JobHook for CgroupHook {
    fn name(&self) -> &str { "cgroup" }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match phase {
            HookPhase::PreExec  => tokio::task::block_in_place(|| self.create_cgroup()),
            HookPhase::PostExec => tokio::task::block_in_place(|| self.kill_and_delete_cgroup()),
            _ => Ok(()),
        }
    }
}
