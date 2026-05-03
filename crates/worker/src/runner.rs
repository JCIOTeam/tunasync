//! Async process runner — mirrors Go's `worker/runner.go`.
//!
//! # cgroup PID race mitigation (Linux)
//!
//! Go uses a reexec + fd3-pipe handshake. In Rust/tokio we use a simpler
//! approach: `SIGSTOP` the child immediately after spawn, write the PID to the
//! cgroup, then `SIGCONT`. The window where the child can exec before the
//! cgroup write is eliminated because the child is stopped.
//! This requires Linux ≥ 3.5 (all supported distros).

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

// ---------------------------------------------------------------------------
// RunningProcess
// ---------------------------------------------------------------------------

pub struct RunningProcess {
    pub(crate) child: Child,
}

impl RunningProcess {
    /// PID of the child process (None after the process has been reaped).
    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    /// Wait for the process to exit, treating `allowed_codes` as success.
    pub async fn wait(mut self, allowed_codes: &[i32]) -> Result<()> {
        let status = self.child.wait().await.context("wait() on child")?;
        if status.success() {
            return Ok(());
        }
        let code = status.code().unwrap_or(-1);
        if allowed_codes.contains(&code) {
            tracing::debug!(code, "non-zero exit treated as success");
            return Ok(());
        }
        let (_, msg) = tunasync_common::util::translate_rsync_error_code(code);
        if msg.is_empty() {
            Err(anyhow::anyhow!("process exited with code {code}"))
        } else {
            Err(anyhow::anyhow!("process exited with code {code}: {msg}"))
        }
    }

    /// SIGTERM → 2 s → SIGKILL. Mirrors Go's `cmdJob.Terminate`.
    pub async fn terminate(mut self) {
        #[cfg(unix)]
        {
            use nix::sys::signal::{kill, Signal};
            use nix::unistd::Pid;
            if let Some(pid) = self.child.id() {
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {
                        tracing::warn!(pid, "SIGTERM timed out — sending SIGKILL");
                        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
                    }
                    _ = self.child.wait() => {}
                }
                return;
            }
        }
        let _ = self.child.kill().await;
    }

    /// SIGSTOP the child so the caller can safely write its PID to a cgroup.
    /// Call [`RunningProcess::cont`] afterwards to resume execution.
    ///
    /// No-op on non-Linux platforms.
    pub fn stop_for_cgroup(&self) {
        #[cfg(target_os = "linux")]
        if let Some(pid) = self.child.id() {
            use nix::sys::signal::{kill, Signal};
            use nix::unistd::Pid;
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGSTOP);
        }
    }

    /// SIGCONT the child after cgroup placement.
    pub fn cont_after_cgroup(&self) {
        #[cfg(target_os = "linux")]
        if let Some(pid) = self.child.id() {
            use nix::sys::signal::{kill, Signal};
            use nix::unistd::Pid;
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGCONT);
        }
    }
}

// ---------------------------------------------------------------------------
// spawn()
// ---------------------------------------------------------------------------

/// Spawn a child process.
pub async fn spawn(
    argv: &[String],
    working_dir: &Path,
    env_overrides: &HashMap<String, String>,
    log_path: Option<&Path>,
) -> Result<RunningProcess> {
    assert!(!argv.is_empty(), "argv must be non-empty");

    if !working_dir.exists() {
        std::fs::create_dir_all(working_dir)
            .with_context(|| format!("create working dir {}", working_dir.display()))?;
    }

    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(env_overrides.iter().map(|(k, v)| (k.clone(), v.clone())));

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .current_dir(working_dir)
        .envs(&env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {:?}", argv[0]))?;

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    if let Some(log_path) = log_path {
        let log_path = log_path.to_owned();
        tokio::spawn(tee_to_log(stdout, stderr, log_path));
    } else {
        tokio::spawn(drain(stdout));
        tokio::spawn(drain(stderr));
    }

    Ok(RunningProcess { child })
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

async fn tee_to_log<Ro, Re>(stdout: Ro, stderr: Re, log_path: std::path::PathBuf)
where
    Ro: tokio::io::AsyncRead + Unpin + Send + 'static,
    Re: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;

    if let Some(dir) = log_path.parent() {
        let _ = tokio::fs::create_dir_all(dir).await;
    }
    let mut file = match OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&log_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(path = %log_path.display(), error = %e, "cannot open log file");
            return;
        }
    };

    let mut out = BufReader::new(stdout).lines();
    let mut err = BufReader::new(stderr).lines();

    // Drain both streams fully — don't stop when one hits EOF, the other
    // may still have buffered data. Loop until both are fully exhausted.
    loop {
        let out_done = loop {
            match out.next_line().await {
                Ok(Some(l)) => {
                    let _ = file.write_all(l.as_bytes()).await;
                    let _ = file.write_all(b"\n").await;
                }
                Ok(None) | Err(_) => break true,
            }
        };
        let err_done = loop {
            match err.next_line().await {
                Ok(Some(l)) => {
                    let _ = file.write_all(l.as_bytes()).await;
                    let _ = file.write_all(b"\n").await;
                }
                Ok(None) | Err(_) => break true,
            }
        };
        if out_done && err_done {
            break;
        }
    }
    let _ = file.flush().await;
}

async fn drain<R: tokio::io::AsyncRead + Unpin>(reader: R) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(_)) = lines.next_line().await {}
}
