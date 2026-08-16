//! Async process runner — mirrors Go's `worker/runner.go`.
//!
//! # cgroup PID race mitigation (Linux)
//!
//! Go uses a re-exec + fd3-pipe handshake so the child is stopped at its
//! very first instruction, before any user code runs. We don't replicate
//! that. Instead the provider's `run()`:
//!
//! 1. `runner::spawn()` — child starts executing immediately
//! 2. `cgroup_hook.add_pid_stopped(&proc)` — sends SIGSTOP to the child,
//!    writes the PID into `cgroup.procs`, then SIGCONT
//!
//! This leaves a small window between (1) and (2) during which the child
//! is already running outside the cgroup, so memory/cpu accounting can be
//! slightly under-reported for the first millisecond or two and a child
//! that exec's something else extremely fast in step (1) could even
//! escape the cgroup entirely. For tunasync's actual workloads (rsync,
//! external shell scripts that take seconds to minutes) the window is
//! invisible in practice; if a stricter guarantee is ever needed, the
//! fix is to spawn the child stopped (e.g. via posix_spawn with the
//! POSIX_SPAWN_SETSIGMASK + a self-pipe pre-exec hook, or via clone3
//! with CLONE_STOPPED on Linux ≥ 5.7), not to issue SIGSTOP after the
//! fact.
//!
//! # Process groups
//!
//! Every spawned child gets its own process group (PGID = child PID),
//! matching Go's `SysProcAttr{Setpgid: true}`. This prevents the child
//! from receiving signals meant for the parent, and allows clean
//! termination of the entire child tree via `kill(-pid, signal)`.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

const BROKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BROKER_IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct BrokerPlacement {
    pub socket: PathBuf,
    pub generation: String,
    pub mirror: String,
    pub operation: String,
    pub namespace: String,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub socket: PathBuf,
    pub generation: String,
    pub mirror: String,
    pub namespace: String,
}

impl BrokerConfig {
    pub fn placement(&self, operation: impl Into<String>) -> ExecutionPlacement {
        ExecutionPlacement::Broker(BrokerPlacement {
            socket: self.socket.clone(),
            generation: self.generation.clone(),
            mirror: self.mirror.clone(),
            operation: operation.into(),
            namespace: self.namespace.clone(),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub enum ExecutionPlacement {
    #[default]
    Host,
    Broker(BrokerPlacement),
}

#[derive(Debug, Clone)]
pub enum ProcessHandle {
    Host(u32),
    Broker { socket: PathBuf, job_id: String },
}

#[derive(Debug, thiserror::Error)]
#[error("network namespace isolation infrastructure failure: {0}")]
pub struct IsolationError(pub String);

// RunningProcess

pub struct RunningProcess {
    inner: RunningProcessInner,
    /// Handles for the spawned stdout/stderr drain tasks (tee-to-log or
    /// publish). `wait()` awaits these AFTER the child exits so the log file
    /// is fully written and flushed before callers read it for fail_on_match
    /// / size extraction. Without this, those readers race the drain task and
    /// can observe a truncated or empty log. Empty when there were no
    /// readers to track.
    io_tasks: Vec<tokio::task::JoinHandle<()>>,
}

enum RunningProcessInner {
    Host(Child),
    Broker {
        pid: u32,
        handle: ProcessHandle,
        exit_rx: oneshot::Receiver<Result<BrokerExit>>,
    },
}

#[derive(Debug)]
struct BrokerExit {
    code: Option<i32>,
    signal: Option<i32>,
}

impl RunningProcess {
    /// PID of the child process (None after the process has been reaped).
    pub fn pid(&self) -> Option<u32> {
        match &self.inner {
            RunningProcessInner::Host(child) => child.id(),
            RunningProcessInner::Broker { pid, .. } => Some(*pid),
        }
    }

    pub fn handle(&self) -> Option<ProcessHandle> {
        match &self.inner {
            RunningProcessInner::Host(child) => child.id().map(ProcessHandle::Host),
            RunningProcessInner::Broker { handle, .. } => Some(handle.clone()),
        }
    }

    /// Wait for the process to exit, treating `allowed_codes` as success.
    ///
    /// After the child exits, this also waits for the stdout/stderr drain
    /// tasks to finish so the log file is completely flushed — callers that
    /// read the log immediately afterwards (fail_on_match, size extraction)
    /// are guaranteed to see the full output.
    pub async fn wait(mut self, allowed_codes: &[i32]) -> Result<()> {
        let outcome = match &mut self.inner {
            RunningProcessInner::Host(child) => {
                let status = child.wait().await.context("wait() on child")?;
                (status.code(), None)
            }
            RunningProcessInner::Broker { exit_rx, .. } => {
                let exit = exit_rx.await.map_err(|_| {
                    IsolationError("broker connection ended without exit status".into())
                })??;
                (exit.code, exit.signal)
            }
        };
        // Drain tasks normally end on their own once the child's pipes close
        // (which happens at exit). Await them so the log is fully
        // written/flushed before we return and the caller inspects it.
        //
        // The wait is BOUNDED: if the child forked a grandchild that inherits
        // the stdout/stderr pipe and stays alive (e.g. a mirror script that
        // daemonizes a helper), the pipes never reach EOF and an unbounded
        // await would hang this sync forever — even with timeout=0 configured.
        // Flushing the buffered pipe contents (≤64 KiB per pipe + channel)
        // takes well under a second, so 10 s is a generous ceiling; past it we
        // abort the drains and warn that the tail of the log may be missing.
        let io_tasks = std::mem::take(&mut self.io_tasks);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for mut handle in io_tasks {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                tracing::warn!(
                    "log drain did not finish within 10s after child exit — a \
                     grandchild process likely inherited the stdout/stderr pipe \
                     and is still running; aborting drain (log tail may be \
                     incomplete)"
                );
            }
        }
        if outcome.0 == Some(0) && outcome.1.is_none() {
            return Ok(());
        }
        let code = outcome
            .0
            .unwrap_or_else(|| outcome.1.map(|sig| 128 + sig).unwrap_or(-1));
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

    /// SIGTERM the process group → 2 s → SIGKILL the process group.
    /// SIGTERM → 2 s grace period → SIGKILL the process group.
    pub async fn terminate(mut self) {
        // The child is being force-killed; we don't need its remaining output
        // flushed, so abort the drain tasks rather than awaiting them.
        for handle in std::mem::take(&mut self.io_tasks) {
            handle.abort();
        }
        match &mut self.inner {
            RunningProcessInner::Host(child) => {
                #[cfg(unix)]
                if let Some(pid) = child.id() {
                    use nix::sys::signal::{kill, Signal};
                    use nix::unistd::Pid;
                    // Kill the entire process group (negative PID).
                    let _ = kill(Pid::from_raw(-(pid as i32)), Signal::SIGTERM);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {
                            tracing::warn!(pid, "SIGTERM timed out — sending SIGKILL to process group");
                            let _ = kill(Pid::from_raw(-(pid as i32)), Signal::SIGKILL);
                        }
                        _ = child.wait() => {}
                    }
                    return;
                }
                let _ = child.kill().await;
            }
            RunningProcessInner::Broker { handle, .. } => {
                let _ = terminate_process(handle.clone()).await;
            }
        }
    }

    /// SIGSTOP the child so the caller can safely write its PID to a cgroup.
    /// Call [`RunningProcess::cont`] afterwards to resume execution.
    ///
    /// No-op on non-Linux platforms.
    pub fn stop_for_cgroup(&self) {
        #[cfg(target_os = "linux")]
        if let RunningProcessInner::Host(child) = &self.inner {
            if let Some(pid) = child.id() {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGSTOP);
            }
        }
    }

    /// SIGCONT the child after cgroup placement.
    pub fn cont_after_cgroup(&self) {
        #[cfg(target_os = "linux")]
        if let RunningProcessInner::Host(child) = &self.inner {
            if let Some(pid) = child.id() {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGCONT);
            }
        }
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        for handle in &self.io_tasks {
            handle.abort();
        }
        #[cfg(unix)]
        match &mut self.inner {
            RunningProcessInner::Host(child) => {
                if let Some(pid) = child.id() {
                    use nix::sys::signal::{kill, Signal};
                    use nix::unistd::Pid;
                    let _ = kill(Pid::from_raw(-(pid as i32)), Signal::SIGKILL);
                }
            }
            RunningProcessInner::Broker { .. } => {
                // Dropping the broker reader closes the launch connection. The
                // broker treats disconnect as an instruction to kill/reap the job.
            }
        }
        #[cfg(not(unix))]
        if let RunningProcessInner::Host(child) = &mut self.inner {
            let _ = child.start_kill();
        }
    }
}

/// SIGTERM the process group → 2 s grace period → SIGKILL the process group.
/// Used by providers' `terminate()` to ensure the child tree is fully killed.
///
/// Unlike `RunningProcess::terminate()` which owns the `Child`, this operates
/// on a raw PID — the `RunningProcess` is consumed by `wait()` inside
/// `provider.run()`, so providers only have the PID to work with.
#[cfg(unix)]
pub async fn terminate_process_group(pid: u32) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;

    let pgid = Pid::from_raw(-(pid as i32));
    if kill(pgid, Signal::SIGTERM).is_err() {
        tracing::debug!(pid, "SIGTERM to process group failed (already dead?)");
        return;
    }
    // Give the process a grace period to shut down cleanly. Poll up to 2s
    // in 50ms increments so a fast-exiting process doesn't pay the full
    // 2-second penalty (which was unconditional before this fix).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Check if the process group is still alive by sending signal 0.
        if kill(pgid, None).is_err() {
            tracing::debug!(pid, "process group exited cleanly after SIGTERM");
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
    }
    // If still alive after 2s, escalate to SIGKILL.
    if kill(pgid, Signal::SIGKILL).is_err() {
        tracing::debug!(pid, "SIGKILL to process group failed (already dead)");
    }
}

pub async fn terminate_process(handle: ProcessHandle) -> Result<()> {
    match handle {
        ProcessHandle::Host(pid) => {
            #[cfg(unix)]
            terminate_process_group(pid).await;
            Ok(())
        }
        ProcessHandle::Broker { socket, job_id } => {
            #[cfg(target_os = "linux")]
            {
                let mut stream = tokio::time::timeout(
                    BROKER_CONNECT_TIMEOUT,
                    tokio::net::UnixStream::connect(&socket),
                )
                .await
                .map_err(|_| IsolationError(format!("connect {} timed out", socket.display())))?
                .map_err(|e| IsolationError(format!("connect {}: {e}", socket.display())))?;
                let request = tunasync_netns::ClientRequest::Terminate { job_id };
                tokio::time::timeout(
                    BROKER_IO_TIMEOUT,
                    tunasync_netns::write_frame_async(&mut stream, &request),
                )
                .await
                .map_err(|_| IsolationError("broker terminate write timed out".into()))?
                .map_err(|e| IsolationError(e.to_string()))?;
                let frame: tunasync_netns::ServerFrame = tokio::time::timeout(
                    BROKER_IO_TIMEOUT,
                    tunasync_netns::read_frame_async(&mut stream),
                )
                .await
                .map_err(|_| IsolationError("broker terminate response timed out".into()))?
                .map_err(|e| IsolationError(e.to_string()))?;
                match frame {
                    tunasync_netns::ServerFrame::Terminated => Ok(()),
                    tunasync_netns::ServerFrame::Error { kind, message } => {
                        Err(IsolationError(format!("broker {kind}: {message}")).into())
                    }
                    _ => Err(IsolationError("unexpected terminate response".into()).into()),
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = (socket, job_id);
                Err(IsolationError("network namespaces require Linux".into()).into())
            }
        }
    }
}

// spawn()

/// Spawn a child process in its own process group (matches Go's Setpgid).
///
/// `publisher`, when present, receives every stdout/stderr line in real
/// time and accumulates a bounded-size replay buffer for clients that
/// connect mid-sync via `GET /jobs/<mirror>/log/stream`. The buffer is
/// **cleared at spawn entry** so each `spawn` invocation gives subscribers
/// a fresh "this run only" view — matching the file-side semantics where
/// `tee_to_log` truncates the rotated log file on open.
pub async fn spawn(
    argv: &[String],
    working_dir: &Path,
    env_overrides: &HashMap<String, String>,
    log_path: Option<&Path>,
    publisher: Option<crate::log_stream::LogPublisher>,
) -> Result<RunningProcess> {
    spawn_placed(
        argv,
        working_dir,
        env_overrides,
        log_path,
        publisher,
        &ExecutionPlacement::Host,
    )
    .await
}

pub async fn spawn_placed(
    argv: &[String],
    working_dir: &Path,
    env_overrides: &HashMap<String, String>,
    log_path: Option<&Path>,
    publisher: Option<crate::log_stream::LogPublisher>,
    placement: &ExecutionPlacement,
) -> Result<RunningProcess> {
    assert!(!argv.is_empty(), "argv must be non-empty");

    // Drop any lines accumulated by a previous spawn — clients now see
    // only this run's output.
    if let Some(ref p) = publisher {
        p.reset();
    }

    if !working_dir.exists() {
        std::fs::create_dir_all(working_dir)
            .with_context(|| format!("create working dir {}", working_dir.display()))?;
    }

    if let ExecutionPlacement::Broker(broker) = placement {
        return spawn_broker(
            argv,
            working_dir,
            env_overrides,
            log_path,
            publisher,
            broker,
        )
        .await;
    }

    let mut env: HashMap<String, String> = std::env::vars().collect();
    env.extend(env_overrides.iter().map(|(k, v)| (k.clone(), v.clone())));

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .current_dir(working_dir)
        .envs(&env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {:?}", argv[0]))?;

    let stdout = child.stdout.take().expect("stdout piped");
    let stderr = child.stderr.take().expect("stderr piped");

    let io_tasks = match (log_path, publisher) {
        (Some(log_path), publisher) => {
            let log_path = log_path.to_owned();
            vec![tokio::spawn(tee_to_log(
                stdout, stderr, log_path, publisher,
            ))]
        }
        (None, Some(p)) => {
            vec![
                tokio::spawn(publish_only(stdout, p.clone())),
                tokio::spawn(publish_only(stderr, p)),
            ]
        }
        (None, None) => {
            vec![tokio::spawn(drain(stdout)), tokio::spawn(drain(stderr))]
        }
    };

    Ok(RunningProcess {
        inner: RunningProcessInner::Host(child),
        io_tasks,
    })
}

async fn spawn_broker(
    argv: &[String],
    working_dir: &Path,
    env_overrides: &HashMap<String, String>,
    log_path: Option<&Path>,
    publisher: Option<crate::log_stream::LogPublisher>,
    broker: &BrokerPlacement,
) -> Result<RunningProcess> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (
            argv,
            working_dir,
            env_overrides,
            log_path,
            publisher,
            broker,
        );
        return Err(IsolationError("network namespaces require Linux".into()).into());
    }
    #[cfg(target_os = "linux")]
    {
        use std::collections::BTreeMap;

        let mut stream = tokio::time::timeout(
            BROKER_CONNECT_TIMEOUT,
            tokio::net::UnixStream::connect(&broker.socket),
        )
        .await
        .map_err(|_| {
            IsolationError(format!(
                "connect broker {} timed out",
                broker.socket.display()
            ))
        })?
        .map_err(|e| IsolationError(format!("connect broker {}: {e}", broker.socket.display())))?;
        let plan = tunasync_netns::LaunchPlan {
            argv: argv.to_vec(),
            cwd: working_dir.to_string_lossy().into_owned(),
            env: env_overrides
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        };
        plan.validate().map_err(IsolationError)?;
        let request = tunasync_netns::ClientRequest::Launch(tunasync_netns::LaunchRequest {
            generation: broker.generation.clone(),
            mirror: broker.mirror.clone(),
            operation: broker.operation.clone(),
            namespace: broker.namespace.clone(),
            plan,
        });
        tokio::time::timeout(
            BROKER_IO_TIMEOUT,
            tunasync_netns::write_frame_async(&mut stream, &request),
        )
        .await
        .map_err(|_| IsolationError("broker launch write timed out".into()))?
        .map_err(|e| IsolationError(e.to_string()))?;
        let started: tunasync_netns::ServerFrame = tokio::time::timeout(
            BROKER_IO_TIMEOUT,
            tunasync_netns::read_frame_async(&mut stream),
        )
        .await
        .map_err(|_| IsolationError("broker launch response timed out".into()))?
        .map_err(|e| IsolationError(e.to_string()))?;
        let (job_id, pid) = match started {
            tunasync_netns::ServerFrame::Started { job_id, pid } => (job_id, pid),
            tunasync_netns::ServerFrame::Error { kind, message } => {
                return Err(IsolationError(format!("broker {kind}: {message}")).into());
            }
            _ => return Err(IsolationError("unexpected broker launch response".into()).into()),
        };
        let handle = ProcessHandle::Broker {
            socket: broker.socket.clone(),
            job_id,
        };
        let (exit_tx, exit_rx) = oneshot::channel();
        let log_path = log_path.map(Path::to_path_buf);
        let task = tokio::spawn(async move {
            let result = broker_read_loop(&mut stream, log_path, publisher).await;
            let _ = exit_tx.send(result);
        });
        Ok(RunningProcess {
            inner: RunningProcessInner::Broker {
                pid,
                handle,
                exit_rx,
            },
            io_tasks: vec![task],
        })
    }
}

pub async fn verify_broker_ready(socket: &Path, expected_generation: &str) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, expected_generation);
        return Err(IsolationError("network namespaces require Linux".into()).into());
    }
    #[cfg(target_os = "linux")]
    {
        let mut stream = tokio::time::timeout(
            BROKER_CONNECT_TIMEOUT,
            tokio::net::UnixStream::connect(socket),
        )
        .await
        .map_err(|_| IsolationError(format!("connect broker {} timed out", socket.display())))?
        .map_err(|e| IsolationError(format!("connect broker {}: {e}", socket.display())))?;
        tokio::time::timeout(
            BROKER_IO_TIMEOUT,
            tunasync_netns::write_frame_async(&mut stream, &tunasync_netns::ClientRequest::Status),
        )
        .await
        .map_err(|_| IsolationError("broker status write timed out".into()))?
        .map_err(|e| IsolationError(e.to_string()))?;
        let frame: tunasync_netns::ServerFrame = tokio::time::timeout(
            BROKER_IO_TIMEOUT,
            tunasync_netns::read_frame_async(&mut stream),
        )
        .await
        .map_err(|_| IsolationError("broker status response timed out".into()))?
        .map_err(|e| IsolationError(e.to_string()))?;
        match frame {
            tunasync_netns::ServerFrame::Ready { generation }
                if generation == expected_generation =>
            {
                Ok(())
            }
            tunasync_netns::ServerFrame::Ready { generation } => Err(IsolationError(format!(
                "broker generation mismatch: expected {expected_generation:?}, got {generation:?}"
            ))
            .into()),
            tunasync_netns::ServerFrame::Error { kind, message } => {
                Err(IsolationError(format!("broker {kind}: {message}")).into())
            }
            _ => Err(IsolationError("unexpected broker status response".into()).into()),
        }
    }
}

#[cfg(target_os = "linux")]
async fn broker_read_loop(
    stream: &mut tokio::net::UnixStream,
    log_path: Option<PathBuf>,
    publisher: Option<crate::log_stream::LogPublisher>,
) -> Result<BrokerExit> {
    use tokio::io::AsyncWriteExt;

    let mut file = if let Some(path) = log_path {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        Some(
            tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await
                .with_context(|| format!("open broker log {}", path.display()))?,
        )
    } else {
        None
    };
    let mut stdout_line = Vec::new();
    let mut stderr_line = Vec::new();
    loop {
        let frame: tunasync_netns::ServerFrame = tunasync_netns::read_frame_async(stream)
            .await
            .map_err(|e| IsolationError(e.to_string()))?;
        match frame {
            tunasync_netns::ServerFrame::Stdout { data } => {
                broker_output(&data, &mut stdout_line, &mut file, publisher.as_ref()).await?;
            }
            tunasync_netns::ServerFrame::Stderr { data } => {
                broker_output(&data, &mut stderr_line, &mut file, publisher.as_ref()).await?;
            }
            tunasync_netns::ServerFrame::Exit { code, signal } => {
                publish_tail(&mut stdout_line, publisher.as_ref());
                publish_tail(&mut stderr_line, publisher.as_ref());
                if let Some(file) = &mut file {
                    file.flush().await?;
                }
                return Ok(BrokerExit { code, signal });
            }
            tunasync_netns::ServerFrame::Error { kind, message } => {
                return Err(IsolationError(format!("broker {kind}: {message}")).into());
            }
            _ => return Err(IsolationError("unexpected broker stream frame".into()).into()),
        }
    }
}

#[cfg(target_os = "linux")]
async fn broker_output(
    data: &[u8],
    line_buffer: &mut Vec<u8>,
    file: &mut Option<tokio::fs::File>,
    publisher: Option<&crate::log_stream::LogPublisher>,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(file) = file {
        file.write_all(data).await?;
    }
    if let Some(publisher) = publisher {
        line_buffer.extend_from_slice(data);
        while let Some(pos) = line_buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = line_buffer.drain(..=pos).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            publisher.push(String::from_utf8_lossy(&line).into_owned());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn publish_tail(line_buffer: &mut Vec<u8>, publisher: Option<&crate::log_stream::LogPublisher>) {
    if let Some(publisher) = publisher {
        if !line_buffer.is_empty() {
            publisher.push(String::from_utf8_lossy(line_buffer).into_owned());
            line_buffer.clear();
        }
    }
}

// I/O helpers

/// Drain stdout and stderr concurrently into a log file via an mpsc channel.
/// Two reader tasks send lines to a shared channel; a single writer task
/// receives from the channel and writes to the file. This avoids pipe
/// deadlock: if stderr fills its pipe buffer while we're blocked on stdout,
/// the child stalls forever. Concurrent drain eliminates this risk.
///
/// When `publisher` is `Some`, each line is also pushed into the per-mirror
/// replay buffer + live broadcast channel that powers the streaming log API.
async fn tee_to_log<Ro, Re>(
    stdout: Ro,
    stderr: Re,
    log_path: std::path::PathBuf,
    publisher: Option<crate::log_stream::LogPublisher>,
) where
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

    let (tx, mut rx) = mpsc::channel::<String>(256);
    let tx_err = tx.clone();

    // stdout reader — sends each line to the channel.
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            if tx.send(l).await.is_err() {
                break;
            }
        }
    });

    // stderr reader — sends each line to the channel. When it finishes,
    // the original `tx` is dropped, closing the channel so the writer knows
    // both readers are done.
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            if tx_err.send(l).await.is_err() {
                break;
            }
        }
    });

    // Writer: receive lines from the channel, append to the log file, and
    // (if configured) republish to the per-mirror live-log channel.
    while let Some(l) = rx.recv().await {
        let _ = file.write_all(l.as_bytes()).await;
        let _ = file.write_all(b"\n").await;
        if let Some(ref p) = publisher {
            p.push(l);
        }
    }
    let _ = file.flush().await;
}

/// When the caller doesn't want a log file but still wants live publishing.
async fn publish_only<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    reader: R,
    publisher: crate::log_stream::LogPublisher,
) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(l)) = lines.next_line().await {
        publisher.push(l);
    }
}

async fn drain<R: tokio::io::AsyncRead + Unpin>(reader: R) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(_)) = lines.next_line().await {}
}

#[cfg(test)]
mod netns_tests {
    use super::*;

    #[tokio::test]
    async fn missing_broker_fails_without_local_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel = dir.path().join("must-not-exist");
        let command = vec![
            "/bin/sh".into(),
            "-c".into(),
            format!("touch {}", sentinel.display()),
        ];
        let placement = ExecutionPlacement::Broker(BrokerPlacement {
            socket: dir.path().join("missing.sock"),
            generation: "g1".into(),
            mirror: "sentinel".into(),
            operation: "sync".into(),
            namespace: "warp0".into(),
        });
        let result = spawn_placed(
            &command,
            dir.path(),
            &HashMap::new(),
            None,
            None,
            &placement,
        )
        .await;
        assert!(result.is_err());
        assert!(!sentinel.exists(), "command was executed in host namespace");
    }

    #[tokio::test]
    async fn broker_placement_creates_missing_working_directory_before_connect() {
        let dir = tempfile::tempdir().unwrap();
        let working_dir = dir.path().join("first/run");
        let placement = ExecutionPlacement::Broker(BrokerPlacement {
            socket: dir.path().join("missing.sock"),
            generation: "g1".into(),
            mirror: "first-run".into(),
            operation: "sync".into(),
            namespace: "warp0".into(),
        });
        let result = spawn_placed(
            &["/bin/true".into()],
            &working_dir,
            &HashMap::new(),
            None,
            None,
            &placement,
        )
        .await;
        assert!(result.is_err());
        assert!(working_dir.is_dir());
    }

    #[tokio::test]
    async fn readiness_checks_generation() {
        use tokio::net::UnixListener;

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: tunasync_netns::ClientRequest =
                tunasync_netns::read_frame_async(&mut stream).await.unwrap();
            assert_eq!(request, tunasync_netns::ClientRequest::Status);
            tunasync_netns::write_frame_async(
                &mut stream,
                &tunasync_netns::ServerFrame::Ready {
                    generation: "g2".into(),
                },
            )
            .await
            .unwrap();
        });
        let error = verify_broker_ready(&socket, "g1").await.unwrap_err();
        assert!(error.to_string().contains("generation mismatch"));
        server.await.unwrap();
    }
}
