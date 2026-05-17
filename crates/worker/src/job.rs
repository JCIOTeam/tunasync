//! Mirror job state machine.
//!
//! Mirror job state machine. Each `MirrorJob` runs in its own tokio task
//! and communicates with the worker scheduler via two channels:
//!
//! - `ctrl_tx` → job: control commands (start, stop, disable, restart, ping, halt)
//! - `status_tx` → worker: status updates (manager is notified from the worker loop)
//!
//! State transitions:
//! ```text
//! None ──start──→ Ready ──schedule──→ Syncing
//!                   ↑                    │
//!                   └──────success/fail──┘
//!                   │
//!               jobStop → Paused
//!               jobDisable → Disabled
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tracing::{debug, error, info};
use tunasync_protocol::SyncStatus;

use crate::hooks::{HookPhase, JobHook};
use crate::priority_semaphore::{Permit as PriorityPermit, PrioritySemaphore};
use crate::provider::MirrorProvider;

// Control actions (manager → job)

/// Control action sent from worker scheduler to a running job task.
///
/// Matches Go's `ctrlAction` constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtrlAction {
    Start,
    Stop,
    Disable,
    Restart,
    Ping,
    /// Worker is halting — all jobs must stop.
    Halt,
    /// Start ignoring the concurrency semaphore.
    ForceStart,
}

// Job state

/// Observable state of a `MirrorJob`.
///
/// Stored as `u32` so it can be shared across tasks via `AtomicU32`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    None = 0,
    Ready = 1,
    Paused = 2,
    Disabled = 3,
    Halting = 4,
}

impl JobState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::Ready,
            2 => Self::Paused,
            3 => Self::Disabled,
            4 => Self::Halting,
            _ => Self::None,
        }
    }
}

// Status message (job → worker scheduler)

/// A status update pushed by a job to the worker scheduler.
///
/// Status update pushed by a job to the worker scheduler.
#[derive(Debug, Clone)]
pub struct JobMessage {
    pub status: SyncStatus,
    pub name: String,
    pub msg: String,
    /// Whether to re-enqueue for next scheduled run.
    pub schedule: bool,
    /// Human-readable data size after a successful sync (empty = unknown).
    pub size: String,
    /// Bytes transferred during this sync (0 = unknown).
    pub transferred_bytes: u64,
}

// MirrorJob

/// A mirror job: wraps a provider + its tokio task handle.
pub struct MirrorJob {
    pub name: String,
    state: Arc<AtomicU32>,
    ctrl_tx: mpsc::Sender<CtrlAction>,
    kill_tx: watch::Sender<bool>,
}

impl MirrorJob {
    /// Spawn a new job task and return the job handle.
    pub fn spawn(
        provider: Box<dyn MirrorProvider>,
        hooks: Vec<Box<dyn JobHook>>,
        status_tx: mpsc::Sender<JobMessage>,
        semaphore: Arc<PrioritySemaphore>,
        per_upstream_semaphore: Option<Arc<tokio::sync::Semaphore>>,
        priority: i32,
    ) -> Self {
        let name = provider.name().to_owned();
        let state = Arc::new(AtomicU32::new(JobState::None as u32));
        let (ctrl_tx, ctrl_rx) = mpsc::channel(4);
        let (kill_tx, kill_rx) = watch::channel(false);

        let task_state = Arc::clone(&state);
        tokio::spawn(run_job_task(
            provider, hooks, ctrl_rx, kill_rx, status_tx, semaphore,
            per_upstream_semaphore, priority, task_state,
        ));

        Self {
            name,
            state,
            ctrl_tx,
            kill_tx,
        }
    }

    /// Current observable state.
    pub fn state(&self) -> JobState {
        JobState::from_u32(self.state.load(Ordering::SeqCst))
    }

    /// Send a control action. Non-blocking — drops the message if the
    /// channel is full (capacity = 4, same spirit as Go's buffered chan(1)).
    pub async fn send(&self, action: CtrlAction) {
        let _ = self.ctrl_tx.send(action).await;
    }

    /// Try to send without awaiting.
    pub fn try_send(&self, action: CtrlAction) {
        let _ = self.ctrl_tx.try_send(action);
    }

    /// Whether the job task is still alive (ctrl channel not closed).
    /// A closed channel means the task has exited (e.g. after Halt).
    pub fn is_alive(&self) -> bool {
        !self.ctrl_tx.is_closed()
    }

    /// Signal the running sync to terminate. Used when Stop/Halt/Disable/
    /// Restart arrives while a sync is in progress.
    pub fn kill(&self) {
        let _ = self.kill_tx.send(true);
    }
}

// Job task loop

#[allow(clippy::too_many_arguments)]
async fn run_job_task(
    provider: Box<dyn MirrorProvider>,
    hooks: Vec<Box<dyn JobHook>>,
    mut ctrl_rx: mpsc::Receiver<CtrlAction>,
    mut kill_rx: watch::Receiver<bool>,
    status_tx: mpsc::Sender<JobMessage>,
    semaphore: Arc<PrioritySemaphore>,
    per_upstream_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    priority: i32,
    state: Arc<AtomicU32>,
) {
    let name = provider.name().to_owned();
    let max_retry = provider.retry();

    set_state(&state, JobState::Ready);
    debug!(mirror = %name, "job task started, state=Ready");

    loop {
        // Wait for a start signal (the scheduler sends Start when the
        // job's next-run time arrives). This matches Go's design where
        // the job itself has no interval sleep — scheduling is external.
        let action = match ctrl_rx.recv().await {
            Some(a) => a,
            None => {
                info!(mirror = %name, "ctrl channel closed — exiting");
                break;
            }
        };

        // Handle the control action. Start/Restart/ForceStart fall through
        // to the sync; Stop/Disable loop back to wait; Halt exits.
        match action {
            CtrlAction::Halt => {
                info!(mirror = %name, "halting job");
                set_state(&state, JobState::Halting);
                break;
            }
            CtrlAction::Disable => {
                info!(mirror = %name, "disabling job");
                set_state(&state, JobState::Disabled);
                // Do NOT break — stay in the loop so the task stays alive
                // and can receive a subsequent Start to re-enable.
                // Matches Go: disabled jobs sit in the bottom ctrl select.
                continue;
            }
            CtrlAction::Stop => {
                set_state(&state, JobState::Paused);
                continue;
            }
            CtrlAction::Start | CtrlAction::Restart | CtrlAction::ForceStart => {
                set_state(&state, JobState::Ready);
                // Brief pause for cleanup after a Restart kill (matches Go's
                // time.Sleep(time.Second) after Restart kill).
                if action == CtrlAction::Restart {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            CtrlAction::Ping => {
                debug!(mirror = %name, "ping");
                continue;
            }
        }

        // Acquire semaphore slot (concurrency limit). ForceStart skips this.
        // We also watch for kill so Halt/Stop during semaphore wait takes effect.
        // PrioritySemaphore wakes waiters in descending priority order.
        let _permit: Option<PriorityPermit> = if action != CtrlAction::ForceStart {
            let sem = Arc::clone(&semaphore);
            tokio::select! {
                permit = sem.acquire(priority) => {
                    Some(permit)
                }
                _ = kill_rx.changed() => {
                    info!(mirror = %name, "killed while waiting for semaphore");
                    continue;
                }
            }
        } else {
            None
        };

        // Acquire per-upstream semaphore (if configured) after the global slot.
        // Acquiring in this order (global first, upstream second) prevents
        // a deadlock between jobs that share an upstream.
        let _upstream_permit = if action != CtrlAction::ForceStart {
            if let Some(ref us) = per_upstream_semaphore {
                let us = Arc::clone(us);
                let permit = tokio::select! {
                    p = us.acquire_owned() => Some(p.expect("upstream semaphore closed")),
                    _ = kill_rx.changed() => {
                        info!(mirror = %name, "killed while waiting for upstream semaphore");
                        continue;
                    }
                };
                permit
            } else {
                None
            }
        } else {
            None
        };

        // Run the sync with retry logic.
        let killed = run_sync_with_retry(
            &*provider,
            &hooks,
            max_retry,
            &status_tx,
            &name,
            &state,
            &mut ctrl_rx,
            &mut kill_rx,
        )
        .await;

        // If the sync was killed (Restart/Stop/Disable/Halt), skip scheduling.
        // Matches Go: `schedule: (m.State() == stateReady)`.
        if killed {
            // If state was set to Halting (either by the top-of-loop Halt branch
            // or by the retry-loop try_recv handler), exit the task.
            if state.load(Ordering::SeqCst) == JobState::Halting as u32 {
                debug!(mirror = %name, "halt detected — exiting job task");
                break;
            }
            continue;
        }

        // If a Halt arrived during the retry loop (try_recv set state to Halting
        // without going through the top-of-loop branch), exit cleanly.
        if state.load(Ordering::SeqCst) == JobState::Halting as u32 {
            debug!(mirror = %name, "halt detected — exiting job task");
            break;
        }

        // Only schedule the next run if the job is still in Ready state.
        // This matches Go's `(m.State() == stateReady)` check in jobMessage.
        let schedule = state.load(Ordering::SeqCst) == JobState::Ready as u32;
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::None,
                name: name.clone(),
                msg: String::new(),
                schedule,
                size: String::new(),
                transferred_bytes: 0,
            })
            .await;

        // Loop back to wait for the scheduler's next Start signal.
    }

    debug!(mirror = %name, "job task exiting");
}

/// Run the sync body (pre-job → retry loop → post-exec → post-success/fail).
///
/// Run the sync body (pre-job → retry loop → post-exec → post-success/fail).
/// `ctrl_rx` is checked between retries so Stop/Halt/Disable takes effect
/// without waiting for all retry attempts to expire.
///
/// Returns `true` if the sync was killed (the caller should not schedule).
#[allow(clippy::too_many_arguments)]
async fn run_sync_with_retry(
    provider: &dyn MirrorProvider,
    hooks: &[Box<dyn JobHook>],
    max_retry: u32,
    status_tx: &mpsc::Sender<JobMessage>,
    name: &str,
    state: &Arc<AtomicU32>,
    ctrl_rx: &mut mpsc::Receiver<CtrlAction>,
    kill_rx: &mut watch::Receiver<bool>,
) -> bool {
    // Disk-quota pre-check: skip (don't fail) if free space at working_dir is
    // below the configured threshold.  schedule=true keeps the mirror in Ready
    // state so it is retried at its next scheduled interval.
    let quota = provider.disk_quota_bytes();
    if quota > 0 {
        let wd = provider.working_dir();
        if let Some((avail, _total)) = tunasync_common::util::disk_space(wd) {
            if avail < quota {
                tracing::warn!(
                    mirror = %name,
                    available = avail,
                    quota,
                    "skipping sync: disk space below quota threshold"
                );
                let _ = status_tx
                    .send(JobMessage {
                        status: SyncStatus::Failed,
                        name: name.to_owned(),
                        msg: format!(
                            "disk quota: only {avail} bytes available, need {quota}"
                        ),
                        schedule: true,
                        size: String::new(),
                        transferred_bytes: 0,
                    })
                    .await;
                return false;
            }
        }
    }

    // Upstream probe: if check_upstream is set, verify at least one of the
    // configured URLs is reachable before wasting bandwidth on a full sync.
    if let Err(e) = provider.probe_upstream().await {
        tracing::warn!(mirror = %name, error = %e, "upstream probe failed; skipping sync");
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::Failed,
                name: name.to_owned(),
                msg: format!("upstream unreachable: {e}"),
                schedule: true,
                size: String::new(),
                transferred_bytes: 0,
            })
            .await;
        return false;
    }

    // Announce pre-syncing.
    let _ = status_tx
        .send(JobMessage {
            status: SyncStatus::PreSyncing,
            name: name.to_owned(),
            msg: String::new(),
            schedule: false,
            size: String::new(),
                transferred_bytes: 0,
        })
        .await;
    set_state(state, JobState::Ready);

    // pre-job hooks
    if run_hooks(hooks, HookPhase::PreJob, name, status_tx)
        .await
        .is_err()
    {
        return false;
    }

    let mut success = false;
    let mut post_exec_ok = false;
    let mut killed = false;
    let mut timed_out = false;
    let mut last_error = String::new();
    let effective_retry = max_retry.max(1);

    'retry: for attempt in 0..effective_retry {
        if attempt > 0 {
            // Check for abort between retries. If killed, exit immediately
            // (matches Go's stopASAP flag).
            //
            // We use `has_changed()` instead of `borrow()` so we only react
            // to kills that arrived *since the last check* — a stale `true`
            // from a previous, already-handled kill (marked seen via
            // `borrow_and_update` at the end of the previous sync) must not
            // poison subsequent retry attempts.
            let fresh_kill = kill_rx.has_changed().unwrap_or(false);
            if killed || fresh_kill {
                debug!(mirror = %name, "aborting retry loop — killed");
                // Mark the new value as seen so the next sync isn't poisoned
                // either.
                let _ = kill_rx.borrow_and_update();
                killed = true;
                break 'retry;
            }
            // Check for Stop/Halt/Disable between retries.
            //
            // Consuming the ctrl message here without applying it would leave
            // the job in `Ready` state and (via `schedule = state == Ready`
            // in the outer loop) silently re-enqueue the job — the user's
            // Stop click would be effectively ignored. Apply the state change
            // here so the outer loop computes `schedule = false` and (for
            // Halt) breaks out of the task.
            if let Ok(ctrl) = ctrl_rx.try_recv() {
                match ctrl {
                    CtrlAction::Halt => {
                        debug!(mirror = %name, "aborting retry loop due to Halt");
                        set_state(state, JobState::Halting);
                        break 'retry;
                    }
                    CtrlAction::Stop => {
                        debug!(mirror = %name, "aborting retry loop due to Stop");
                        set_state(state, JobState::Paused);
                        break 'retry;
                    }
                    CtrlAction::Disable => {
                        debug!(mirror = %name, "aborting retry loop due to Disable");
                        set_state(state, JobState::Disabled);
                        break 'retry;
                    }
                    _ => {}
                }
            }
            info!(mirror = %name, attempt, "retrying sync");
        }

        // Announce syncing.
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::Syncing,
                name: name.to_owned(),
                msg: String::new(),
                schedule: false,
                size: String::new(),
                transferred_bytes: 0,
            })
            .await;

        // pre-exec hooks
        if run_hooks(hooks, HookPhase::PreExec, name, status_tx)
            .await
            .is_err()
        {
            break 'retry;
        }

        // Run the actual sync (with optional timeout and kill signal).
        let run_result = {
            let run_fut = provider.run();
            let timeout_dur = provider.timeout();

            let result = if timeout_dur == Duration::ZERO {
                // No timeout configured — rely on rsync's own --timeout for
                // I/O-level stalls. A hard ceiling would be too aggressive for
                // large mirrors (several TB) that legitimately take days.
                tokio::select! {
                    r = run_fut => r,
                    _ = kill_rx.changed() => {
                        killed = true;
                        provider.terminate().await.ok();
                        Err(anyhow::anyhow!("killed by manager"))
                    }
                }
            } else {
                tokio::select! {
                    r = timeout(timeout_dur, run_fut) => {
                        match r {
                            Ok(r) => r,
                            Err(_) => {
                                error!(mirror = %name, secs = timeout_dur.as_secs(),
                                    "sync timed out");
                                provider.terminate().await.ok();
                                timed_out = true;
                                Err(anyhow::anyhow!("sync timed out"))
                            }
                        }
                    }
                    _ = kill_rx.changed() => {
                        killed = true;
                        provider.terminate().await.ok();
                        Err(anyhow::anyhow!("killed by manager"))
                    }
                }
            };

            if killed {
                info!(mirror = %name, "sync was killed");
            }
            result
        };

        // post-exec hooks — run in reverse order per Go's behaviour.
        post_exec_ok = run_hooks(hooks, HookPhase::PostExec, name, status_tx)
            .await
            .is_ok();

        match run_result {
            Ok(()) => {
                success = true;
                break 'retry;
            }
            Err(e) => {
                // Always record the error message so the Failed status report
                // includes a human-readable reason (e.g. "sync timed out").
                last_error = e.to_string();
                // If killed or timed out, don't retry — break immediately and
                // wait for the next scheduled sync cycle.
                if killed || timed_out {
                    break 'retry;
                }
            }
        }
    }

    if killed {
        // Consume any pending kill notification so subsequent changed()
        // calls don't fire on a stale version (e.g. when the kill was
        // detected via borrow() between retries rather than via changed()
        // in a select block). borrow_and_update() marks the current value
        // as "seen" without waiting.
        let _ = kill_rx.borrow_and_update();
        return true;
    }

    if success && post_exec_ok {
        let size = provider.data_size();
        let transferred = provider.transferred_bytes();
        run_hooks(hooks, HookPhase::PostSuccess, name, status_tx)
            .await
            .ok();
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::Success,
                name: name.to_owned(),
                msg: String::new(),
                schedule: false,
                size,
                transferred_bytes: transferred,
            })
            .await;
    } else if post_exec_ok {
        run_hooks(hooks, HookPhase::PostFail, name, status_tx)
            .await
            .ok();
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::Failed,
                name: name.to_owned(),
                msg: last_error,
                schedule: true,
                size: String::new(),
                transferred_bytes: 0,
            })
            .await;
    }

    false
}

/// Run all hooks for a given phase.
///
/// Returns `Err(())` if any hook fails (caller should abort the job).
async fn run_hooks(
    hooks: &[Box<dyn JobHook>],
    phase: HookPhase,
    name: &str,
    status_tx: &mpsc::Sender<JobMessage>,
) -> Result<(), ()> {
    let hooks_to_run: Vec<_> = match phase {
        HookPhase::PostExec | HookPhase::PostSuccess | HookPhase::PostFail => {
            hooks.iter().rev().collect()
        }
        _ => hooks.iter().collect(),
    };
    for hook in hooks_to_run {
        if let Err(e) = hook.on_phase(phase).await {
            error!(
                mirror = %name,
                hook = %hook.name(),
                phase = ?phase,
                error = %e,
                "hook failed"
            );
            let _ = status_tx
                .send(JobMessage {
                    status: SyncStatus::Failed,
                    name: name.to_owned(),
                    msg: format!("hook {} {:?} failed: {e}", hook.name(), phase),
                    schedule: true,
                    size: String::new(),
                transferred_bytes: 0,
                })
                .await;
            return Err(());
        }
    }
    Ok(())
}

fn set_state(state: &Arc<AtomicU32>, s: JobState) {
    state.store(s as u32, Ordering::SeqCst);
}

#[cfg(test)]
mod disk_quota_tests {
    //! Tests for the disk-quota pre-sync check in `run_sync_with_retry`.

    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use tunasync_protocol::SyncStatus;

    use crate::hooks::DockerConfig;
    use crate::job::{run_sync_with_retry, JobMessage, JobState, CtrlAction};
    use crate::provider::MirrorProvider;

    /// A no-op provider with configurable working_dir and disk_quota_bytes.
    struct QuotaStubProvider {
        working_dir: PathBuf,
        disk_quota_bytes: u64,
    }

    #[async_trait]
    impl MirrorProvider for QuotaStubProvider {
        fn name(&self) -> &str { "quota-stub" }
        fn upstream(&self) -> &str { "rsync://localhost/test/" }
        fn is_master(&self) -> bool { true }
        fn interval(&self) -> Duration { Duration::from_secs(3600) }
        fn retry(&self) -> u32 { 1 }
        fn timeout(&self) -> Duration { Duration::ZERO }
        fn working_dir(&self) -> &Path { &self.working_dir }
        fn disk_quota_bytes(&self) -> u64 { self.disk_quota_bytes }
        async fn run(&self) -> anyhow::Result<()> { Ok(()) }
        async fn terminate(&self) -> anyhow::Result<()> { Ok(()) }
        fn set_docker_config(&mut self, _: DockerConfig) {}
        fn set_log_path_shared(&mut self, _: Arc<Mutex<PathBuf>>) {}
    }

    /// Drive `run_sync_with_retry` to completion and return every message sent.
    async fn run_and_collect(provider: QuotaStubProvider) -> Vec<JobMessage> {
        let (status_tx, mut status_rx) = mpsc::channel::<JobMessage>(16);
        let (_ctrl_tx, mut ctrl_rx) = mpsc::channel::<CtrlAction>(4);
        let (_kill_tx, mut kill_rx) = tokio::sync::watch::channel(false);
        let state = Arc::new(AtomicU32::new(JobState::Ready as u32));

        run_sync_with_retry(
            &provider,
            &[],
            1,
            &status_tx,
            "quota-stub",
            &state,
            &mut ctrl_rx,
            &mut kill_rx,
        )
        .await;

        drop(status_tx); // close sender so recv() terminates
        let mut msgs = Vec::new();
        while let Some(m) = status_rx.recv().await {
            msgs.push(m);
        }
        msgs
    }

    /// When quota > available space the sync is skipped: exactly one Failed
    /// message with "disk quota" in the body; no PreSyncing is sent.
    #[tokio::test]
    async fn quota_exceeded_sends_failed_and_skips() {
        let provider = QuotaStubProvider {
            working_dir: PathBuf::from("/"),
            disk_quota_bytes: u64::MAX, // impossible to satisfy
        };
        let msgs = run_and_collect(provider).await;

        assert_eq!(msgs.len(), 1, "expected exactly one message, got {msgs:?}");
        let msg = &msgs[0];
        assert_eq!(msg.status, SyncStatus::Failed);
        assert!(
            msg.msg.contains("disk quota"),
            "expected 'disk quota' in msg, got {:?}",
            msg.msg
        );
        assert!(msg.schedule, "schedule must be true so mirror stays Ready");
    }

    /// When quota == 0 (disabled) the pre-check is skipped and the sync
    /// proceeds normally.  The stub run() returns Ok, so we get a Success.
    #[tokio::test]
    async fn quota_zero_skips_check() {
        let provider = QuotaStubProvider {
            working_dir: PathBuf::from("/"),
            disk_quota_bytes: 0,
        };
        let msgs = run_and_collect(provider).await;

        let has_failed = msgs.iter().any(|m| m.status == SyncStatus::Failed);
        assert!(!has_failed, "unexpected Failed message with quota=0: {msgs:?}");
        let has_success = msgs.iter().any(|m| m.status == SyncStatus::Success);
        assert!(has_success, "expected Success message with quota=0: {msgs:?}");
    }
}

#[cfg(test)]
mod per_upstream_semaphore_tests {
    //! Tests for per-upstream concurrency limiting.
    //!
    //! We verify that with max=1 for an upstream host, a second job cannot
    //! start until the first releases its permit.

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::{mpsc, Semaphore};

    use crate::hooks::DockerConfig;
    use crate::job::{CtrlAction, JobMessage, MirrorJob};
    use crate::priority_semaphore::PrioritySemaphore;
    use crate::provider::MirrorProvider;

    struct SlowProvider {
        /// Unblocks run() when set.
        running: Arc<tokio::sync::Notify>,
        /// Unblocks run() when set.
        unblock: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl MirrorProvider for SlowProvider {
        fn name(&self) -> &str { "slow" }
        fn upstream(&self) -> &str { "rsync://upstream.example.com/test/" }
        fn is_master(&self) -> bool { true }
        fn interval(&self) -> Duration { Duration::from_secs(3600) }
        fn retry(&self) -> u32 { 1 }
        fn timeout(&self) -> Duration { Duration::ZERO }
        async fn run(&self) -> anyhow::Result<()> {
            self.running.notify_one();
            self.unblock.notified().await;
            Ok(())
        }
        async fn terminate(&self) -> anyhow::Result<()> { Ok(()) }
        fn set_docker_config(&mut self, _: DockerConfig) {}
        fn set_log_path_shared(&mut self, _: Arc<Mutex<PathBuf>>) {}
    }

    /// Two jobs with the same upstream and max=1: the second job must not
    /// start (acquire the upstream semaphore) until the first has finished.
    #[tokio::test]
    async fn second_job_waits_for_upstream_semaphore() {
        let global_sem = Arc::new(PrioritySemaphore::new(4)); // plenty of global permits
        let upstream_sem = Arc::new(Semaphore::new(1)); // only 1 upstream slot

        let running1 = Arc::new(tokio::sync::Notify::new());
        let unblock1 = Arc::new(tokio::sync::Notify::new());
        let running2 = Arc::new(tokio::sync::Notify::new());
        let unblock2 = Arc::new(tokio::sync::Notify::new());

        let (tx1, rx1) = mpsc::channel::<JobMessage>(8);
        let (tx2, rx2) = mpsc::channel::<JobMessage>(8);

        let p1 = SlowProvider {
            running: Arc::clone(&running1),
            unblock: Arc::clone(&unblock1),
        };
        let p2 = SlowProvider {
            running: Arc::clone(&running2),
            unblock: Arc::clone(&unblock2),
        };

        let job1 = MirrorJob::spawn(
            Box::new(p1), vec![], tx1,
            Arc::clone(&global_sem), Some(Arc::clone(&upstream_sem)), 50,
        );
        let job2 = MirrorJob::spawn(
            Box::new(p2), vec![], tx2,
            Arc::clone(&global_sem), Some(Arc::clone(&upstream_sem)), 50,
        );

        // Start both jobs concurrently.
        job1.send(CtrlAction::Start).await;
        job2.send(CtrlAction::Start).await;

        // Wait for job1 to enter run().
        tokio::time::timeout(Duration::from_secs(2), running1.notified())
            .await
            .expect("job1 should start running");

        // job2 should NOT have started yet (upstream semaphore is held by job1).
        // Give it a brief moment to make sure it's stuck.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The upstream semaphore should have 0 permits available — job1 holds the only one.
        assert_eq!(
            upstream_sem.available_permits(), 0,
            "upstream semaphore should be fully held while job1 runs"
        );

        // Unblock job1 so it completes and releases the upstream semaphore.
        unblock1.notify_one();

        // Now job2 should be able to start.
        tokio::time::timeout(Duration::from_secs(2), running2.notified())
            .await
            .expect("job2 should start after job1 finishes");

        // Unblock job2 and clean up.
        unblock2.notify_one();
        drop(job1); drop(job2);
        drop(rx1); drop(rx2);
    }
}
