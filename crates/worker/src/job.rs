//! Mirror job state machine.
//!
//! Mirrors Go's `worker/job.go`. Each `MirrorJob` runs in its own tokio task
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
use tracing::{debug, error, info, warn};
use tunasync_protocol::SyncStatus;

use crate::hooks::{HookPhase, JobHook};
use crate::provider::MirrorProvider;

// ---------------------------------------------------------------------------
// Control actions (manager → job)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Job state
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Status message (job → worker scheduler)
// ---------------------------------------------------------------------------

/// A status update pushed by a job to the worker scheduler.
///
/// Mirrors Go's `jobMessage`.
#[derive(Debug, Clone)]
pub struct JobMessage {
    pub status: SyncStatus,
    pub name: String,
    pub msg: String,
    /// Whether to re-enqueue for next scheduled run.
    pub schedule: bool,
    /// Human-readable data size after a successful sync (empty = unknown).
    pub size: String,
}

// ---------------------------------------------------------------------------
// MirrorJob
// ---------------------------------------------------------------------------

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
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let name = provider.name().to_owned();
        let state = Arc::new(AtomicU32::new(JobState::None as u32));
        let (ctrl_tx, ctrl_rx) = mpsc::channel(4);
        let (kill_tx, kill_rx) = watch::channel(false);

        let task_state = Arc::clone(&state);
        tokio::spawn(run_job_task(
            provider, hooks, ctrl_rx, kill_rx, status_tx, semaphore, task_state,
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

    /// Signal the running sync to terminate. Used when Stop/Halt/Disable
    /// arrives while a sync is in progress.
    pub fn kill(&self) {
        let _ = self.kill_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// Job task loop
// ---------------------------------------------------------------------------

async fn run_job_task(
    provider: Box<dyn MirrorProvider>,
    hooks: Vec<Box<dyn JobHook>>,
    mut ctrl_rx: mpsc::Receiver<CtrlAction>,
    mut kill_rx: watch::Receiver<bool>,
    status_tx: mpsc::Sender<JobMessage>,
    semaphore: Arc<tokio::sync::Semaphore>,
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
        // to the sync; Stop/Disable/Halt exit or loop back.
        match action {
            CtrlAction::Halt => {
                info!(mirror = %name, "halting job");
                set_state(&state, JobState::Halting);
                break;
            }
            CtrlAction::Disable => {
                info!(mirror = %name, "disabling job");
                set_state(&state, JobState::Disabled);
                break;
            }
            CtrlAction::Stop => {
                set_state(&state, JobState::Paused);
                continue;
            }
            CtrlAction::Start | CtrlAction::Restart | CtrlAction::ForceStart => {
                set_state(&state, JobState::Ready);
            }
            CtrlAction::Ping => {
                debug!(mirror = %name, "ping");
                continue;
            }
        }

        // Acquire semaphore slot (concurrency limit). ForceStart skips this.
        let _permit = if action != CtrlAction::ForceStart {
            Some(
                Arc::clone(&semaphore)
                    .acquire_owned()
                    .await
                    .expect("semaphore closed"),
            )
        } else {
            None
        };

        // Run the sync with retry logic.
        run_sync_with_retry(
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

        // Notify the scheduler that this job completed so it can enqueue
        // the next run at now + interval (matching Go's jobMessage with
        // schedule=true).
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::None,
                name: name.clone(),
                msg: String::new(),
                schedule: true,
                size: String::new(),
            })
            .await;

        // Loop back to wait for the scheduler's next Start signal.
        // The job does NOT sleep for its interval here — that is the
        // scheduler's responsibility, matching Go's design.
    }

    debug!(mirror = %name, "job task exiting");
}

/// Run the sync body (pre-job → retry loop → post-exec → post-success/fail).
///
/// Mirrors Go's `runJobWrapper` + outer retry loop in `mirrorJob.Run`.
/// `ctrl_rx` is checked between retries so Stop/Halt/Disable takes effect
/// without waiting for all retry attempts to expire.
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
) {
    // Announce pre-syncing.
    let _ = status_tx
        .send(JobMessage {
            status: SyncStatus::PreSyncing,
            name: name.to_owned(),
            msg: String::new(),
            schedule: false,
            size: String::new(),
        })
        .await;
    set_state(state, JobState::Ready); // stays Ready while syncing

    // pre-job hooks
    if run_hooks(hooks, HookPhase::PreJob, name, status_tx)
        .await
        .is_err()
    {
        return;
    }

    let mut success = false;
    let mut post_exec_ok = false;
    let effective_retry = max_retry.max(1);

    'retry: for attempt in 0..effective_retry {
        if attempt > 0 {
            // Check for abort between retries so Stop/Halt don't wait for all retries.
            if let Ok(ctrl) = ctrl_rx.try_recv() {
                if matches!(
                    ctrl,
                    CtrlAction::Halt | CtrlAction::Stop | CtrlAction::Disable
                ) {
                    debug!(mirror = %name, "aborting retry loop due to {:?}", ctrl);
                    break 'retry;
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
        // Matches Go's select on syncDone / kill / timeout.
        let run_result = {
            let run_fut = provider.run();
            let timeout_dur = provider.timeout();
            let mut killed = false;

            let result = if timeout_dur == Duration::ZERO {
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
                                error!(mirror = %name, "sync timed out");
                                provider.terminate().await.ok();
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
        // If post-exec fails, post-success/post-fail do NOT run.
        post_exec_ok = run_hooks(hooks, HookPhase::PostExec, name, status_tx)
            .await
            .is_ok();

        match run_result {
            Ok(()) => {
                success = true;
                break 'retry;
            }
            Err(e) => {
                warn!(mirror = %name, error = %e, attempt, "sync failed");
            }
        }
    }

    if success && post_exec_ok {
        let size = provider.data_size();
        // PostSuccess hooks — order handled by run_hooks (reversed per Go).
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
            })
            .await;
    } else if post_exec_ok {
        // PostFail hooks — order handled by run_hooks (reversed per Go).
        run_hooks(hooks, HookPhase::PostFail, name, status_tx)
            .await
            .ok();
        let _ = status_tx
            .send(JobMessage {
                status: SyncStatus::Failed,
                name: name.to_owned(),
                msg: "sync failed after all retries".into(),
                schedule: true,
                size: String::new(),
            })
            .await;
    }
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
    // Go reverses hooks for PostExec, PostSuccess, and PostFail.
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
