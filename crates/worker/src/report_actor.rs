//! Fair, bounded manager I/O actor.
//!
//! The scheduler only mutates local state and synchronously enqueues work here.
//! Manager registration, persisted-state restore, heartbeat, report delivery,
//! pending replay, and manager reconfiguration all run outside the scheduler.

mod mailbox;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use reqwest::Client;
#[cfg(test)]
use tokio::sync::Semaphore;
use tokio::sync::{oneshot, watch, Notify};
use tokio::task::{JoinError, JoinHandle};
#[cfg(test)]
use tunasync_protocol::MirrorSchedules;
use tunasync_protocol::{MirrorStatus, WorkerStatus};

use crate::manager_client::ManagerClient;
pub use mailbox::ReportHandle;
#[cfg(test)]
use mailbox::ORDINARY_CAPACITY;
use mailbox::{ControlItem, MailboxState, Report, SequencedReport};

const MAX_REPLAY_BATCH: usize = 8;
const LIVE_REPORTS_PER_REPLAY: usize = 8;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ReportActor {
    manager: Arc<ManagerClient>,
    worker_id: String,
    mailbox: Arc<Mutex<MailboxState>>,
    notify: Arc<Notify>,
    results: watch::Sender<ActorResultState>,
    heartbeat_interval: Duration,
    last_delivered_sequence: u64,
    pending_limit: usize,
    shutdown_drain_timeout: Duration,
    #[cfg(test)]
    start_ready_hook: Option<Arc<TestStartReadyHook>>,
}

#[derive(Clone)]
pub struct BootstrapCommand {
    pub registration: WorkerStatus,
    pub restore_versions: HashMap<String, u64>,
}

#[derive(Clone)]
pub struct ReconfigureCommand {
    pub generation: u64,
    pub bases: Vec<String>,
    pub client: Client,
    pub token: String,
    pub registration: WorkerStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfigureOutcome {
    Success,
    Failed(String),
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconfigureResult {
    pub generation: u64,
    pub outcome: ReconfigureOutcome,
}

#[derive(Debug, Clone)]
pub struct RestoreResult {
    pub captured_versions: HashMap<String, u64>,
    pub outcome: RestoreOutcome,
}

#[derive(Debug, Clone)]
pub enum RestoreOutcome {
    Success(Vec<MirrorStatus>),
    Failed(String),
}

#[derive(Debug, Clone, Default)]
pub struct ActorResultState {
    pub restore: Option<RevisedResult<RestoreResult>>,
    pub reconfigure: Option<RevisedResult<ReconfigureResult>>,
}

#[derive(Debug, Clone)]
pub struct RevisedResult<T> {
    pub revision: u64,
    pub value: T,
}

struct CandidateTask {
    generation: u64,
    handle: JoinHandle<(ReconfigureCommand, Result<(), String>)>,
}

struct LiveDeliveryTask {
    report: SequencedReport,
    handle: JoinHandle<SequencedReport>,
}

struct DrainState {
    done: oneshot::Sender<()>,
    deadline: tokio::time::Instant,
}

struct ActorRuntime {
    next_heartbeat: tokio::time::Instant,
    bootstrap: Option<BootstrapCommand>,
    registered: bool,
    restore_started: bool,
    bootstrap_task: Option<JoinHandle<Result<(), String>>>,
    heartbeat_task: Option<JoinHandle<anyhow::Result<()>>>,
    restore_task: Option<JoinHandle<RestoreResult>>,
    candidate_task: Option<CandidateTask>,
    live_task: Option<LiveDeliveryTask>,
    replay_task: Option<JoinHandle<bool>>,
    replay_requested: bool,
    heartbeat_requested: bool,
    live_reports_since_replay: usize,
    draining: Option<DrainState>,
}

impl ActorRuntime {
    fn new(heartbeat_interval: Duration) -> Self {
        Self {
            next_heartbeat: tokio::time::Instant::now() + heartbeat_interval,
            bootstrap: None,
            registered: false,
            restore_started: false,
            bootstrap_task: None,
            heartbeat_task: None,
            restore_task: None,
            candidate_task: None,
            live_task: None,
            replay_task: None,
            replay_requested: false,
            heartbeat_requested: false,
            live_reports_since_replay: 0,
            draining: None,
        }
    }
}

#[cfg(test)]
struct TestStartReadyHook {
    armed: AtomicBool,
    reached: Notify,
    release: Semaphore,
}

#[cfg(test)]
impl TestStartReadyHook {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            reached: Notify::new(),
            release: Semaphore::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    fn release(&self) {
        self.release.add_permits(1);
    }

    async fn pause_if_armed(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.reached.notify_one();
            self.release
                .acquire()
                .await
                .expect("test hook closed")
                .forget();
        }
    }
}

enum ActorEvent {
    Notified,
    CandidateSuperseded,
    HeartbeatDue,
    DrainDeadline,
    BootstrapFinished(Result<Result<(), String>, JoinError>),
    HeartbeatFinished(Result<anyhow::Result<()>, JoinError>),
    RestoreFinished(Result<RestoreResult, JoinError>),
    CandidateFinished(Result<(ReconfigureCommand, Result<(), String>), JoinError>),
    LiveFinished(Result<SequencedReport, JoinError>),
    ReplayFinished(Result<bool, JoinError>),
}

impl ReportActor {
    pub fn new(
        manager: Arc<ManagerClient>,
        worker_id: String,
        heartbeat_interval: Duration,
        max_resources: usize,
    ) -> (ReportHandle, Self, watch::Receiver<ActorResultState>) {
        let max_resources = crate::config::effective_report_max_resources(max_resources);
        let mailbox = Arc::new(Mutex::new(MailboxState::new(max_resources)));
        let notify = Arc::new(Notify::new());
        let (results, result_rx) = watch::channel(ActorResultState::default());
        let handle = ReportHandle::new(Arc::clone(&mailbox), Arc::clone(&notify));
        let actor = Self {
            manager,
            worker_id,
            mailbox,
            notify,
            results,
            heartbeat_interval,
            last_delivered_sequence: 0,
            pending_limit: max_resources,
            shutdown_drain_timeout: SHUTDOWN_DRAIN_TIMEOUT,
            #[cfg(test)]
            start_ready_hook: None,
        };
        (handle, actor, result_rx)
    }

    pub async fn run(mut self) {
        let mut runtime = ActorRuntime::new(self.heartbeat_interval);
        loop {
            if let Some(control) = self.take_control(&runtime) {
                self.handle_control(control, &mut runtime).await;
                continue;
            }
            if runtime.heartbeat_requested {
                self.start_ready_work(&mut runtime).await;
                continue;
            }
            if let Some(event) = self.collect_finished_tasks(&mut runtime).await {
                if matches!(event, ActorEvent::Notified) {
                    continue;
                }
                if self.handle_event(event, &mut runtime).await {
                    return;
                }
                continue;
            }
            self.start_ready_work(&mut runtime).await;
            if self.finish_or_expire_drain(&mut runtime).await {
                return;
            }
            let event = self.wait_for_event(&mut runtime).await;
            if matches!(event, ActorEvent::Notified) {
                continue;
            }
            if self.handle_event(event, &mut runtime).await {
                return;
            }
        }
    }

    fn take_control(&self, runtime: &ActorRuntime) -> Option<ControlItem> {
        runtime
            .draining
            .is_none()
            .then(|| self.mailbox.lock().take_control())
            .flatten()
    }

    async fn handle_control(&self, control: ControlItem, runtime: &mut ActorRuntime) {
        match control {
            ControlItem::Shutdown(done) => self.begin_shutdown(done, runtime).await,
            ControlItem::Bootstrap(command) => {
                runtime.bootstrap = Some(command);
                runtime.registered = false;
                runtime.restore_started = false;
                abort_task(&mut runtime.bootstrap_task);
                abort_task(&mut runtime.restore_task);
                runtime.heartbeat_requested = true;
            }
            ControlItem::Reconfigure(command) => {
                self.supersede_candidate(runtime);
                runtime.candidate_task = Some(self.spawn_candidate(command));
            }
            ControlItem::ForgetMirror(mirror) => self.forget_mirror(mirror, runtime).await,
        }
    }

    async fn begin_shutdown(&self, done: oneshot::Sender<()>, runtime: &mut ActorRuntime) {
        abort_task(&mut runtime.bootstrap_task);
        abort_task(&mut runtime.heartbeat_task);
        abort_task(&mut runtime.restore_task);
        if let Some(task) = runtime.candidate_task.take() {
            task.handle.abort();
        }
        if let Some(task) = runtime.replay_task.take() {
            task.abort();
            let _ = task.await;
        }
        runtime.replay_requested = false;
        self.mailbox.lock().discard_control_after_shutdown();
        runtime.draining = Some(DrainState {
            done,
            deadline: tokio::time::Instant::now() + self.shutdown_drain_timeout,
        });
    }

    async fn forget_mirror(&self, mirror: String, runtime: &mut ActorRuntime) {
        // The synchronous handle-side purge protects a same-name mirror queued
        // after forget_mirror returns. Only pre-existing active work is cancelled.
        if runtime
            .live_task
            .as_ref()
            .is_some_and(|task| task.report.report.belongs_to(&mirror))
        {
            let task = runtime.live_task.take().expect("checked");
            task.handle.abort();
            let _ = task.handle.await;
            self.mailbox.lock().release_in_flight(&task.report);
        }
        if let Some(task) = runtime.replay_task.take() {
            task.abort();
            let _ = task.await;
            runtime.replay_requested = true;
        }
        self.manager.remove_pending_mirror(&mirror).await;
    }

    async fn collect_finished_tasks(&self, runtime: &mut ActorRuntime) -> Option<ActorEvent> {
        if self.mailbox.lock().control_pending() {
            return Some(ActorEvent::Notified);
        }
        if runtime.candidate_task.as_ref().is_some_and(|task| {
            self.mailbox.lock().latest_reconfigure_generation != task.generation
        }) {
            return Some(ActorEvent::CandidateSuperseded);
        }
        if runtime
            .live_task
            .as_ref()
            .is_some_and(|task| task.handle.is_finished())
            && runtime.draining.is_some()
        {
            return Some(ActorEvent::LiveFinished(
                (&mut runtime.live_task.as_mut().expect("checked").handle).await,
            ));
        }
        if runtime.draining.is_none() && tokio::time::Instant::now() >= runtime.next_heartbeat {
            return Some(ActorEvent::HeartbeatDue);
        }
        self.take_finished_event(runtime).await
    }

    async fn start_ready_work(&self, runtime: &mut ActorRuntime) {
        #[cfg(test)]
        if let Some(hook) = &self.start_ready_hook {
            hook.pause_if_armed().await;
        }

        if runtime.draining.is_some() {
            if runtime.live_task.is_none() {
                self.start_live_delivery(runtime);
            }
            return;
        }

        if runtime.heartbeat_requested {
            let mailbox = self.mailbox.lock();
            if mailbox.control_pending() {
                return;
            }
            runtime.heartbeat_requested = false;
            if !runtime.registered {
                self.start_bootstrap(runtime);
            } else if runtime.heartbeat_task.is_none() {
                let manager = Arc::clone(&self.manager);
                let worker_id = self.worker_id.clone();
                runtime.heartbeat_task =
                    Some(tokio::spawn(
                        async move { manager.heartbeat(&worker_id).await },
                    ));
            }
            drop(mailbox);
        }

        if runtime.live_task.is_some() || runtime.replay_task.is_some() {
            return;
        }
        let mut mailbox = self.mailbox.lock();
        if mailbox.control_pending() {
            return;
        }
        let live_available = mailbox.has_reports();
        if runtime.replay_requested
            && runtime.candidate_task.is_none()
            && (!live_available || runtime.live_reports_since_replay >= LIVE_REPORTS_PER_REPLAY)
        {
            let manager = Arc::clone(&self.manager);
            let worker_id = self.worker_id.clone();
            runtime.replay_task = Some(tokio::spawn(async move {
                manager
                    .flush_pending_batch(&worker_id, MAX_REPLAY_BATCH)
                    .await
            }));
            runtime.replay_requested = false;
        } else if live_available {
            if let Some(report) = mailbox.take_next_report_for_delivery() {
                runtime.live_task = Some(self.spawn_delivery(report));
            }
        }
    }

    fn start_live_delivery(&self, runtime: &mut ActorRuntime) {
        if let Some(report) = self.mailbox.lock().take_next_report_for_delivery() {
            runtime.live_task = Some(self.spawn_delivery(report));
        }
    }

    async fn finish_or_expire_drain(&self, runtime: &mut ActorRuntime) -> bool {
        let Some(drain) = runtime.draining.as_ref() else {
            return false;
        };
        if runtime.live_task.is_none() && !self.mailbox.lock().has_reports() {
            let drain = runtime.draining.take().expect("draining state present");
            let _ = drain.done.send(());
            return true;
        }
        if tokio::time::Instant::now() < drain.deadline {
            return false;
        }
        if let Some(task) = runtime.live_task.take() {
            task.handle.abort();
            let _ = task.handle.await;
            self.mailbox.lock().release_in_flight(&task.report);
        }
        let remaining = self.mailbox.lock().queued_report_count();
        tracing::warn!(
            remaining,
            "report actor shutdown drain deadline reached; dropping queued reports"
        );
        self.mailbox.lock().drop_queued_reports();
        let drain = runtime.draining.take().expect("draining state present");
        let _ = drain.done.send(());
        true
    }

    async fn wait_for_event(&self, runtime: &mut ActorRuntime) -> ActorEvent {
        if self.mailbox.lock().control_pending() {
            return ActorEvent::Notified;
        }
        if let Some(event) = self.take_finished_event(runtime).await {
            return event;
        }
        if runtime.draining.is_some() {
            self.wait_for_drain_event(runtime).await
        } else {
            self.wait_for_active_event(runtime).await
        }
    }

    async fn take_finished_event(&self, runtime: &mut ActorRuntime) -> Option<ActorEvent> {
        if runtime
            .bootstrap_task
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Some(ActorEvent::BootstrapFinished(
                runtime.bootstrap_task.as_mut().expect("checked").await,
            ));
        }
        if runtime
            .heartbeat_task
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Some(ActorEvent::HeartbeatFinished(
                runtime.heartbeat_task.as_mut().expect("checked").await,
            ));
        }
        if runtime
            .restore_task
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Some(ActorEvent::RestoreFinished(
                runtime.restore_task.as_mut().expect("checked").await,
            ));
        }
        if runtime
            .candidate_task
            .as_ref()
            .is_some_and(|task| task.handle.is_finished())
        {
            return Some(ActorEvent::CandidateFinished(
                (&mut runtime.candidate_task.as_mut().expect("checked").handle).await,
            ));
        }
        if runtime
            .live_task
            .as_ref()
            .is_some_and(|task| task.handle.is_finished())
        {
            return Some(ActorEvent::LiveFinished(
                (&mut runtime.live_task.as_mut().expect("checked").handle).await,
            ));
        }
        if runtime
            .replay_task
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Some(ActorEvent::ReplayFinished(
                runtime.replay_task.as_mut().expect("checked").await,
            ));
        }
        None
    }

    async fn wait_for_drain_event(&self, runtime: &mut ActorRuntime) -> ActorEvent {
        let deadline = runtime
            .draining
            .as_ref()
            .expect("draining state present")
            .deadline;
        tokio::select! {
            biased;
            _ = self.notify.notified() => ActorEvent::Notified,
            _ = tokio::time::sleep_until(deadline) => ActorEvent::DrainDeadline,
            result = async { (&mut runtime.live_task.as_mut().expect("live task started").handle).await }, if runtime.live_task.is_some() => ActorEvent::LiveFinished(result),
        }
    }

    async fn wait_for_active_event(&self, runtime: &mut ActorRuntime) -> ActorEvent {
        tokio::select! {
            biased;
            _ = self.notify.notified() => ActorEvent::Notified,
            _ = tokio::time::sleep_until(runtime.next_heartbeat) => ActorEvent::HeartbeatDue,
            result = async { runtime.bootstrap_task.as_mut().expect("guarded").await }, if runtime.bootstrap_task.is_some() => ActorEvent::BootstrapFinished(result),
            result = async { runtime.heartbeat_task.as_mut().expect("guarded").await }, if runtime.heartbeat_task.is_some() => ActorEvent::HeartbeatFinished(result),
            result = async { runtime.restore_task.as_mut().expect("guarded").await }, if runtime.restore_task.is_some() => ActorEvent::RestoreFinished(result),
            result = async { (&mut runtime.candidate_task.as_mut().expect("guarded").handle).await }, if runtime.candidate_task.is_some() => ActorEvent::CandidateFinished(result),
            result = async { (&mut runtime.live_task.as_mut().expect("guarded").handle).await }, if runtime.live_task.is_some() => ActorEvent::LiveFinished(result),
            result = async { runtime.replay_task.as_mut().expect("guarded").await }, if runtime.replay_task.is_some() => ActorEvent::ReplayFinished(result),
        }
    }

    async fn handle_event(&mut self, event: ActorEvent, runtime: &mut ActorRuntime) -> bool {
        match event {
            ActorEvent::Notified => {}
            ActorEvent::CandidateSuperseded => self.supersede_candidate(runtime),
            ActorEvent::HeartbeatDue => self.handle_heartbeat_due(runtime),
            ActorEvent::DrainDeadline => {}
            ActorEvent::BootstrapFinished(result) => {
                runtime.bootstrap_task = None;
                self.handle_bootstrap_completion(result, runtime);
            }
            ActorEvent::HeartbeatFinished(result) => {
                runtime.heartbeat_task = None;
                self.handle_heartbeat_completion(result, runtime);
            }
            ActorEvent::RestoreFinished(result) => {
                runtime.restore_task = None;
                self.handle_restore_completion(result);
            }
            ActorEvent::CandidateFinished(result) => {
                runtime.candidate_task = None;
                if let Ok((command, registration)) = result {
                    self.finish_candidate(command, registration, runtime);
                }
            }
            ActorEvent::LiveFinished(result) => self.handle_live_completion(result, runtime),
            ActorEvent::ReplayFinished(result) => {
                runtime.replay_task = None;
                runtime.replay_requested = match result {
                    Ok(has_more) => has_more,
                    Err(error) if !error.is_cancelled() => {
                        tracing::warn!(%error, "pending replay task failed");
                        true
                    }
                    Err(_) => true,
                };
                runtime.live_reports_since_replay = 0;
            }
        }
        self.finish_or_expire_drain(runtime).await
    }

    fn handle_heartbeat_due(&self, runtime: &mut ActorRuntime) {
        runtime.next_heartbeat = tokio::time::Instant::now() + self.heartbeat_interval;
        runtime.heartbeat_requested = true;
    }

    fn handle_bootstrap_completion(
        &self,
        result: Result<Result<(), String>, JoinError>,
        runtime: &mut ActorRuntime,
    ) {
        match result {
            Ok(Ok(())) => {
                runtime.registered = true;
                if !runtime.restore_started {
                    if let Some(command) = &runtime.bootstrap {
                        runtime.restore_started = true;
                        runtime.restore_task =
                            Some(self.spawn_restore(command.restore_versions.clone()));
                    }
                }
            }
            Ok(Err(error)) => tracing::warn!(%error, "manager bootstrap registration failed"),
            Err(error) if !error.is_cancelled() => {
                tracing::warn!(%error, "manager bootstrap task failed")
            }
            Err(_) => {}
        }
    }

    fn handle_heartbeat_completion(
        &self,
        result: Result<anyhow::Result<()>, JoinError>,
        runtime: &mut ActorRuntime,
    ) {
        match result {
            Ok(Ok(())) => {
                if !runtime.replay_requested {
                    runtime.live_reports_since_replay = 0;
                }
                runtime.replay_requested = true;
            }
            Ok(Err(error)) => {
                tracing::warn!(worker = %self.worker_id, %error, "heartbeat failed")
            }
            Err(error) if !error.is_cancelled() => tracing::warn!(%error, "heartbeat task failed"),
            Err(_) => {}
        }
    }

    fn handle_restore_completion(&self, result: Result<RestoreResult, JoinError>) {
        match result {
            Ok(result) => self.publish_restore(result),
            Err(error) if !error.is_cancelled() => tracing::warn!(%error, "restore task failed"),
            Err(_) => {}
        }
    }

    fn handle_live_completion(
        &mut self,
        result: Result<SequencedReport, JoinError>,
        runtime: &mut ActorRuntime,
    ) {
        let task = runtime
            .live_task
            .take()
            .expect("live completion without task");
        match result {
            Ok(report) => {
                self.mailbox.lock().release_in_flight(&report);
                assert!(
                    report.sequence > self.last_delivered_sequence,
                    "report mailbox violated global sequence order"
                );
                self.last_delivered_sequence = report.sequence;
                if runtime.draining.is_none() {
                    runtime.live_reports_since_replay += 1;
                }
            }
            Err(error) => {
                if !error.is_cancelled() {
                    if runtime.draining.is_some() {
                        tracing::warn!(%error, "live report delivery task failed during shutdown");
                    } else {
                        tracing::warn!(%error, "live report delivery task failed");
                    }
                }
                self.mailbox.lock().requeue_in_flight(task.report);
            }
        }
    }

    fn supersede_candidate(&self, runtime: &mut ActorRuntime) {
        if let Some(task) = runtime.candidate_task.take() {
            task.handle.abort();
            self.publish_reconfigure(ReconfigureResult {
                generation: task.generation,
                outcome: ReconfigureOutcome::Superseded,
            });
        }
    }

    fn start_bootstrap(&self, runtime: &mut ActorRuntime) {
        if runtime.bootstrap_task.is_some() {
            return;
        }
        let Some(command) = &runtime.bootstrap else {
            return;
        };
        let manager = Arc::clone(&self.manager);
        let registration = command.registration.clone();
        runtime.bootstrap_task = Some(tokio::spawn(async move {
            manager
                .register(&registration)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }));
    }

    fn spawn_restore(&self, captured_versions: HashMap<String, u64>) -> JoinHandle<RestoreResult> {
        let manager = Arc::clone(&self.manager);
        let worker_id = self.worker_id.clone();
        tokio::spawn(async move {
            let outcome = match manager.fetch_job_status(&worker_id).await {
                Ok(statuses) => RestoreOutcome::Success(statuses),
                Err(error) => RestoreOutcome::Failed(error.to_string()),
            };
            RestoreResult {
                captured_versions,
                outcome,
            }
        })
    }

    fn spawn_candidate(&self, command: ReconfigureCommand) -> CandidateTask {
        let generation = command.generation;
        let pending_limit = self.pending_limit;
        let handle = tokio::spawn(async move {
            let candidate = ManagerClient::new_with_pending_limit(
                command.bases.clone(),
                command.client.clone(),
                command.token.clone(),
                pending_limit,
            );
            let result = candidate
                .register(&command.registration)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
            (command, result)
        });
        CandidateTask { generation, handle }
    }

    fn finish_candidate(
        &self,
        command: ReconfigureCommand,
        registration: Result<(), String>,
        runtime: &mut ActorRuntime,
    ) {
        let generation = command.generation;
        if self.mailbox.lock().latest_reconfigure_generation != generation {
            self.publish_reconfigure(ReconfigureResult {
                generation,
                outcome: ReconfigureOutcome::Superseded,
            });
            return;
        }
        match registration {
            Ok(()) => {
                if let Some(task) = runtime.live_task.take() {
                    task.handle.abort();
                    self.mailbox.lock().requeue_in_flight(task.report);
                }
                abort_task(&mut runtime.replay_task);
                self.manager.set_registration(command.registration.clone());
                self.manager
                    .reconfigure(command.bases, command.client, command.token);
                self.publish_reconfigure(ReconfigureResult {
                    generation,
                    outcome: ReconfigureOutcome::Success,
                });
                self.mailbox.lock().requeue_latest_schedules();
                self.notify.notify_one();
            }
            Err(error) => self.publish_reconfigure(ReconfigureResult {
                generation,
                outcome: ReconfigureOutcome::Failed(error),
            }),
        }
    }

    fn spawn_delivery(&self, report: SequencedReport) -> LiveDeliveryTask {
        let manager = Arc::clone(&self.manager);
        let worker_id = self.worker_id.clone();
        let task_report = report.clone();
        let handle = tokio::spawn(async move {
            match &task_report.report {
                Report::Status(status) => {
                    if let Err(error) = manager.report_status(&worker_id, status).await {
                        tracing::warn!(mirror = %status.name, %error, "failed to report status to manager");
                    }
                }
                Report::Size { mirror, size } => {
                    if let Err(error) = manager.report_size(&worker_id, mirror, size).await {
                        tracing::warn!(mirror = %mirror, %error, "failed to report size to manager");
                    }
                }
                Report::Schedules(schedules) => {
                    if let Err(error) = manager.report_schedules_shared(&worker_id, schedules).await
                    {
                        tracing::warn!(%error, "failed to report schedules to manager");
                    }
                }
            }
            task_report
        });
        LiveDeliveryTask { report, handle }
    }

    fn publish_restore(&self, result: RestoreResult) {
        self.results.send_modify(|state| {
            let revision = state
                .restore
                .as_ref()
                .map_or(1, |result| result.revision + 1);
            state.restore = Some(RevisedResult {
                revision,
                value: result,
            });
        });
    }

    fn publish_reconfigure(&self, result: ReconfigureResult) {
        self.results.send_modify(|state| {
            let revision = state
                .reconfigure
                .as_ref()
                .map_or(1, |result| result.revision + 1);
            state.reconfigure = Some(RevisedResult {
                revision,
                value: result,
            });
        });
    }

    #[cfg(test)]
    fn with_shutdown_drain_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_drain_timeout = timeout;
        self
    }

    #[cfg(test)]
    fn with_start_ready_hook(mut self, hook: Arc<TestStartReadyHook>) -> Self {
        self.start_ready_hook = Some(hook);
        self
    }
}

fn abort_task<T>(task: &mut Option<JoinHandle<T>>) {
    if let Some(task) = task.take() {
        task.abort();
    }
}
