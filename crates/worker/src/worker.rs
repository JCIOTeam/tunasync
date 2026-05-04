//! Worker runtime.
//!
//! Mirrors Go's `worker/worker.go Run()` + `runSchedule()` + `registerWorker()`.
//!
//! Architecture:
//! ```text
//!  ┌──────────────────────────────────────────────────────────┐
//!  │  Worker                                                  │
//!  │                                                          │
//!  │  ┌──────────┐   status_tx   ┌────────────────────────┐  │
//!  │  │ MirrorJob │──────────────▶│  Scheduler loop        │  │
//!  │  └──────────┘               │  (report to manager,   │  │
//!  │       ▲                     │   enqueue next run)     │  │
//!  │       │ CtrlAction          └────────────────────────┘  │
//!  │       │                              ▲                   │
//!  │  ┌──────────┐   WorkerCmd            │                   │
//!  │  │ HTTP srv │────────────────────────┘                   │
//!  │  └──────────┘                                            │
//!  └──────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{error, info, warn};
use tunasync_protocol::{
    zero_time, CmdVerb, MirrorSchedule, MirrorSchedules, MirrorStatus, SyncStatus, WorkerCmd,
    WorkerStatus,
};

use crate::config::WorkerConfig;
use crate::hooks::JobHook;
use crate::http_server::{build_router, cmd_to_ctrl, WorkerHttpState};
use crate::job::{CtrlAction, JobMessage, JobState, MirrorJob};
use crate::manager_client::ManagerClient;
use crate::provider::MirrorProvider;
use crate::schedule::ScheduleQueue;

/// The worker: holds all jobs and coordinates the scheduler.
pub struct Worker {
    cfg: WorkerConfig,
    /// Original config file path — used by hot-reload to re-read from disk.
    config_path: PathBuf,
    jobs: HashMap<String, MirrorJob>,
    manager: Arc<ManagerClient>,
    #[allow(dead_code)] // cloned into job tasks; field not read directly
    status_tx: mpsc::Sender<JobMessage>,
    status_rx: mpsc::Receiver<JobMessage>,
    cmd_tx: mpsc::Sender<WorkerCmd>,
    cmd_rx: mpsc::Receiver<WorkerCmd>,
    #[allow(dead_code)] // permit count persists in Arc; field not read directly
    semaphore: Arc<Semaphore>,
    schedule: ScheduleQueue,
    mirror_statuses: HashMap<String, MirrorStatus>,
    /// Shared mirror name set — kept in sync with `self.jobs` so the HTTP
    /// handler can validate mirror_id before accepting a command.
    mirror_names: Arc<RwLock<HashSet<String>>>,
    /// Function to build a single mirror's provider+hooks pair.
    /// Used at startup and on hot-reload for new/modified mirrors.
    #[allow(clippy::type_complexity)]
    build_one_provider: fn(
        &crate::config::MirrorConfig,
        &WorkerConfig,
    ) -> anyhow::Result<(
        Box<dyn crate::provider::MirrorProvider>,
        Vec<Box<dyn crate::hooks::JobHook>>,
    )>,
}

impl Worker {
    /// Build a worker from config, using the supplied provider factory.
    ///
    /// `build_jobs` is a closure that receives the resolved config and returns
    /// `(provider, hooks)` pairs for every mirror. This keeps the worker struct
    /// independent of concrete provider types (which land in stage 4).
    pub fn new<F>(
        cfg: WorkerConfig,
        config_path: PathBuf,
        build_jobs: F,
        http_client: reqwest::Client,
    ) -> Self
    where
        F: Fn(&WorkerConfig) -> Vec<(Box<dyn MirrorProvider>, Vec<Box<dyn JobHook>>)>,
    {
        let concurrent = cfg.global.concurrent.max(1);
        let semaphore = Arc::new(Semaphore::new(concurrent));

        let (status_tx, status_rx) = mpsc::channel::<JobMessage>(128);
        let (cmd_tx, cmd_rx) = mpsc::channel::<WorkerCmd>(32);

        let bases = cfg
            .manager
            .api_base_list()
            .into_iter()
            .map(String::from)
            .collect();
        let manager = Arc::new(ManagerClient::new(bases, http_client));

        let provider_list = build_jobs(&cfg);
        let mut jobs = HashMap::new();
        let mut mirror_statuses = HashMap::new();

        for (provider, hooks) in provider_list {
            let name = provider.name().to_owned();
            let upstream = provider.upstream().to_owned();
            let is_master = provider.is_master();

            // Initial zero-value status.
            mirror_statuses.insert(
                name.clone(),
                MirrorStatus {
                    name: name.clone(),
                    worker: cfg.global.name.clone(),
                    is_master,
                    status: SyncStatus::None,
                    last_update: zero_time(),
                    last_started: zero_time(),
                    last_ended: zero_time(),
                    scheduled: zero_time(),
                    upstream,
                    size: String::new(),
                    error_msg: String::new(),
                },
            );

            let job = MirrorJob::spawn(provider, hooks, status_tx.clone(), Arc::clone(&semaphore));
            jobs.insert(name, job);
        }

        // Schedule queue is populated in restore_job_state() after startup,
        // using last_update + interval from the manager (matching Go's runSchedule).
        let schedule = ScheduleQueue::new();

        let mirror_names = Arc::new(RwLock::new(jobs.keys().cloned().collect()));

        Self {
            cfg,
            config_path,
            jobs,
            manager,
            status_tx,
            status_rx,
            cmd_tx,
            cmd_rx,
            semaphore,
            schedule,
            mirror_statuses,
            mirror_names,
            build_one_provider: crate::build_one_provider,
        }
    }

    /// Register with manager, start HTTP server, run scheduler loop.
    ///
    /// Returns only on fatal error or graceful shutdown (SIGTERM/SIGINT).
    pub async fn run(mut self) -> Result<()> {
        let worker_status = self.register_worker().await?;
        let worker_id = worker_status.id.clone();

        // Spawn HTTP server task.
        let http_state = WorkerHttpState {
            cmd_tx: self.cmd_tx.clone(),
            worker_name: worker_id.clone(),
            mirror_names: Arc::clone(&self.mirror_names),
        };
        let bind_addr = self.cfg.server.bind_addr();
        tokio::spawn(run_http_server(
            http_state,
            bind_addr,
            self.cfg.server.clone(),
        ));

        // Fetch persisted job status from manager and restore Paused/Disabled state.
        // Matches Go's `fetchJobStatus()` in `runSchedule`.
        self.restore_job_state(&worker_id).await;

        // Announce initial schedules to manager.
        self.report_schedules(&worker_id).await;

        // Spawn periodic heartbeat (every 60 s) to keep last_online fresh.
        let mgr = Arc::clone(&self.manager);
        let wid = worker_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                if let Err(e) = mgr.heartbeat(&wid).await {
                    warn!(worker = %wid, error = %e, "heartbeat failed");
                }
            }
        });

        // Set up Unix signal handlers.
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let cmd_tx = self.cmd_tx.clone();
            // SIGHUP → send Reload command into the worker's cmd channel.
            if let Ok(mut sighup) = signal(SignalKind::hangup()) {
                let tx = cmd_tx.clone();
                tokio::spawn(async move {
                    loop {
                        sighup.recv().await;
                        info!("received SIGHUP — triggering config reload");
                        let _ = tx.try_send(WorkerCmd {
                            cmd: CmdVerb::Reload,
                            mirror_id: String::new(),
                            args: vec![],
                            options: std::collections::HashMap::new(),
                        });
                    }
                });
            }
        }

        // Main scheduler loop (exits on SIGTERM/SIGINT).
        self.run_schedule(worker_id).await;

        Ok(())
    }

    /// Fetch persisted job status from manager and restore Paused/Disabled mirrors.
    ///
    /// Mirrors Go's `fetchJobStatus()` + initial schedule setup in `runSchedule()`:
    ///
    /// - Disabled → send Disable, remove from schedule
    /// - Paused   → send Stop, remove from schedule
    /// - All other known mirrors → schedule at `last_update + interval`
    ///   (if that time has passed, fires immediately; otherwise waits)
    /// - Mirrors not yet in manager (brand new) → schedule now (immediate)
    ///
    /// This is the ONLY place that populates the schedule queue on startup.
    async fn restore_job_state(&mut self, worker_id: &str) {
        // Collect all job names; we'll subtract the ones seen in manager response.
        let mut unseen: HashSet<String> = self.jobs.keys().cloned().collect();

        match self.manager.fetch_job_status(worker_id).await {
            Ok(statuses) => {
                for status in &statuses {
                    unseen.remove(&status.name);

                    // Update local mirror_status with persisted data.
                    if let Some(entry) = self.mirror_statuses.get_mut(&status.name) {
                        *entry = status.clone();
                    }

                    match status.status {
                        // Paused/Disabled mirrors stay paused — remove from schedule.
                        SyncStatus::Disabled => {
                            if let Some(job) = self.jobs.get(&status.name) {
                                job.try_send(CtrlAction::Disable);
                            }
                            self.schedule.remove(&status.name);
                            tracing::info!(mirror = %status.name, "restored Disabled state");
                            continue; // do not enqueue
                        }
                        SyncStatus::Paused => {
                            if let Some(job) = self.jobs.get(&status.name) {
                                job.try_send(CtrlAction::Stop);
                            }
                            self.schedule.remove(&status.name);
                            tracing::info!(mirror = %status.name, "restored Paused state");
                            continue; // do not enqueue
                        }
                        // Syncing/PreSyncing means the previous run was interrupted.
                        // Correct the stale status so the manager and UI don't show
                        // a phantom "syncing" state, then fall through to scheduling.
                        SyncStatus::Syncing | SyncStatus::PreSyncing => {
                            tracing::warn!(
                                mirror = %status.name,
                                status = %status.status,
                                "stale syncing status from previous run — correcting to Failed"
                            );
                            if let Some(entry) = self.mirror_statuses.get_mut(&status.name) {
                                entry.status = SyncStatus::Failed;
                                entry.error_msg = "previous sync was interrupted".into();
                                entry.last_ended = Utc::now();
                            }
                            // Report the corrected status to manager immediately.
                            if let Some(entry) = self.mirror_statuses.get(&status.name) {
                                if let Err(e) = self.manager.report_status(worker_id, entry).await {
                                    warn!(mirror = %status.name, error = %e, "failed to report corrected status");
                                }
                            }
                            // Fall through — schedule a re-sync below.
                        }
                        _ => {}
                    }

                    // Compute next_run = last_update + interval (matching Go's
                    // `stime := m.LastUpdate.Add(job.provider.Interval())`).
                    // If last_update is the zero time (mirror exists in manager but
                    // has never completed a sync), or if next_run is in the past,
                    // the job fires immediately.
                    let interval = self
                        .cfg
                        .mirrors
                        .iter()
                        .find(|m| m.name == status.name)
                        .map(|m| m.effective_interval(&self.cfg.global))
                        .unwrap_or_else(|| Duration::from_secs(3600));

                    let next_run = if tunasync_protocol::is_zero_time(&status.last_update) {
                        // Never synced before — start immediately.
                        Instant::now()
                    } else {
                        let next_utc = status.last_update
                            + chrono::Duration::from_std(interval)
                                .unwrap_or(chrono::Duration::seconds(3600));
                        let now_utc = Utc::now();
                        if next_utc <= now_utc {
                            Instant::now()
                        } else {
                            let delay = (next_utc - now_utc).to_std().unwrap_or(Duration::ZERO);
                            Instant::now() + delay
                        }
                    };

                    tracing::info!(
                        mirror = %status.name,
                        next_run_secs = next_run.saturating_duration_since(Instant::now()).as_secs(),
                        "scheduled (from last_update)"
                    );
                    self.schedule.push(status.name.clone(), next_run);
                }
                tracing::info!(mirrors = statuses.len(), "restored job states from manager");
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch job status from manager — all jobs start immediately");
                // Fall through: unseen still contains all job names, so they
                // all get scheduled now below.
            }
        }

        // Mirrors not found in manager (brand new, never registered) — schedule
        // for immediate run, matching Go's `w.schedule.AddJob(time.Now(), job)`.
        for name in &unseen {
            tracing::info!(mirror = %name, "new mirror (not in manager) — scheduling immediately");
            self.schedule.push(name.clone(), Instant::now());
        }
    }

    /// `POST /workers` — register with every configured manager.
    async fn register_worker(&self) -> Result<WorkerStatus> {
        let public_url = self.cfg.server.public_url(&self.cfg);
        let status = WorkerStatus {
            id: self.cfg.global.name.clone(),
            url: public_url,
            token: String::new(), // manager generates / stores the token
            last_online: zero_time(),
            last_register: zero_time(),
        };
        let mut registered = None;
        for attempt in 0..10 {
            match self.manager.register(&status).await {
                Ok(r) => {
                    registered = Some(r);
                    break;
                }
                Err(e) => {
                    warn!(attempt, error = %e, "registration attempt failed");
                    if attempt < 9 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        let worker_status = registered.context("failed to register after 10 attempts")?;
        info!(worker = %worker_status.id, "registered with manager");
        Ok(worker_status)
    }

    /// Send current schedule table to the manager.
    async fn report_schedules(&self, worker_id: &str) {
        let schedules: Vec<MirrorSchedule> = self
            .mirror_statuses
            .values()
            .map(|s| MirrorSchedule {
                mirror_name: s.name.clone(),
                next_schedule: s.scheduled,
            })
            .collect();

        if let Err(e) = self
            .manager
            .report_schedules(worker_id, &MirrorSchedules { schedules })
            .await
        {
            warn!(error = %e, "failed to report schedules to manager");
        }
    }

    /// Main scheduler loop — matches Go's `runSchedule`.
    /// Exits gracefully on SIGTERM or SIGINT.
    async fn run_schedule(&mut self, worker_id: String) {
        // Shutdown signal future (SIGTERM or SIGINT).
        #[cfg(unix)]
        let shutdown = {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
            // Return a future that resolves when either signal fires.
            async move {
                tokio::select! {
                    _ = sigterm.recv() => {},
                    _ = sigint.recv()  => {},
                }
            }
        };
        #[cfg(not(unix))]
        let mut shutdown = tokio::signal::ctrl_c();

        tokio::pin!(shutdown);

        loop {
            // Fire any jobs that are due.
            while let Some(entry) = self.schedule.peek() {
                if entry.next_run <= Instant::now() {
                    let entry = self.schedule.pop().unwrap();
                    if let Some(job) = self.jobs.get(&entry.name) {
                        job.try_send(CtrlAction::Start);
                    }
                } else {
                    break;
                }
            }

            // Compute sleep duration until next scheduled job.
            let sleep_dur = self
                .schedule
                .peek()
                .map(|e| e.next_run.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_secs(60));

            tokio::select! {
                // A job reported a status update.
                Some(msg) = self.status_rx.recv() => {
                    self.handle_job_message(msg, &worker_id).await;
                }

                // Manager/CLI sent us a command via HTTP.
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_worker_cmd(cmd).await;
                }

                // Time to check the schedule again.
                _ = tokio::time::sleep(sleep_dur) => {}

                // Graceful shutdown on SIGTERM / SIGINT.
                _ = &mut shutdown => {
                    info!("received shutdown signal — halting all jobs");
                    for job in self.jobs.values() {
                        job.try_send(CtrlAction::Halt);
                        job.kill();
                    }
                    // Brief grace period for jobs to finish (matches Go's
                    // WaitGroup approach — we wait a few seconds for tasks
                    // to drain their final status messages before exiting).
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    info!("shutdown complete");
                    return;
                }
            }
        }
    }

    /// Process a status update from a job task.
    async fn handle_job_message(&mut self, msg: JobMessage, worker_id: &str) {
        let status_entry = self
            .mirror_statuses
            .entry(msg.name.clone())
            .or_insert_with(|| MirrorStatus {
                name: msg.name.clone(),
                worker: worker_id.to_owned(),
                is_master: true,
                status: SyncStatus::None,
                last_update: zero_time(),
                last_started: zero_time(),
                last_ended: zero_time(),
                scheduled: zero_time(),
                upstream: String::new(),
                size: String::new(),
                error_msg: String::new(),
            });

        // Update local status — but skip overwriting status when the message
        // carries SyncStatus::None (used for scheduling-only updates after a
        // sync completes, where the real terminal status was already reported).
        if msg.status != SyncStatus::None {
            status_entry.status = msg.status;
        }
        status_entry.error_msg = msg.msg.clone();
        if !msg.size.is_empty() {
            status_entry.size = msg.size.clone();
        }

        // Report status to manager.
        let status_to_report = status_entry.clone();
        if let Err(e) = self
            .manager
            .report_status(worker_id, &status_to_report)
            .await
        {
            warn!(
                mirror = %msg.name,
                status = %msg.status,
                error = %e,
                "failed to report status to manager"
            );
        }

        // Report size separately when a sync succeeds and has a non-empty size.
        if msg.status == SyncStatus::Success && !msg.size.is_empty() {
            if let Err(e) = self
                .manager
                .report_size(worker_id, &msg.name, &msg.size)
                .await
            {
                warn!(mirror = %msg.name, error = %e, "failed to report size to manager");
            }
        }

        // Re-enqueue if this was a terminal status update with schedule=true.
        // Matches Go: `if jobMsg.schedule { schedTime := time.Now().Add(...) }`.
        // The schedule flag is set by the job task only when state == Ready,
        // so Stop/Disable during sync correctly prevents re-scheduling.
        if msg.schedule {
            if let Some(job_cfg) = self.cfg.mirrors.iter().find(|m| m.name == msg.name) {
                let interval = job_cfg.effective_interval(&self.cfg.global);
                let next_run = Instant::now() + interval;
                let next_dt = Utc::now() + chrono::Duration::from_std(interval).unwrap_or_default();
                self.schedule.push(msg.name.clone(), next_run);

                // Update scheduled time.
                if let Some(s) = self.mirror_statuses.get_mut(&msg.name) {
                    s.scheduled = next_dt;
                }

                // Report updated schedule to manager (matches Go: updateSchedInfo after every jobMessage with schedule=true).
                self.report_schedules(worker_id).await;
            }
        }
    }

    /// Dispatch an incoming `WorkerCmd` to the appropriate job.
    async fn handle_worker_cmd(&mut self, cmd: WorkerCmd) {
        use CmdVerb::*;
        match cmd.cmd {
            Reload => {
                tracing::info!("received Reload command — hot-reloading mirror config");
                self.handle_reload().await;
            }
            Ping => {
                for job in self.jobs.values() {
                    job.try_send(CtrlAction::Ping);
                }
            }
            _ => {
                // Mirror Go: "No matter what command, the existing job
                // schedule should be flushed" — always remove from schedule.
                if !cmd.mirror_id.is_empty() {
                    self.schedule.remove(&cmd.mirror_id);
                }

                if cmd.mirror_id.is_empty() {
                    for job in self.jobs.values() {
                        if let Some(action) = cmd_to_ctrl(&cmd) {
                            job.try_send(action);
                            // Kill running sync on Stop/Disable/Halt/Restart so
                            // it terminates promptly.
                            if matches!(
                                action,
                                CtrlAction::Stop
                                    | CtrlAction::Disable
                                    | CtrlAction::Halt
                                    | CtrlAction::Restart
                            ) {
                                job.kill();
                            }
                        }
                    }
                } else if let Some(job) = self.jobs.get(&cmd.mirror_id) {
                    if let Some(action) = cmd_to_ctrl(&cmd) {
                        // If the job task is dead (Disabled earlier), re-spawn it.
                        // Matches Go: `if job.State() == stateDisabled { go job.Run() }`.
                        if !job.is_alive()
                            && matches!(action, CtrlAction::Start | CtrlAction::Restart)
                        {
                            let name = &cmd.mirror_id;
                            if let Some(job_cfg) = self.cfg.mirrors.iter().find(|m| m.name == *name)
                            {
                                match (self.build_one_provider)(job_cfg, &self.cfg) {
                                    Ok((provider, hooks)) => {
                                        let new_job = MirrorJob::spawn(
                                            provider,
                                            hooks,
                                            self.status_tx.clone(),
                                            Arc::clone(&self.semaphore),
                                        );
                                        self.jobs.insert(name.clone(), new_job);
                                        // Fall through — send Start to the new job.
                                        if let Some(new_job) = self.jobs.get(name) {
                                            new_job.try_send(CtrlAction::Start);
                                        }
                                        // Schedule immediately.
                                        self.schedule.push(name.clone(), Instant::now());
                                        tracing::info!(
                                            mirror = %name,
                                            "re-enabled disabled job via Start/Restart"
                                        );
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            mirror = %name,
                                            error = %e,
                                            "failed to rebuild provider for re-enable"
                                        );
                                    }
                                }
                            }
                            return;
                        }

                        job.try_send(action);
                        if matches!(
                            action,
                            CtrlAction::Stop
                                | CtrlAction::Disable
                                | CtrlAction::Halt
                                | CtrlAction::Restart
                        ) {
                            job.kill();
                        }
                    }
                } else {
                    tracing::warn!(mirror = %cmd.mirror_id, "cmd for unknown mirror");
                }
            }
        }
    }

    /// Hot-reload mirror configuration from disk.
    ///
    /// Re-reads the config file, merges include files, then diffs against the
    /// current mirror list and applies Add / Modify / Delete operations.
    /// Mirrors Go's `Worker.ReloadMirrorConfig`.
    async fn handle_reload(&mut self) {
        use crate::diff_config::{diff_mirror_config, DiffOp};

        // Re-read the config file from disk.
        let mut new_cfg: crate::config::WorkerConfig = match tunasync_common::config::load_toml(
            &self.config_path,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "hot-reload: failed to read config — keeping current");
                return;
            }
        };

        // Merge include files and flatten nested mirror configs,
        // matching the startup path in lib.rs exactly.
        crate::load_include_mirrors(&mut new_cfg);
        new_cfg.mirrors = crate::config::flatten_mirrors(&new_cfg.mirrors_conf);

        let diff = diff_mirror_config(&self.cfg.mirrors, &new_cfg.mirrors);

        if diff.is_empty() {
            tracing::info!("hot-reload: config unchanged");
            return;
        }

        tracing::info!(changes = diff.len(), "hot-reload: applying config diff");

        for trans in &diff {
            let name = &trans.config.name;
            match trans.op {
                DiffOp::Delete => {
                    if let Some(job) = self.jobs.get(name) {
                        job.try_send(CtrlAction::Disable);
                        job.kill();
                    }
                    self.jobs.remove(name);
                    self.mirror_statuses.remove(name);
                    self.schedule.remove(name);
                    tracing::info!(mirror = %name, "hot-reload: deleted job");
                }
                DiffOp::Modify => {
                    // Remember the old job's state so we can preserve it.
                    let old_state = self.jobs.get(name).map(|j| j.state());

                    // Disable and remove the old job.
                    if let Some(job) = self.jobs.get(name) {
                        job.try_send(CtrlAction::Disable);
                        job.kill();
                    }
                    self.jobs.remove(name);
                    self.mirror_statuses.remove(name);
                    self.schedule.remove(name);

                    // Update config.
                    if let Some(pos) = self.cfg.mirrors.iter().position(|m| &m.name == name) {
                        self.cfg.mirrors[pos] = trans.config.clone();
                    } else {
                        self.cfg.mirrors.push(trans.config.clone());
                    }

                    // Build new provider + hooks and spawn a new MirrorJob.
                    match (self.build_one_provider)(&trans.config, &self.cfg) {
                        Ok((provider, hooks)) => {
                            let upstream = provider.upstream().to_owned();
                            let is_master = provider.is_master();

                            self.mirror_statuses.insert(
                                name.clone(),
                                MirrorStatus {
                                    name: name.clone(),
                                    worker: self.cfg.global.name.clone(),
                                    is_master,
                                    status: SyncStatus::None,
                                    last_update: zero_time(),
                                    last_started: zero_time(),
                                    last_ended: zero_time(),
                                    scheduled: zero_time(),
                                    upstream,
                                    size: String::new(),
                                    error_msg: String::new(),
                                },
                            );

                            let job = MirrorJob::spawn(
                                provider,
                                hooks,
                                self.status_tx.clone(),
                                Arc::clone(&self.semaphore),
                            );
                            self.jobs.insert(name.clone(), job);
                        }
                        Err(e) => {
                            tracing::error!(mirror = %name, error = %e, "hot-reload: failed to rebuild provider");
                            continue;
                        }
                    }

                    // Preserve the old job's state (matches Go's ReloadMirrorConfig
                    // which checks the previous state when re-spawning a modified job).
                    match old_state {
                        Some(JobState::Paused) => {
                            if let Some(job) = self.jobs.get(name) {
                                job.try_send(CtrlAction::Stop);
                            }
                            tracing::info!(mirror = %name, "hot-reload: modified job — kept Paused");
                        }
                        Some(JobState::Disabled) => {
                            if let Some(job) = self.jobs.get(name) {
                                job.try_send(CtrlAction::Disable);
                            }
                            tracing::info!(mirror = %name, "hot-reload: modified job — kept Disabled");
                        }
                        _ => {
                            // Ready/None — schedule for immediate sync.
                            tracing::info!(mirror = %name, "hot-reload: modified job — scheduling");
                            self.schedule.push(name.clone(), Instant::now());
                        }
                    }
                }
                DiffOp::Add => {
                    self.cfg.mirrors.push(trans.config.clone());

                    // Build provider + hooks and spawn new MirrorJob.
                    match (self.build_one_provider)(&trans.config, &self.cfg) {
                        Ok((provider, hooks)) => {
                            let upstream = provider.upstream().to_owned();
                            let is_master = provider.is_master();

                            self.mirror_statuses.insert(
                                name.clone(),
                                MirrorStatus {
                                    name: name.clone(),
                                    worker: self.cfg.global.name.clone(),
                                    is_master,
                                    status: SyncStatus::None,
                                    last_update: zero_time(),
                                    last_started: zero_time(),
                                    last_ended: zero_time(),
                                    scheduled: zero_time(),
                                    upstream,
                                    size: String::new(),
                                    error_msg: String::new(),
                                },
                            );

                            let job = MirrorJob::spawn(
                                provider,
                                hooks,
                                self.status_tx.clone(),
                                Arc::clone(&self.semaphore),
                            );
                            self.jobs.insert(name.clone(), job);
                        }
                        Err(e) => {
                            tracing::error!(mirror = %name, error = %e, "hot-reload: failed to build new provider");
                            continue;
                        }
                    }

                    tracing::info!(mirror = %name, "hot-reload: new job");
                    self.schedule.push(name.clone(), std::time::Instant::now());
                }
            }
        }

        // Update global config (interval/retry defaults etc.) from new file.
        self.cfg.global = new_cfg.global;
        self.cfg.manager = new_cfg.manager;

        // Keep mirror_names in sync so the HTTP handler can validate new mirrors.
        {
            let mut names = self.mirror_names.write().await;
            names.clear();
            for name in self.jobs.keys() {
                names.insert(name.clone());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP server task
// ---------------------------------------------------------------------------

async fn run_http_server(
    state: WorkerHttpState,
    bind_addr: std::net::SocketAddr,
    server_cfg: crate::config::ServerConfig,
) {
    let router = build_router(state);

    if server_cfg.tls_enabled() {
        info!(%bind_addr, "worker binding HTTPS listener");
        if let Err(e) = axum_server::bind_rustls(
            bind_addr,
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &server_cfg.ssl_cert,
                &server_cfg.ssl_key,
            )
            .await
            .expect("load worker TLS cert/key"),
        )
        .serve(router.into_make_service())
        .await
        {
            error!(error = %e, "worker HTTPS server error");
        }
    } else {
        info!(%bind_addr, "worker binding HTTP listener");
        let listener = TcpListener::bind(bind_addr)
            .await
            .expect("bind worker HTTP listener");
        if let Err(e) = axum::serve(listener, router).await {
            error!(error = %e, "worker HTTP server error");
        }
    }
}
