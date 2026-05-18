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

use crate::priority_semaphore::PrioritySemaphore;
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

/// Extract the hostname from an upstream URL for per-upstream concurrency keying.
///
/// Handles:
/// - `rsync://host/module` or `http://host/path` → URL parse → host_str
/// - rsync daemon syntax `host::module` → split on `::`
/// - bare paths or anything else → returns `None` (no per-host limit applied)
fn upstream_host(upstream: &str) -> Option<String> {
    if upstream.contains("://") {
        url::Url::parse(upstream)
            .ok()
            .and_then(|u| u.host_str().map(str::to_owned))
    } else if upstream.contains("::") {
        upstream.split("::").next().map(str::to_owned)
    } else {
        None
    }
}

/// Compute the next `Instant` a mirror should run.
///
/// `cached_cron` is the precomputed `cron::Schedule` for this mirror, if any
/// — built once at startup so we don't re-parse the TOML string on every
/// scheduler tick.
fn next_run_for(
    mc: &crate::config::MirrorConfig,
    global: &crate::config::GlobalConfig,
    cached_cron: Option<&cron::Schedule>,
) -> (Instant, chrono::DateTime<chrono::Utc>) {
    let now_instant = Instant::now();
    let now_utc = chrono::Utc::now();
    let interval = mc.effective_interval(global);

    if let Some(sched) = cached_cron {
        // Interpret the cron expression in the mirror's configured timezone.
        // `sched.upcoming(tz)` returns DateTime<Tz>; we convert to UTC for the
        // returned schedule timestamp and convert the delta to an Instant.
        //
        // Why this matters: a cron of "0 3 * * *" in Asia/Shanghai must fire
        // at 03:00 CST = 19:00 UTC the previous day, not at 03:00 UTC. The
        // global default is UTC so existing UTC-based configs keep working.
        let tz_name = mc.effective_timezone(global);
        // tz_name has been validated at config load — parse error here would
        // mean someone hot-reloaded with a bad value past startup validation;
        // fall back to UTC defensively rather than panicking.
        let tz: chrono_tz::Tz = tz_name.parse().unwrap_or(chrono_tz::UTC);
        if let Some(next_in_tz) = sched.upcoming(tz).next() {
            let next_utc = next_in_tz.with_timezone(&chrono::Utc);
            let delta = (next_utc - now_utc).to_std().unwrap_or(interval);
            return (now_instant + delta, next_utc);
        }
    }
    let next_dt = now_utc + chrono::Duration::from_std(interval).unwrap_or_default();
    (now_instant + interval, next_dt)
}

/// Build the per-mirror blackout-windows cache from config.
///
/// Done once at startup (and on hot-reload) so the scheduler hot path doesn't
/// re-parse TOML strings on every tick.
fn build_blackout_cache(
    mirrors: &[crate::config::MirrorConfig],
) -> HashMap<String, Vec<crate::blackout::BlackoutWindow>> {
    mirrors
        .iter()
        .filter(|mc| !mc.blackout.is_empty())
        .map(|mc| {
            (
                mc.name.clone(),
                crate::blackout::parse_blackout_windows(&mc.blackout),
            )
        })
        .collect()
}

/// Build the per-mirror precomputed `cron::Schedule` cache.
///
/// Invalid expressions are silently skipped here — the canonical validation
/// happens at startup in `lib.rs::run`, which fails the worker outright on a
/// bad cron string. This function is defensive in case hot-reload introduces
/// a bad expression mid-flight; the scheduler then falls back to interval
/// for that mirror until the next reload fixes the config.
fn build_cron_cache(mirrors: &[crate::config::MirrorConfig]) -> HashMap<String, cron::Schedule> {
    mirrors
        .iter()
        .filter(|mc| !mc.cron.is_empty())
        .filter_map(|mc| {
            parse_cron_lenient(&mc.cron)
                .ok()
                .map(|s| (mc.name.clone(), s))
        })
        .collect()
}

/// Build the per-mirror IANA timezone cache.
///
/// Every mirror has an effective timezone (per-mirror → global → UTC default).
/// We resolve and parse it once here so the scheduler hot path can convert
/// `Utc::now()` to the mirror's local time without re-parsing the string each
/// tick. Invalid names fall back to UTC defensively — the canonical
/// validation happens at startup in `lib.rs::run`, which fails the worker
/// outright on a bad timezone name (so this branch should be unreachable in
/// practice except after a hot-reload with a bad value).
fn build_timezone_cache(
    mirrors: &[crate::config::MirrorConfig],
    global: &crate::config::GlobalConfig,
) -> HashMap<String, chrono_tz::Tz> {
    mirrors
        .iter()
        .map(|mc| {
            let name = mc.effective_timezone(global);
            let tz: chrono_tz::Tz = name.parse().unwrap_or_else(|_| {
                tracing::warn!(
                    mirror = %mc.name,
                    timezone = %name,
                    "invalid timezone after hot-reload; falling back to UTC"
                );
                chrono_tz::UTC
            });
            (mc.name.clone(), tz)
        })
        .collect()
}

/// Parse a cron expression accepting both 5-field POSIX format
/// (`minute hour dom month dow`) and the cron-crate native 6/7-field format
/// (`sec minute hour dom month dow [year]`).
///
/// The `cron` crate (0.12) requires 6 or 7 fields. Operators are far more
/// familiar with the classic 5-field syntax used by crontab(5), Kubernetes,
/// systemd timers, and just about every other scheduler — so accepting both
/// is essential. A 5-field expression is widened by prepending `"0 "` (run
/// at second 0 of the matching minute).
///
/// Returns the same error type the `cron` crate uses, so callers can render
/// it verbatim in startup error messages.
pub(crate) fn parse_cron_lenient(expr: &str) -> Result<cron::Schedule, cron::error::Error> {
    use std::str::FromStr;
    let trimmed = expr.trim();
    let field_count = trimmed.split_ascii_whitespace().count();
    if field_count == 5 {
        // Widen to 6 fields by prepending "0 " for the seconds field.
        let widened = format!("0 {trimmed}");
        cron::Schedule::from_str(&widened)
    } else {
        cron::Schedule::from_str(trimmed)
    }
}

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
    semaphore: Arc<PrioritySemaphore>,
    /// Per-upstream-host concurrency semaphores.
    /// Built from `GlobalConfig::per_upstream_concurrent` at startup.
    /// Each job acquires both the global semaphore and its host semaphore (if
    /// any) before starting, in that order, to avoid deadlock.
    per_upstream_semaphores: HashMap<String, Arc<Semaphore>>,
    schedule: ScheduleQueue,
    mirror_statuses: HashMap<String, MirrorStatus>,
    /// Shared mirror name set — kept in sync with `self.jobs` so the HTTP
    /// handler can validate mirror_id before accepting a command.
    mirror_names: Arc<RwLock<HashSet<String>>>,
    /// Precomputed blackout windows per mirror name. Built once at startup
    /// and rebuilt on hot-reload. Cached because the scheduler hot path
    /// (every pop) would otherwise re-parse the TOML string list every tick.
    blackouts_by_mirror: HashMap<String, Vec<crate::blackout::BlackoutWindow>>,
    /// Precomputed cron::Schedule per mirror name (only for mirrors with a
    /// non-empty `cron` field). Cached because `cron::Schedule::parse` is
    /// non-trivial and the scheduler computes next_run frequently.
    crons_by_mirror: HashMap<String, cron::Schedule>,
    /// Precomputed IANA timezone per mirror name (every mirror has one,
    /// defaulting to UTC). Cached so the scheduler doesn't reparse the
    /// timezone string on every tick when evaluating blackout windows
    /// and cron schedules. Built once at startup and rebuilt on hot-reload.
    timezones_by_mirror: HashMap<String, chrono_tz::Tz>,
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
        let semaphore = Arc::new(PrioritySemaphore::new(concurrent));

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

        // Build per-upstream semaphores from config.
        let per_upstream_semaphores: HashMap<String, Arc<Semaphore>> = cfg
            .global
            .per_upstream_concurrent
            .iter()
            .map(|(host, &limit)| (host.clone(), Arc::new(Semaphore::new(limit.max(1)))))
            .collect();

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
                    upstream: upstream.clone(),
                    ..Default::default()
                },
            );

            // Look up the per-upstream semaphore for this provider's host.
            let upstream_sem =
                upstream_host(&upstream).and_then(|h| per_upstream_semaphores.get(&h).cloned());
            // Look up configured priority for this mirror (default 50).
            let priority = cfg
                .mirrors
                .iter()
                .find(|m| m.name == name)
                .map(|m| m.priority)
                .unwrap_or(50);

            let job = MirrorJob::spawn(
                provider,
                hooks,
                status_tx.clone(),
                Arc::clone(&semaphore),
                upstream_sem,
                priority,
            );
            jobs.insert(name, job);
        }

        // Schedule queue is populated in restore_job_state() after startup,
        // using last_update + interval from the manager (matching Go's runSchedule).
        let schedule = ScheduleQueue::new();

        let mirror_names = Arc::new(RwLock::new(jobs.keys().cloned().collect()));

        // Precompute blackout, cron and timezone caches.
        let blackouts_by_mirror = build_blackout_cache(&cfg.mirrors);
        let crons_by_mirror = build_cron_cache(&cfg.mirrors);
        let timezones_by_mirror = build_timezone_cache(&cfg.mirrors, &cfg.global);

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
            per_upstream_semaphores,
            schedule,
            mirror_statuses,
            mirror_names,
            blackouts_by_mirror,
            crons_by_mirror,
            timezones_by_mirror,
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

                    // Compute next_run. When the mirror has a cron expression it
                    // takes precedence over `last_update + interval`. Otherwise
                    // next_run = last_update + interval — matching Go's
                    // `stime := m.LastUpdate.Add(job.provider.Interval())`.
                    // If last_update is zero (never synced) or next_run is in the
                    // past the job fires immediately.
                    let job_cfg = self.cfg.mirrors.iter().find(|m| m.name == status.name);

                    let next_run = if let Some(mc) = job_cfg {
                        if !mc.cron.is_empty() {
                            // Cron: ignore last_update and compute the next tick.
                            let cached = self.crons_by_mirror.get(&mc.name);
                            let (inst, _) = next_run_for(mc, &self.cfg.global, cached);
                            inst
                        } else {
                            // Interval: last_update + interval, clamped to now.
                            let interval = mc.effective_interval(&self.cfg.global);
                            if tunasync_protocol::is_zero_time(&status.last_update) {
                                Instant::now()
                            } else {
                                let next_utc = status.last_update
                                    + chrono::Duration::from_std(interval)
                                        .unwrap_or(chrono::Duration::seconds(3600));
                                let now_utc = Utc::now();
                                if next_utc <= now_utc {
                                    Instant::now()
                                } else {
                                    let delay =
                                        (next_utc - now_utc).to_std().unwrap_or(Duration::ZERO);
                                    Instant::now() + delay
                                }
                            }
                        }
                    } else {
                        Instant::now()
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

                    // Blackout check: if this mirror is inside a blackout window,
                    // push it back by 5 minutes instead of starting it.
                    // In-progress syncs are never interrupted — only new starts are gated.
                    // Uses precomputed caches so we don't re-parse the TOML
                    // strings or timezone name on every scheduler tick.
                    //
                    // The blackout windows are interpreted in the mirror's
                    // effective timezone (`MirrorConfig::timezone` or
                    // `GlobalConfig::timezone`, falling back to UTC). We
                    // convert `Utc::now()` to that timezone before passing
                    // it to `is_active_at`, which is itself generic over Tz.
                    let in_blackout = self
                        .blackouts_by_mirror
                        .get(&entry.name)
                        .map(|windows| {
                            let tz = self
                                .timezones_by_mirror
                                .get(&entry.name)
                                .copied()
                                .unwrap_or(chrono_tz::UTC);
                            let now_local = chrono::Utc::now().with_timezone(&tz);
                            crate::blackout::is_in_blackout(windows, &now_local)
                        })
                        .unwrap_or(false);

                    if in_blackout {
                        let retry_at = Instant::now() + Duration::from_secs(300);
                        tracing::info!(
                            mirror = %entry.name,
                            "in blackout window — deferring sync by 5 minutes"
                        );
                        self.schedule.push(entry.name, retry_at);
                    } else if let Some(job) = self.jobs.get(&entry.name) {
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
                ..Default::default()
            });

        // Update local status — but skip overwriting status and error_msg when
        // the message carries SyncStatus::None. None messages are scheduling-only
        // updates sent by run_job_task after a sync completes; the real terminal
        // status (and error message) was already reported by the preceding
        // Failed/Success message. Overwriting error_msg here would clear the
        // "sync timed out" or hook-failure reason shown in the manager UI.
        if msg.status != SyncStatus::None {
            status_entry.status = msg.status;
            status_entry.error_msg = msg.msg.clone();
        }
        if !msg.size.is_empty() {
            status_entry.size = msg.size.clone();
        }
        if msg.transferred_bytes > 0 {
            status_entry.last_transferred_bytes = msg.transferred_bytes;
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
                let cached_cron = self.crons_by_mirror.get(&msg.name);
                let (next_run, next_dt) = next_run_for(job_cfg, &self.cfg.global, cached_cron);
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
                                        let upstream_sem = upstream_host(provider.upstream())
                                            .and_then(|h| {
                                                self.per_upstream_semaphores.get(&h).cloned()
                                            });
                                        let new_job = MirrorJob::spawn(
                                            provider,
                                            hooks,
                                            self.status_tx.clone(),
                                            Arc::clone(&self.semaphore),
                                            upstream_sem,
                                            job_cfg.priority,
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
                                    upstream,
                                    ..Default::default()
                                },
                            );

                            let upstream_sem2 = upstream_host(provider.upstream())
                                .and_then(|h| self.per_upstream_semaphores.get(&h).cloned());
                            let job = MirrorJob::spawn(
                                provider,
                                hooks,
                                self.status_tx.clone(),
                                Arc::clone(&self.semaphore),
                                upstream_sem2,
                                trans.config.priority,
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
                                    upstream,
                                    ..Default::default()
                                },
                            );

                            let upstream_sem3 = upstream_host(provider.upstream())
                                .and_then(|h| self.per_upstream_semaphores.get(&h).cloned());
                            let job = MirrorJob::spawn(
                                provider,
                                hooks,
                                self.status_tx.clone(),
                                Arc::clone(&self.semaphore),
                                upstream_sem3,
                                trans.config.priority,
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

        // Apply global concurrency changes. tokio's Semaphore exposes
        // `add_permits` for grow-only changes but no `remove_permits` (you
        // can't take back permits that may be currently held by running
        // syncs), so we can grow live but not shrink. Shrinks require a
        // worker restart — warn so the operator isn't surprised.
        let old_concurrent = self.cfg.global.concurrent.max(1);
        let new_concurrent = new_cfg.global.concurrent.max(1);
        if new_concurrent > old_concurrent {
            let added = new_concurrent - old_concurrent;
            self.semaphore.add_permits(added);
            tracing::info!(
                from = old_concurrent,
                to = new_concurrent,
                added,
                "hot-reload: increased concurrency limit"
            );
        } else if new_concurrent < old_concurrent {
            tracing::warn!(
                from = old_concurrent,
                to = new_concurrent,
                "hot-reload: cannot shrink concurrency limit on a running worker — \
                 keeping {old_concurrent} until restart"
            );
        }

        // Apply per-upstream concurrency changes. Mirrors per-mirror
        // semantics of the global semaphore reload above:
        //
        // * New host         → insert a new Semaphore. Visible to subsequent
        //                      MirrorJob::spawn() calls (including any
        //                      Modify or Add transitions in this same diff).
        // * Removed host     → drop the map entry. Currently-running jobs
        //                      that hold a clone of the old Arc continue to
        //                      use it until they finish — we cannot revoke a
        //                      permit that is in active use. Subsequent
        //                      respawns of those jobs (Modify, restart) will
        //                      pick up None and run unconstrained.
        // * Limit increased  → add_permits on the existing Arc, so already-
        //                      spawned jobs that share the Arc see the new
        //                      ceiling immediately.
        // * Limit decreased  → warn. tokio's Semaphore has no
        //                      `remove_permits` analogue, and silently
        //                      revoking a permit held by a running sync
        //                      would deadlock that sync. Operator must
        //                      restart the worker to shrink.
        //
        // Important: read the OLD limits from `self.cfg.global` *before*
        // we overwrite it with `new_cfg.global` lower down.
        {
            let old_limits = &self.cfg.global.per_upstream_concurrent;
            let new_limits = &new_cfg.global.per_upstream_concurrent;

            // Removed hosts.
            let removed: Vec<String> = old_limits
                .keys()
                .filter(|h| !new_limits.contains_key(*h))
                .cloned()
                .collect();
            for host in &removed {
                self.per_upstream_semaphores.remove(host);
                tracing::info!(
                    host = %host,
                    "hot-reload: removed per-upstream concurrency limit \
                     (running jobs keep the old limit until they finish or restart)"
                );
            }

            // New / modified hosts.
            for (host, &new_limit) in new_limits {
                let new_limit = new_limit.max(1);
                match old_limits.get(host) {
                    None => {
                        self.per_upstream_semaphores
                            .insert(host.clone(), Arc::new(Semaphore::new(new_limit)));
                        tracing::info!(
                            host = %host,
                            limit = new_limit,
                            "hot-reload: added per-upstream concurrency limit \
                             (existing jobs for this host are not retroactively constrained)"
                        );
                    }
                    Some(&old) => {
                        let old_limit = old.max(1);
                        if new_limit > old_limit {
                            let added = new_limit - old_limit;
                            if let Some(sem) = self.per_upstream_semaphores.get(host) {
                                sem.add_permits(added);
                            }
                            tracing::info!(
                                host = %host,
                                from = old_limit,
                                to = new_limit,
                                added,
                                "hot-reload: grew per-upstream concurrency limit"
                            );
                        } else if new_limit < old_limit {
                            tracing::warn!(
                                host = %host,
                                from = old_limit,
                                to = new_limit,
                                "hot-reload: cannot shrink per-upstream concurrency limit \
                                 on a running worker — keeping {old_limit} until restart"
                            );
                        }
                    }
                }
            }
        }

        // Detect manager-list changes. The ManagerClient was built at startup
        // with a fixed list of base URLs (it does not currently support
        // runtime swap), so we cannot honour this change without a restart.
        // At least tell the operator instead of silently doing nothing.
        if new_cfg.manager.api_base_list() != self.cfg.manager.api_base_list() {
            tracing::warn!(
                "hot-reload: manager URL list changed in config but the running \
                 worker still uses the old list — restart the worker to apply"
            );
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

        // Rebuild the precomputed blackout, cron and timezone caches against
        // the new mirror list so scheduler decisions reflect the hot-reloaded
        // config.
        self.blackouts_by_mirror = build_blackout_cache(&self.cfg.mirrors);
        self.crons_by_mirror = build_cron_cache(&self.cfg.mirrors);
        self.timezones_by_mirror = build_timezone_cache(&self.cfg.mirrors, &self.cfg.global);
    }
}

// HTTP server task

async fn run_http_server(
    state: WorkerHttpState,
    bind_addr: std::net::SocketAddr,
    server_cfg: crate::config::ServerConfig,
) {
    let router = build_router(state);

    if server_cfg.tls_enabled() {
        info!(%bind_addr, "worker binding HTTPS listener");
        let tls_config = match axum_server::tls_rustls::RustlsConfig::from_pem_file(
            &server_cfg.ssl_cert,
            &server_cfg.ssl_key,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    cert = %server_cfg.ssl_cert,
                    key = %server_cfg.ssl_key,
                    error = %e,
                    "failed to load worker TLS cert/key — HTTP server not started; \
                     the worker will run blind to manager commands"
                );
                return;
            }
        };
        if let Err(e) = axum_server::bind_rustls(bind_addr, tls_config)
            .serve(router.into_make_service())
            .await
        {
            error!(error = %e, "worker HTTPS server error");
        }
    } else {
        info!(%bind_addr, "worker binding HTTP listener");
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(
                    %bind_addr,
                    error = %e,
                    "failed to bind worker HTTP listener — HTTP server not started; \
                     the worker will run blind to manager commands"
                );
                return;
            }
        };
        if let Err(e) = axum::serve(listener, router).await {
            error!(error = %e, "worker HTTP server error");
        }
    }
}

#[cfg(test)]
mod cron_schedule_tests {
    //! Unit tests for `next_run_for` — cron vs interval scheduling.

    use std::time::Duration;

    use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};

    fn mirror_with_cron(expr: &str) -> (MirrorConfig, GlobalConfig) {
        let global = GlobalConfig::default();
        let mc = MirrorConfig {
            name: "cron-test".into(),
            provider: ProviderKind::Rsync,
            upstream: "rsync://localhost/test/".into(),
            cron: expr.to_owned(),
            ..MirrorConfig::default()
        };
        (mc, global)
    }

    fn mirror_with_interval(secs: u64) -> (MirrorConfig, GlobalConfig) {
        let global = GlobalConfig {
            interval: secs,
            ..GlobalConfig::default()
        };
        let mc = MirrorConfig {
            name: "interval-test".into(),
            provider: ProviderKind::Rsync,
            upstream: "rsync://localhost/test/".into(),
            // leave cron empty → use interval
            ..MirrorConfig::default()
        };
        (mc, global)
    }

    /// A cron expression `0 3 * * *` (daily at 03:00, POSIX 5-field) must
    /// produce a `next_run` that's strictly in the future, and the accompanying
    /// DateTime must be at exactly 03:00 — not some interval-fallback time.
    #[test]
    fn cron_expression_produces_future_next_run() {
        let (mc, global) = mirror_with_cron("0 3 * * *");
        let now = std::time::Instant::now();
        let cached = super::parse_cron_lenient(&mc.cron).ok();
        assert!(
            cached.is_some(),
            "parse_cron_lenient must accept POSIX 5-field syntax"
        );
        let (next_run, next_dt) = super::next_run_for(&mc, &global, cached.as_ref());

        assert!(next_run > now, "next_run must be in the future");

        let delta = next_dt - chrono::Utc::now();
        assert!(
            delta > chrono::Duration::zero(),
            "next_dt must be in the future"
        );
        assert!(
            delta <= chrono::Duration::hours(25),
            "daily cron next_dt should be ≤ 25h away, got {delta}"
        );
        // Verify it's actually 03:00 UTC, not some interval-fallback value.
        // Allowing seconds=0..1 because the cron crate may produce 03:00:00.
        use chrono::Timelike;
        assert_eq!(
            next_dt.hour(),
            3,
            "next_dt hour must be 03 (the cron time), got {next_dt}"
        );
        assert_eq!(next_dt.minute(), 0, "minute must be 0");
    }

    /// Without a cron expression, next_run must be approximately
    /// `now + interval` (within a 2-second tolerance for test execution time).
    #[test]
    fn interval_fallback_produces_now_plus_interval() {
        // GlobalConfig::interval is in minutes.
        let interval_mins = 60u64;
        let interval_secs = interval_mins * 60;
        let (mc, global) = mirror_with_interval(interval_mins);
        let now = std::time::Instant::now();
        let (next_run, _) = super::next_run_for(&mc, &global, None);

        let expected_min = now + Duration::from_secs(interval_secs - 2);
        let expected_max = now + Duration::from_secs(interval_secs + 2);
        assert!(
            next_run >= expected_min && next_run <= expected_max,
            "interval-based next_run should be ≈ now + 1h"
        );
    }

    /// An invalid cron expression falls back to interval scheduling without
    /// panicking. When passed `None` for cached_cron — which is exactly what
    /// `build_cron_cache` does for unparseable expressions — the helper falls
    /// back to interval. The validation pass in lib.rs catches bad crons at
    /// startup so this path is only reached if the cache is bypassed.
    #[test]
    fn invalid_cron_falls_back_to_interval() {
        let (mut mc, global) = mirror_with_interval(60);
        mc.cron = "not a cron expression".into();
        let now = std::time::Instant::now();
        let cached = super::parse_cron_lenient(&mc.cron).ok();
        let (next_run, _) = super::next_run_for(&mc, &global, cached.as_ref());
        // Should still produce a valid future instant, not panic.
        assert!(next_run > now);
    }

    /// build_cron_cache: parseable expressions get cached, unparseable are skipped.
    #[test]
    fn build_cron_cache_filters_unparseable() {
        use crate::config::MirrorConfig;
        let mirrors = vec![
            MirrorConfig {
                name: "ok".into(),
                cron: "0 3 * * *".into(),
                ..MirrorConfig::default()
            },
            MirrorConfig {
                name: "bad".into(),
                cron: "garbage".into(),
                ..MirrorConfig::default()
            },
            MirrorConfig {
                name: "empty".into(),
                cron: String::new(),
                ..MirrorConfig::default()
            },
        ];
        let cache = super::build_cron_cache(&mirrors);
        assert!(
            cache.contains_key("ok"),
            "expected 'ok' in cache, got {:?}",
            cache.keys().collect::<Vec<_>>()
        );
        assert!(!cache.contains_key("bad"));
        assert!(!cache.contains_key("empty"));
        assert!(!cache.contains_key("bad"));
        assert!(!cache.contains_key("empty"));
    }

    /// build_blackout_cache: only mirrors with non-empty blackout get an entry.
    #[test]
    fn build_blackout_cache_skips_empty() {
        use crate::config::MirrorConfig;
        let mirrors = vec![
            MirrorConfig {
                name: "has".into(),
                blackout: vec!["08:00-18:00 Mon-Fri".into()],
                ..MirrorConfig::default()
            },
            MirrorConfig {
                name: "none".into(),
                blackout: vec![],
                ..MirrorConfig::default()
            },
        ];
        let cache = super::build_blackout_cache(&mirrors);
        assert!(cache.contains_key("has"));
        assert_eq!(cache.get("has").unwrap().len(), 1);
        assert!(!cache.contains_key("none"));
    }

    /// parse_cron_lenient: 5-field POSIX must work (documented user format).
    #[test]
    fn parse_cron_lenient_accepts_5_field_posix() {
        assert!(super::parse_cron_lenient("0 3 * * *").is_ok());
        assert!(super::parse_cron_lenient("*/15 * * * *").is_ok());
        assert!(super::parse_cron_lenient("30 22 * * 1-5").is_ok());
    }

    /// parse_cron_lenient: native 6-field cron-crate syntax must still work.
    #[test]
    fn parse_cron_lenient_accepts_6_field_native() {
        assert!(super::parse_cron_lenient("0 0 3 * * *").is_ok());
        assert!(super::parse_cron_lenient("0 */15 * * * *").is_ok());
    }

    /// parse_cron_lenient: garbage rejected.
    #[test]
    fn parse_cron_lenient_rejects_garbage() {
        assert!(super::parse_cron_lenient("not a cron").is_err());
        assert!(super::parse_cron_lenient("99 99 99 99 99").is_err());
    }

    /// Direct unit test for the per-upstream-semaphore diff logic.
    /// We don't run a full Worker — we just exercise the same algorithm
    /// against synthetic old/new maps to ensure the four cases
    /// (added / removed / grown / shrunk) take the right code path.
    #[test]
    fn per_upstream_hot_reload_diff_logic() {
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        // Initial state: kernel.org=2, debian.org=3.
        let mut sems: HashMap<String, Arc<Semaphore>> = HashMap::new();
        sems.insert("kernel.org".into(), Arc::new(Semaphore::new(2)));
        sems.insert("debian.org".into(), Arc::new(Semaphore::new(3)));
        let mut old_limits: HashMap<String, usize> = HashMap::new();
        old_limits.insert("kernel.org".into(), 2);
        old_limits.insert("debian.org".into(), 3);

        // New config:
        // - kernel.org grew 2 → 5
        // - debian.org SHRUNK 3 → 1 (must NOT actually shrink; warn instead)
        // - fedora.org is new (limit 4)
        // - ubuntu.com (not in old map, hence absent in `removed`)
        // - apache.org was never in either map (no change)
        // - the absence of any new entry for an old key triggers Remove
        let mut new_limits: HashMap<String, usize> = HashMap::new();
        new_limits.insert("kernel.org".into(), 5);
        new_limits.insert("debian.org".into(), 1);
        new_limits.insert("fedora.org".into(), 4);

        // Apply the diff (same shape as the hot-reload code).
        let removed: Vec<String> = old_limits
            .keys()
            .filter(|h| !new_limits.contains_key(*h))
            .cloned()
            .collect();
        for host in &removed {
            sems.remove(host);
        }
        for (host, &new_limit) in &new_limits {
            let new_limit = new_limit.max(1);
            match old_limits.get(host) {
                None => {
                    sems.insert(host.clone(), Arc::new(Semaphore::new(new_limit)));
                }
                Some(&old) => {
                    let old_limit = old.max(1);
                    if new_limit > old_limit {
                        if let Some(sem) = sems.get(host) {
                            sem.add_permits(new_limit - old_limit);
                        }
                    }
                    // shrink case: deliberately don't touch the semaphore.
                }
            }
        }

        // Assertions:
        // 1. kernel.org grew from 2 to 5 → available permits 5.
        assert_eq!(sems["kernel.org"].available_permits(), 5);
        // 2. debian.org tried to shrink 3 → 1: must still hold 3 permits
        //    (we cannot revoke a permit safely).
        assert_eq!(sems["debian.org"].available_permits(), 3);
        // 3. fedora.org is new with 4 permits.
        assert_eq!(sems["fedora.org"].available_permits(), 4);
        // 4. apache.org never existed.
        assert!(!sems.contains_key("apache.org"));
        // 5. No host was removed in this scenario.
        assert!(removed.is_empty());
        assert_eq!(sems.len(), 3);
    }

    /// Verify that removing a host from `new_limits` drops it from the map.
    /// Existing jobs that hold a clone of the Arc keep it alive externally,
    /// but the worker's lookup map for new spawns no longer contains it.
    #[test]
    fn per_upstream_hot_reload_removal() {
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        let mut sems: HashMap<String, Arc<Semaphore>> = HashMap::new();
        sems.insert("removed-host".into(), Arc::new(Semaphore::new(2)));

        let old_limits: HashMap<String, usize> = [("removed-host".to_string(), 2usize)]
            .into_iter()
            .collect();
        let new_limits: HashMap<String, usize> = HashMap::new();

        let removed: Vec<String> = old_limits
            .keys()
            .filter(|h| !new_limits.contains_key(*h))
            .cloned()
            .collect();
        for host in &removed {
            sems.remove(host);
        }

        assert_eq!(removed, vec!["removed-host".to_string()]);
        assert!(!sems.contains_key("removed-host"));
        assert!(sems.is_empty());
    }

    // ── Timezone-aware cron tests ────────────────────────────────────────

    /// effective_timezone falls back from per-mirror to global to UTC.
    #[test]
    fn effective_timezone_falls_back() {
        let mut global = GlobalConfig::default();
        let mut mc = MirrorConfig::default();

        // Both empty → UTC.
        assert_eq!(mc.effective_timezone(&global), "UTC");

        // Only global set.
        global.timezone = "Asia/Shanghai".into();
        assert_eq!(mc.effective_timezone(&global), "Asia/Shanghai");

        // Mirror overrides global.
        mc.timezone = "America/New_York".into();
        assert_eq!(mc.effective_timezone(&global), "America/New_York");

        // Mirror unset, global unset → UTC.
        mc.timezone = String::new();
        global.timezone = String::new();
        assert_eq!(mc.effective_timezone(&global), "UTC");
    }

    /// `cron = "0 3 * * *"` with `timezone = "Asia/Shanghai"` must fire at
    /// 03:00 in Shanghai, which is 19:00 UTC the previous day. With UTC
    /// it would fire at 03:00 UTC. The returned DateTime is in UTC; we
    /// convert it to Shanghai time and check the hour/minute.
    #[test]
    fn cron_respects_per_mirror_timezone() {
        let global = GlobalConfig::default();
        let mut mc = MirrorConfig::default();
        mc.name = "tz-test".into();
        mc.provider = ProviderKind::Rsync;
        mc.upstream = "rsync://localhost/test/".into();
        mc.cron = "0 3 * * *".into();
        mc.timezone = "Asia/Shanghai".into();

        let sched = super::parse_cron_lenient(&mc.cron).expect("cron parses");
        let (_inst, next_utc) = super::next_run_for(&mc, &global, Some(&sched));

        // Convert the returned UTC time into Shanghai and check 03:00 local.
        let tz: chrono_tz::Tz = "Asia/Shanghai".parse().unwrap();
        let next_local = next_utc.with_timezone(&tz);
        use chrono::Timelike;
        assert_eq!(next_local.hour(), 3, "cron must fire at 03:00 local time");
        assert_eq!(next_local.minute(), 0);
    }

    /// `cron = "0 3 * * *"` without any timezone override defaults to UTC.
    /// Converting back to UTC should still show hour=3, minute=0.
    #[test]
    fn cron_defaults_to_utc_when_unset() {
        let global = GlobalConfig::default();
        let mut mc = MirrorConfig::default();
        mc.name = "utc-default".into();
        mc.provider = ProviderKind::Rsync;
        mc.upstream = "rsync://localhost/test/".into();
        mc.cron = "0 3 * * *".into();

        let sched = super::parse_cron_lenient(&mc.cron).expect("cron parses");
        let (_inst, next_utc) = super::next_run_for(&mc, &global, Some(&sched));

        use chrono::Timelike;
        assert_eq!(next_utc.hour(), 3);
        assert_eq!(next_utc.minute(), 0);
    }

    /// Global `timezone` applies to mirrors that don't set their own.
    #[test]
    fn cron_uses_global_timezone_when_mirror_unset() {
        let mut global = GlobalConfig::default();
        global.timezone = "Asia/Shanghai".into();

        let mut mc = MirrorConfig::default();
        mc.name = "uses-global".into();
        mc.provider = ProviderKind::Rsync;
        mc.upstream = "rsync://localhost/test/".into();
        mc.cron = "0 3 * * *".into();
        // mc.timezone left empty → inherit from global.

        let sched = super::parse_cron_lenient(&mc.cron).expect("cron parses");
        let (_inst, next_utc) = super::next_run_for(&mc, &global, Some(&sched));

        let tz: chrono_tz::Tz = "Asia/Shanghai".parse().unwrap();
        let next_local = next_utc.with_timezone(&tz);
        use chrono::Timelike;
        assert_eq!(next_local.hour(), 3);
    }

    /// build_timezone_cache populates a Tz for every mirror, defaulting to UTC.
    #[test]
    fn build_timezone_cache_populates_every_mirror() {
        let global = GlobalConfig::default();
        let mut a = MirrorConfig::default();
        a.name = "a".into();
        a.timezone = "Asia/Tokyo".into();
        let mut b = MirrorConfig::default();
        b.name = "b".into();
        // b.timezone empty → UTC default.

        let cache = super::build_timezone_cache(&[a, b], &global);
        assert_eq!(cache["a"], chrono_tz::Asia::Tokyo);
        assert_eq!(cache["b"], chrono_tz::UTC);
    }
}
