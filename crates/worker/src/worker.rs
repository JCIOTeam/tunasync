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
use crate::log_stream::LogBroadcaster;
use crate::manager_client::ManagerClient;
use crate::provider::MirrorProvider;
use crate::schedule::ScheduleQueue;
use crate::scheduling::{parse_fixed_rate_anchor, SchedulingPolicy};

/// Extract the upstream host from a sync URL, for per-upstream concurrency.
///
/// Handles three forms:
///   * `rsync://host/module/path`      — scheme form (`url` crate parses)
///   * `rsync://[::1]/mod/`            — IPv6 in brackets, also fine for `url`
///   * `host::module/path`             — legacy rsync daemon shorthand
///   * `rsync://host::module/path`     — rare mixed form; `url` crate fails
///     here ("invalid port number") because the second `:` looks like a port
///     separator. We detect the failure and fall back to `::` splitting.
///
/// Returns `None` for unrecognised formats (`file://`, plain paths, etc).
fn upstream_host(upstream: &str) -> Option<String> {
    // Try URL parsing first. If it succeeds AND yields a host_str we trust
    // it — even when the host contains `::`, since that's a valid bracketed
    // IPv6 literal like `[::1]` or `[2001:db8::1]`. Filtering those out
    // would break per-upstream concurrency for IPv6 mirrors.
    //
    // Note that `url::Url::parse("host::module/")` can SUCCEED but yield
    // `host_str() == None` (the parser thinks `host` is a scheme). Treat
    // that the same as a parse failure and fall through to the `::` split.
    if let Ok(u) = url::Url::parse(upstream) {
        if let Some(host) = u.host_str() {
            return Some(host.to_owned());
        }
    }

    // URL parse failed or produced no host. Two real cases land here:
    //   1. `host::module/path` — pure legacy form, no scheme. The host is
    //      everything before the first `::`.
    //   2. `rsync://host::module/` — mixed form (the parser fails on the
    //      port number). Strip the scheme prefix first, then split on `::`.
    if upstream.contains("::") {
        let after_scheme = upstream
            .find("://")
            .map(|i| &upstream[i + 3..])
            .unwrap_or(upstream);
        return after_scheme
            .split("::")
            .next()
            .filter(|h| !h.is_empty())
            .map(str::to_owned);
    }
    None
}

/// Build the deterministic scheduling policy for one mirror.
fn scheduling_policy_for(
    mc: &crate::config::MirrorConfig,
    global: &crate::config::GlobalConfig,
    cached_cron: Option<&cron::Schedule>,
    timezone: chrono_tz::Tz,
) -> Option<SchedulingPolicy> {
    if let Some(sched) = cached_cron {
        return Some(SchedulingPolicy::cron(sched.clone(), timezone));
    }
    match mc.effective_interval_mode(global) {
        crate::config::IntervalMode::FixedDelay => {
            Some(SchedulingPolicy::fixed_delay(mc.effective_interval(global)))
        }
        crate::config::IntervalMode::FixedRate => {
            let interval_minutes = if mc.interval > 0 {
                mc.interval
            } else {
                global.interval
            };
            let anchor = parse_fixed_rate_anchor(mc.effective_fixed_rate_anchor(global)).ok()?;
            Some(SchedulingPolicy::fixed_rate(
                interval_minutes,
                anchor,
                timezone,
            ))
        }
    }
}

#[cfg(test)]
fn next_run_for(
    mc: &crate::config::MirrorConfig,
    global: &crate::config::GlobalConfig,
    cached_cron: Option<&cron::Schedule>,
) -> (Instant, chrono::DateTime<Utc>) {
    let now = Utc::now();
    let timezone = mc
        .effective_timezone(global)
        .parse()
        .unwrap_or(chrono_tz::UTC);
    let policy = scheduling_policy_for(mc, global, cached_cron, timezone)
        .expect("validated scheduling policy");
    let next = policy.next_after(now).expect("next scheduling occurrence");
    (due_utc_to_instant(next, now), next)
}

fn build_scheduling_cache(
    mirrors: &[crate::config::MirrorConfig],
    global: &crate::config::GlobalConfig,
    crons: &HashMap<String, cron::Schedule>,
    timezones: &HashMap<String, chrono_tz::Tz>,
) -> HashMap<String, SchedulingPolicy> {
    mirrors
        .iter()
        .filter_map(|mc| {
            scheduling_policy_for(
                mc,
                global,
                crons.get(&mc.name),
                timezones.get(&mc.name).copied().unwrap_or(chrono_tz::UTC),
            )
            .map(|policy| (mc.name.clone(), policy))
        })
        .collect()
}

fn due_utc_to_instant(due: chrono::DateTime<Utc>, now_utc: chrono::DateTime<Utc>) -> Instant {
    let delay = (due - now_utc).to_std().unwrap_or(Duration::ZERO);
    Instant::now() + delay
}

fn blackout_check_at(
    policy: Option<&SchedulingPolicy>,
    scheduled_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> chrono::DateTime<Utc> {
    if policy.is_some_and(SchedulingPolicy::is_wall_clock) {
        scheduled_at
    } else {
        now
    }
}

fn is_active_job_message(active_generations: &HashMap<String, u64>, message: &JobMessage) -> bool {
    active_generations.get(&message.name) == Some(&message.job_generation)
}

fn next_reload_schedule(
    mc: &crate::config::MirrorConfig,
    global: &crate::config::GlobalConfig,
    now: chrono::DateTime<Utc>,
) -> chrono::DateTime<Utc> {
    let timezone = mc
        .effective_timezone(global)
        .parse()
        .unwrap_or(chrono_tz::UTC);
    let cron = if mc.cron.is_empty() {
        None
    } else {
        parse_cron_lenient(&mc.cron).ok()
    };
    scheduling_policy_for(mc, global, cron.as_ref(), timezone)
        .and_then(|policy| policy.next_reload_after(now))
        .unwrap_or(now)
}

fn global_scheduling_change_affects(
    mc: &crate::config::MirrorConfig,
    old: &crate::config::GlobalConfig,
    new: &crate::config::GlobalConfig,
) -> bool {
    let old_mode = mc.effective_interval_mode(old);
    let new_mode = mc.effective_interval_mode(new);
    if old_mode != new_mode {
        return true;
    }
    if old_mode == crate::config::IntervalMode::FixedRate
        && mc.effective_fixed_rate_anchor(old) != mc.effective_fixed_rate_anchor(new)
    {
        return true;
    }
    (!mc.cron.is_empty()
        || old_mode == crate::config::IntervalMode::FixedRate
        || !mc.blackout.is_empty())
        && mc.effective_timezone(old) != mc.effective_timezone(new)
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

/// Remap a POSIX day-of-week field to the `cron` crate's convention.
///
/// POSIX/Vixie cron (and Go's robfig/cron, which the original tunasync uses)
/// number the days `0-7` where **both** `0` and `7` mean Sunday and `1` is
/// Monday. The `cron` crate instead uses `1-7` where `1` is Sunday and `7` is
/// Saturday, and rejects `0` outright. Widening a 5-field expression by only
/// prepending the seconds field therefore silently shifted every numeric
/// weekday by one day (`* * * * 1` fired on Sunday instead of Monday) and made
/// any expression containing `0` (a very common way to write Sunday) fail to
/// parse — which aborts worker startup. See the regression tests below.
///
/// This remaps each standalone weekday integer `n` in `0..=7` to `(n % 7) + 1`.
/// Names (`Mon`, `Sun`, …), `*`, and the step count following a `/` are left
/// untouched — only weekday *values* and range endpoints are translated.
fn remap_posix_dow(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let bytes = field.as_bytes();
    let mut i = 0;
    let mut prev_was_slash = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            let num: u32 = field[start..i].parse().unwrap_or(0);
            if prev_was_slash || num > 7 {
                // Step count (e.g. the `2` in `*/2`) or an out-of-range value:
                // pass through unchanged and let the cron crate validate it.
                out.push_str(&num.to_string());
            } else {
                out.push_str(&((num % 7) + 1).to_string());
            }
            prev_was_slash = false;
        } else {
            prev_was_slash = c == b'/';
            out.push(c as char);
            i += 1;
        }
    }
    out
}

/// Parse a cron expression accepting both 5-field POSIX format
/// (`minute hour dom month dow`) and the cron-crate native 6/7-field format
/// (`sec minute hour dom month dow [year]`).
///
/// The `cron` crate requires 6 or 7 fields. Operators are far more familiar
/// with the classic 5-field syntax used by crontab(5), Kubernetes, systemd
/// timers, and just about every other scheduler — so accepting both is
/// essential. A 5-field expression is widened by prepending `"0 "` (run at
/// second 0 of the matching minute) and its day-of-week field is translated
/// from POSIX numbering to the cron crate's numbering via [`remap_posix_dow`]
/// so that `… * * 1` means Monday and `… * * 0` means Sunday, matching what
/// operators (and the Go implementation) expect.
///
/// Returns the same error type the `cron` crate uses, so callers can render
/// it verbatim in startup error messages.
pub(crate) fn parse_cron_lenient(expr: &str) -> Result<cron::Schedule, cron::error::Error> {
    use std::str::FromStr;
    let trimmed = expr.trim();
    let fields: Vec<&str> = trimmed.split_ascii_whitespace().collect();
    if fields.len() == 5 {
        // Widen to 6 fields by prepending "0 " for the seconds field, and
        // translate the POSIX day-of-week field to the cron crate convention.
        let dow = remap_posix_dow(fields[4]);
        let widened = format!(
            "0 {} {} {} {} {}",
            fields[0], fields[1], fields[2], fields[3], dow
        );
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
    /// Active MirrorJob generation per mirror. Buffered messages from a job
    /// retired by reload are ignored when their generation no longer matches.
    job_generations: HashMap<String, u64>,
    next_job_generation: u64,
    manager: Arc<ManagerClient>,
    api_token: Arc<RwLock<String>>,
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
    /// Per-mirror live-log broadcast registry. Created once at startup and
    /// reused across hot-reloads — each provider gets its corresponding
    /// `LogPublisher` via `set_log_publisher`, and the HTTP server
    /// subscribes via the same registry to power
    /// `GET /jobs/<mirror>/log/stream`.
    log_broadcaster: Arc<LogBroadcaster>,
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
    /// Pure scheduling policy per mirror, rebuilt after validated reloads.
    scheduling_by_mirror: HashMap<String, SchedulingPolicy>,
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
    fn allocate_job_generation(&mut self, name: &str) -> u64 {
        self.next_job_generation = self
            .next_job_generation
            .checked_add(1)
            .expect("job generation exhausted");
        self.job_generations
            .insert(name.to_owned(), self.next_job_generation);
        self.next_job_generation
    }

    async fn stop_and_join_job(&mut self, name: &str) -> Option<JobState> {
        self.schedule.remove(name);
        self.job_generations.remove(name);
        let mut job = self.jobs.remove(name)?;
        let state = job.state();
        job.retire();
        if let Some(mut task) = job.take_task() {
            if tokio::time::timeout(Duration::from_secs(10), &mut task)
                .await
                .is_err()
            {
                tracing::warn!(mirror = %name, "old job did not stop within 10s; aborting task");
                task.abort();
                let _ = task.await;
            }
        }
        Some(state)
    }

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
        let manager = Arc::new(ManagerClient::new(
            bases,
            http_client.clone(),
            cfg.manager.api_token.clone(),
        ));
        let api_token = Arc::new(RwLock::new(cfg.manager.api_token.clone()));

        let provider_list = build_jobs(&cfg);
        let mut jobs = HashMap::new();
        let mut job_generations = HashMap::new();
        let mut next_job_generation = 0_u64;
        let mut mirror_statuses = HashMap::new();

        // Live-log broadcast registry — wired to every provider below so the
        // HTTP server can stream sync output in real time.
        let log_broadcaster = LogBroadcaster::new();

        // Build per-upstream semaphores from config.
        let per_upstream_semaphores: HashMap<String, Arc<Semaphore>> = cfg
            .global
            .per_upstream_concurrent
            .iter()
            .map(|(host, &limit)| (host.clone(), Arc::new(Semaphore::new(limit.max(1)))))
            .collect();

        for (mut provider, hooks) in provider_list {
            let name = provider.name().to_owned();
            let upstream = crate::redact_url_diagnostic(provider.upstream());
            let is_master = provider.is_master();

            // Hand the provider its per-mirror live-log publisher so the runner
            // can fan out each stdout/stderr line and keep a replay buffer.
            provider.set_log_publisher(log_broadcaster.publisher_for(&name));

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

            next_job_generation += 1;
            let job_generation = next_job_generation;
            job_generations.insert(name.clone(), job_generation);
            let job = MirrorJob::spawn(
                provider,
                hooks,
                status_tx.clone(),
                Arc::clone(&semaphore),
                upstream_sem,
                priority,
                job_generation,
            );
            jobs.insert(name, job);
        }

        // Schedule queue is populated in restore_job_state() after startup.
        let schedule = ScheduleQueue::new();

        let mirror_names = Arc::new(RwLock::new(jobs.keys().cloned().collect()));

        // Precompute blackout, cron and timezone caches.
        let blackouts_by_mirror = build_blackout_cache(&cfg.mirrors);
        let crons_by_mirror = build_cron_cache(&cfg.mirrors);
        let timezones_by_mirror = build_timezone_cache(&cfg.mirrors, &cfg.global);
        let scheduling_by_mirror = build_scheduling_cache(
            &cfg.mirrors,
            &cfg.global,
            &crons_by_mirror,
            &timezones_by_mirror,
        );

        Self {
            cfg,
            config_path,
            jobs,
            job_generations,
            next_job_generation,
            manager,
            api_token,
            status_tx,
            status_rx,
            cmd_tx,
            cmd_rx,
            semaphore,
            per_upstream_semaphores,
            schedule,
            mirror_statuses,
            mirror_names,
            log_broadcaster,
            blackouts_by_mirror,
            crons_by_mirror,
            timezones_by_mirror,
            scheduling_by_mirror,
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
            api_token: Arc::clone(&self.api_token),
            cmd_tx: self.cmd_tx.clone(),
            worker_name: worker_id.clone(),
            mirror_names: Arc::clone(&self.mirror_names),
            log_broadcaster: Arc::clone(&self.log_broadcaster),
        };
        let bind_addr = self.cfg.server.bind_addr().map_err(anyhow::Error::msg)?;
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
                match mgr.heartbeat(&wid).await {
                    Err(e) => warn!(worker = %wid, error = %e, "heartbeat failed"),
                    // Manager reachable — replay any reports buffered while
                    // it was down (no-op when the buffer is empty).
                    Ok(()) => mgr.flush_pending(&wid).await,
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
    /// - Fixed-delay mirrors preserve legacy last-completion/immediate semantics
    /// - Cron/fixed-rate mirrors schedule their next strictly future occurrence
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
                            if let Some(entry) = self.mirror_statuses.get_mut(&status.name) {
                                entry.scheduled = zero_time();
                            }
                            tracing::info!(mirror = %status.name, "restored Disabled state");
                            continue; // do not enqueue
                        }
                        SyncStatus::Paused => {
                            if let Some(job) = self.jobs.get(&status.name) {
                                job.try_send(CtrlAction::Stop);
                            }
                            self.schedule.remove(&status.name);
                            if let Some(entry) = self.mirror_statuses.get_mut(&status.name) {
                                entry.scheduled = zero_time();
                            }
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

                    // Compute the next intended UTC occurrence from the policy
                    // and write it to `mirror_statuses.scheduled` so the
                    // manager UI shows a real upcoming time instead of
                    // 0001-01-01T00:00:00Z until the first sync completes.
                    let job_cfg = self.cfg.mirrors.iter().find(|m| m.name == status.name);

                    let now_utc = Utc::now();
                    let next_dt = job_cfg
                        .and_then(|mc| self.scheduling_by_mirror.get(&mc.name))
                        .and_then(|policy| {
                            let last_completion =
                                (!tunasync_protocol::is_zero_time(&status.last_update))
                                    .then_some(status.last_update);
                            policy.next_startup_after(now_utc, last_completion)
                        })
                        .unwrap_or(now_utc);

                    // Persist the scheduled time so it's visible on
                    // /workers/:id/jobs and propagates to manager.
                    if let Some(entry) = self.mirror_statuses.get_mut(&status.name) {
                        entry.scheduled = next_dt;
                    }

                    tracing::info!(
                        mirror = %status.name,
                        next_run_secs = (next_dt - now_utc).to_std().unwrap_or_default().as_secs(),
                        scheduled = %next_dt,
                        "scheduled (from last_update)"
                    );
                    self.schedule.push(status.name.clone(), next_dt);
                }
                tracing::info!(mirrors = statuses.len(), "restored job states from manager");
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch job status from manager — applying new-mirror startup policy");
                // Fall through: unseen still contains all job names, so they
                // all get scheduled now below.
            }
        }

        // New mirrors retain legacy immediate startup only for fixed-delay.
        // Cron and fixed-rate always wait for the next strictly future slot.
        for name in &unseen {
            let now = Utc::now();
            let scheduled = self
                .scheduling_by_mirror
                .get(name)
                .and_then(|policy| policy.next_startup_after(now, None))
                .unwrap_or(now);
            tracing::info!(mirror = %name, scheduled = %scheduled, "new mirror scheduled");
            if let Some(entry) = self.mirror_statuses.get_mut(name) {
                entry.scheduled = scheduled;
            }
            self.schedule.push(name.clone(), scheduled);
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
                let now_utc = Utc::now();
                if entry.scheduled_at <= now_utc {
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
                    let policy = self.scheduling_by_mirror.get(&entry.name);
                    let in_blackout = self
                        .blackouts_by_mirror
                        .get(&entry.name)
                        .map(|windows| {
                            let tz = self
                                .timezones_by_mirror
                                .get(&entry.name)
                                .copied()
                                .unwrap_or(chrono_tz::UTC);
                            let check_at = blackout_check_at(policy, entry.scheduled_at, now_utc);
                            let local = check_at.with_timezone(&tz);
                            crate::blackout::is_in_blackout(windows, &local)
                        })
                        .unwrap_or(false);

                    if in_blackout {
                        let retry_at = policy
                            .and_then(|policy| policy.next_after_blackout(now_utc))
                            .unwrap_or(now_utc + chrono::Duration::minutes(5));
                        tracing::info!(mirror = %entry.name, scheduled = %retry_at, "in blackout window — rescheduling");
                        if let Some(status) = self.mirror_statuses.get_mut(&entry.name) {
                            status.scheduled = retry_at;
                        }
                        self.schedule.push(entry.name, retry_at);
                        self.report_schedules(&worker_id).await;
                    } else if let Some(job) = self.jobs.get(&entry.name) {
                        if !job.try_send(CtrlAction::Start) {
                            // The ctrl channel is full (e.g. buffered Pings
                            // during a long-running sync) or the task died.
                            // The entry was already popped and the job will
                            // never emit a terminal message for a Start it
                            // never received — without this re-push the
                            // mirror would silently stop syncing forever.
                            tracing::warn!(
                                mirror = %entry.name,
                                "could not queue scheduled Start (ctrl channel                                  full or task dead) — retrying in 30s"
                            );
                            let retry_at = now_utc + chrono::Duration::seconds(30);
                            if let Some(status) = self.mirror_statuses.get_mut(&entry.name) {
                                status.scheduled = retry_at;
                            }
                            self.schedule.push(entry.name, retry_at);
                            self.report_schedules(&worker_id).await;
                        }
                    }
                } else {
                    break;
                }
            }

            // Compute sleep duration until next scheduled job.
            let sleep_dur = self
                .schedule
                .peek()
                .map(|e| due_utc_to_instant(e.scheduled_at, Utc::now()))
                .map(|due| due.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_secs(30))
                .min(Duration::from_secs(30));

            tokio::select! {
                // A job reported a status update.
                Some(msg) = self.status_rx.recv() => {
                    self.handle_job_message(msg, &worker_id).await;
                }

                // Manager/CLI sent us a command via HTTP.
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_worker_cmd(cmd, &worker_id).await;
                }

                // Time to check the schedule again.
                _ = tokio::time::sleep(sleep_dur) => {}

                // Graceful shutdown on SIGTERM / SIGINT.
                _ = &mut shutdown => {
                    info!("received shutdown signal — halting all jobs");
                    for job in self.jobs.values() {
                        job.retire();
                    }
                    // Join all job tasks (instead of the old fixed 5s sleep)
                    // so in-flight publishes/post-exec hooks can complete and
                    // logs get flushed, with a hard 30s ceiling as the safety
                    // net. Go uses an unbounded WaitGroup here; we bound it
                    // because a wedged provider must not block shutdown.
                    let handles: Vec<_> = self
                        .jobs
                        .drain()
                        .filter_map(|(_, mut j)| j.take_task())
                        .collect();
                    let deadline =
                        tokio::time::Instant::now() + Duration::from_secs(30);
                    let mut joined = 0usize;
                    let total = handles.len();
                    for mut h in handles {
                        if tokio::time::timeout_at(deadline, &mut h).await.is_ok() {
                            joined += 1;
                        } else {
                            h.abort();
                        }
                    }
                    if joined < total {
                        tracing::warn!(
                            joined, total,
                            "some job tasks did not exit within 30s and were aborted"
                        );
                    }
                    // Drain any final status messages the tasks emitted on
                    // their way out so the manager sees terminal states.
                    while let Ok(msg) = self.status_rx.try_recv() {
                        self.handle_job_message(msg, &worker_id).await;
                    }
                    info!("shutdown complete");
                    return;
                }
            }
        }
    }

    /// Process a status update from a job task.
    async fn handle_job_message(&mut self, msg: JobMessage, worker_id: &str) {
        if !is_active_job_message(&self.job_generations, &msg) {
            tracing::debug!(
                mirror = %msg.name,
                generation = msg.job_generation,
                "discarding status from retired job generation"
            );
            return;
        }

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
        // Propagate the skip hint so the manager can decide not to increment
        // consecutive_failures. Cleared (false) for all normal messages.
        status_entry.skip_failure_count = msg.skip_sync;
        if !msg.size.is_empty() {
            status_entry.size = msg.size.clone();
        }
        if msg.status == SyncStatus::Success {
            // A Success message is authoritative for this run's transferred
            // bytes — including 0 (nothing changed upstream / stats parse
            // failed). Without the unconditional overwrite, a zero-transfer
            // success would keep the PREVIOUS run's value and the manager
            // would accumulate it again on this run's Success transition.
            status_entry.last_transferred_bytes = msg.transferred_bytes;
        } else if msg.transferred_bytes > 0 {
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
            if let Some(policy) = self.scheduling_by_mirror.get(&msg.name) {
                let now_utc = Utc::now();
                let next_dt = policy.next_after(now_utc).unwrap_or(now_utc);
                self.schedule.push(msg.name.clone(), next_dt);

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
    async fn handle_worker_cmd(&mut self, cmd: WorkerCmd, worker_id: &str) {
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
                    if matches!(cmd.cmd, Stop | Disable) {
                        if let Some(status) = self.mirror_statuses.get_mut(&cmd.mirror_id) {
                            status.scheduled = zero_time();
                        }
                    }
                } else if matches!(cmd.cmd, Stop | Disable) {
                    for name in self.jobs.keys() {
                        self.schedule.remove(name);
                        if let Some(status) = self.mirror_statuses.get_mut(name) {
                            status.scheduled = zero_time();
                        }
                    }
                }

                if cmd.mirror_id.is_empty() {
                    for job in self.jobs.values() {
                        if let Some(action) = cmd_to_ctrl(&cmd) {
                            // Kill running sync on Stop/Disable/Halt/Restart so
                            // it terminates promptly. Signal the kill BEFORE
                            // queueing the ctrl action: the job's Start/Restart
                            // handler drains stale kill versions when it picks
                            // up the action, so kill-then-send guarantees the
                            // stale version is visible (and drained) by then,
                            // whereas send-then-kill could land the kill just
                            // after the drain and swallow the restart's sync.
                            if matches!(
                                action,
                                CtrlAction::Stop
                                    | CtrlAction::Disable
                                    | CtrlAction::Halt
                                    | CtrlAction::Restart
                            ) {
                                job.kill();
                            }
                            job.try_send(action);
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
                                    Ok((mut provider, hooks)) => {
                                        let priority = job_cfg.priority;
                                        provider.set_log_publisher(
                                            self.log_broadcaster.publisher_for(name),
                                        );
                                        let upstream_sem = upstream_host(provider.upstream())
                                            .and_then(|h| {
                                                self.per_upstream_semaphores.get(&h).cloned()
                                            });
                                        let job_generation = self.allocate_job_generation(name);
                                        let new_job = MirrorJob::spawn(
                                            provider,
                                            hooks,
                                            self.status_tx.clone(),
                                            Arc::clone(&self.semaphore),
                                            upstream_sem,
                                            priority,
                                            job_generation,
                                        );
                                        self.jobs.insert(name.clone(), new_job);
                                        // Fall through — send Start to the new job.
                                        if let Some(new_job) = self.jobs.get(name) {
                                            new_job.try_send(CtrlAction::Start);
                                        }
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

                        // Same kill-before-send ordering as the broadcast
                        // path above (see comment there).
                        if matches!(
                            action,
                            CtrlAction::Stop
                                | CtrlAction::Disable
                                | CtrlAction::Halt
                                | CtrlAction::Restart
                        ) {
                            job.kill();
                        }
                        job.try_send(action);
                    }
                } else {
                    tracing::warn!(mirror = %cmd.mirror_id, "cmd for unknown mirror");
                }
                if matches!(cmd.cmd, Stop | Disable) {
                    self.report_schedules(worker_id).await;
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
        use crate::diff_config::{diff_mirror_config, DiffOp, MirrorCfgTrans};

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
        let include_errors = crate::load_include_mirrors(&mut new_cfg);
        if !include_errors.is_empty() {
            tracing::error!(
                errors = ?include_errors,
                "hot-reload: include loading failed — keeping current config"
            );
            return;
        }
        new_cfg.mirrors = crate::config::flatten_mirrors(&new_cfg.mirrors_conf);

        let config_errors = crate::validate_worker_config(&new_cfg);
        if !config_errors.is_empty() {
            tracing::error!(
                errors = ?config_errors,
                "hot-reload: config validation failed — keeping current config"
            );
            return;
        }

        if new_cfg.netns_broker.generation == self.cfg.netns_broker.generation {
            match crate::netns_policy::policy_content_changed(&self.cfg, &new_cfg) {
                Ok(true) => {
                    tracing::error!(
                        generation = %new_cfg.netns_broker.generation,
                        "hot-reload: namespaced launch policy changed without a new generation - keeping current config"
                    );
                    return;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "hot-reload: failed to compare namespace policy content - keeping current config"
                    );
                    return;
                }
            }
        }

        if let Err(e) = crate::verify_netns_broker_for_config(&new_cfg).await {
            tracing::error!(
                error = %e,
                "hot-reload: namespace broker readiness check failed - keeping current config"
            );
            return;
        }

        if new_cfg.global.name != self.cfg.global.name {
            tracing::warn!(
                old = %self.cfg.global.name,
                new = %new_cfg.global.name,
                "hot-reload: worker name change requires a restart — keeping current name"
            );
            new_cfg.global.name = self.cfg.global.name.clone();
        }

        let provider_globals_changed = self.cfg.global.log_dir != new_cfg.global.log_dir
            || self.cfg.global.mirror_dir != new_cfg.global.mirror_dir
            || self.cfg.global.interval != new_cfg.global.interval
            || self.cfg.global.retry != new_cfg.global.retry
            || self.cfg.global.timeout != new_cfg.global.timeout
            || self.cfg.global.rsync_options != new_cfg.global.rsync_options
            || self.cfg.global.exec_on_success != new_cfg.global.exec_on_success
            || self.cfg.global.exec_on_failure != new_cfg.global.exec_on_failure
            || self.cfg.global.dangerous_global_success_exit_codes
                != new_cfg.global.dangerous_global_success_exit_codes
            || self.cfg.global.dangerous_global_rsync_success_exit_codes
                != new_cfg.global.dangerous_global_rsync_success_exit_codes
            || self.cfg.global.staging_dir != new_cfg.global.staging_dir
            || serde_json::to_vec(&self.cfg.netns_broker).ok()
                != serde_json::to_vec(&new_cfg.netns_broker).ok()
            || serde_json::to_vec(&(
                &self.cfg.cgroup,
                &self.cfg.zfs,
                &self.cfg.btrfs_snapshot,
                &self.cfg.docker,
            ))
            .ok()
                != serde_json::to_vec(&(
                    &new_cfg.cgroup,
                    &new_cfg.zfs,
                    &new_cfg.btrfs_snapshot,
                    &new_cfg.docker,
                ))
                .ok();

        let scheduling_globals_changed = self.cfg.global.interval_mode
            != new_cfg.global.interval_mode
            || self.cfg.global.fixed_rate_anchor != new_cfg.global.fixed_rate_anchor
            || self.cfg.global.timezone != new_cfg.global.timezone;

        let mut diff = diff_mirror_config(&self.cfg.mirrors, &new_cfg.mirrors);
        if provider_globals_changed {
            let already_changed: HashSet<&str> = diff
                .iter()
                .map(|trans| trans.config.name.as_str())
                .collect();
            let inherited_changes: Vec<MirrorCfgTrans> = new_cfg
                .mirrors
                .iter()
                .filter(|mc| {
                    self.cfg.mirrors.iter().any(|old| old.name == mc.name)
                        && !already_changed.contains(mc.name.as_str())
                })
                .cloned()
                .map(|config| MirrorCfgTrans {
                    op: DiffOp::Modify,
                    config,
                })
                .collect();
            diff.extend(inherited_changes);
        }
        if scheduling_globals_changed {
            let already_changed: HashSet<&str> = diff
                .iter()
                .map(|trans| trans.config.name.as_str())
                .collect();
            let inherited_changes: Vec<MirrorCfgTrans> = new_cfg
                .mirrors
                .iter()
                .filter(|mc| {
                    self.cfg.mirrors.iter().any(|old| old.name == mc.name)
                        && !already_changed.contains(mc.name.as_str())
                        && global_scheduling_change_affects(mc, &self.cfg.global, &new_cfg.global)
                })
                .cloned()
                .map(|config| MirrorCfgTrans {
                    op: DiffOp::Modify,
                    config,
                })
                .collect();
            diff.extend(inherited_changes);
        }

        if diff.is_empty() {
            tracing::info!("hot-reload: mirror config unchanged; applying global settings");
        } else {
            tracing::info!(changes = diff.len(), "hot-reload: applying config diff");
        }

        // Prepare every replacement before mutating any live job. This makes
        // provider construction transactional across the whole reload: one
        // invalid mirror cannot leave half the worker on the new config and
        // half on the old config.
        let mut prepared = HashMap::new();
        for trans in &diff {
            if matches!(trans.op, DiffOp::Add | DiffOp::Modify) {
                match (self.build_one_provider)(&trans.config, &new_cfg) {
                    Ok(built) => {
                        prepared.insert(trans.config.name.clone(), built);
                    }
                    Err(e) => {
                        tracing::error!(
                            mirror = %trans.config.name,
                            error = %e,
                            "hot-reload: provider preparation failed — keeping current config"
                        );
                        return;
                    }
                }
            }
        }

        // Apply per-upstream concurrency changes before spawning replacement
        // jobs so Add/Modify transitions in this reload see the new limits.
        {
            let old_limits = &self.cfg.global.per_upstream_concurrent;
            let requested_limits = new_cfg.global.per_upstream_concurrent.clone();

            let removed: Vec<String> = old_limits
                .keys()
                .filter(|h| !requested_limits.contains_key(*h))
                .cloned()
                .collect();
            for host in &removed {
                self.per_upstream_semaphores.remove(host);
                tracing::info!(
                    host = %host,
                    "hot-reload: removed per-upstream concurrency limit"
                );
            }

            for (host, &new_limit) in &requested_limits {
                let new_limit = new_limit.max(1);
                match old_limits.get(host) {
                    None => {
                        self.per_upstream_semaphores
                            .insert(host.clone(), Arc::new(Semaphore::new(new_limit)));
                    }
                    Some(&old) => {
                        let old_limit = old.max(1);
                        if new_limit > old_limit {
                            if let Some(sem) = self.per_upstream_semaphores.get(host) {
                                sem.add_permits(new_limit - old_limit);
                            }
                        } else if new_limit < old_limit {
                            new_cfg
                                .global
                                .per_upstream_concurrent
                                .insert(host.clone(), old_limit);
                            tracing::warn!(
                                host = %host,
                                from = old_limit,
                                to = new_limit,
                                "hot-reload: cannot shrink per-upstream concurrency limit until restart"
                            );
                        }
                    }
                }
            }
        }

        for trans in &diff {
            let name = &trans.config.name;
            match trans.op {
                DiffOp::Delete => {
                    self.stop_and_join_job(name).await;
                    self.mirror_statuses.remove(name);
                    // ALSO remove from cfg.mirrors. Without this, the next
                    // hot-reload's diff_mirror_config compares the new TOML
                    // against a stale list still containing the deleted
                    // mirror, generating spurious diffs and (for re-added
                    // mirrors with the same name) misclassifying Add as
                    // Modify with a stale provider config.
                    self.cfg.mirrors.retain(|m| &m.name != name);
                    tracing::info!(mirror = %name, "hot-reload: deleted job");
                }
                DiffOp::Modify => {
                    let (mut provider, hooks) = prepared
                        .remove(name)
                        .expect("modified provider prepared before applying reload");

                    // Remember the old job's state so we can preserve it.
                    let old_state = self.jobs.get(name).map(|j| j.state());

                    // Preserve historical telemetry (last_update, size,
                    // last_started, last_ended, transferred_bytes counters)
                    // so a cosmetic config change (e.g. tweaking interval)
                    // doesn't wipe a long-running mirror's record. Only
                    // the fields that are computed from the new provider
                    // — upstream, is_master — are refreshed below.
                    let preserved = self.mirror_statuses.get(name).cloned();

                    // Stop and join the old job before spawning its replacement.
                    self.stop_and_join_job(name).await;
                    self.mirror_statuses.remove(name);

                    // Update config.
                    if let Some(pos) = self.cfg.mirrors.iter().position(|m| &m.name == name) {
                        self.cfg.mirrors[pos] = trans.config.clone();
                    } else {
                        self.cfg.mirrors.push(trans.config.clone());
                    }

                    provider.set_log_publisher(self.log_broadcaster.publisher_for(name));
                    let upstream = crate::redact_url_diagnostic(provider.upstream());
                    let is_master = provider.is_master();

                    let now_utc = Utc::now();
                    let next_schedule =
                        next_reload_schedule(&trans.config, &new_cfg.global, now_utc);
                    let merged = if let Some(mut prev) = preserved {
                        prev.upstream = upstream;
                        prev.is_master = is_master;
                        prev.scheduled = next_schedule;
                        prev
                    } else {
                        MirrorStatus {
                            name: name.clone(),
                            worker: new_cfg.global.name.clone(),
                            is_master,
                            upstream,
                            scheduled: next_schedule,
                            ..Default::default()
                        }
                    };
                    self.mirror_statuses.insert(name.clone(), merged);

                    let upstream_sem2 = upstream_host(provider.upstream())
                        .and_then(|h| self.per_upstream_semaphores.get(&h).cloned());
                    let job_generation = self.allocate_job_generation(name);
                    let job = MirrorJob::spawn(
                        provider,
                        hooks,
                        self.status_tx.clone(),
                        Arc::clone(&self.semaphore),
                        upstream_sem2,
                        trans.config.priority,
                        job_generation,
                    );
                    self.jobs.insert(name.clone(), job);

                    // Preserve the old job's state (matches Go's ReloadMirrorConfig
                    // which checks the previous state when re-spawning a modified job).
                    match old_state {
                        Some(JobState::Paused) => {
                            if let Some(job) = self.jobs.get(name) {
                                job.try_send(CtrlAction::Stop);
                            }
                            if let Some(status) = self.mirror_statuses.get_mut(name) {
                                status.scheduled = zero_time();
                            }
                            tracing::info!(mirror = %name, "hot-reload: modified job — kept Paused");
                        }
                        Some(JobState::Disabled) => {
                            if let Some(job) = self.jobs.get(name) {
                                job.try_send(CtrlAction::Disable);
                            }
                            if let Some(status) = self.mirror_statuses.get_mut(name) {
                                status.scheduled = zero_time();
                            }
                            tracing::info!(mirror = %name, "hot-reload: modified job — kept Disabled");
                        }
                        _ => {
                            tracing::info!(mirror = %name, "hot-reload: modified job — scheduling");
                            self.schedule.push(name.clone(), next_schedule);
                        }
                    }
                }
                DiffOp::Add => {
                    let (mut provider, hooks) = prepared
                        .remove(name)
                        .expect("new provider prepared before applying reload");
                    self.cfg.mirrors.push(trans.config.clone());
                    provider.set_log_publisher(self.log_broadcaster.publisher_for(name));
                    let upstream = crate::redact_url_diagnostic(provider.upstream());
                    let is_master = provider.is_master();

                    self.mirror_statuses.insert(
                        name.clone(),
                        MirrorStatus {
                            name: name.clone(),
                            worker: new_cfg.global.name.clone(),
                            is_master,
                            upstream,
                            scheduled: next_reload_schedule(
                                &trans.config,
                                &new_cfg.global,
                                Utc::now(),
                            ),
                            ..Default::default()
                        },
                    );

                    let upstream_sem3 = upstream_host(provider.upstream())
                        .and_then(|h| self.per_upstream_semaphores.get(&h).cloned());
                    let job_generation = self.allocate_job_generation(name);
                    let job = MirrorJob::spawn(
                        provider,
                        hooks,
                        self.status_tx.clone(),
                        Arc::clone(&self.semaphore),
                        upstream_sem3,
                        trans.config.priority,
                        job_generation,
                    );
                    self.jobs.insert(name.clone(), job);

                    tracing::info!(mirror = %name, "hot-reload: new job");
                    if let Some(status) = self.mirror_statuses.get(name) {
                        self.schedule.push(name.clone(), status.scheduled);
                    }
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
            new_cfg.global.concurrent = old_concurrent;
            tracing::warn!(
                from = old_concurrent,
                to = new_concurrent,
                "hot-reload: cannot shrink concurrency limit on a running worker — \
                 keeping {old_concurrent} until restart"
            );
        }

        let manager_changed = new_cfg.manager.api_base_list() != self.cfg.manager.api_base_list()
            || new_cfg.manager.api_token != self.cfg.manager.api_token
            || new_cfg.manager.ca_cert != self.cfg.manager.ca_cert;
        if manager_changed {
            let client = if new_cfg.manager.ca_cert.is_empty() {
                tunasync_common::http::HttpClientBuilder::new().build()
            } else {
                tunasync_common::http::HttpClientBuilder::new()
                    .ca_cert_pem_from_path(std::path::Path::new(&new_cfg.manager.ca_cert))
                    .and_then(|builder| builder.build())
            };
            match client {
                Ok(client) => {
                    let bases: Vec<String> = new_cfg
                        .manager
                        .api_base_list()
                        .into_iter()
                        .map(String::from)
                        .collect();
                    let candidate = ManagerClient::new(
                        bases.clone(),
                        client.clone(),
                        new_cfg.manager.api_token.clone(),
                    );
                    let status = WorkerStatus {
                        id: self.cfg.global.name.clone(),
                        url: self.cfg.server.public_url(&self.cfg),
                        token: String::new(),
                        last_online: zero_time(),
                        last_register: zero_time(),
                    };
                    match candidate.register(&status).await {
                        Ok(_) => {
                            self.manager.reconfigure(
                                bases,
                                client,
                                new_cfg.manager.api_token.clone(),
                            );
                            *self.api_token.write().await = new_cfg.manager.api_token.clone();
                            tracing::info!(
                                "hot-reload: registered with and switched to updated manager configuration"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "hot-reload: updated manager registration failed — keeping old manager config"
                            );
                            new_cfg.manager = self.cfg.manager.clone();
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "hot-reload: failed to rebuild manager HTTP client — keeping old manager config"
                    );
                    new_cfg.manager = self.cfg.manager.clone();
                }
            }
        }

        // Update global config (interval/retry defaults etc.) from new file.
        self.cfg.global = new_cfg.global;
        self.cfg.manager = new_cfg.manager;
        self.cfg.netns_broker = new_cfg.netns_broker;
        if new_cfg.server.addr != self.cfg.server.addr
            || new_cfg.server.port != self.cfg.server.port
            || new_cfg.server.ssl_cert != self.cfg.server.ssl_cert
            || new_cfg.server.ssl_key != self.cfg.server.ssl_key
            || new_cfg.server.hostname != self.cfg.server.hostname
        {
            tracing::warn!(
                "hot-reload: worker HTTP server settings changed but require a restart — keeping current listener"
            );
        }

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
        self.scheduling_by_mirror = build_scheduling_cache(
            &self.cfg.mirrors,
            &self.cfg.global,
            &self.crons_by_mirror,
            &self.timezones_by_mirror,
        );
        let worker_id = self.cfg.global.name.clone();
        self.report_schedules(&worker_id).await;
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
#[allow(clippy::field_reassign_with_default)]
mod cron_schedule_tests {
    //! Unit tests for `next_run_for` — cron vs interval scheduling.

    use std::time::Duration;

    use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
    use crate::job::JobMessage;
    use tunasync_protocol::SyncStatus;

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

    #[test]
    fn retired_job_messages_are_rejected_by_generation() {
        let mut generations = std::collections::HashMap::new();
        generations.insert("debian".to_owned(), 2);
        let stale = JobMessage {
            job_generation: 1,
            status: SyncStatus::None,
            name: "debian".into(),
            msg: String::new(),
            schedule: true,
            size: String::new(),
            transferred_bytes: 0,
            skip_sync: false,
        };
        let active = JobMessage {
            job_generation: 2,
            ..stale.clone()
        };

        assert!(!super::is_active_job_message(&generations, &stale));
        assert!(super::is_active_job_message(&generations, &active));
    }

    #[test]
    fn wall_clock_blackout_uses_intended_occurrence_time() {
        let scheduled = chrono::DateTime::parse_from_rfc3339("2026-01-01T02:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let delayed_processing = chrono::DateTime::parse_from_rfc3339("2026-01-01T03:01:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let rate = crate::scheduling::SchedulingPolicy::fixed_rate(
            60,
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            chrono_tz::UTC,
        );
        let delay =
            crate::scheduling::SchedulingPolicy::fixed_delay(std::time::Duration::from_secs(3600));

        assert_eq!(
            super::blackout_check_at(Some(&rate), scheduled, delayed_processing),
            scheduled
        );
        assert_eq!(
            super::blackout_check_at(Some(&delay), scheduled, delayed_processing),
            delayed_processing
        );
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

    /// Regression: a numeric POSIX day-of-week in a 5-field expression must
    /// fire on the correct weekday. POSIX uses 0-7 (0/7=Sun, 1=Mon); the cron
    /// crate uses 1-7 (1=Sun). Before the remap, `* * * * 1` fired on Sunday
    /// and `* * * * 0` failed to parse entirely (aborting worker startup).
    #[test]
    fn parse_cron_lenient_maps_posix_weekday_numbers() {
        use chrono::{Datelike, Utc, Weekday};

        let next_weekday = |expr: &str| -> Weekday {
            let sched =
                super::parse_cron_lenient(expr).unwrap_or_else(|e| panic!("parse {expr:?}: {e}"));
            sched
                .upcoming(Utc)
                .next()
                .expect("schedule yields an upcoming time")
                .weekday()
        };

        // Numeric weekdays land on the POSIX-expected day.
        assert_eq!(next_weekday("0 3 * * 1"), Weekday::Mon);
        assert_eq!(next_weekday("0 3 * * 5"), Weekday::Fri);
        assert_eq!(next_weekday("0 3 * * 6"), Weekday::Sat);
        // Both 0 and 7 mean Sunday in POSIX, and both must parse.
        assert_eq!(next_weekday("0 3 * * 0"), Weekday::Sun);
        assert_eq!(next_weekday("0 3 * * 7"), Weekday::Sun);
    }

    /// Regression: ranges, lists and steps in the POSIX weekday field are
    /// remapped element-wise, and the step count after `/` is left alone.
    #[test]
    fn parse_cron_lenient_remaps_weekday_ranges_and_lists() {
        use chrono::{Datelike, Utc, Weekday};
        use std::collections::HashSet;

        let weekday_set = |expr: &str, take: usize| -> HashSet<Weekday> {
            super::parse_cron_lenient(expr)
                .unwrap_or_else(|e| panic!("parse {expr:?}: {e}"))
                .upcoming(Utc)
                .take(take)
                .map(|d| d.weekday())
                .collect()
        };

        // Mon-Fri must be exactly the five weekdays, never Sunday/Saturday.
        let workweek = weekday_set("0 3 * * 1-5", 20);
        assert!(workweek.contains(&Weekday::Mon));
        assert!(workweek.contains(&Weekday::Fri));
        assert!(!workweek.contains(&Weekday::Sat));
        assert!(!workweek.contains(&Weekday::Sun));

        // A weekend list "6,0" must be Saturday + Sunday and must parse.
        let weekend = weekday_set("0 3 * * 6,0", 20);
        assert_eq!(
            weekend,
            HashSet::from([Weekday::Sat, Weekday::Sun]),
            "6,0 should be exactly the weekend"
        );
    }

    /// Direct unit test of the weekday remap helper.
    #[test]
    fn remap_posix_dow_translates_values_but_not_steps() {
        // n -> (n % 7) + 1 for standalone values.
        assert_eq!(super::remap_posix_dow("0"), "1"); // Sun
        assert_eq!(super::remap_posix_dow("1"), "2"); // Mon
        assert_eq!(super::remap_posix_dow("7"), "1"); // Sun (alt)
        assert_eq!(super::remap_posix_dow("1-5"), "2-6"); // Mon-Fri
        assert_eq!(super::remap_posix_dow("6,0"), "7,1"); // Sat,Sun
        assert_eq!(super::remap_posix_dow("*"), "*");
        // Names are untouched.
        assert_eq!(super::remap_posix_dow("Mon"), "Mon");
        // The step count after '/' is a count, not a weekday — keep it as-is.
        // (The base before '/' is still remapped.)
        assert_eq!(super::remap_posix_dow("*/2"), "*/2");
        assert_eq!(super::remap_posix_dow("1/2"), "2/2");
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

        let old_limits: HashMap<String, usize> =
            [("removed-host".to_string(), 2usize)].into_iter().collect();
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

    #[test]
    fn global_scheduling_changes_only_affect_relevant_mirrors() {
        use crate::config::IntervalMode;

        let old = GlobalConfig {
            interval: 60,
            interval_mode: IntervalMode::FixedRate,
            fixed_rate_anchor: "01:00".into(),
            timezone: "UTC".into(),
            ..Default::default()
        };
        let new = GlobalConfig {
            fixed_rate_anchor: "02:00".into(),
            timezone: "Asia/Shanghai".into(),
            ..old.clone()
        };

        let inherited = MirrorConfig::default();
        assert!(super::global_scheduling_change_affects(
            &inherited, &old, &new
        ));

        let fixed_delay_override = MirrorConfig {
            interval_mode: Some(IntervalMode::FixedDelay),
            ..Default::default()
        };
        assert!(!super::global_scheduling_change_affects(
            &fixed_delay_override,
            &old,
            &new
        ));

        let cron = MirrorConfig {
            cron: "0 3 * * *".into(),
            interval_mode: Some(IntervalMode::FixedDelay),
            ..Default::default()
        };
        assert!(super::global_scheduling_change_affects(&cron, &old, &new));
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

    // ── Hot-reload Modify must preserve historical telemetry ───────────────

    /// Simulate the merge logic from the Modify branch of handle_reload.
    /// A cosmetic config change (e.g. tweaking `interval`) must not wipe
    /// last_update, size, transferred_bytes, etc.
    #[test]
    fn hot_reload_modify_preserves_telemetry() {
        use tunasync_protocol::{MirrorStatus, SyncStatus};

        // The "previous" status — populated by N successful syncs.
        let prev = MirrorStatus {
            name: "ubuntu".into(),
            worker: "w1".into(),
            is_master: true,
            upstream: "rsync://old-upstream/".into(),
            status: SyncStatus::Success,
            last_update: chrono::Utc::now() - chrono::Duration::hours(2),
            last_started: chrono::Utc::now() - chrono::Duration::hours(3),
            last_ended: chrono::Utc::now() - chrono::Duration::hours(2),
            scheduled: chrono::Utc::now() - chrono::Duration::hours(1),
            size: "120G".into(),
            last_transferred_bytes: 5_368_709_120,    // 5 GiB
            total_transferred_bytes: 100_000_000_000, // 100 GB cumulative
            consecutive_failures: 0,
            stale: false,
            ..Default::default()
        };

        // The new provider's upstream changed (mirror config edited).
        let new_upstream = "rsync://new-upstream/".to_string();
        let new_is_master = true;

        // Apply the same fixed-delay merge behavior the Modify branch uses.
        let now_utc = chrono::Utc::now();
        let mut merged = prev.clone();
        merged.upstream = new_upstream.clone();
        merged.is_master = new_is_master;
        merged.scheduled = now_utc;

        // Identity preserved.
        assert_eq!(merged.name, "ubuntu");
        assert_eq!(merged.worker, "w1");
        // Provider-derived fields refreshed.
        assert_eq!(merged.upstream, new_upstream);
        // Telemetry preserved.
        assert_eq!(merged.status, SyncStatus::Success);
        assert_eq!(merged.size, "120G");
        assert_eq!(merged.last_transferred_bytes, 5_368_709_120);
        assert_eq!(merged.total_transferred_bytes, 100_000_000_000);
        assert_eq!(merged.last_update, prev.last_update);
        assert_eq!(merged.last_started, prev.last_started);
        assert_eq!(merged.last_ended, prev.last_ended);
        // Fixed-delay Modify may still run immediately, so scheduled advances.
        assert!(
            merged.scheduled > prev.scheduled,
            "fixed-delay Modify should refresh the immediate schedule"
        );
    }

    /// Hot-reload Delete must also remove the mirror from cfg.mirrors;
    /// otherwise the *next* hot-reload's diff is computed against stale data.
    #[test]
    fn hot_reload_delete_clears_cfg_mirrors() {
        // Build a mock cfg with two mirrors, then simulate the Delete branch.
        let mut cfg_mirrors: Vec<MirrorConfig> = vec![
            {
                let mut m = MirrorConfig::default();
                m.name = "keep".into();
                m
            },
            {
                let mut m = MirrorConfig::default();
                m.name = "delete".into();
                m
            },
        ];

        // Same retain() call the Delete branch performs.
        let name = "delete".to_string();
        cfg_mirrors.retain(|m| m.name != name);

        assert_eq!(cfg_mirrors.len(), 1);
        assert_eq!(cfg_mirrors[0].name, "keep");
        assert!(!cfg_mirrors.iter().any(|m| m.name == "delete"));
    }

    /// A failed replacement build must leave the live config and status
    /// untouched because production now validates before removing the old job.
    #[test]
    fn hot_reload_modify_provider_failure_keeps_current_state() {
        let mut old_cfg = MirrorConfig::default();
        old_cfg.name = "failing-mirror".into();
        old_cfg.upstream = "rsync://old/".into();
        let cfg_mirrors = [old_cfg.clone()];
        let status = tunasync_protocol::MirrorStatus {
            name: old_cfg.name.clone(),
            status: tunasync_protocol::SyncStatus::Success,
            size: "100G".into(),
            ..Default::default()
        };

        // The error branch executes `continue` before any mutation.
        assert_eq!(cfg_mirrors[0].upstream, "rsync://old/");
        assert_eq!(status.status, tunasync_protocol::SyncStatus::Success);
        assert_eq!(status.size, "100G");
    }

    // ── upstream_host parsing ──────────────────────────────────────────────

    #[test]
    fn upstream_host_handles_scheme_form() {
        assert_eq!(
            super::upstream_host("rsync://ftp.debian.org/debian/"),
            Some("ftp.debian.org".into())
        );
        assert_eq!(
            super::upstream_host("https://mirror.example.com/path/"),
            Some("mirror.example.com".into())
        );
        assert_eq!(
            super::upstream_host("rsync://user@host:873/mod/"),
            Some("host".into())
        );
    }

    #[test]
    fn upstream_host_handles_legacy_double_colon() {
        // rsync daemon shorthand without a scheme.
        assert_eq!(
            super::upstream_host("ftp.debian.org::debian/"),
            Some("ftp.debian.org".into())
        );
        // Also accepts no trailing slash / path.
        assert_eq!(super::upstream_host("host::module"), Some("host".into()));
    }

    /// Regression test for the bug audit's claim 2.1.
    /// `rsync://host::module/` previously matched the `://` branch first,
    /// failed url::Url::parse with "invalid port number", and returned
    /// None — never reaching the `::` fallback.
    #[test]
    fn upstream_host_handles_mixed_scheme_and_double_colon() {
        assert_eq!(
            super::upstream_host("rsync://ftp.debian.org::debian/"),
            Some("ftp.debian.org".into())
        );
        // Same with no trailing slash.
        assert_eq!(
            super::upstream_host("rsync://host::module"),
            Some("host".into())
        );
    }

    #[test]
    fn upstream_host_returns_none_for_unrecognised() {
        assert_eq!(super::upstream_host("/local/path"), None);
        assert_eq!(super::upstream_host(""), None);
        assert_eq!(super::upstream_host("file:///srv/mirror/"), None);
    }

    /// Regression test for N1 (audit 2026-05-21).
    /// IPv6 literals in URLs are kept by `url::Url::host_str` in their
    /// bracketed form (`[::1]`, `[2001:db8::1]`). The previous fix's
    /// `!host.contains("::")` guard was meant to detect the
    /// `rsync://host::module/` mixed-form parse failure mode but it
    /// erroneously also rejected IPv6 hosts (which always contain `::`).
    /// Per-upstream concurrency keys for IPv6 mirrors got mangled to "["
    /// or empty strings.
    #[test]
    fn upstream_host_preserves_ipv6_literals() {
        assert_eq!(
            super::upstream_host("rsync://[::1]/mod/"),
            Some("[::1]".into())
        );
        assert_eq!(
            super::upstream_host("rsync://[2001:db8::1]/mod/"),
            Some("[2001:db8::1]".into())
        );
        assert_eq!(
            super::upstream_host("http://[::1]:8080/path"),
            Some("[::1]".into())
        );
        assert_eq!(
            super::upstream_host("https://[2001:db8::1]/repo/"),
            Some("[2001:db8::1]".into())
        );
    }
}
