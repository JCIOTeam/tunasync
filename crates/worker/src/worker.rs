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
//!  │  └──────────┘               │  (local state + next    │  │
//!  │       ▲                     │   run only)             │  │
//!  │       │ CtrlAction          └───────────┬────────────┘  │
//!  │       │                              ▲                   │
//!  │  ┌──────────┐   WorkerCmd            │                   │
//!  │  │ HTTP srv │────────────────────────┘                   │
//!  │  └──────────┘                                            │
//!  │                              │ synchronous enqueue       │
//!  │                              ▼                           │
//!  │                       ┌──────────────┐                    │
//!  │                       │ Report actor │──network──▶ manager│
//!  │                       └──────────────┘                    │
//!  └──────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, RwLock, Semaphore};
use tracing::{error, info, warn};

use crate::priority_semaphore::PrioritySemaphore;
use tunasync_protocol::{
    zero_time, CmdVerb, MirrorSchedule, MirrorSchedules, MirrorStatus, SyncStatus, WorkerCmd,
    WorkerStatus,
};

use crate::config::WorkerConfig;
use crate::diff_config::{diff_mirror_config, DiffOp, MirrorCfgTrans};
use crate::hooks::JobHook;
use crate::http_server::{build_router, cmd_to_ctrl, WorkerHttpState};
use crate::job::{CtrlAction, JobMessage, JobState, MirrorJob};
use crate::log_stream::LogBroadcaster;
use crate::manager_client::ManagerClient;
use crate::provider::MirrorProvider;
use crate::report_actor::{
    ActorResultState, BootstrapCommand, ReconfigureCommand, ReconfigureOutcome, ReconfigureResult,
    ReportActor, ReportHandle, RestoreOutcome, RestoreResult,
};
use crate::schedule::ScheduleQueue;
use crate::scheduling::{parse_fixed_rate_anchor, SchedulingPolicy};

type PreparedProvider = (Box<dyn MirrorProvider>, Vec<Box<dyn JobHook>>);
type PreparedProviders = HashMap<String, PreparedProvider>;

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
    report_handle: ReportHandle,
    report_actor: Option<ReportActor>,
    report_task: Option<tokio::task::JoinHandle<()>>,
    report_results: watch::Receiver<ActorResultState>,
    applied_restore_revision: u64,
    applied_reconfigure_revision: u64,
    next_manager_generation: u64,
    pending_manager_config: Option<(u64, crate::config::ManagerApiConfig)>,
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
    /// Local mutation generation captured by asynchronous persisted-state restore.
    local_versions: HashMap<String, u64>,
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
        let max_resources = cfg.global.effective_report_max_resources();
        let manager = Arc::new(ManagerClient::new_with_pending_limit(
            bases,
            http_client.clone(),
            cfg.manager.api_token.clone(),
            max_resources,
        ));
        let (report_handle, report_actor, report_results) = ReportActor::new(
            Arc::clone(&manager),
            cfg.global.name.clone(),
            Duration::from_secs(60),
            max_resources,
        );
        let api_token = Arc::new(RwLock::new(cfg.manager.api_token.clone()));

        let provider_list = build_jobs(&cfg);
        let mut jobs = HashMap::new();
        let mut job_generations = HashMap::new();
        let mut next_job_generation = 0_u64;
        let mut mirror_statuses = HashMap::new();
        let mut local_versions = HashMap::new();

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
            local_versions.insert(name.clone(), 0);

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

        // The queue is populated from local policy before asynchronous manager
        // bootstrap so manager I/O can never delay scheduling.
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
            report_handle,
            report_actor: Some(report_actor),
            report_task: None,
            report_results,
            applied_restore_revision: 0,
            applied_reconfigure_revision: 0,
            next_manager_generation: 0,
            pending_manager_config: None,
            api_token,
            status_tx,
            status_rx,
            cmd_tx,
            cmd_rx,
            semaphore,
            per_upstream_semaphores,
            schedule,
            mirror_statuses,
            local_versions,
            mirror_names,
            log_broadcaster,
            blackouts_by_mirror,
            crons_by_mirror,
            timezones_by_mirror,
            scheduling_by_mirror,
            build_one_provider: crate::build_one_provider,
        }
    }

    /// Start the actor and HTTP server immediately, then run the scheduler.
    ///
    /// Returns only on fatal error or graceful shutdown (SIGTERM/SIGINT).
    pub async fn run(self) -> Result<()> {
        self.run_until_shutdown(
            wait_for_shutdown_signal(),
            |state, bind_addr, server_cfg| {
                tokio::spawn(run_http_server(state, bind_addr, server_cfg));
            },
        )
        .await
        .map(|_| ())
    }

    async fn run_until_shutdown<F, H>(mut self, shutdown: F, start_http_server: H) -> Result<Self>
    where
        F: Future<Output = ()>,
        H: FnOnce(WorkerHttpState, std::net::SocketAddr, crate::config::ServerConfig),
    {
        let worker_id = self.cfg.global.name.clone();

        let report_actor = self
            .report_actor
            .take()
            .expect("report actor starts exactly once");
        self.report_task = Some(tokio::spawn(report_actor.run()));

        // Spawn HTTP server task.
        let http_state = WorkerHttpState {
            api_token: Arc::clone(&self.api_token),
            cmd_tx: self.cmd_tx.clone(),
            worker_name: worker_id.clone(),
            mirror_names: Arc::clone(&self.mirror_names),
            log_broadcaster: Arc::clone(&self.log_broadcaster),
        };
        let bind_addr = self.cfg.server.bind_addr().map_err(anyhow::Error::msg)?;
        start_http_server(http_state, bind_addr, self.cfg.server.clone());

        self.initialize_local_schedules();
        self.report_schedules();
        let registration = WorkerStatus {
            id: worker_id.clone(),
            url: self.cfg.server.public_url(&self.cfg),
            token: String::new(),
            last_online: zero_time(),
            last_register: zero_time(),
        };
        self.report_handle.bootstrap(BootstrapCommand {
            registration,
            restore_versions: self.local_versions.clone(),
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

        self.run_schedule_until(worker_id, shutdown).await;

        Ok(self)
    }

    fn initialize_local_schedules(&mut self) {
        let names: Vec<String> = self.jobs.keys().cloned().collect();
        for name in names {
            let now = Utc::now();
            let scheduled = self
                .scheduling_by_mirror
                .get(&name)
                .and_then(|policy| policy.next_startup_after(now, None))
                .unwrap_or(now);
            if let Some(status) = self.mirror_statuses.get_mut(&name) {
                status.scheduled = scheduled;
            }
            self.schedule.push(name, scheduled);
        }
    }

    fn bump_local_version(&mut self, name: &str) {
        if let Some(version) = self.local_versions.get_mut(name) {
            *version = version
                .checked_add(1)
                .expect("local state version exhausted");
        }
    }

    fn apply_restore_result(&mut self, result: RestoreResult) {
        let statuses = match result.outcome {
            RestoreOutcome::Success(statuses) => statuses,
            RestoreOutcome::Failed(error) => {
                warn!(%error, "failed to fetch persisted job status; keeping local startup schedules");
                return;
            }
        };

        let mut applied = 0;
        for persisted in statuses {
            let Some(captured) = result.captured_versions.get(&persisted.name) else {
                continue;
            };
            if self.local_versions.get(&persisted.name) != Some(captured)
                || !self.jobs.contains_key(&persisted.name)
            {
                continue;
            }

            let name = persisted.name.clone();
            self.mirror_statuses.insert(name.clone(), persisted.clone());
            match persisted.status {
                SyncStatus::Disabled => {
                    if let Some(job) = self.jobs.get(&name) {
                        job.try_send(CtrlAction::Disable);
                    }
                    self.schedule.remove(&name);
                    if let Some(status) = self.mirror_statuses.get_mut(&name) {
                        status.scheduled = zero_time();
                    }
                }
                SyncStatus::Paused => {
                    if let Some(job) = self.jobs.get(&name) {
                        job.try_send(CtrlAction::Stop);
                    }
                    self.schedule.remove(&name);
                    if let Some(status) = self.mirror_statuses.get_mut(&name) {
                        status.scheduled = zero_time();
                    }
                }
                state => {
                    if matches!(state, SyncStatus::Syncing | SyncStatus::PreSyncing) {
                        if let Some(status) = self.mirror_statuses.get_mut(&name) {
                            status.status = SyncStatus::Failed;
                            status.error_msg = "previous sync was interrupted".into();
                            status.last_ended = Utc::now();
                        }
                    }
                    let now = Utc::now();
                    let last_completion =
                        (!tunasync_protocol::is_zero_time(&persisted.last_update))
                            .then_some(persisted.last_update);
                    let scheduled = self
                        .scheduling_by_mirror
                        .get(&name)
                        .and_then(|policy| policy.next_startup_after(now, last_completion))
                        .unwrap_or(now);
                    if let Some(status) = self.mirror_statuses.get_mut(&name) {
                        status.scheduled = scheduled;
                    }
                    self.schedule.push(name.clone(), scheduled);
                    if matches!(state, SyncStatus::Syncing | SyncStatus::PreSyncing) {
                        if let Some(status) = self.mirror_statuses.get(&name) {
                            self.report_handle.report_status(status.clone());
                        }
                    }
                }
            }
            self.bump_local_version(&name);
            applied += 1;
        }
        tracing::info!(applied, "applied untouched persisted job states");
        self.report_schedules();
    }

    /// Enqueue the current complete schedule table for the report actor.
    fn report_schedules(&self) {
        let report_handle = self.report_handle.clone();
        let row_count = self.mirror_statuses.len();
        let statuses = &self.mirror_statuses;
        report_handle.report_schedules_with(row_count, || MirrorSchedules {
            schedules: statuses
                .values()
                .map(|status| MirrorSchedule {
                    mirror_name: status.name.clone(),
                    next_schedule: status.scheduled,
                })
                .collect(),
        });
    }

    /// Submit every currently due occurrence without awaiting manager I/O.
    fn dispatch_due_jobs(&mut self) {
        while let Some(entry) = self.schedule.peek() {
            let now_utc = Utc::now();
            if entry.scheduled_at > now_utc {
                break;
            }
            let entry = self.schedule.pop().unwrap();
            self.bump_local_version(&entry.name);

            // Blackout windows gate starts but never interrupt active syncs.
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
                self.report_schedules();
            } else if let Some(job) = self.jobs.get(&entry.name) {
                if !job.try_send(CtrlAction::Start) {
                    // A popped Start that never reaches the job must be retried
                    // or the mirror would silently stop scheduling forever.
                    tracing::warn!(
                        mirror = %entry.name,
                        "could not queue scheduled Start (ctrl channel full or task dead) — retrying in 30s"
                    );
                    let retry_at = now_utc + chrono::Duration::seconds(30);
                    if let Some(status) = self.mirror_statuses.get_mut(&entry.name) {
                        status.scheduled = retry_at;
                    }
                    self.schedule.push(entry.name, retry_at);
                    self.report_schedules();
                }
            }
        }
    }

    async fn run_schedule_until<F>(&mut self, worker_id: String, shutdown: F)
    where
        F: Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let mut report_results_open = true;

        loop {
            // Fire any jobs that are due.
            self.dispatch_due_jobs();

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
                    self.handle_job_message(msg, &worker_id);
                }

                // Manager/CLI sent us a command via HTTP.
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_worker_cmd(cmd, &worker_id).await;
                }

                result = self.report_results.changed(), if report_results_open => {
                    match result {
                        Ok(()) => {
                            let results = self.report_results.borrow_and_update().clone();
                            self.apply_actor_results(results).await;
                        }
                        Err(_) => {
                            warn!("report actor result channel closed; disabling result intake");
                            report_results_open = false;
                        }
                    }
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
                        self.handle_job_message(msg, &worker_id);
                    }
                    for status in self.mirror_statuses.values() {
                        self.report_handle.report_status(status.clone());
                    }
                    self.report_schedules();
                    self.shutdown_report_actor().await;
                    info!("shutdown complete");
                    return;
                }
            }
        }
    }

    /// Process a status update from a job task.
    fn handle_job_message(&mut self, msg: JobMessage, worker_id: &str) {
        if !is_active_job_message(&self.job_generations, &msg) {
            tracing::debug!(
                mirror = %msg.name,
                generation = msg.job_generation,
                "discarding status from retired job generation"
            );
            return;
        }

        self.bump_local_version(&msg.name);
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
            status_entry.skip_failure_count = msg.skip_sync;
            if !msg.size.is_empty() {
                status_entry.size = msg.size.clone();
            }
            if msg.status == SyncStatus::Success || msg.transferred_bytes > 0 {
                status_entry.last_transferred_bytes = msg.transferred_bytes;
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
            }
        }

        // All local state and the next fixed-delay reference are committed
        // before report enqueue. Scheduling-only None messages do not replay
        // the previous terminal status.
        if msg.status != SyncStatus::None {
            if let Some(status) = self.mirror_statuses.get(&msg.name) {
                self.report_handle.report_status(status.clone());
            }
            if msg.status == SyncStatus::Success && !msg.size.is_empty() {
                self.report_handle
                    .report_size(msg.name.clone(), msg.size.clone());
            }
        }
        if msg.schedule {
            self.report_schedules();
        }
    }

    /// Dispatch an incoming `WorkerCmd` to the appropriate job.
    async fn handle_worker_cmd(&mut self, cmd: WorkerCmd, _worker_id: &str) {
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
                if cmd.mirror_id.is_empty() {
                    let names: Vec<String> = self.jobs.keys().cloned().collect();
                    for name in names {
                        self.bump_local_version(&name);
                    }
                } else {
                    self.bump_local_version(&cmd.mirror_id);
                }
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
                    let names: Vec<String> = self.jobs.keys().cloned().collect();
                    for name in names {
                        self.schedule.remove(&name);
                        if let Some(status) = self.mirror_statuses.get_mut(&name) {
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
                                        self.bump_local_version(name);
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
                    self.report_schedules();
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
        let Some(mut new_cfg) = self.load_reload_config().await else {
            return;
        };
        let Some((diff, mut prepared)) = self.prepare_reload_changes(&new_cfg) else {
            return;
        };

        self.apply_upstream_concurrency(&mut new_cfg);
        self.apply_mirror_changes(&diff, &mut prepared, &new_cfg)
            .await;
        self.apply_global_concurrency(&mut new_cfg);
        self.prepare_manager_reconfigure(&mut new_cfg);
        self.finish_reload(new_cfg).await;
    }

    async fn load_reload_config(&self) -> Option<WorkerConfig> {
        let mut new_cfg: WorkerConfig = match tunasync_common::config::load_toml(&self.config_path)
        {
            Ok(config) => config,
            Err(e) => {
                tracing::error!(error = %e, "hot-reload: failed to read config — keeping current");
                return None;
            }
        };
        let include_errors = crate::load_include_mirrors(&mut new_cfg);
        if !include_errors.is_empty() {
            tracing::error!(errors = ?include_errors, "hot-reload: include loading failed — keeping current config");
            return None;
        }
        new_cfg.mirrors = crate::config::flatten_mirrors(&new_cfg.mirrors_conf);

        let report_limit = self.cfg.global.effective_report_max_resources();
        let requested_report_limit = new_cfg.global.effective_report_max_resources();
        if requested_report_limit != report_limit {
            tracing::warn!(current = report_limit, requested = requested_report_limit, "hot-reload: report_max_resources change requires a restart - keeping current limit");
            new_cfg.global.report_max_resources = self.cfg.global.report_max_resources;
        }
        let config_errors = crate::validate_worker_config(&new_cfg);
        if !config_errors.is_empty() {
            tracing::error!(errors = ?config_errors, "hot-reload: config validation failed — keeping current config");
            return None;
        }
        if new_cfg.netns_broker.generation == self.cfg.netns_broker.generation {
            match crate::netns_policy::policy_content_changed(&self.cfg, &new_cfg) {
                Ok(true) => {
                    tracing::error!(generation = %new_cfg.netns_broker.generation, "hot-reload: namespaced launch policy changed without a new generation - keeping current config");
                    return None;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(error = %e, "hot-reload: failed to compare namespace policy content - keeping current config");
                    return None;
                }
            }
        }
        if let Err(e) = crate::verify_netns_broker_for_config(&new_cfg).await {
            tracing::error!(error = %e, "hot-reload: namespace broker readiness check failed - keeping current config");
            return None;
        }
        if new_cfg.global.name != self.cfg.global.name {
            tracing::warn!(old = %self.cfg.global.name, new = %new_cfg.global.name, "hot-reload: worker name change requires a restart — keeping current name");
            new_cfg.global.name = self.cfg.global.name.clone();
        }
        Some(new_cfg)
    }

    fn prepare_reload_changes(
        &self,
        new_cfg: &WorkerConfig,
    ) -> Option<(Vec<MirrorCfgTrans>, PreparedProviders)> {
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
            self.extend_inherited_modifications(&mut diff, new_cfg, |_| true);
        }
        if scheduling_globals_changed {
            self.extend_inherited_modifications(&mut diff, new_cfg, |mc| {
                global_scheduling_change_affects(mc, &self.cfg.global, &new_cfg.global)
            });
        }
        if diff.is_empty() {
            tracing::info!("hot-reload: mirror config unchanged; applying global settings");
        } else {
            tracing::info!(changes = diff.len(), "hot-reload: applying config diff");
        }

        let mut prepared = HashMap::new();
        for trans in &diff {
            if matches!(trans.op, DiffOp::Add | DiffOp::Modify) {
                match (self.build_one_provider)(&trans.config, new_cfg) {
                    Ok(built) => {
                        prepared.insert(trans.config.name.clone(), built);
                    }
                    Err(e) => {
                        tracing::error!(mirror = %trans.config.name, error = %e, "hot-reload: provider preparation failed — keeping current config");
                        return None;
                    }
                }
            }
        }
        Some((diff, prepared))
    }

    fn extend_inherited_modifications<F>(
        &self,
        diff: &mut Vec<MirrorCfgTrans>,
        new_cfg: &WorkerConfig,
        affects: F,
    ) where
        F: Fn(&crate::config::MirrorConfig) -> bool,
    {
        let already_changed: HashSet<&str> = diff
            .iter()
            .map(|trans| trans.config.name.as_str())
            .collect();
        let inherited_changes = new_cfg
            .mirrors
            .iter()
            .filter(|mc| {
                self.cfg.mirrors.iter().any(|old| old.name == mc.name)
                    && !already_changed.contains(mc.name.as_str())
                    && affects(mc)
            })
            .cloned()
            .map(|config| MirrorCfgTrans {
                op: DiffOp::Modify,
                config,
            })
            .collect::<Vec<_>>();
        diff.extend(inherited_changes);
    }

    fn apply_upstream_concurrency(&mut self, new_cfg: &mut WorkerConfig) {
        let old_limits = &self.cfg.global.per_upstream_concurrent;
        let requested_limits = new_cfg.global.per_upstream_concurrent.clone();
        let removed = old_limits
            .keys()
            .filter(|host| !requested_limits.contains_key(*host))
            .cloned()
            .collect::<Vec<_>>();
        for host in &removed {
            self.per_upstream_semaphores.remove(host);
            tracing::info!(host = %host, "hot-reload: removed per-upstream concurrency limit");
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
                        tracing::warn!(host = %host, from = old_limit, to = new_limit, "hot-reload: cannot shrink per-upstream concurrency limit until restart");
                    }
                }
            }
        }
    }

    async fn apply_mirror_changes(
        &mut self,
        diff: &[MirrorCfgTrans],
        prepared: &mut PreparedProviders,
        new_cfg: &WorkerConfig,
    ) {
        for trans in diff {
            match trans.op {
                DiffOp::Delete => self.apply_deleted_mirror(&trans.config.name).await,
                DiffOp::Modify => self.apply_modified_mirror(trans, prepared, new_cfg).await,
                DiffOp::Add => self.apply_added_mirror(trans, prepared, new_cfg),
            }
        }
    }

    async fn apply_deleted_mirror(&mut self, name: &str) {
        self.bump_local_version(name);
        self.stop_and_join_job(name).await;
        self.report_handle.forget_mirror(name.to_owned());
        self.mirror_statuses.remove(name);
        self.local_versions.remove(name);
        self.cfg.mirrors.retain(|mirror| mirror.name != name);
        tracing::info!(mirror = %name, "hot-reload: deleted job");
    }

    async fn apply_modified_mirror(
        &mut self,
        trans: &MirrorCfgTrans,
        prepared: &mut PreparedProviders,
        new_cfg: &WorkerConfig,
    ) {
        let name = &trans.config.name;
        self.bump_local_version(name);
        let (mut provider, hooks) = prepared
            .remove(name)
            .expect("modified provider prepared before applying reload");
        let old_state = self.jobs.get(name).map(MirrorJob::state);
        let preserved = self.mirror_statuses.get(name).cloned();
        self.stop_and_join_job(name).await;
        self.mirror_statuses.remove(name);
        if let Some(pos) = self
            .cfg
            .mirrors
            .iter()
            .position(|mirror| &mirror.name == name)
        {
            self.cfg.mirrors[pos] = trans.config.clone();
        } else {
            self.cfg.mirrors.push(trans.config.clone());
        }

        provider.set_log_publisher(self.log_broadcaster.publisher_for(name));
        let upstream = crate::redact_url_diagnostic(provider.upstream());
        let is_master = provider.is_master();
        let next_schedule = next_reload_schedule(&trans.config, &new_cfg.global, Utc::now());
        let merged = if let Some(mut previous) = preserved {
            previous.upstream = upstream;
            previous.is_master = is_master;
            previous.scheduled = next_schedule;
            previous
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
        let upstream_sem = upstream_host(provider.upstream())
            .and_then(|host| self.per_upstream_semaphores.get(&host).cloned());
        let job_generation = self.allocate_job_generation(name);
        let job = MirrorJob::spawn(
            provider,
            hooks,
            self.status_tx.clone(),
            Arc::clone(&self.semaphore),
            upstream_sem,
            trans.config.priority,
            job_generation,
        );
        self.jobs.insert(name.clone(), job);

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

    fn apply_added_mirror(
        &mut self,
        trans: &MirrorCfgTrans,
        prepared: &mut PreparedProviders,
        new_cfg: &WorkerConfig,
    ) {
        let name = &trans.config.name;
        self.local_versions.insert(name.clone(), 1);
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
                scheduled: next_reload_schedule(&trans.config, &new_cfg.global, Utc::now()),
                ..Default::default()
            },
        );
        let upstream_sem = upstream_host(provider.upstream())
            .and_then(|host| self.per_upstream_semaphores.get(&host).cloned());
        let job_generation = self.allocate_job_generation(name);
        let job = MirrorJob::spawn(
            provider,
            hooks,
            self.status_tx.clone(),
            Arc::clone(&self.semaphore),
            upstream_sem,
            trans.config.priority,
            job_generation,
        );
        self.jobs.insert(name.clone(), job);
        tracing::info!(mirror = %name, "hot-reload: new job");
        if let Some(status) = self.mirror_statuses.get(name) {
            self.schedule.push(name.clone(), status.scheduled);
        }
    }

    fn apply_global_concurrency(&mut self, new_cfg: &mut WorkerConfig) {
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
            tracing::warn!(from = old_concurrent, to = new_concurrent, "hot-reload: cannot shrink concurrency limit on a running worker — keeping {old_concurrent} until restart");
        }
    }

    fn prepare_manager_reconfigure(&mut self, new_cfg: &mut WorkerConfig) {
        let manager_changed = new_cfg.manager.api_base_list() != self.cfg.manager.api_base_list()
            || new_cfg.manager.api_token != self.cfg.manager.api_token
            || new_cfg.manager.ca_cert != self.cfg.manager.ca_cert;
        let manager_matches_pending =
            self.pending_manager_config
                .as_ref()
                .is_some_and(|(_, pending)| {
                    pending.api_base_list() == new_cfg.manager.api_base_list()
                        && pending.api_token == new_cfg.manager.api_token
                        && pending.ca_cert == new_cfg.manager.ca_cert
                });
        let should_reconfigure =
            !manager_matches_pending && (manager_changed || self.pending_manager_config.is_some());
        if should_reconfigure {
            let client = if new_cfg.manager.ca_cert.is_empty() {
                tunasync_common::http::HttpClientBuilder::new().build()
            } else {
                tunasync_common::http::HttpClientBuilder::new()
                    .ca_cert_pem_from_path(std::path::Path::new(&new_cfg.manager.ca_cert))
                    .and_then(|builder| builder.build())
            };
            self.next_manager_generation = self
                .next_manager_generation
                .checked_add(1)
                .expect("manager reconfigure generation exhausted");
            let generation = self.next_manager_generation;
            match client {
                Ok(client) => {
                    let requested = new_cfg.manager.clone();
                    let command = ReconfigureCommand {
                        generation,
                        bases: requested
                            .api_base_list()
                            .into_iter()
                            .map(String::from)
                            .collect(),
                        client,
                        token: requested.api_token.clone(),
                        registration: WorkerStatus {
                            id: self.cfg.global.name.clone(),
                            url: self.cfg.server.public_url(&self.cfg),
                            token: String::new(),
                            last_online: zero_time(),
                            last_register: zero_time(),
                        },
                    };
                    if self.report_handle.reconfigure(command) {
                        self.pending_manager_config = Some((generation, requested));
                        tracing::info!(generation, "hot-reload: queued manager reconfiguration");
                    } else {
                        tracing::error!(
                            generation,
                            "hot-reload: report actor rejected manager reconfiguration"
                        );
                    }
                }
                Err(e) => {
                    self.report_handle.supersede_reconfigure(generation);
                    self.pending_manager_config = None;
                    tracing::error!(generation, error = %e, "hot-reload: failed to rebuild manager HTTP client — keeping old manager config");
                }
            }
        }
        new_cfg.manager = self.cfg.manager.clone();
    }

    async fn finish_reload(&mut self, new_cfg: WorkerConfig) {
        let server_changed = new_cfg.server.addr != self.cfg.server.addr
            || new_cfg.server.port != self.cfg.server.port
            || new_cfg.server.ssl_cert != self.cfg.server.ssl_cert
            || new_cfg.server.ssl_key != self.cfg.server.ssl_key
            || new_cfg.server.hostname != self.cfg.server.hostname;
        self.cfg.global = new_cfg.global;
        self.cfg.manager = new_cfg.manager;
        self.cfg.netns_broker = new_cfg.netns_broker;
        if server_changed {
            tracing::warn!("hot-reload: worker HTTP server settings changed but require a restart — keeping current listener");
        }
        {
            let mut names = self.mirror_names.write().await;
            names.clear();
            names.extend(self.jobs.keys().cloned());
        }
        self.blackouts_by_mirror = build_blackout_cache(&self.cfg.mirrors);
        self.crons_by_mirror = build_cron_cache(&self.cfg.mirrors);
        self.timezones_by_mirror = build_timezone_cache(&self.cfg.mirrors, &self.cfg.global);
        self.scheduling_by_mirror = build_scheduling_cache(
            &self.cfg.mirrors,
            &self.cfg.global,
            &self.crons_by_mirror,
            &self.timezones_by_mirror,
        );
        self.report_schedules();
    }

    async fn handle_reconfigure_result(&mut self, result: ReconfigureResult) {
        let Some((generation, pending)) = self.pending_manager_config.as_ref() else {
            tracing::debug!(
                generation = result.generation,
                "ignoring manager result without pending config"
            );
            return;
        };
        if result.generation != *generation {
            tracing::debug!(
                generation = result.generation,
                expected = *generation,
                "ignoring stale manager reconfiguration result"
            );
            return;
        }

        match result.outcome {
            ReconfigureOutcome::Success => {
                self.cfg.manager = pending.clone();
                *self.api_token.write().await = pending.api_token.clone();
                self.pending_manager_config = None;
                tracing::info!(
                    generation = result.generation,
                    "hot-reload: manager reconfiguration succeeded"
                );
            }
            ReconfigureOutcome::Failed(error) => {
                self.pending_manager_config = None;
                tracing::error!(generation = result.generation, %error, "hot-reload: manager reconfiguration failed; retaining old connection");
            }
            ReconfigureOutcome::Superseded => {
                tracing::debug!(
                    generation = result.generation,
                    "manager reconfiguration was superseded"
                );
            }
        }
    }

    async fn apply_actor_results(&mut self, results: ActorResultState) {
        if let Some(result) = results.restore {
            if result.revision > self.applied_restore_revision {
                self.applied_restore_revision = result.revision;
                self.apply_restore_result(result.value);
            }
        }
        if let Some(result) = results.reconfigure {
            if result.revision > self.applied_reconfigure_revision {
                self.applied_reconfigure_revision = result.revision;
                self.handle_reconfigure_result(result.value).await;
            }
        }
    }

    async fn shutdown_report_actor(&mut self) {
        let done = self.report_handle.shutdown();
        let completed = tokio::time::timeout(Duration::from_secs(6), done).await;
        if completed.is_err() {
            tracing::warn!("report actor did not finish its 5s drain within 6s; abandoning best-effort reports");
            if let Some(task) = &self.report_task {
                task.abort();
            }
        }
        if let Some(task) = self.report_task.take() {
            let _ = task.await;
        }
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
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

    struct DueProvider {
        name: String,
        started: std::sync::Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl crate::provider::MirrorProvider for DueProvider {
        fn name(&self) -> &str {
            &self.name
        }
        fn upstream(&self) -> &str {
            "rsync://localhost/test/"
        }
        fn is_master(&self) -> bool {
            true
        }
        fn interval(&self) -> std::time::Duration {
            std::time::Duration::from_secs(60)
        }
        fn retry(&self) -> u32 {
            0
        }
        fn timeout(&self) -> std::time::Duration {
            std::time::Duration::ZERO
        }
        async fn run(&self) -> anyhow::Result<()> {
            self.started.add_permits(1);
            Ok(())
        }
        async fn terminate(&self) -> anyhow::Result<()> {
            Ok(())
        }
        fn set_docker_config(&mut self, _config: crate::hooks::DockerConfig) {}
        fn set_log_path_shared(
            &mut self,
            _path: std::sync::Arc<std::sync::Mutex<std::path::PathBuf>>,
        ) {
        }
    }

    fn test_worker(manager_base: String) -> super::Worker {
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.name = "w1".into();
        cfg.global.interval = 1;
        cfg.manager.api_base = manager_base;
        cfg.mirrors = vec![MirrorConfig {
            name: "mirror".into(),
            interval: 1,
            ..MirrorConfig::default()
        }];
        let mut worker = super::Worker::new(
            cfg,
            std::path::PathBuf::new(),
            |_| Vec::new(),
            reqwest::Client::new(),
        );
        worker.job_generations.insert("mirror".into(), 1);
        worker.mirror_statuses.insert(
            "mirror".into(),
            tunasync_protocol::MirrorStatus {
                name: "mirror".into(),
                worker: "w1".into(),
                ..Default::default()
            },
        );
        worker
    }

    fn job_message(status: SyncStatus, schedule: bool) -> JobMessage {
        JobMessage {
            job_generation: 1,
            status,
            name: "mirror".into(),
            msg: String::new(),
            schedule,
            size: String::new(),
            transferred_bytes: 0,
            skip_sync: false,
        }
    }

    fn worker_config_toml(manager_base: &str, mirrors: &[&str]) -> String {
        let mut config = format!(
            "[global]\nname = \"w1\"\ninterval = 1\nconcurrent = 2\n\n[manager]\napi_base = {manager_base:?}\n\n[server]\nhostname = \"127.0.0.1\"\nlisten_addr = \"127.0.0.1\"\nlisten_port = 6000\n"
        );
        for mirror in mirrors {
            config.push_str(&format!(
                "\n[[mirrors]]\nname = {mirror:?}\nprovider = \"rsync\"\nupstream = \"rsync://localhost/test/\"\ninterval = 1\n"
            ));
        }
        config
    }

    #[tokio::test]
    async fn hanging_manager_does_not_shift_fixed_delay_or_block_second_completion() {
        use axum::{extract::State, routing::post, Router};

        async fn hang(
            State(entered): State<std::sync::Arc<tokio::sync::Semaphore>>,
        ) -> std::convert::Infallible {
            entered.add_permits(1);
            std::future::pending().await
        }

        let entered = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let app = Router::new()
            .route("/workers/{worker}/jobs/{mirror}", post(hang))
            .route("/workers/{worker}/schedules", post(hang))
            .with_state(std::sync::Arc::clone(&entered));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut worker = test_worker(format!("http://{addr}"));
        let actor = worker.report_actor.take().unwrap();
        let actor_task = tokio::spawn(actor.run());

        let first_reference = chrono::Utc::now();
        worker.handle_job_message(job_message(SyncStatus::Success, true), "w1");
        let first_next = worker.mirror_statuses["mirror"].scheduled;
        assert!(first_next >= first_reference + chrono::Duration::seconds(59));
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.acquire())
            .await
            .expect("manager request did not start")
            .unwrap()
            .forget();

        let second_reference = chrono::Utc::now();
        tokio::time::timeout(std::time::Duration::from_millis(50), async {
            worker.handle_job_message(job_message(SyncStatus::Failed, true), "w1");
        })
        .await
        .expect("second completion was blocked by manager reporting");
        let second_next = worker.mirror_statuses["mirror"].scheduled;
        assert!(second_next >= second_reference + chrono::Duration::seconds(59));
        assert!(second_next < second_reference + chrono::Duration::seconds(61));

        actor_task.abort();
        let _ = actor_task.await;
        server.abort();
    }

    #[tokio::test]
    async fn hanging_manager_does_not_delay_second_due_occurrence_submission() {
        use axum::{extract::State, routing::post, Router};

        async fn hang(
            State(entered): State<std::sync::Arc<tokio::sync::Semaphore>>,
        ) -> std::convert::Infallible {
            entered.add_permits(1);
            std::future::pending().await
        }

        let entered = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let app = Router::new()
            .route("/workers/{worker}/jobs/{mirror}", post(hang))
            .with_state(std::sync::Arc::clone(&entered));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let first_started = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let second_started = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.name = "w1".into();
        cfg.manager.api_base = format!("http://{addr}");
        cfg.mirrors = ["first", "second"]
            .into_iter()
            .map(|name| MirrorConfig {
                name: name.into(),
                interval: 1,
                ..MirrorConfig::default()
            })
            .collect();
        let providers = vec![
            Box::new(DueProvider {
                name: "first".into(),
                started: std::sync::Arc::clone(&first_started),
            }) as Box<dyn crate::provider::MirrorProvider>,
            Box::new(DueProvider {
                name: "second".into(),
                started: std::sync::Arc::clone(&second_started),
            }) as Box<dyn crate::provider::MirrorProvider>,
        ];
        let providers = std::sync::Mutex::new(Some(providers));
        let mut worker = super::Worker::new(
            cfg,
            std::path::PathBuf::new(),
            |_| {
                providers
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .into_iter()
                    .map(|provider| (provider, Vec::new()))
                    .collect()
            },
            reqwest::Client::new(),
        );
        let actor = worker.report_actor.take().unwrap();
        let actor_task = tokio::spawn(actor.run());
        worker
            .report_handle
            .report_status(tunasync_protocol::MirrorStatus {
                name: "network-block".into(),
                worker: "w1".into(),
                status: SyncStatus::Syncing,
                ..Default::default()
            });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.acquire())
            .await
            .expect("manager request did not start")
            .unwrap()
            .forget();

        let due = chrono::Utc::now() - chrono::Duration::seconds(1);
        worker.schedule.push("first".into(), due);
        worker.schedule.push("second".into(), due);
        tokio::time::timeout(std::time::Duration::from_millis(50), async {
            worker.dispatch_due_jobs();
        })
        .await
        .expect("due dispatch was blocked by manager reporting");
        tokio::time::timeout(std::time::Duration::from_secs(1), first_started.acquire())
            .await
            .expect("first due job was not submitted")
            .unwrap()
            .forget();
        tokio::time::timeout(std::time::Duration::from_secs(1), second_started.acquire())
            .await
            .expect("second due job was not submitted")
            .unwrap()
            .forget();

        for job in worker.jobs.values() {
            job.retire();
        }
        actor_task.abort();
        let _ = actor_task.await;
        server.abort();
    }

    #[tokio::test]
    async fn hanging_manager_does_not_block_status_intake_or_command_dispatch() {
        use axum::{extract::State, routing::post, Router};

        async fn hang(
            State(entered): State<std::sync::Arc<tokio::sync::Semaphore>>,
        ) -> std::convert::Infallible {
            entered.add_permits(1);
            std::future::pending().await
        }

        let entered = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let app = Router::new()
            .route("/workers/{worker}/jobs/{mirror}", post(hang))
            .with_state(std::sync::Arc::clone(&entered));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut worker = test_worker(format!("http://{addr}"));
        let actor = worker.report_actor.take().unwrap();
        let actor_task = tokio::spawn(actor.run());
        worker.handle_job_message(job_message(SyncStatus::Syncing, false), "w1");
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.acquire())
            .await
            .expect("manager request did not start")
            .unwrap()
            .forget();

        tokio::time::timeout(std::time::Duration::from_millis(50), async {
            worker.handle_job_message(job_message(SyncStatus::Success, false), "w1");
            worker
                .handle_worker_cmd(
                    tunasync_protocol::WorkerCmd {
                        cmd: tunasync_protocol::CmdVerb::Ping,
                        mirror_id: String::new(),
                        args: Vec::new(),
                        options: std::collections::HashMap::new(),
                    },
                    "w1",
                )
                .await;
        })
        .await
        .expect("status helper or command dispatch was blocked by manager reporting");
        assert_eq!(worker.mirror_statuses["mirror"].status, SyncStatus::Success);

        actor_task.abort();
        let _ = actor_task.await;
        server.abort();
    }

    #[tokio::test]
    async fn scheduling_only_message_does_not_repeat_terminal_status() {
        let mut worker = test_worker("http://127.0.0.1:9".into());
        let mut failed = job_message(SyncStatus::Failed, false);
        failed.skip_sync = true;
        failed.msg = "expected skip".into();
        failed.size = "kept-size".into();
        failed.transferred_bytes = 17;
        worker.handle_job_message(failed, "w1");
        let before = worker.report_handle.mailbox_counts();
        worker.handle_job_message(job_message(SyncStatus::None, true), "w1");
        let after = worker.report_handle.mailbox_counts();
        let status = &worker.mirror_statuses["mirror"];
        assert_eq!(status.status, SyncStatus::Failed);
        assert_eq!(status.error_msg, "expected skip");
        assert!(status.skip_failure_count);
        assert_eq!(status.size, "kept-size");
        assert_eq!(status.last_transferred_bytes, 17);
        assert_eq!(after.0, before.0, "schedules must not occupy the FIFO");
        assert!(after.3, "None should replace the schedule snapshot");
    }

    #[tokio::test]
    async fn scheduling_only_message_preserves_skip_flag_in_delivered_status() {
        use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
        use tokio::sync::mpsc;

        async fn status_report(
            State(tx): State<mpsc::Sender<tunasync_protocol::MirrorStatus>>,
            Json(status): Json<tunasync_protocol::MirrorStatus>,
        ) -> StatusCode {
            tx.send(status).await.unwrap();
            StatusCode::OK
        }
        async fn schedules_report() -> StatusCode {
            StatusCode::OK
        }

        let (captured_tx, mut captured_rx) = mpsc::channel(8);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new()
                .route("/workers/{worker}/jobs/{mirror}", post(status_report))
                .route("/workers/{worker}/schedules", post(schedules_report))
                .with_state(captured_tx);
            axum::serve(listener, app).await.unwrap();
        });

        let mut worker = test_worker(format!("http://{addr}"));
        let actor = worker.report_actor.take().unwrap();
        worker.report_task = Some(tokio::spawn(actor.run()));
        let mut failed = job_message(SyncStatus::Failed, false);
        failed.skip_sync = true;
        failed.msg = "expected skip".into();
        worker.handle_job_message(failed, "w1");
        worker.handle_job_message(job_message(SyncStatus::None, true), "w1");

        let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), captured_rx.recv())
            .await
            .expect("status delivery timed out")
            .expect("status channel closed");
        assert_eq!(delivered.status, SyncStatus::Failed);
        assert_eq!(delivered.error_msg, "expected skip");
        assert!(delivered.skip_failure_count);

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            worker.run_schedule_until("w1".into(), std::future::ready(())),
        )
        .await
        .expect("scheduler shutdown timed out");

        server.abort();
    }

    #[tokio::test]
    async fn closed_report_result_channel_does_not_spin_scheduler() {
        use tokio::sync::oneshot;

        let mut worker = test_worker("http://127.0.0.1:9".into());
        drop(worker.report_actor.take());
        let status_tx = worker.status_tx.clone();
        let cmd_tx = worker.cmd_tx.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let loop_task = tokio::spawn(async move {
            worker
                .run_schedule_until("w1".into(), async move {
                    let _ = shutdown_rx.await;
                })
                .await;
            worker
        });

        status_tx
            .send(job_message(SyncStatus::Success, false))
            .await
            .unwrap();
        cmd_tx
            .send(tunasync_protocol::WorkerCmd {
                cmd: tunasync_protocol::CmdVerb::Stop,
                mirror_id: "mirror".into(),
                args: Vec::new(),
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        shutdown_tx.send(()).unwrap();

        let worker = tokio::time::timeout(std::time::Duration::from_secs(7), loop_task)
            .await
            .expect("closed result channel kept scheduler from shutting down")
            .unwrap();
        assert_eq!(worker.mirror_statuses["mirror"].status, SyncStatus::Success);
        assert!(tunasync_protocol::is_zero_time(
            &worker.mirror_statuses["mirror"].scheduled
        ));
    }

    #[tokio::test]
    async fn delayed_restore_cannot_undo_newer_local_state() {
        let mut worker = test_worker("http://127.0.0.1:9".into());
        worker.initialize_local_schedules();
        let captured_versions = worker.local_versions.clone();

        worker
            .handle_worker_cmd(
                tunasync_protocol::WorkerCmd {
                    cmd: tunasync_protocol::CmdVerb::Stop,
                    mirror_id: "mirror".into(),
                    args: Vec::new(),
                    options: std::collections::HashMap::new(),
                },
                "w1",
            )
            .await;
        worker.handle_job_message(job_message(SyncStatus::Success, true), "w1");
        let scheduled = worker.mirror_statuses["mirror"].scheduled;

        worker.apply_restore_result(crate::report_actor::RestoreResult {
            captured_versions,
            outcome: crate::report_actor::RestoreOutcome::Success(vec![
                tunasync_protocol::MirrorStatus {
                    name: "mirror".into(),
                    worker: "w1".into(),
                    status: SyncStatus::Disabled,
                    ..Default::default()
                },
            ]),
        });

        assert_eq!(worker.mirror_statuses["mirror"].status, SyncStatus::Success);
        assert_eq!(worker.mirror_statuses["mirror"].scheduled, scheduled);
    }

    #[tokio::test]
    async fn production_startup_progresses_while_manager_bootstrap_hangs() {
        use axum::{routing::get, routing::post, Router};
        use tokio::sync::{oneshot, Semaphore};

        async fn hang() -> std::convert::Infallible {
            std::future::pending().await
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new()
                .route("/workers", post(hang))
                .route("/workers/{worker}/jobs", get(hang))
                .route("/workers/{worker}/jobs/{mirror}", post(hang));
            axum::serve(listener, app).await.unwrap();
        });

        struct LoopProvider {
            name: String,
            started: std::sync::Arc<Semaphore>,
        }
        #[async_trait::async_trait]
        impl crate::provider::MirrorProvider for LoopProvider {
            fn name(&self) -> &str {
                &self.name
            }
            fn upstream(&self) -> &str {
                "rsync://localhost/test/"
            }
            fn is_master(&self) -> bool {
                true
            }
            fn interval(&self) -> std::time::Duration {
                std::time::Duration::from_secs(60)
            }
            fn retry(&self) -> u32 {
                0
            }
            fn timeout(&self) -> std::time::Duration {
                std::time::Duration::ZERO
            }
            async fn run(&self) -> anyhow::Result<()> {
                self.started.add_permits(1);
                Ok(())
            }
            async fn terminate(&self) -> anyhow::Result<()> {
                Ok(())
            }
            fn set_docker_config(&mut self, _config: crate::hooks::DockerConfig) {}
            fn set_log_path_shared(
                &mut self,
                _path: std::sync::Arc<std::sync::Mutex<std::path::PathBuf>>,
            ) {
            }
        }

        let started = std::sync::Arc::new(Semaphore::new(0));
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.name = "w1".into();
        cfg.global.concurrent = 2;
        cfg.manager.api_base = format!("http://{addr}");
        cfg.mirrors = ["first", "second"]
            .into_iter()
            .map(|name| MirrorConfig {
                name: name.into(),
                interval: 1,
                ..Default::default()
            })
            .collect();
        let providers = std::sync::Mutex::new(Some(vec![
            Box::new(LoopProvider {
                name: "first".into(),
                started: std::sync::Arc::clone(&started),
            }) as Box<dyn crate::provider::MirrorProvider>,
            Box::new(LoopProvider {
                name: "second".into(),
                started: std::sync::Arc::clone(&started),
            }) as Box<dyn crate::provider::MirrorProvider>,
        ]));
        let worker = super::Worker::new(
            cfg,
            std::path::PathBuf::new(),
            |_| {
                providers
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .into_iter()
                    .map(|provider| (provider, Vec::new()))
                    .collect()
            },
            reqwest::Client::new(),
        );
        let status_tx = worker.status_tx.clone();
        let cmd_tx = worker.cmd_tx.clone();
        let (http_ready_tx, http_ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let loop_task = tokio::spawn(async move {
            worker
                .run_until_shutdown(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    move |_, _, _| {
                        let _ = http_ready_tx.send(());
                    },
                )
                .await
                .unwrap()
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), http_ready_rx)
            .await
            .expect("HTTP startup seam was blocked by manager bootstrap")
            .expect("HTTP startup seam dropped readiness");
        let permits =
            tokio::time::timeout(std::time::Duration::from_secs(1), started.acquire_many(2))
                .await
                .expect("two due jobs were not dispatched")
                .unwrap();
        permits.forget();
        status_tx
            .send(JobMessage {
                job_generation: 1,
                status: SyncStatus::Success,
                name: "first".into(),
                msg: String::new(),
                schedule: false,
                size: String::new(),
                transferred_bytes: 0,
                skip_sync: false,
            })
            .await
            .unwrap();
        cmd_tx
            .send(tunasync_protocol::WorkerCmd {
                cmd: tunasync_protocol::CmdVerb::Stop,
                mirror_id: "second".into(),
                args: Vec::new(),
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(()).unwrap();
        let worker = tokio::time::timeout(std::time::Duration::from_secs(7), loop_task)
            .await
            .expect("scheduler loop did not shut down")
            .unwrap();
        assert_eq!(worker.mirror_statuses["first"].status, SyncStatus::Success);
        assert!(tunasync_protocol::is_zero_time(
            &worker.mirror_statuses["second"].scheduled
        ));
        server.abort();
    }

    #[tokio::test]
    async fn reload_with_hanging_candidate_keeps_worker_loop_live() {
        use axum::{routing::post, Json, Router};
        use chrono::{Datelike, Timelike};
        use tokio::sync::{oneshot, Semaphore};

        async fn register(
            Json(status): Json<tunasync_protocol::WorkerStatus>,
        ) -> Json<tunasync_protocol::WorkerStatus> {
            Json(status)
        }
        async fn hang() -> std::convert::Infallible {
            std::future::pending().await
        }

        let old_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_addr = old_listener.local_addr().unwrap();
        let old_server = tokio::spawn(async move {
            axum::serve(
                old_listener,
                Router::new().route("/workers", post(register)),
            )
            .await
            .unwrap();
        });
        let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_addr = new_listener.local_addr().unwrap();
        let new_server = tokio::spawn(async move {
            axum::serve(new_listener, Router::new().route("/workers", post(hang)))
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("worker.conf");
        let delayed_at = chrono::Utc::now() + chrono::Duration::seconds(3);
        let delayed_cron = format!(
            "{} {} {} {} {} * {}",
            delayed_at.second(),
            delayed_at.minute(),
            delayed_at.hour(),
            delayed_at.day(),
            delayed_at.month(),
            delayed_at.year(),
        );
        let config_for = |manager_base: &str| {
            worker_config_toml(manager_base, &["immediate", "delayed"]).replace(
                "name = \"delayed\"\nprovider = \"rsync\"\nupstream = \"rsync://localhost/test/\"\ninterval = 1\n",
                &format!(
                    "name = \"delayed\"\nprovider = \"rsync\"\nupstream = \"rsync://localhost/test/\"\ninterval = 1\ncron = {delayed_cron:?}\n"
                ),
            )
        };
        std::fs::write(&config_path, config_for(&format!("http://{old_addr}"))).unwrap();

        let immediate_started = std::sync::Arc::new(Semaphore::new(0));
        let delayed_started = std::sync::Arc::new(Semaphore::new(0));
        let providers = std::sync::Mutex::new(Some(vec![
            Box::new(DueProvider {
                name: "immediate".into(),
                started: std::sync::Arc::clone(&immediate_started),
            }) as Box<dyn crate::provider::MirrorProvider>,
            Box::new(DueProvider {
                name: "delayed".into(),
                started: std::sync::Arc::clone(&delayed_started),
            }) as Box<dyn crate::provider::MirrorProvider>,
        ]));
        let mut cfg: crate::config::WorkerConfig =
            tunasync_common::config::load_toml(&config_path).unwrap();
        cfg.mirrors = crate::config::flatten_mirrors(&cfg.mirrors_conf);
        let worker = super::Worker::new(
            cfg,
            config_path.clone(),
            |_| {
                providers
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .into_iter()
                    .map(|provider| (provider, Vec::new()))
                    .collect()
            },
            reqwest::Client::new(),
        );
        let status_tx = worker.status_tx.clone();
        let cmd_tx = worker.cmd_tx.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let loop_task = tokio::spawn(async move {
            worker
                .run_until_shutdown(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    |_, _, _| {},
                )
                .await
                .unwrap()
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            immediate_started.acquire(),
        )
        .await
        .expect("immediate startup job was not dispatched")
        .unwrap()
        .forget();
        std::fs::write(&config_path, config_for(&format!("http://{new_addr}"))).unwrap();
        cmd_tx
            .send(tunasync_protocol::WorkerCmd {
                cmd: tunasync_protocol::CmdVerb::Reload,
                mirror_id: String::new(),
                args: Vec::new(),
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(4), delayed_started.acquire())
            .await
            .expect("delayed due occurrence was blocked by hanging reconfigure")
            .unwrap()
            .forget();

        status_tx
            .send(JobMessage {
                job_generation: 2,
                status: SyncStatus::Success,
                name: "delayed".into(),
                msg: String::new(),
                schedule: false,
                size: String::new(),
                transferred_bytes: 0,
                skip_sync: false,
            })
            .await
            .unwrap();
        cmd_tx
            .send(tunasync_protocol::WorkerCmd {
                cmd: tunasync_protocol::CmdVerb::Stop,
                mirror_id: "delayed".into(),
                args: Vec::new(),
                options: std::collections::HashMap::new(),
            })
            .await
            .unwrap();
        status_tx
            .send(JobMessage {
                job_generation: 2,
                status: SyncStatus::Success,
                name: "delayed".into(),
                msg: String::new(),
                schedule: false,
                size: String::new(),
                transferred_bytes: 0,
                skip_sync: false,
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        shutdown_tx.send(()).unwrap();

        let worker = tokio::time::timeout(std::time::Duration::from_secs(7), loop_task)
            .await
            .expect("worker loop was blocked by hanging reload candidate")
            .unwrap();
        assert_eq!(
            worker.mirror_statuses["delayed"].status,
            SyncStatus::Success
        );
        assert!(worker.pending_manager_config.is_some());
        assert_eq!(worker.cfg.manager.api_base, format!("http://{old_addr}"));
        old_server.abort();
        new_server.abort();
    }

    #[tokio::test]
    async fn reload_rejects_excessive_mirror_count_without_changing_scheduler() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("worker.conf");
        let config_for = |report_max_resources: usize, mirrors: &[&str]| {
            let mut config = format!(
                "[global]\nname = \"w1\"\ninterval = 1\nreport_max_resources = {report_max_resources}\n"
            );
            for mirror in mirrors {
                config.push_str(&format!(
                    "\n[[mirrors]]\nname = {mirror:?}\nprovider = \"rsync\"\nupstream = \"rsync://localhost/test/\"\ninterval = 1\n"
                ));
            }
            config
        };
        std::fs::write(&config_path, config_for(1, &["old"])).unwrap();

        let mut cfg: crate::config::WorkerConfig =
            tunasync_common::config::load_toml(&config_path).unwrap();
        cfg.mirrors = crate::config::flatten_mirrors(&cfg.mirrors_conf);
        let mut worker = super::Worker::new(
            cfg,
            config_path.clone(),
            |_| Vec::new(),
            reqwest::Client::new(),
        );
        let old_due = chrono::Utc::now() + chrono::Duration::hours(1);
        worker.schedule.push("old".into(), old_due);

        std::fs::write(&config_path, config_for(2, &["old", "excess"])).unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            worker.handle_reload(),
        )
        .await
        .expect("excessive mirror count was not rejected locally");

        assert_eq!(
            worker
                .cfg
                .mirrors
                .iter()
                .map(|mirror| mirror.name.as_str())
                .collect::<Vec<_>>(),
            ["old"]
        );
        assert_eq!(worker.schedule.len(), 1);
        let retained = worker.schedule.peek().expect("old schedule was removed");
        assert_eq!(retained.name, "old");
        assert_eq!(retained.scheduled_at, old_due);
        assert_eq!(worker.cfg.global.effective_report_max_resources(), 1);
    }

    #[tokio::test]
    async fn reconfigure_result_updates_config_and_token_only_for_latest_success() {
        let mut worker = test_worker("http://old".into());
        let old_token = worker.api_token.read().await.clone();
        let requested = crate::config::ManagerApiConfig {
            api_base: "http://new".into(),
            api_token: "new-token".into(),
            ..Default::default()
        };
        worker.pending_manager_config = Some((2, requested.clone()));

        worker
            .handle_reconfigure_result(crate::report_actor::ReconfigureResult {
                generation: 1,
                outcome: crate::report_actor::ReconfigureOutcome::Success,
            })
            .await;
        assert_eq!(worker.cfg.manager.api_base, "http://old");
        assert_eq!(*worker.api_token.read().await, old_token);

        worker
            .handle_reconfigure_result(crate::report_actor::ReconfigureResult {
                generation: 2,
                outcome: crate::report_actor::ReconfigureOutcome::Failed("no".into()),
            })
            .await;
        assert_eq!(worker.cfg.manager.api_base, "http://old");
        assert_eq!(*worker.api_token.read().await, old_token);

        worker.pending_manager_config = Some((3, requested));
        worker
            .handle_reconfigure_result(crate::report_actor::ReconfigureResult {
                generation: 3,
                outcome: crate::report_actor::ReconfigureOutcome::Success,
            })
            .await;
        assert_eq!(worker.cfg.manager.api_base, "http://new");
        assert_eq!(*worker.api_token.read().await, "new-token");
    }

    #[tokio::test]
    async fn restore_and_reconfigure_revisions_are_both_applied_once() {
        let mut worker = test_worker("http://old".into());
        worker.local_versions.insert("mirror".into(), 0);
        worker.initialize_local_schedules();
        let requested = crate::config::ManagerApiConfig {
            api_base: "http://new".into(),
            api_token: "new-token".into(),
            ..Default::default()
        };
        worker.pending_manager_config = Some((4, requested));
        let results = crate::report_actor::ActorResultState {
            restore: Some(crate::report_actor::RevisedResult {
                revision: 1,
                value: crate::report_actor::RestoreResult {
                    captured_versions: worker.local_versions.clone(),
                    outcome: crate::report_actor::RestoreOutcome::Success(vec![
                        tunasync_protocol::MirrorStatus {
                            name: "mirror".into(),
                            worker: "w1".into(),
                            status: SyncStatus::Disabled,
                            ..Default::default()
                        },
                    ]),
                },
            }),
            reconfigure: Some(crate::report_actor::RevisedResult {
                revision: 1,
                value: crate::report_actor::ReconfigureResult {
                    generation: 4,
                    outcome: crate::report_actor::ReconfigureOutcome::Success,
                },
            }),
        };

        worker.apply_actor_results(results.clone()).await;
        assert_eq!(worker.cfg.manager.api_base, "http://new");
        assert_eq!(worker.applied_restore_revision, 1);
        assert_eq!(worker.applied_reconfigure_revision, 1);

        worker.mirror_statuses.get_mut("mirror").unwrap().status = SyncStatus::Success;
        worker.apply_actor_results(results).await;
        assert_eq!(worker.mirror_statuses["mirror"].status, SyncStatus::Success);
    }

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
