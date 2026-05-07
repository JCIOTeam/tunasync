//! axum HTTP server for the tunasync manager.
//!
//! Route table (matches Go's `GetTUNASyncManager` router setup exactly):
//!
//! ```text
//! GET    /ping
//! GET    /jobs
//! HEAD   /jobs
//! DELETE /jobs/disabled
//! GET    /workers
//! POST   /workers
//! DELETE /workers/:id
//! GET    /workers/:id/jobs
//! POST   /workers/:id/jobs/:job
//! POST   /workers/:id/jobs/:job/size
//! POST   /workers/:id/schedules
//! POST   /cmd
//! ```

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tunasync_protocol::{
    ClientCmd, CmdVerb, MirrorSchedules, MirrorStatus, SyncStatus, WebMirrorStatus, WorkerCmd,
    WorkerStatus,
};

use crate::db::{DbAdapter, DbError};

// App state

/// Shared state injected into every axum handler via `State`.
pub struct AppState {
    pub db: Box<dyn DbAdapter>,
    pub http_client: reqwest::Client,
}

/// JSON error response matching Go's `{ "error": "..." }` shape.
#[derive(Serialize)]
struct ErrBody {
    error: String,
}

/// JSON info response matching Go's `{ "message": "..." }` shape.
#[derive(Serialize)]
struct MsgBody {
    message: String,
}

fn ok_msg(msg: impl Into<String>) -> Json<MsgBody> {
    Json(MsgBody {
        message: msg.into(),
    })
}

/// Map a `DbError` to an axum `Response`.
fn db_err(e: DbError) -> Response {
    let status = match &e {
        DbError::NotFound(_) => StatusCode::NOT_FOUND,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrBody {
            error: e.to_string(),
        }),
    )
        .into_response()
}

fn bad_req(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(ErrBody { error: msg.into() })).into_response()
}

// Router factory

/// Build the axum router for the manager service.
///
/// Takes a pre-built `Arc<AppState>` so the caller can retain a clone for
/// background tasks (status-file writer, etc.) without needing to go through
/// the router.
pub fn build_router(shared: Arc<AppState>) -> Router {
    Router::new()
        .route("/ping", get(ping))
        .route("/jobs", get(list_all_jobs).head(list_all_jobs_head))
        .route("/jobs/disabled", delete(flush_disabled_jobs))
        .route("/jobs/:name", get(list_mirror_by_name))
        .route("/workers", get(list_workers).post(register_worker))
        .route("/workers/:id", delete(delete_worker))
        .route("/workers/:id/heartbeat", post(heartbeat_worker))
        .route("/workers/:id/jobs", get(list_jobs_of_worker))
        .route("/workers/:id/jobs/:job", post(update_job_of_worker))
        .route("/workers/:id/jobs/:job/size", post(update_mirror_size))
        .route("/workers/:id/schedules", post(update_schedules_of_worker))
        .route("/cmd", post(handle_client_cmd))
        .route("/metrics", get(metrics))
        .with_state(shared)
}

// Handlers

/// `GET /ping` — liveness check.
async fn ping() -> impl IntoResponse {
    ok_msg("pong")
}

/// `GET /jobs` — list all mirrors across all workers, in `WebMirrorStatus` format.
async fn list_all_jobs(State(state): State<Arc<AppState>>) -> Response {
    match state.db.list_all_mirror_status() {
        Err(e) => db_err(e),
        Ok(statuses) => {
            let web: Vec<WebMirrorStatus> = statuses
                .iter()
                .map(WebMirrorStatus::from_mirror_status)
                .collect();
            Json(web).into_response()
        }
    }
}

/// `HEAD /jobs` — check availability and count without returning body.
async fn list_all_jobs_head(State(state): State<Arc<AppState>>) -> Response {
    match state.db.list_all_mirror_status() {
        Err(e) => db_err(e),
        Ok(_) => StatusCode::OK.into_response(),
    }
}

/// `GET /jobs/:name` — detailed mirror status (including `error_msg`) across all workers.
async fn list_mirror_by_name(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.db.list_all_mirror_status() {
        Err(e) => db_err(e),
        Ok(statuses) => {
            let filtered: Vec<_> = statuses.into_iter().filter(|s| s.name == name).collect();
            Json(filtered).into_response()
        }
    }
}

/// `DELETE /jobs/disabled` — flush all disabled job rows.
async fn flush_disabled_jobs(State(state): State<Arc<AppState>>) -> Response {
    match state.db.flush_disabled_jobs() {
        Err(e) => db_err(e),
        Ok(()) => ok_msg("flushed").into_response(),
    }
}

/// `GET /workers` — list all registered workers (token REDACTED).
async fn list_workers(State(state): State<Arc<AppState>>) -> Response {
    match state.db.list_workers() {
        Err(e) => db_err(e),
        Ok(workers) => {
            // Redact tokens as Go does.
            let redacted: Vec<WorkerStatus> = workers
                .into_iter()
                .map(|mut w| {
                    w.token = "REDACTED".into();
                    w
                })
                .collect();
            Json(redacted).into_response()
        }
    }
}

/// `POST /workers` — register a new worker.
///
/// Matches Go's `registerWorker`: sets `last_online` and `last_register` to now.
async fn register_worker(
    State(state): State<Arc<AppState>>,
    Json(mut worker): Json<WorkerStatus>,
) -> Response {
    let now = Utc::now();
    worker.last_online = now;
    worker.last_register = now;

    tracing::info!(id = %worker.id, "worker registered");

    match state.db.create_worker(worker) {
        Err(e) => db_err(e),
        Ok(w) => (StatusCode::OK, Json(w)).into_response(),
    }
}

/// `DELETE /workers/:id` — remove a worker.
async fn delete_worker(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.db.delete_worker(&id) {
        Err(DbError::NotFound(_)) => bad_req(format!("invalid workerID {id}")),
        Err(e) => db_err(e),
        Ok(()) => {
            tracing::info!(id = %id, "worker deleted");
            ok_msg("deleted").into_response()
        }
    }
}

/// `POST /workers/:id/heartbeat` — worker signals it is still alive.
async fn heartbeat_worker(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.db.refresh_worker(&id) {
        Err(DbError::NotFound(_)) => bad_req(format!("invalid workerID {id}")),
        Err(e) => db_err(e),
        Ok(_) => ok_msg("pong").into_response(),
    }
}

/// `GET /workers/:id/jobs` — list mirrors of one worker.
async fn list_jobs_of_worker(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    // Validate worker exists (matches Go's workerIDValidator middleware).
    if let Err(e) = state.db.get_worker(&id) {
        return match e {
            DbError::NotFound(_) => bad_req(format!("invalid workerID {id}")),
            other => db_err(other),
        };
    }
    match state.db.list_mirror_status(&id) {
        Err(e) => db_err(e),
        Ok(statuses) => Json(statuses).into_response(),
    }
}

/// `POST /workers/:id/jobs/:job` — worker reports a new job status.
///
/// Implements Go's `updateJobOfWorker` timestamp-merge logic:
/// - `PreSyncing` transition → set `last_started`
/// - `Success` → set `last_update`
/// - `Success` | `Failed` → set `last_ended`
/// - Preserve non-empty `size` from current record when incoming size is blank.
async fn update_job_of_worker(
    State(state): State<Arc<AppState>>,
    Path((worker_id, _job)): Path<(String, String)>,
    Json(mut incoming): Json<MirrorStatus>,
) -> Response {
    if incoming.name.is_empty() {
        return bad_req("mirror Name should not be empty");
    }

    // Validate worker, refresh last_online.
    if let Err(e) = state.db.get_worker(&worker_id) {
        return match e {
            DbError::NotFound(_) => bad_req(format!("invalid workerID {worker_id}")),
            other => db_err(other),
        };
    }
    let _ = state.db.refresh_worker(&worker_id);

    // Fetch previous state (if any; miss = zero-value defaults).
    let cur = state
        .db
        .get_mirror_status(&worker_id, &incoming.name)
        .unwrap_or_else(|_| zero_mirror_status(&incoming.name, &worker_id));

    let now = Utc::now();

    // Timestamp merge — mirrors Go's logic exactly.
    incoming.last_started =
        if incoming.status == SyncStatus::PreSyncing && cur.status != SyncStatus::PreSyncing {
            now
        } else {
            cur.last_started
        };

    incoming.last_update = if incoming.status == SyncStatus::Success {
        now
    } else {
        cur.last_update
    };

    incoming.last_ended = if matches!(incoming.status, SyncStatus::Success | SyncStatus::Failed) {
        now
    } else {
        cur.last_ended
    };

    // Preserve meaningful size from current record.
    if !cur.size.is_empty()
        && cur.size != "unknown"
        && (incoming.size.is_empty() || incoming.size == "unknown")
    {
        incoming.size = cur.size;
    }

    match incoming.status {
        SyncStatus::Syncing => {
            tracing::info!(mirror = %incoming.name, worker = %incoming.worker, "job starts syncing");
        }
        other => {
            tracing::info!(mirror = %incoming.name, worker = %incoming.worker, status = %other, "job status update");
        }
    }

    let mirror_name = incoming.name.clone();
    match state
        .db
        .update_mirror_status(&worker_id, &mirror_name, incoming)
    {
        Err(e) => db_err(e),
        Ok(stored) => Json(stored).into_response(),
    }
}

/// Size-only update message body.
#[derive(Debug, Deserialize)]
struct SizeMsg {
    name: String,
    size: String,
}

/// `POST /workers/:id/jobs/:job/size` — update mirror size only.
async fn update_mirror_size(
    State(state): State<Arc<AppState>>,
    Path((worker_id, _job)): Path<(String, String)>,
    Json(msg): Json<SizeMsg>,
) -> Response {
    if let Err(e) = state.db.get_worker(&worker_id) {
        return match e {
            DbError::NotFound(_) => bad_req(format!("invalid workerID {worker_id}")),
            other => db_err(other),
        };
    }
    let _ = state.db.refresh_worker(&worker_id);

    let mut status = match state.db.get_mirror_status(&worker_id, &msg.name) {
        Err(e) => return db_err(e),
        Ok(s) => s,
    };

    if !msg.size.is_empty() && msg.size != "unknown" {
        status.size = msg.size.clone();
    }

    tracing::info!(
        mirror = %status.name,
        worker = %status.worker,
        size = %status.size,
        "mirror size update"
    );

    match state.db.update_mirror_status(&worker_id, &msg.name, status) {
        Err(e) => db_err(e),
        Ok(stored) => Json(stored).into_response(),
    }
}

/// `POST /workers/:id/schedules` — worker announces its schedule table.
async fn update_schedules_of_worker(
    State(state): State<Arc<AppState>>,
    Path(worker_id): Path<String>,
    Json(schedules): Json<MirrorSchedules>,
) -> Response {
    if let Err(e) = state.db.get_worker(&worker_id) {
        return match e {
            DbError::NotFound(_) => bad_req(format!("invalid workerID {worker_id}")),
            other => db_err(other),
        };
    }

    for schedule in schedules.schedules {
        if schedule.mirror_name.is_empty() {
            return bad_req("mirror Name should not be empty");
        }

        let _ = state.db.refresh_worker(&worker_id);

        let mut cur = match state
            .db
            .get_mirror_status(&worker_id, &schedule.mirror_name)
        {
            Err(_) => continue, // not tracked yet — skip
            Ok(s) => s,
        };

        if cur.scheduled == schedule.next_schedule {
            continue; // no change
        }

        cur.scheduled = schedule.next_schedule;

        if let Err(e) = state
            .db
            .update_mirror_status(&worker_id, &schedule.mirror_name, cur)
        {
            return db_err(e);
        }
    }

    // Go returns `{}` (empty JSON object) on success.
    Json(serde_json::json!({})).into_response()
}

/// `POST /cmd` — CLI sends a command to a specific worker.
///
/// Matches Go's `handleClientCmd`: routes the command to the worker's URL and
/// optionally pre-updates the job status (Disable→disabled, Stop→paused).
async fn handle_client_cmd(
    State(state): State<Arc<AppState>>,
    Json(client_cmd): Json<ClientCmd>,
) -> Response {
    let worker_id = &client_cmd.worker_id;
    if worker_id.is_empty() {
        // Go has a TODO here; we replicate the 500 response.
        tracing::error!("handleClientCmd with empty workerID not implemented");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let worker = match state.db.get_worker(worker_id) {
        Err(DbError::NotFound(_)) => {
            return bad_req(format!("worker {worker_id} is not registered yet"));
        }
        Err(e) => return db_err(e),
        Ok(w) => w,
    };

    // Pre-update job status for Disable/Stop.
    let changed = matches!(client_cmd.cmd, CmdVerb::Disable | CmdVerb::Stop);
    if changed {
        if let Ok(mut cur) = state.db.get_mirror_status(worker_id, &client_cmd.mirror_id) {
            cur.status = match client_cmd.cmd {
                CmdVerb::Disable => SyncStatus::Disabled,
                CmdVerb::Stop => SyncStatus::Paused,
                _ => unreachable!(),
            };
            let _ = state
                .db
                .update_mirror_status(worker_id, &client_cmd.mirror_id, cur);
        }
    }

    // Forward command to worker.
    let worker_cmd = WorkerCmd {
        cmd: client_cmd.cmd,
        mirror_id: client_cmd.mirror_id.clone(),
        args: client_cmd.args.clone(),
        options: client_cmd.options.clone(),
    };

    tracing::info!(
        cmd = %client_cmd.cmd,
        mirror = %client_cmd.mirror_id,
        worker = %worker_id,
        "posting command to worker"
    );

    match state
        .http_client
        .post(&worker.url)
        .json(&worker_cmd)
        .send()
        .await
    {
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrBody {
                error: format!(
                    "post command to worker {worker_id} ({}) failed: {e}",
                    worker.url
                ),
            }),
        )
            .into_response(),
        Ok(_) => ok_msg(format!("successfully send command to worker {worker_id}")).into_response(),
    }
}

// Helper: zero-value MirrorStatus for a mirror that hasn't reported yet

fn zero_mirror_status(name: &str, worker: &str) -> MirrorStatus {
    use tunasync_protocol::zero_time;
    MirrorStatus {
        name: name.to_owned(),
        worker: worker.to_owned(),
        is_master: true,
        status: SyncStatus::None,
        last_update: zero_time(),
        last_started: zero_time(),
        last_ended: zero_time(),
        scheduled: zero_time(),
        upstream: String::new(),
        size: String::new(),
        error_msg: String::new(),
    }
}

// Metrics

/// `GET /metrics` — Prometheus text exposition (format version 0.0.4).
///
/// Exposes per-mirror and aggregate metrics readable by any Prometheus-
/// compatible scraper (Prometheus, VictoriaMetrics, Grafana Agent, etc.).
///
/// Status codes for `tunasync_mirror_status`:
///   0 none | 1 pre-syncing | 2 syncing | 3 success | 4 failed | 5 paused | 6 disabled
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let mirrors = match state.db.list_all_mirror_status() {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("# ERROR {e}\n")).into_response()
        }
    };
    let workers = match state.db.list_workers() {
        Ok(w) => w,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("# ERROR {e}\n")).into_response()
        }
    };

    let body = render_metrics(&mirrors, workers.len());
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

/// Render the Prometheus text payload from current mirror status.
/// Extracted for testability.
pub(crate) fn render_metrics(mirrors: &[MirrorStatus], worker_count: usize) -> String {
    use std::collections::HashMap;

    let mut out = String::with_capacity(mirrors.len() * 256);

    // ── tunasync_workers_total ──────────────────────────────────────────────
    out.push_str("# HELP tunasync_workers_total Number of registered workers.\n");
    out.push_str("# TYPE tunasync_workers_total gauge\n");
    out.push_str(&format!("tunasync_workers_total {worker_count}\n"));

    // ── tunasync_mirrors_total ─────────────────────────────────────────────
    let mut by_status: HashMap<&str, usize> = HashMap::new();
    for m in mirrors {
        *by_status.entry(m.status.as_str()).or_insert(0) += 1;
    }
    out.push_str("# HELP tunasync_mirrors_total Number of mirrors in each status.\n");
    out.push_str("# TYPE tunasync_mirrors_total gauge\n");
    for status_str in &[
        "none",
        "pre-syncing",
        "syncing",
        "success",
        "failed",
        "paused",
        "disabled",
    ] {
        let n = by_status.get(*status_str).copied().unwrap_or(0);
        out.push_str(&format!(
            "tunasync_mirrors_total{{status=\"{status_str}\"}} {n}\n"
        ));
    }

    // ── per-mirror metrics ──────────────────────────────────────────────────
    out.push_str(concat!(
        "# HELP tunasync_mirror_status Sync status code: ",
        "0=none 1=pre-syncing 2=syncing 3=success 4=failed 5=paused 6=disabled.\n",
        "# TYPE tunasync_mirror_status gauge\n",
    ));
    out.push_str(concat!(
        "# HELP tunasync_mirror_size_bytes Mirror size in bytes ",
        "(-1 if unknown or unparseable).\n",
        "# TYPE tunasync_mirror_size_bytes gauge\n",
    ));
    out.push_str(concat!(
        "# HELP tunasync_mirror_last_success_timestamp_seconds ",
        "Unix timestamp of the last successful sync (0 if never succeeded).\n",
        "# TYPE tunasync_mirror_last_success_timestamp_seconds gauge\n",
    ));
    out.push_str(concat!(
        "# HELP tunasync_mirror_last_sync_duration_seconds ",
        "Duration of the most recent completed sync in seconds (0 if never run).\n",
        "# TYPE tunasync_mirror_last_sync_duration_seconds gauge\n",
    ));

    let epoch = tunasync_protocol::zero_time();
    for m in mirrors {
        let labels = format!("mirror=\"{}\",worker=\"{}\"", m.name, m.worker);
        let code = status_code(m.status);

        // status
        out.push_str(&format!("tunasync_mirror_status{{{labels}}} {code}\n"));

        // size
        let size_bytes = parse_size_bytes(&m.size);
        out.push_str(&format!(
            "tunasync_mirror_size_bytes{{{labels}}} {size_bytes}\n"
        ));

        // last success timestamp
        let ts = if m.last_update > epoch {
            m.last_update.timestamp()
        } else {
            0
        };
        out.push_str(&format!(
            "tunasync_mirror_last_success_timestamp_seconds{{{labels}}} {ts}\n"
        ));

        // last sync duration
        let duration =
            if m.last_ended > epoch && m.last_started > epoch && m.last_ended >= m.last_started {
                (m.last_ended - m.last_started).num_seconds().max(0)
            } else {
                0
            };
        out.push_str(&format!(
            "tunasync_mirror_last_sync_duration_seconds{{{labels}}} {duration}\n"
        ));
    }

    out
}

/// Map `SyncStatus` to an integer code for Prometheus gauges.
fn status_code(s: SyncStatus) -> u8 {
    match s {
        SyncStatus::None => 0,
        SyncStatus::PreSyncing => 1,
        SyncStatus::Syncing => 2,
        SyncStatus::Success => 3,
        SyncStatus::Failed => 4,
        SyncStatus::Paused => 5,
        SyncStatus::Disabled => 6,
    }
}

/// Parse a human-readable size string produced by rsync `--stats` into bytes.
///
/// Handles formats produced by tunasync's `extract_size_from_rsync_log`:
/// `"1.23G"`, `"500M"`, `"10.5T"`, `"100K"`, plain `"12345"`.
/// Returns `-1` for empty, `"unknown"`, or unparseable input.
fn parse_size_bytes(s: &str) -> i64 {
    let s = s.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("unknown") {
        return -1;
    }
    // Strip trailing 'B' if present (e.g. "1.5GB" → "1.5G")
    let s = s.strip_suffix('B').unwrap_or(s);
    let (num_str, multiplier) = match s.chars().last() {
        Some('K') | Some('k') => (&s[..s.len() - 1], 1_024i64),
        Some('M') | Some('m') => (&s[..s.len() - 1], 1_024 * 1_024),
        Some('G') | Some('g') => (&s[..s.len() - 1], 1_024 * 1_024 * 1_024),
        Some('T') | Some('t') => (&s[..s.len() - 1], 1_024 * 1_024 * 1_024 * 1_024),
        Some('P') | Some('p') => (&s[..s.len() - 1], 1_024 * 1_024 * 1_024 * 1_024 * 1_024),
        _ => (s, 1i64),
    };
    num_str
        .parse::<f64>()
        .map(|n| (n * multiplier as f64).round() as i64)
        .unwrap_or(-1)
}

// Size parsing is also used by the status-file writer in lib.rs via render_metrics.

#[cfg(test)]
mod metrics_tests {
    use super::*;

    #[test]
    fn parse_size_bytes_variants() {
        assert_eq!(parse_size_bytes("1K"), 1_024);
        assert_eq!(
            parse_size_bytes("1.5G"),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as i64
        );
        assert_eq!(parse_size_bytes("100M"), 100 * 1024 * 1024);
        assert_eq!(parse_size_bytes("2T"), 2 * 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size_bytes("1GB"), 1024 * 1024 * 1024);
        assert_eq!(parse_size_bytes("12345"), 12345);
        assert_eq!(parse_size_bytes("unknown"), -1);
        assert_eq!(parse_size_bytes(""), -1);
    }

    #[test]
    fn render_metrics_smoke() {
        use tunasync_protocol::{zero_time, SyncStatus};
        let mirrors = vec![MirrorStatus {
            name: "ubuntu".into(),
            worker: "w1".into(),
            is_master: true,
            status: SyncStatus::Success,
            last_update: chrono::Utc::now(),
            last_started: chrono::Utc::now() - chrono::Duration::seconds(3600),
            last_ended: chrono::Utc::now(),
            scheduled: zero_time(),
            upstream: "rsync://example.com/".into(),
            size: "1.5G".into(),
            error_msg: String::new(),
        }];
        let out = render_metrics(&mirrors, 2);
        assert!(out.contains("tunasync_workers_total 2"));
        assert!(out.contains("tunasync_mirror_status{mirror=\"ubuntu\",worker=\"w1\"} 3"));
        assert!(out.contains("tunasync_mirror_size_bytes{mirror=\"ubuntu\",worker=\"w1\"}"));
        assert!(out.contains("tunasync_mirrors_total{status=\"success\"} 1"));
        assert!(out.contains("tunasync_mirrors_total{status=\"failed\"} 0"));
    }
}
