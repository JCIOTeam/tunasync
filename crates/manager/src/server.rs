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
    /// Client for proxying long-lived SSE streams from workers. Built with
    /// NO total timeout (a log stream legitimately stays open for hours) but
    /// a short connect timeout. Never use it for ordinary API calls.
    pub sse_client: reqwest::Client,
    /// Read-only maintenance mode. When true, all mutating endpoints return
    /// 503 Service Unavailable. Toggle via `POST /maintenance` / `DELETE /maintenance`.
    pub maintenance: std::sync::atomic::AtomicBool,
    /// Notify config (webhook URL, stale_after, alert thresholds).
    pub notify: crate::config::NotifyConfig,
    /// Shared API token (empty = auth disabled). See `ServerConfig::api_token`.
    pub api_token: String,
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
        .route("/jobs/{name}", get(list_mirror_by_name))
        .route("/jobs/{name}/log/stream", get(proxy_job_log_stream))
        .route("/jobs/{name}/history", get(get_job_history))
        .route("/workers", get(list_workers).post(register_worker))
        .route("/workers/{id}", delete(delete_worker))
        .route("/workers/{id}/heartbeat", post(heartbeat_worker))
        .route("/workers/{id}/jobs", get(list_jobs_of_worker))
        .route("/workers/{id}/jobs/{job}", post(update_job_of_worker))
        .route("/workers/{id}/jobs/{job}/size", post(update_mirror_size))
        .route("/workers/{id}/schedules", post(update_schedules_of_worker))
        .route("/cmd", post(handle_client_cmd))
        .route("/metrics", get(metrics))
        .route(
            "/maintenance",
            post(enable_maintenance)
                .delete(disable_maintenance)
                .get(get_maintenance),
        )
        .layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            auth_middleware,
        ))
        .with_state(shared)
}

/// Bearer-token auth for the manager API.
///
/// No-op when `api_token` is empty (default — backward compatible). When
/// set, every request must present `Authorization: Bearer <token>` EXCEPT
/// the public read-only surface used by web frontends and load balancers:
///
/// - `GET/HEAD /ping`           — liveness probes
/// - `GET/HEAD /jobs...`        — mirror status, detail, SSE log stream
/// - `GET /metrics`             — Prometheus scrapes
/// - `GET /maintenance`         — maintenance status read
///
/// Everything else — worker registration/reports, `/cmd`, deletes,
/// maintenance toggles — is authenticated. Failures return `401` with a
/// JSON error body.
async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if state.api_token.is_empty() {
        return next.run(req).await;
    }
    let method = req.method();
    let path = req.uri().path();
    let read_only = method == axum::http::Method::GET || method == axum::http::Method::HEAD;
    let public = read_only
        && (path == "/ping"
            || path == "/jobs"
            || path.starts_with("/jobs/")
            || path == "/metrics"
            || path == "/maintenance");
    if public {
        return next.run(req).await;
    }
    let auth = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if tunasync_common::util::check_bearer(auth, &state.api_token) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "missing or invalid API token" })),
        )
            .into_response()
    }
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

/// `GET /jobs/:name/history?limit=N` — most-recent-first completed runs for
/// one mirror. `limit` defaults to 20, capped at the per-mirror retention
/// (100). Backends without history support (redb/redis) return `[]`.
async fn get_job_history(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit = q
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(20)
        .clamp(1, crate::db::SYNC_HISTORY_KEEP_PER_MIRROR);
    match state.db.get_sync_history(&name, limit) {
        Err(e) => db_err(e),
        Ok(entries) => Json(entries).into_response(),
    }
}

/// `GET /jobs/:name/log/stream` — proxy the live sync-log SSE stream from
/// the worker that owns the mirror.
///
/// The actual SSE endpoint lives on the **worker's** HTTP server, which is
/// typically bound to an internal interface and not reachable by browsers.
/// This route lets frontends talk to a single origin (the manager): we look
/// up which worker owns `name`, open the worker's stream with the
/// no-total-timeout `sse_client`, and pipe the bytes through unbuffered.
///
/// Responses:
/// - `404` — no mirror with this name is known to the manager
/// - `502` — owning worker is registered but unreachable
/// - upstream non-2xx — forwarded with the worker's status code
/// - `200 text/event-stream` — live proxied stream (replay + live + keep-alives
///   are produced by the worker; we add no events of our own)
async fn proxy_job_log_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    // Find the worker that owns this mirror.
    let statuses = match state.db.list_all_mirror_status() {
        Ok(s) => s,
        Err(e) => return db_err(e),
    };
    let Some(mirror) = statuses.iter().find(|s| s.name == name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no mirror named {name:?}") })),
        )
            .into_response();
    };
    let worker = match state.db.get_worker(&mirror.worker) {
        Ok(w) => w,
        Err(DbError::NotFound(_)) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": format!("owning worker {:?} is not registered", mirror.worker)
                })),
            )
                .into_response();
        }
        Err(e) => return db_err(e),
    };

    // worker.url is the worker's command endpoint base (public_url, e.g.
    // "http://host:6000"). Mirror names come from operator-controlled config
    // and the DB, but percent-encode defensively anyway.
    let base = worker.url.trim_end_matches('/');
    let encoded: String = name
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect();
    let url = format!("{base}/jobs/{encoded}/log/stream");

    let upstream = match state.sse_client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(mirror = %name, worker = %mirror.worker, url = %url, error = %e,
                "SSE proxy: worker unreachable");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": format!("worker {:?} unreachable: {e}", mirror.worker)
                })),
            )
                .into_response();
        }
    };

    let status = upstream.status();
    if !status.is_success() {
        // Forward the worker's status (e.g. its own 404) with a JSON body.
        let code = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let body = upstream.text().await.unwrap_or_default();
        return (code, body).into_response();
    }

    // Pipe the byte stream through. axum streams each chunk as it arrives;
    // pair this with `proxy_buffering off` / `X-Accel-Buffering: no` on any
    // fronting nginx so intermediaries don't batch the events.
    let body = axum::body::Body::from_stream(upstream.bytes_stream());
    Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
        .header(axum::http::header::CACHE_CONTROL, "no-cache, no-transform")
        .header("x-accel-buffering", "no")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `DELETE /jobs/disabled` — flush all disabled job rows.
///
/// Gated by maintenance mode: this is a destructive operator action and must
/// not be permitted while maintenance is engaged.
async fn flush_disabled_jobs(State(state): State<Arc<AppState>>) -> Response {
    if let Some(r) = check_maintenance(&state) {
        return r;
    }
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
    if let Some(r) = check_maintenance(&state) {
        return r;
    }
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
    // Note: this endpoint is NOT gated by maintenance mode. Workers must
    // continue reporting status updates even while operators are doing
    // destructive maintenance — otherwise the UI freezes mid-sync and the
    // worker's local mirror_statuses diverge from the manager's view, with
    // no way to reconcile after maintenance is disabled.
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
        incoming.size = cur.size.clone();
    }

    // ── Extension fields ──────────────────────────────────────────────

    // Traffic stats: accumulate the running total exactly once per completed
    // run, on the *transition into* Success.
    //
    // The previous rule ("accumulate when last_started changed", i.e. at the
    // PreSyncing report carrying the persisted value of the PREVIOUS run)
    // double-counted whenever a failed run sat between two starts: failures
    // report transferred_bytes=0, so the worker's persisted
    // last_transferred_bytes kept the old successful value, which then got
    // re-accumulated at the next PreSyncing transition.
    //
    // Guarding on `cur.status != Success` (rather than just
    // `incoming.status == Success`) also protects against the worker's
    // scheduling-only follow-up report, which re-sends the full status entry
    // (still Success) right after the Success report itself.
    if incoming.status == SyncStatus::Success && cur.status != SyncStatus::Success {
        incoming.total_transferred_bytes =
            cur.total_transferred_bytes + incoming.last_transferred_bytes;
    } else {
        // Preserve running total from current record.
        incoming.total_transferred_bytes = cur.total_transferred_bytes;
        // Preserve last_transferred if worker didn't report a new one.
        if incoming.last_transferred_bytes == 0 {
            incoming.last_transferred_bytes = cur.last_transferred_bytes;
        }
    }

    // Sync history: append one row per completed run. The guard mirrors the
    // traffic accumulation above — record only on the transition from an
    // ACTIVE state into a terminal one, so the worker's scheduling-only
    // follow-up report (same terminal status again) and skipped runs (which
    // jump terminal→terminal without ever going active) don't add rows.
    // Best-effort: a history write failure must never fail the report.
    let was_active = matches!(cur.status, SyncStatus::PreSyncing | SyncStatus::Syncing);
    let is_terminal = matches!(incoming.status, SyncStatus::Success | SyncStatus::Failed);
    if was_active && is_terminal {
        let entry = crate::db::SyncHistoryEntry {
            mirror: incoming.name.clone(),
            worker: incoming.worker.clone(),
            status: incoming.status,
            started: incoming.last_started,
            ended: incoming.last_ended,
            transferred_bytes: incoming.last_transferred_bytes,
            error_msg: incoming.error_msg.clone(),
        };
        if let Err(e) = state.db.record_sync_history(&entry) {
            tracing::warn!(mirror = %incoming.name, error = %e, "record sync history failed");
        }
    }

    // Consecutive failures + stale tracking.
    match incoming.status {
        SyncStatus::Failed => {
            // A skipped sync (disk quota exceeded, upstream unreachable) is
            // reported as Failed so the UI shows a reason, but it must NOT
            // count as a failure for alerting purposes. Only a genuine sync
            // attempt that went wrong should accumulate the counter.
            if incoming.skip_failure_count {
                incoming.consecutive_failures = cur.consecutive_failures;
            } else {
                incoming.consecutive_failures = cur.consecutive_failures + 1;
            }
            incoming.stale = cur.stale; // preserve stale flag

            // Webhook: alert on consecutive failure threshold.
            let threshold = state.notify.alert_after_failures;
            if threshold > 0 && incoming.consecutive_failures == threshold {
                let url = state.notify.webhook_url.clone();
                if !url.is_empty() {
                    let client = state.http_client.clone();
                    let text = format!(
                        "⚠️ Mirror {} on worker {} has failed {} consecutive times. Last error: {}",
                        incoming.name, worker_id, incoming.consecutive_failures, incoming.error_msg
                    );
                    tokio::spawn(async move {
                        crate::webhook::send(&client, &url, &text).await;
                    });
                }
            }
        }
        SyncStatus::Success => {
            // Recovery webhook: was previously failing or stale.
            if cur.consecutive_failures > 0 || cur.stale {
                let url = state.notify.webhook_url.clone();
                if !url.is_empty() {
                    let client = state.http_client.clone();
                    let text = format!(
                        "✅ Mirror {} on worker {} recovered (was: {} consecutive failures, stale: {})",
                        incoming.name, worker_id, cur.consecutive_failures, cur.stale
                    );
                    tokio::spawn(async move {
                        crate::webhook::send(&client, &url, &text).await;
                    });
                }
            }
            incoming.consecutive_failures = 0;
            incoming.stale = false;
        }
        _ => {
            // Preserve for transient states (Syncing, PreSyncing, etc.)
            incoming.consecutive_failures = cur.consecutive_failures;
            incoming.stale = cur.stale;
        }
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
    if let Some(r) = check_maintenance(&state) {
        return r;
    }
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
    MirrorStatus {
        name: name.to_owned(),
        worker: worker.to_owned(),
        is_master: true,
        ..Default::default()
    }
}

// Read-only (maintenance) mode guard

/// Check if the manager is in maintenance mode. Mutating endpoints call
/// this at the top and return 503 if true.
fn check_maintenance(state: &AppState) -> Option<Response> {
    if state.maintenance.load(std::sync::atomic::Ordering::Relaxed) {
        Some(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrBody {
                    error: "manager is in maintenance (read-only) mode".into(),
                }),
            )
                .into_response(),
        )
    } else {
        None
    }
}

/// `POST /maintenance` — enable read-only mode (rejects all mutating requests).
async fn enable_maintenance(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state
        .maintenance
        .store(true, std::sync::atomic::Ordering::Relaxed);
    tracing::info!("maintenance mode ENABLED — all mutating endpoints will return 503");
    ok_msg("maintenance mode enabled")
}

/// `DELETE /maintenance` — disable read-only mode (resume normal operations).
async fn disable_maintenance(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state
        .maintenance
        .store(false, std::sync::atomic::Ordering::Relaxed);
    tracing::info!("maintenance mode DISABLED — normal operations resumed");
    ok_msg("maintenance mode disabled")
}

/// `GET /maintenance` — check current maintenance mode status.
async fn get_maintenance(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let enabled = state.maintenance.load(std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::json!({ "maintenance": enabled }))
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
    out.push_str(concat!(
        "# HELP tunasync_mirror_last_transferred_bytes ",
        "Bytes transferred during the most recent sync (0 if unknown or never run).\n",
        "# TYPE tunasync_mirror_last_transferred_bytes gauge\n",
    ));
    out.push_str(concat!(
        "# HELP tunasync_mirror_total_transferred_bytes ",
        "Cumulative bytes transferred across all syncs of this mirror. ",
        "Monotonically non-decreasing per (mirror, worker) — use Prometheus rate() ",
        "for traffic-per-second.\n",
        "# TYPE tunasync_mirror_total_transferred_bytes counter\n",
    ));

    let epoch = tunasync_protocol::zero_time();
    for m in mirrors {
        // Prometheus text format requires label values to escape backslashes,
        // double-quotes, and newlines (Exposition Format spec §3.3).
        // Mirror and worker names are typically alphanumeric slugs, but we
        // escape defensively to avoid producing malformed output if someone
        // creates a mirror named e.g. `foo"bar` or `line\nbreak`.
        let escape = |s: &str| {
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        };
        let labels = format!(
            "mirror=\"{}\",worker=\"{}\"",
            escape(&m.name),
            escape(&m.worker)
        );
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

        // Last and total transferred bytes (worker fills last_transferred_bytes
        // from the rsync log; manager accumulates total_transferred_bytes
        // across syncs). Both are reset to 0 when the mirror is recreated.
        out.push_str(&format!(
            "tunasync_mirror_last_transferred_bytes{{{labels}}} {}\n",
            m.last_transferred_bytes
        ));
        out.push_str(&format!(
            "tunasync_mirror_total_transferred_bytes{{{labels}}} {}\n",
            m.total_transferred_bytes
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
    // Strip trailing 'B'/'b' if present (e.g. "1.5GB" → "1.5G", "1.5gb" → "1.5g"),
    // then trim any whitespace between number and unit (e.g. "1.5 G").
    let s = if s.ends_with('B') || s.ends_with('b') {
        &s[..s.len() - 1]
    } else {
        s
    };
    let s = s.trim();
    let (num_str, multiplier) = match s.chars().last() {
        Some('K') | Some('k') => (s[..s.len() - 1].trim_end(), 1_024i64),
        Some('M') | Some('m') => (s[..s.len() - 1].trim_end(), 1_024 * 1_024),
        Some('G') | Some('g') => (s[..s.len() - 1].trim_end(), 1_024 * 1_024 * 1_024),
        Some('T') | Some('t') => (s[..s.len() - 1].trim_end(), 1_024 * 1_024 * 1_024 * 1_024),
        Some('P') | Some('p') => (
            s[..s.len() - 1].trim_end(),
            1_024 * 1_024 * 1_024 * 1_024 * 1_024,
        ),
        _ => (s, 1i64),
    };
    // Strip thousands separators. extract_size_from_rsync_log preserves
    // commas in the raw form (e.g. "1,234,567" for the UI), but f64::parse
    // rejects them. Without this strip, the Prometheus /metrics endpoint
    // reports -1 ("unknown") for any mirror whose rsync produced
    // comma-separated digit groups, while the UI shows the correct value.
    let cleaned = num_str.replace(',', "");
    cleaned
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
        use tunasync_protocol::SyncStatus;
        let mirrors = vec![MirrorStatus {
            name: "ubuntu".into(),
            worker: "w1".into(),
            is_master: true,
            status: SyncStatus::Success,
            last_update: chrono::Utc::now(),
            last_started: chrono::Utc::now() - chrono::Duration::seconds(3600),
            last_ended: chrono::Utc::now(),
            upstream: "rsync://example.com/".into(),
            size: "1.5G".into(),
            last_transferred_bytes: 5_368_709_120,
            total_transferred_bytes: 100_000_000_000,
            ..Default::default()
        }];
        let out = render_metrics(&mirrors, 2);
        assert!(out.contains("tunasync_workers_total 2"));
        assert!(out.contains("tunasync_mirror_status{mirror=\"ubuntu\",worker=\"w1\"} 3"));
        assert!(out.contains("tunasync_mirror_size_bytes{mirror=\"ubuntu\",worker=\"w1\"}"));
        assert!(out.contains("tunasync_mirrors_total{status=\"success\"} 1"));
        assert!(out.contains("tunasync_mirrors_total{status=\"failed\"} 0"));
        // Traffic stats — also emitted now.
        assert!(out.contains(
            "tunasync_mirror_last_transferred_bytes{mirror=\"ubuntu\",worker=\"w1\"} 5368709120"
        ));
        assert!(out.contains(
            "tunasync_mirror_total_transferred_bytes{mirror=\"ubuntu\",worker=\"w1\"} 100000000000"
        ));
    }

    /// Verifies the type metadata for the traffic metrics — last_* is
    /// a gauge (resets on a fresh sync), total_* is a counter
    /// (monotonically non-decreasing).
    #[test]
    fn render_metrics_traffic_has_correct_help_and_type() {
        use tunasync_protocol::SyncStatus;
        let mirrors = vec![MirrorStatus {
            name: "x".into(),
            worker: "w".into(),
            status: SyncStatus::Success,
            last_update: chrono::Utc::now(),
            ..Default::default()
        }];
        let out = render_metrics(&mirrors, 1);
        assert!(out.contains("# TYPE tunasync_mirror_last_transferred_bytes gauge"));
        assert!(out.contains("# TYPE tunasync_mirror_total_transferred_bytes counter"));
        // Both HELP lines present.
        assert!(out.contains("# HELP tunasync_mirror_last_transferred_bytes"));
        assert!(out.contains("# HELP tunasync_mirror_total_transferred_bytes"));
    }

    /// L5: parse_size_bytes should handle space between number and unit
    #[test]
    fn parse_size_bytes_with_space() {
        assert_eq!(
            parse_size_bytes("1.5 G"),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as i64
        );
        assert_eq!(parse_size_bytes("100 M"), 100 * 1024 * 1024);
        assert_eq!(parse_size_bytes("2 TB"), 2 * 1024 * 1024 * 1024 * 1024);
        assert_eq!(
            parse_size_bytes("1.5gb"),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as i64
        );
    }

    /// N2 (audit 2026-05-21): parse_size_bytes must strip thousands
    /// separators. extract_size_from_rsync_log was changed in v0.2.2 to
    /// accept comma-separated digit groups, but parse_size_bytes still
    /// called f64::parse() which fails on commas — the Prometheus /metrics
    /// endpoint returned -1 ("unknown") for mirrors whose rsync stats came
    /// out with commas, while the UI showed the real value. Now consistent.
    #[test]
    fn parse_size_bytes_strips_thousands_separators() {
        assert_eq!(parse_size_bytes("1,234,567"), 1_234_567);
        assert_eq!(parse_size_bytes("1,234,567,890"), 1_234_567_890);
        // With a unit suffix.
        assert_eq!(
            parse_size_bytes("1,234.5K"),
            (1234.5_f64 * 1024.0).round() as i64
        );
    }

    /// L1: Prometheus label values with special chars must be escaped
    #[test]
    fn render_metrics_escapes_special_chars_in_labels() {
        use tunasync_protocol::SyncStatus;
        let mirrors = vec![MirrorStatus {
            name: "tricky\"name".into(),
            worker: "work\\er".into(),
            is_master: true,
            status: SyncStatus::Success,
            last_update: chrono::Utc::now(),
            ..Default::default()
        }];
        let out = render_metrics(&mirrors, 1);
        // Quote and backslash must be escaped.
        assert!(
            out.contains(r#"mirror="tricky\"name""#),
            "double-quote in mirror name not escaped: {out}"
        );
        assert!(
            out.contains(r#"worker="work\\er""#),
            "backslash in worker name not escaped: {out}"
        );
        // No raw unescaped quote sequence in a label value.
        assert!(
            !out.contains(r#"mirror="tricky"name""#),
            "unescaped quote slipped through: {out}"
        );
    }
}

#[cfg(test)]
mod router_tests {
    //! Smoke tests that actually build the router and route requests through
    //! it. These guard the axum 0.8 path-parameter migration: the pre-0.8
    //! `:param` syntax panics at router-build time under axum 0.8, so simply
    //! constructing the router and hitting a parameterized route is enough to
    //! catch a missed conversion. The metrics_tests module above only exercises
    //! pure helpers and would not have caught it.

    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    fn test_state() -> Arc<AppState> {
        // A real on-disk-temp SQLite db keeps the test hermetic while
        // exercising the actual DbAdapter the handlers use.
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = crate::db::sqlite_adapter::SqliteAdapter::open(&tmp.path().join("t.db"))
            .expect("open sqlite");
        // Keep tmp alive for the duration of the test by leaking it; the OS
        // reclaims the file when the process exits. (Tests are short-lived.)
        std::mem::forget(tmp);
        Arc::new(AppState {
            db: Box::new(db),
            http_client: reqwest::Client::new(),
            sse_client: reqwest::Client::new(),
            api_token: String::new(),
            maintenance: std::sync::atomic::AtomicBool::new(false),
            notify: crate::config::NotifyConfig::default(),
        })
    }

    async fn get(uri: &str) -> StatusCode {
        let router = build_router(test_state());
        let resp = router
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("build request"),
            )
            .await
            .expect("router response");
        resp.status()
    }

    /// Baseline: the router builds and a static route responds. If any route
    /// still used the old `:param` syntax, build_router would have panicked
    /// before we got here.
    #[tokio::test]
    async fn ping_route_responds() {
        assert_eq!(get("/ping").await, StatusCode::OK);
    }

    /// Single path parameter: `/jobs/{name}`. Empty db -> 200 with an empty
    /// JSON array. Proves the param route matches and the extractor binds.
    #[tokio::test]
    async fn single_param_route_matches() {
        assert_eq!(get("/jobs/some-mirror").await, StatusCode::OK);
    }

    /// Single param on the workers tree: `/workers/{id}/jobs`. Unknown worker
    /// -> 400 ("invalid workerID"). The point is that the request routes to the
    /// handler at all (not a 404/405 from a misdeclared route).
    #[tokio::test]
    async fn worker_param_route_matches() {
        assert_eq!(
            get("/workers/nonexistent/jobs").await,
            StatusCode::BAD_REQUEST
        );
    }

    /// Unknown path still 404s — confirms routing is real, not a catch-all.
    #[tokio::test]
    async fn unknown_route_is_404() {
        assert_eq!(get("/no/such/path").await, StatusCode::NOT_FOUND);
    }
}
