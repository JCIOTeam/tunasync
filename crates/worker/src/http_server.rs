//! HTTP server running inside the worker.
//!
//! The manager (and `tunasynctl`) POST `WorkerCmd` to the worker's URL.
//! The worker also exposes `GET /jobs` for introspection.
//!
//! Matches Go's `worker/worker.go makeHTTPServer` — response bodies and
//! status codes are identical:
//!
//! ```text
//! 200  { "msg": "OK" }            — valid command accepted
//! 400  { "msg": "Invalid request" } — malformed JSON body
//! 404  { "msg": "Mirror '...' not found" } — unknown mirror_id
//! 406  { "msg": "Invalid Command" } — empty mirror_id + non-Reload, or unknown verb
//! 503                              — internal channel full/closed
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tokio::sync::{mpsc, RwLock};
use tunasync_protocol::{CmdVerb, WorkerCmd};

use crate::job::CtrlAction;

// ---------------------------------------------------------------------------
// JSON response shapes (matching Go's _infoKey / _errorKey convention)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct MsgResponse {
    msg: String,
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// State shared with axum handlers.
pub struct WorkerHttpState {
    /// Channel to forward validated commands to the scheduler.
    pub cmd_tx: mpsc::Sender<WorkerCmd>,
    /// Worker name (for informational GET /jobs response).
    pub worker_name: String,
    /// Set of currently known mirror names — updated by the scheduler loop.
    /// Used by the HTTP handler to validate `mirror_id` before accepting a command.
    pub mirror_names: Arc<RwLock<HashSet<String>>>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn build_router(state: WorkerHttpState) -> Router {
    let shared = Arc::new(state);
    Router::new()
        .route("/", post(handle_cmd))
        .route("/jobs", get(list_jobs))
        .with_state(shared)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /` — receive a `WorkerCmd` from the manager.
///
/// Matches Go's behavior exactly:
/// - Valid command → 200 + `{ "msg": "OK" }`
/// - Bad JSON → 400 + `{ "msg": "Invalid request" }`
/// - Empty mirror_id + non-Reload → 406 + `{ "msg": "Invalid Command" }`
/// - Unknown mirror_id → 404 + `{ "msg": "Mirror '...' not found" }`
/// - Unknown CmdVerb → 406 + `{ "msg": "Invalid Command" }`
async fn handle_cmd(
    State(state): State<Arc<WorkerHttpState>>,
    body: axum::body::Bytes,
) -> Response {
    // Parse JSON — Go returns 400 on invalid JSON.
    let cmd: WorkerCmd = match serde_json::from_slice(&body) {
        Ok(c) => c,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(MsgResponse {
                    msg: "Invalid request".into(),
                }),
            )
                .into_response();
        }
    };

    tracing::info!(cmd = ?cmd, "received command from manager");

    // --- Validate: empty mirror_id ---
    // Go: only Reload is allowed with empty mirror_id; everything else → 406.
    if cmd.mirror_id.is_empty() && cmd.cmd != CmdVerb::Reload {
        return (
            StatusCode::NOT_ACCEPTABLE,
            Json(MsgResponse {
                msg: "Invalid Command".into(),
            }),
        )
            .into_response();
    }

    // --- Validate: unknown CmdVerb ---
    // Go: unrecognized verbs → 406. We check via cmd_to_ctrl.
    if cmd.cmd != CmdVerb::Reload && cmd_to_ctrl(&cmd).is_none() {
        return (
            StatusCode::NOT_ACCEPTABLE,
            Json(MsgResponse {
                msg: "Invalid Command".into(),
            }),
        )
            .into_response();
    }

    // --- Validate: unknown mirror_id ---
    // Go: mirror not found → 404.
    if !cmd.mirror_id.is_empty() {
        let names = state.mirror_names.read().await;
        if !names.contains(&cmd.mirror_id) {
            return (
                StatusCode::NOT_FOUND,
                Json(MsgResponse {
                    msg: format!("Mirror '{}' not found", cmd.mirror_id),
                }),
            )
                .into_response();
        }
    }

    // --- Forward to scheduler ---
    match state.cmd_tx.try_send(cmd) {
        Ok(()) => (StatusCode::OK, Json(MsgResponse { msg: "OK".into() })).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// `GET /jobs` — basic introspection (not in Go's original, but harmless).
async fn list_jobs(State(state): State<Arc<WorkerHttpState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct Info {
        worker: String,
    }
    Json(Info {
        worker: state.worker_name.clone(),
    })
}

// ---------------------------------------------------------------------------
// Map WorkerCmd → CtrlAction
// ---------------------------------------------------------------------------

/// Convert a wire `WorkerCmd` to a job-level `CtrlAction`.
///
/// Returns `None` for worker-wide commands that aren't per-job
/// (e.g. `Reload` is handled at the worker level, not forwarded to a job task).
pub fn cmd_to_ctrl(cmd: &WorkerCmd) -> Option<CtrlAction> {
    use CmdVerb::*;
    match cmd.cmd {
        Start => {
            if cmd.options.get("force").copied().unwrap_or(false) {
                Some(CtrlAction::ForceStart)
            } else {
                Some(CtrlAction::Start)
            }
        }
        Stop => Some(CtrlAction::Stop),
        Disable => Some(CtrlAction::Disable),
        Restart => Some(CtrlAction::Restart),
        Ping => Some(CtrlAction::Ping),
        Reload => None,
    }
}
