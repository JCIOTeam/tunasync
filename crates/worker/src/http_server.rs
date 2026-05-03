//! HTTP server running inside the worker.
//!
//! The manager (and `tunasynctl`) POST `WorkerCmd` to the worker's URL.
//! The worker also exposes `GET /jobs` for introspection.
//!
//! Matches Go's `worker/worker.go makeHTTPServer`.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tokio::sync::mpsc;
use tunasync_protocol::WorkerCmd;

use crate::job::CtrlAction;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// State shared with axum handlers.
pub struct WorkerHttpState {
    /// Channel to forward received commands to the scheduler.
    pub cmd_tx: mpsc::Sender<WorkerCmd>,
    /// Worker name (for informational GET /jobs response).
    pub worker_name: String,
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
async fn handle_cmd(
    State(state): State<Arc<WorkerHttpState>>,
    Json(cmd): Json<WorkerCmd>,
) -> Response {
    tracing::info!(cmd = %cmd, "received command from manager");
    match state.cmd_tx.try_send(cmd) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "cmd channel full or closed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

/// `GET /jobs` — basic introspection (not in Go's original, but handy).
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
    use tunasync_protocol::CmdVerb::*;
    match cmd.cmd {
        Start => {
            // Go's worker checks `cmd.Options["force"]` to decide ForceStart.
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
        Reload => None, // handled at worker level
    }
}
