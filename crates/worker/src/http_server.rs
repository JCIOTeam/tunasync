//! HTTP server running inside the worker.
//!
//! The manager (and `tunasynctl`) POST `WorkerCmd` to the worker's URL.
//! The worker also exposes `GET /jobs` for introspection and
//! `GET /jobs/:mirror/log/stream` for live (SSE) sync-log streaming.
//!
//! Matches Go's `worker/worker.go makeHTTPServer` — response bodies and
//! status codes for the command endpoint are identical:
//!
//! ```text
//! 200  { "msg": "OK" }            — valid command accepted
//! 400  { "msg": "Invalid request" } — malformed JSON body
//! 404  { "msg": "Mirror '...' not found" } — unknown mirror_id
//! 406  { "msg": "Invalid Command" } — empty mirror_id + non-Reload, or unknown verb
//! 503                              — internal channel full/closed
//! ```
//!
//! New (Rust-only) endpoint:
//!
//! ```text
//! GET  /jobs/:mirror/log/stream    — text/event-stream of stdout/stderr lines
//!                                   produced by the running sync (if any). Lines
//!                                   are observed from the moment of subscription
//!                                   only — historical content lives in the
//!                                   rotated log file. 404 if `mirror` is unknown.
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, StreamExt};
use serde::Serialize;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tunasync_protocol::{CmdVerb, WorkerCmd};

use crate::job::CtrlAction;
use crate::log_stream::LogBroadcaster;

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
    /// Per-mirror live-log broadcast registry — used by the SSE endpoint.
    pub log_broadcaster: Arc<LogBroadcaster>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn build_router(state: WorkerHttpState) -> Router {
    let shared = Arc::new(state);
    Router::new()
        .route("/", post(handle_cmd))
        .route("/jobs", get(list_jobs))
        .route("/jobs/:mirror/log/stream", get(stream_log))
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

/// `GET /jobs/:mirror/log/stream` — stream stdout/stderr lines of the
/// currently running (or just-finished) sync as Server-Sent Events.
///
/// # Behaviour
///
/// - Returns `404` with a JSON body when `mirror` is not a known mirror name.
/// - On connect, the most recent ~10 lines of the *current* sync are
///   replayed as ordinary SSE `data:` events, then the connection seamlessly
///   continues into live mode and forwards each new line as it appears.
///   The replay buffer is cleared at the start of every sync, so the client
///   never sees stale content from a previous run.
/// - A keep-alive comment is sent every 15 s so intermediaries that drop
///   idle connections (proxies, load balancers, browser defaults) keep
///   the stream open during long quiescent periods between rsync output.
/// - If the subscriber falls behind by more than the broadcast channel
///   capacity, a single `event: lag` notification is emitted and the
///   stream resumes from the newest line.
async fn stream_log(
    State(state): State<Arc<WorkerHttpState>>,
    Path(mirror): Path<String>,
) -> Response {
    // 404 fast-path for unknown mirrors — symmetric with the POST handler.
    {
        let names = state.mirror_names.read().await;
        if !names.contains(&mirror) {
            return (
                StatusCode::NOT_FOUND,
                Json(MsgResponse {
                    msg: format!("Mirror '{mirror}' not found"),
                }),
            )
                .into_response();
        }
    }

    // Atomic snapshot+subscribe: any line published between these two
    // operations is guaranteed to land on `rx` (never both, never neither).
    let (history, rx) = state.log_broadcaster.snapshot_and_subscribe(&mirror);

    // Preamble: an SSE comment confirming the subscription (so `curl -N`
    // shows something immediately), followed by every buffered line as
    // an ordinary `data:` event. Replay events use the same shape as
    // live events so client code doesn't need a separate code path.
    let preamble =
        stream::once(async move { Ok::<_, Infallible>(Event::default().comment("subscribed")) });
    let replay = stream::iter(
        history
            .into_iter()
            .map(|line| Ok::<_, Infallible>(Event::default().data(line))),
    );

    let live = BroadcastStream::new(rx).map(|item| -> Result<Event, Infallible> {
        match item {
            Ok(line) => Ok(Event::default().data(line)),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                Ok(Event::default()
                    .event("lag")
                    .data(format!("dropped {n} line(s); subscriber too slow")))
            }
        }
    });

    let stream = preamble.chain(replay).chain(live);

    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
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

#[cfg(test)]
mod sse_tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    /// Spin up a real axum server on a random port, then connect with a raw
    /// TCP client to read the SSE stream. We avoid `reqwest` to keep the
    /// test free of extra dependencies and to inspect the wire bytes
    /// directly (events, comments, keep-alives).
    async fn start_server() -> (SocketAddr, Arc<LogBroadcaster>) {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<WorkerCmd>(8);
        let mut names = HashSet::new();
        names.insert("debian".to_owned());
        let mirror_names = Arc::new(RwLock::new(names));
        let log_broadcaster = LogBroadcaster::new();
        let state = WorkerHttpState {
            cmd_tx,
            worker_name: "test".to_owned(),
            mirror_names,
            log_broadcaster: Arc::clone(&log_broadcaster),
        };
        let app = build_router(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, log_broadcaster)
    }

    #[tokio::test]
    async fn unknown_mirror_returns_404() {
        let (addr, _b) = start_server().await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /jobs/unknown/log/stream HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut buf = BufReader::new(s);
        let mut status = String::new();
        buf.read_line(&mut status).await.unwrap();
        assert!(
            status.contains("404"),
            "expected 404, got status line: {status:?}"
        );
    }

    #[tokio::test]
    async fn lines_flow_to_subscriber() {
        let (addr, broadcaster) = start_server().await;

        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(
            b"GET /jobs/debian/log/stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

        let mut buf = BufReader::new(s);

        // Read response head + chunked-transfer headers until blank line.
        loop {
            let mut line = String::new();
            buf.read_line(&mut line).await.unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }

        // Give axum a beat to fully wire up the subscription, then publish.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        broadcaster
            .publisher_for("debian")
            .push("hello world".to_owned());

        // SSE bodies are sent over chunked transfer-encoding. Reading line-
        // by-line works because each "data: ...\n\n" sits inside its own
        // chunk that the kernel hands us. We scan until we find our payload.
        let mut found = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            let mut line = String::new();
            tokio::select! {
                r = buf.read_line(&mut line) => {
                    if r.unwrap() == 0 { break; }
                    if line.contains("hello world") { found = true; break; }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
        assert!(found, "expected to see broadcast line on SSE stream");
    }

    #[tokio::test]
    async fn mid_sync_client_sees_buffered_replay() {
        let (addr, broadcaster) = start_server().await;

        // Pre-populate the buffer with two lines — simulates rsync output
        // produced before the client managed to connect.
        let publisher = broadcaster.publisher_for("debian");
        publisher.push("line-before-1".into());
        publisher.push("line-before-2".into());

        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(
            b"GET /jobs/debian/log/stream HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

        let mut buf = BufReader::new(s);
        // Skip response headers.
        loop {
            let mut line = String::new();
            buf.read_line(&mut line).await.unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }

        // Both buffered lines should arrive without us pushing anything new.
        let mut seen1 = false;
        let mut seen2 = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline && !(seen1 && seen2) {
            let mut line = String::new();
            tokio::select! {
                r = buf.read_line(&mut line) => {
                    if r.unwrap() == 0 { break; }
                    if line.contains("line-before-1") { seen1 = true; }
                    if line.contains("line-before-2") { seen2 = true; }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
        assert!(
            seen1 && seen2,
            "expected both buffered lines to be replayed"
        );
    }
}
