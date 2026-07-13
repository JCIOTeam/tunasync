//! Worker-side manager API client.
//!
//! The Go worker uses `PostJSON` / `GetJSON` helpers from `internal/util.go`.
//! We replace that with typed `reqwest` calls. Retry / round-robin across
//! `api_base_list` is handled here.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tunasync_protocol::{MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus};

/// Reports that failed to reach one or more manager bases and await replay.
///
/// Coalesced: only the LATEST status/size per mirror and the latest
/// schedules snapshot are kept, so the buffer is bounded by mirror count
/// and replay can never flood a freshly recovered manager.
#[derive(Default)]
struct PendingReports {
    statuses: HashMap<String, MirrorStatus>,
    sizes: HashMap<String, String>,
    schedules: Option<MirrorSchedules>,
}

impl PendingReports {
    fn is_empty(&self) -> bool {
        self.statuses.is_empty() && self.sizes.is_empty() && self.schedules.is_none()
    }
}

/// Thin async client for the tunasync manager REST API.
///
/// One instance is shared across the worker via `Arc<ManagerClient>`.
///
/// Reports that fail against one or more manager bases are stashed in a pending
/// buffer and replayed by [`ManagerClient::flush_pending`] (called from the
/// heartbeat task once the manager is reachable again), so a manager outage
/// no longer permanently loses terminal sync states or traffic stats.
pub struct ManagerClient {
    connection: parking_lot::RwLock<ManagerConnection>,
    registration: parking_lot::RwLock<Option<WorkerStatus>>,
    pending: tokio::sync::Mutex<PendingReports>,
    /// Serialize live delivery with pending replay. Without this, a stale
    /// snapshot taken by flush_pending could arrive after a newer live report
    /// and overwrite manager state.
    delivery: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
struct ManagerConnection {
    bases: Vec<String>,
    client: Client,
    token: String,
}

struct PostAllResult {
    attempted: usize,
    errors: Vec<(String, anyhow::Error)>,
}

impl ManagerClient {
    pub fn new(bases: Vec<String>, client: Client, token: String) -> Self {
        Self {
            connection: parking_lot::RwLock::new(ManagerConnection {
                bases,
                client,
                token,
            }),
            registration: parking_lot::RwLock::new(None),
            pending: tokio::sync::Mutex::new(PendingReports::default()),
            delivery: tokio::sync::Mutex::new(()),
        }
    }

    /// Replace manager endpoints and credentials without dropping buffered
    /// reports or interrupting requests already in flight.
    pub fn reconfigure(&self, bases: Vec<String>, client: Client, token: String) {
        *self.connection.write() = ManagerConnection {
            bases,
            client,
            token,
        };
    }

    /// Attach bearer auth when a token is configured.
    fn auth(req: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
        if token.is_empty() {
            req
        } else {
            req.bearer_auth(token)
        }
    }

    // ------------------------------------------------------------------
    // Worker registration / heartbeat
    // ------------------------------------------------------------------

    /// `POST {manager}/workers` — register this worker.
    pub async fn register(&self, status: &WorkerStatus) -> Result<WorkerStatus> {
        *self.registration.write() = Some(status.clone());
        let connection = self.connection.read().clone();
        let mut first_success = None;
        let mut last_err = anyhow::anyhow!("no manager URLs configured");
        for base in &connection.bases {
            match Self::register_one(&connection, base, status).await {
                Ok(registered) => {
                    if first_success.is_none() {
                        first_success = Some(registered);
                    }
                }
                Err(e) => {
                    tracing::warn!(manager = %base, error = %e, "manager registration failed");
                    last_err = e;
                }
            }
        }
        first_success.ok_or(last_err)
    }

    /// `POST {manager}/workers/{worker_id}/heartbeat` — keep last_online fresh.
    pub async fn heartbeat(&self, worker_id: &str) -> Result<()> {
        let connection = self.connection.read().clone();
        let registration = self.registration.read().clone();
        let mut any_success = false;
        let mut last_err = anyhow::anyhow!("no manager URLs configured");

        for base in &connection.bases {
            let url = format!("{base}/workers/{worker_id}/heartbeat");
            let heartbeat = Self::auth(connection.client.post(&url), &connection.token)
                .timeout(Duration::from_secs(30))
                .json(&serde_json::json!({}))
                .send()
                .await;
            match heartbeat {
                Ok(resp) if resp.status().is_success() => {
                    any_success = true;
                    continue;
                }
                Ok(resp) => {
                    let code = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    last_err = anyhow::anyhow!("POST {url} returned {code}: {body}");
                }
                Err(e) => last_err = e.into(),
            }

            // A manager that was unavailable during startup has no worker row,
            // so replaying reports alone can never recover it. Register again
            // as soon as that manager becomes reachable.
            if let Some(status) = &registration {
                match Self::register_one(&connection, base, status).await {
                    Ok(_) => {
                        any_success = true;
                        tracing::info!(manager = %base, worker = %worker_id, "re-registered worker with manager");
                    }
                    Err(e) => last_err = e,
                }
            }
        }

        if any_success {
            Ok(())
        } else {
            Err(last_err)
        }
    }

    async fn register_one(
        connection: &ManagerConnection,
        base: &str,
        status: &WorkerStatus,
    ) -> Result<WorkerStatus> {
        let url = format!("{base}/workers");
        let resp = Self::auth(connection.client.post(&url), &connection.token)
            .timeout(Duration::from_secs(30))
            .json(status)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let code = resp.status();
        if !code.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {url} returned {code}: {body}");
        }
        resp.json::<WorkerStatus>()
            .await
            .with_context(|| format!("decode registration response from {url}"))
    }

    // ------------------------------------------------------------------
    // Mirror status reporting
    // ------------------------------------------------------------------

    /// `POST {manager}/workers/{worker_id}/jobs/{mirror_id}` — POST to all bases.
    /// Returns Ok if at least one base succeeded (matches Go's behaviour).
    pub async fn report_status(
        &self,
        worker_id: &str,
        status: &MirrorStatus,
    ) -> Result<MirrorStatus> {
        let _delivery = self.delivery.lock().await;
        self.report_status_unlocked(worker_id, status).await
    }

    async fn report_status_unlocked(
        &self,
        worker_id: &str,
        status: &MirrorStatus,
    ) -> Result<MirrorStatus> {
        let path = format!("/workers/{worker_id}/jobs/{}", status.name);
        let result = self.post_all(&path, status).await;
        if result.errors.is_empty() {
            // A fresh report supersedes any stashed (older) one.
            self.pending.lock().await.statuses.remove(&status.name);
            Ok(status.clone())
        } else if result.errors.len() < result.attempted {
            // At least one base succeeded — partial failure is acceptable.
            for (url, e) in &result.errors {
                tracing::warn!(url = %url, error = %e, "partial status report failure");
            }
            self.pending
                .lock()
                .await
                .statuses
                .insert(status.name.clone(), status.clone());
            Ok(status.clone())
        } else {
            // All bases failed — stash the latest status for replay once the
            // manager comes back (see flush_pending). Latest-wins per mirror.
            self.pending
                .lock()
                .await
                .statuses
                .insert(status.name.clone(), status.clone());
            Err(result.errors.into_iter().last().unwrap().1)
        }
    }

    /// Replay reports that previously failed against all manager bases.
    ///
    /// Called from the heartbeat task right after a successful heartbeat —
    /// i.e. the moment we KNOW the manager is reachable again. Entries are
    /// taken out of the buffer before sending and re-stashed on failure;
    /// a concurrent live report for the same mirror simply wins (it removes
    /// the pending entry on its own success, and manager-side state is
    /// last-write-wins anyway).
    pub async fn flush_pending(&self, worker_id: &str) {
        let _delivery = self.delivery.lock().await;
        // Take a snapshot so we don't hold the lock across network I/O.
        let snapshot = {
            let mut p = self.pending.lock().await;
            if p.is_empty() {
                return;
            }
            std::mem::take(&mut *p)
        };
        let n = snapshot.statuses.len() + snapshot.sizes.len();
        tracing::info!(
            worker = %worker_id,
            pending = n,
            "manager reachable again — replaying buffered reports"
        );
        for (_, status) in snapshot.statuses {
            // report_status re-stashes on failure.
            let _ = self.report_status_unlocked(worker_id, &status).await;
        }
        for (mirror, size) in snapshot.sizes {
            let _ = self.report_size_unlocked(worker_id, &mirror, &size).await;
        }
        if let Some(schedules) = snapshot.schedules {
            let _ = self.report_schedules_unlocked(worker_id, &schedules).await;
        }
    }

    /// `POST {manager}/workers/{worker_id}/jobs/{mirror_id}/size` — POST to all bases.
    /// Returns Ok if at least one base succeeded.
    pub async fn report_size(&self, worker_id: &str, mirror_id: &str, size: &str) -> Result<()> {
        let _delivery = self.delivery.lock().await;
        self.report_size_unlocked(worker_id, mirror_id, size).await
    }

    async fn report_size_unlocked(
        &self,
        worker_id: &str,
        mirror_id: &str,
        size: &str,
    ) -> Result<()> {
        #[derive(serde::Serialize)]
        struct SizeMsg<'a> {
            name: &'a str,
            size: &'a str,
        }
        let path = format!("/workers/{worker_id}/jobs/{mirror_id}/size");
        let result = self
            .post_all(
                &path,
                &SizeMsg {
                    name: mirror_id,
                    size,
                },
            )
            .await;
        if result.errors.is_empty() {
            self.pending.lock().await.sizes.remove(mirror_id);
            Ok(())
        } else if result.errors.len() < result.attempted {
            for (url, e) in &result.errors {
                tracing::warn!(url = %url, error = %e, "partial size report failure");
            }
            self.pending
                .lock()
                .await
                .sizes
                .insert(mirror_id.to_string(), size.to_string());
            Ok(())
        } else {
            self.pending
                .lock()
                .await
                .sizes
                .insert(mirror_id.to_string(), size.to_string());
            Err(result.errors.into_iter().last().unwrap().1)
        }
    }

    /// `POST {manager}/workers/{worker_id}/schedules` — POST to all bases.
    /// Returns Ok if at least one base succeeded.
    pub async fn report_schedules(
        &self,
        worker_id: &str,
        schedules: &MirrorSchedules,
    ) -> Result<()> {
        let _delivery = self.delivery.lock().await;
        self.report_schedules_unlocked(worker_id, schedules).await
    }

    async fn report_schedules_unlocked(
        &self,
        worker_id: &str,
        schedules: &MirrorSchedules,
    ) -> Result<()> {
        let path = format!("/workers/{worker_id}/schedules");
        let result = self.post_all(&path, schedules).await;
        if result.errors.is_empty() {
            self.pending.lock().await.schedules = None;
            Ok(())
        } else if result.errors.len() < result.attempted {
            for (url, e) in &result.errors {
                tracing::warn!(url = %url, error = %e, "partial schedule report failure");
            }
            self.pending.lock().await.schedules = Some(schedules.clone());
            Ok(())
        } else {
            self.pending.lock().await.schedules = Some(schedules.clone());
            Err(result.errors.into_iter().last().unwrap().1)
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// POST to ALL manager base URLs, collecting errors. Matches Go's behaviour
    /// where status/schedule updates go to every manager in the list.
    async fn post_all<Req>(&self, path: &str, body: &Req) -> PostAllResult
    where
        Req: serde::Serialize,
    {
        let connection = self.connection.read().clone();
        let attempted = connection.bases.len();
        let mut errors = Vec::new();
        for base in &connection.bases {
            let url = format!("{base}{path}");
            match Self::auth(connection.client.post(&url), &connection.token)
                .timeout(Duration::from_secs(30))
                .json(body)
                .send()
                .await
            {
                Err(e) => {
                    tracing::warn!(url = %url, error = %e, "manager request failed");
                    errors.push((base.clone(), e.into()));
                }
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        errors.push((
                            base.clone(),
                            anyhow::anyhow!("POST {url} returned {}: {body}", status),
                        ));
                    }
                    // success — no need to record it
                }
            }
        }
        PostAllResult { attempted, errors }
    }

    // ------------------------------------------------------------------
    // Job status recovery
    // ------------------------------------------------------------------

    /// `GET {manager}/workers/{worker_id}/jobs` — fetch persisted mirror
    /// statuses from the manager (used at startup to restore Paused/Disabled).
    /// Matches Go's `fetchJobStatus()`.
    pub async fn fetch_job_status(
        &self,
        worker_id: &str,
    ) -> Result<Vec<tunasync_protocol::MirrorStatus>> {
        let path = format!("/workers/{worker_id}/jobs");
        self.get::<Vec<tunasync_protocol::MirrorStatus>>(&path)
            .await
    }

    /// GET the first available manager base URL.
    pub async fn get<Res>(&self, path: &str) -> Result<Res>
    where
        Res: serde::de::DeserializeOwned,
    {
        let connection = self.connection.read().clone();
        let mut last_err = anyhow::anyhow!("no manager URLs configured");
        for base in &connection.bases {
            let url = format!("{base}{path}");
            match Self::auth(connection.client.get(&url), &connection.token)
                .timeout(Duration::from_secs(30))
                .send()
                .await
            {
                Err(e) => {
                    last_err = e.into();
                }
                Ok(resp) => {
                    if resp.status().is_success() {
                        return resp.json::<Res>().await.context("decode response");
                    }
                }
            }
        }
        Err(last_err)
    }

    /// Parse a `WorkerCmd` from an incoming request body bytes.
    /// Helper used by the worker's HTTP server when receiving commands.
    pub fn parse_cmd(bytes: &[u8]) -> Result<WorkerCmd> {
        serde_json::from_slice(bytes).context("parse WorkerCmd")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn reconfigure_switches_endpoint_and_token() {
        async fn handler(
            State(expected): State<Arc<String>>,
            headers: axum::http::HeaderMap,
            Json(body): Json<Value>,
        ) -> (StatusCode, Json<Value>) {
            let auth = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok());
            if auth != Some(expected.as_str()) {
                return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({})));
            }
            (StatusCode::OK, Json(body))
        }

        async fn server(expected: &str) -> String {
            let app = Router::new()
                .route("/workers", post(handler))
                .with_state(Arc::new(expected.to_string()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            format!("http://{addr}")
        }

        let first = server("Bearer first").await;
        let second = server("Bearer second").await;
        let client = reqwest::Client::new();
        let manager = ManagerClient::new(vec![first], client.clone(), "first".into());
        let status = WorkerStatus {
            id: "w1".into(),
            url: "http://127.0.0.1:6000".into(),
            token: String::new(),
            last_online: tunasync_protocol::zero_time(),
            last_register: tunasync_protocol::zero_time(),
        };

        manager.register(&status).await.unwrap();
        manager.reconfigure(vec![second], client, "second".into());
        manager.register(&status).await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_re_registers_missing_worker() {
        #[derive(Default)]
        struct RegistrationState {
            registered: AtomicBool,
        }

        async fn register(
            State(state): State<Arc<RegistrationState>>,
            Json(status): Json<WorkerStatus>,
        ) -> Json<WorkerStatus> {
            state.registered.store(true, Ordering::SeqCst);
            Json(status)
        }

        async fn heartbeat(State(state): State<Arc<RegistrationState>>) -> StatusCode {
            if state.registered.load(Ordering::SeqCst) {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            }
        }

        let state = Arc::new(RegistrationState::default());
        let app = Router::new()
            .route("/workers", post(register))
            .route("/workers/{id}/heartbeat", post(heartbeat))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let manager = ManagerClient::new(
            vec![format!("http://{addr}")],
            reqwest::Client::new(),
            String::new(),
        );
        let status = WorkerStatus {
            id: "w1".into(),
            url: "http://127.0.0.1:6000".into(),
            token: String::new(),
            last_online: tunasync_protocol::zero_time(),
            last_register: tunasync_protocol::zero_time(),
        };
        manager.register(&status).await.unwrap();

        state.registered.store(false, Ordering::SeqCst);
        manager.heartbeat("w1").await.unwrap();
        assert!(state.registered.load(Ordering::SeqCst));
    }
}
