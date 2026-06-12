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

/// Reports that failed to reach ANY manager base and are awaiting replay.
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
/// Reports that fail against ALL manager bases are stashed in a pending
/// buffer and replayed by [`ManagerClient::flush_pending`] (called from the
/// heartbeat task once the manager is reachable again), so a manager outage
/// no longer permanently loses terminal sync states or traffic stats.
pub struct ManagerClient {
    bases: Vec<String>,
    client: Client,
    /// `Authorization: Bearer` token attached to every request when
    /// non-empty (see `[manager] api_token` in worker.conf).
    token: String,
    pending: tokio::sync::Mutex<PendingReports>,
}

impl ManagerClient {
    pub fn new(bases: Vec<String>, client: Client, token: String) -> Self {
        Self {
            bases,
            client,
            token,
            pending: tokio::sync::Mutex::new(PendingReports::default()),
        }
    }

    /// Attach bearer auth when a token is configured.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }

    // ------------------------------------------------------------------
    // Worker registration / heartbeat
    // ------------------------------------------------------------------

    /// `POST {manager}/workers` — register this worker.
    pub async fn register(&self, status: &WorkerStatus) -> Result<WorkerStatus> {
        self.post_first("/workers", status).await
    }

    /// `POST {manager}/workers/{worker_id}/heartbeat` — keep last_online fresh.
    pub async fn heartbeat(&self, worker_id: &str) -> Result<()> {
        let path = format!("/workers/{worker_id}/heartbeat");
        let _: serde_json::Value = self.post_first(&path, &serde_json::json!({})).await?;
        Ok(())
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
        let path = format!("/workers/{worker_id}/jobs/{}", status.name);
        let errs = self.post_all(&path, status).await;
        if errs.is_empty() {
            // A fresh report supersedes any stashed (older) one.
            self.pending.lock().await.statuses.remove(&status.name);
            Ok(status.clone())
        } else if errs.len() < self.bases.len() {
            // At least one base succeeded — partial failure is acceptable.
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial status report failure");
            }
            self.pending.lock().await.statuses.remove(&status.name);
            Ok(status.clone())
        } else {
            // All bases failed — stash the latest status for replay once the
            // manager comes back (see flush_pending). Latest-wins per mirror.
            self.pending
                .lock()
                .await
                .statuses
                .insert(status.name.clone(), status.clone());
            Err(errs.into_iter().last().unwrap().1)
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
            let _ = self.report_status(worker_id, &status).await;
        }
        for (mirror, size) in snapshot.sizes {
            let _ = self.report_size(worker_id, &mirror, &size).await;
        }
        if let Some(schedules) = snapshot.schedules {
            let _ = self.report_schedules(worker_id, &schedules).await;
        }
    }

    /// `POST {manager}/workers/{worker_id}/jobs/{mirror_id}/size` — POST to all bases.
    /// Returns Ok if at least one base succeeded.
    pub async fn report_size(&self, worker_id: &str, mirror_id: &str, size: &str) -> Result<()> {
        #[derive(serde::Serialize)]
        struct SizeMsg<'a> {
            name: &'a str,
            size: &'a str,
        }
        let path = format!("/workers/{worker_id}/jobs/{mirror_id}/size");
        let errs = self
            .post_all(
                &path,
                &SizeMsg {
                    name: mirror_id,
                    size,
                },
            )
            .await;
        if errs.is_empty() {
            self.pending.lock().await.sizes.remove(mirror_id);
            Ok(())
        } else if errs.len() < self.bases.len() {
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial size report failure");
            }
            self.pending.lock().await.sizes.remove(mirror_id);
            Ok(())
        } else {
            self.pending
                .lock()
                .await
                .sizes
                .insert(mirror_id.to_string(), size.to_string());
            Err(errs.into_iter().last().unwrap().1)
        }
    }

    /// `POST {manager}/workers/{worker_id}/schedules` — POST to all bases.
    /// Returns Ok if at least one base succeeded.
    pub async fn report_schedules(
        &self,
        worker_id: &str,
        schedules: &MirrorSchedules,
    ) -> Result<()> {
        let path = format!("/workers/{worker_id}/schedules");
        let errs = self.post_all(&path, schedules).await;
        if errs.is_empty() {
            self.pending.lock().await.schedules = None;
            Ok(())
        } else if errs.len() < self.bases.len() {
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial schedule report failure");
            }
            Ok(())
        } else {
            self.pending.lock().await.schedules = Some(schedules.clone());
            Err(errs.into_iter().last().unwrap().1)
        }
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// POST to ALL manager base URLs, collecting errors. Matches Go's behaviour
    /// where status/schedule updates go to every manager in the list.
    async fn post_all<Req>(&self, path: &str, body: &Req) -> Vec<(String, anyhow::Error)>
    where
        Req: serde::Serialize,
    {
        let mut errs = Vec::new();
        for base in &self.bases {
            let url = format!("{base}{path}");
            match self
                .auth(self.client.post(&url))
                .timeout(Duration::from_secs(30))
                .json(body)
                .send()
                .await
            {
                Err(e) => {
                    tracing::warn!(url = %url, error = %e, "manager request failed");
                    errs.push((base.clone(), e.into()));
                }
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        errs.push((
                            base.clone(),
                            anyhow::anyhow!("POST {url} returned {}: {body}", status),
                        ));
                    }
                    // success — no need to record it
                }
            }
        }
        errs
    }

    /// POST to the first available manager base URL, returning the parsed
    /// response body. Falls through to each base in turn on connection errors.
    async fn post_first<Req, Res>(&self, path: &str, body: &Req) -> Result<Res>
    where
        Req: serde::Serialize,
        Res: serde::de::DeserializeOwned,
    {
        let mut last_err = anyhow::anyhow!("no manager URLs configured");
        for base in &self.bases {
            let url = format!("{base}{path}");
            match self
                .auth(self.client.post(&url))
                .timeout(Duration::from_secs(30))
                .json(body)
                .send()
                .await
            {
                Err(e) => {
                    tracing::warn!(url = %url, error = %e, "manager request failed");
                    last_err = e.into();
                }
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json::<Res>()
                            .await
                            .with_context(|| format!("decode response from {url}"));
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        last_err = anyhow::anyhow!("POST {url} returned {status}: {body}");
                    }
                }
            }
        }
        Err(last_err)
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
        let mut last_err = anyhow::anyhow!("no manager URLs configured");
        for base in &self.bases {
            let url = format!("{base}{path}");
            match self
                .auth(self.client.get(&url))
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
