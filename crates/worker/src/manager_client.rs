//! Worker-side manager API client.
//!
//! The Go worker uses `PostJSON` / `GetJSON` helpers from `internal/util.go`.
//! We replace that with typed `reqwest` calls. Retry / round-robin across
//! `api_base_list` is handled here.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tunasync_protocol::{MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus};

/// Thin async client for the tunasync manager REST API.
///
/// One instance is shared across the worker via `Arc<ManagerClient>`.
pub struct ManagerClient {
    bases: Vec<String>,
    client: Client,
}

impl ManagerClient {
    pub fn new(bases: Vec<String>, client: Client) -> Self {
        Self { bases, client }
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
            Ok(status.clone())
        } else if errs.len() < self.bases.len() {
            // At least one base succeeded — partial failure is acceptable.
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial status report failure");
            }
            Ok(status.clone())
        } else {
            // All bases failed.
            Err(errs.into_iter().last().unwrap().1)
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
            Ok(())
        } else if errs.len() < self.bases.len() {
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial size report failure");
            }
            Ok(())
        } else {
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
            Ok(())
        } else if errs.len() < self.bases.len() {
            for (url, e) in &errs {
                tracing::warn!(url = %url, error = %e, "partial schedule report failure");
            }
            Ok(())
        } else {
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
                .client
                .post(&url)
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
                .client
                .post(&url)
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
            match self.client.get(&url).send().await {
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
