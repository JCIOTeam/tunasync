//! Worker-side manager API client.
//!
//! The Go worker uses `PostJSON` / `GetJSON` helpers from `internal/util.go`.
//! We replace that with typed `reqwest` calls. Retry / round-robin across
//! `api_base_list` is handled here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tunasync_protocol::{MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus};

const DEFAULT_PENDING_LIMIT: usize = crate::config::MAX_REPORT_RESOURCES;

/// Reports that failed to reach one or more manager bases and await replay.
///
/// Coalesced with two independent bounds using the same N: at most N distinct
/// status/size keys, plus one latest complete schedule snapshot of at most N
/// rows. Schedule snapshots are never truncated.
struct PendingReports {
    next_sequence: u64,
    limit: usize,
    statuses: HashMap<String, PendingEntry<MirrorStatus>>,
    sizes: HashMap<String, PendingEntry<String>>,
    schedules: Option<PendingEntry<Arc<MirrorSchedules>>>,
}

#[derive(Clone)]
struct PendingEntry<T> {
    sequence: u64,
    value: T,
}

impl PendingReports {
    fn new(limit: usize) -> Self {
        Self {
            next_sequence: 0,
            limit: limit.clamp(1, DEFAULT_PENDING_LIMIT),
            statuses: HashMap::new(),
            sizes: HashMap::new(),
            schedules: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.statuses.is_empty() && self.sizes.is_empty() && self.schedules.is_none()
    }

    fn next_sequence(&mut self) -> u64 {
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("pending report sequence exhausted");
        self.next_sequence
    }

    fn insert_status(&mut self, status: MirrorStatus) {
        if !self.statuses.contains_key(&status.name) {
            self.make_resource_room();
        }
        let sequence = self.next_sequence();
        self.statuses.insert(
            status.name.clone(),
            PendingEntry {
                sequence,
                value: status,
            },
        );
    }

    fn insert_size(&mut self, mirror: String, size: String) {
        if !self.sizes.contains_key(&mirror) {
            self.make_resource_room();
        }
        let sequence = self.next_sequence();
        self.sizes.insert(
            mirror,
            PendingEntry {
                sequence,
                value: size,
            },
        );
    }

    fn insert_schedules(&mut self, schedules: Arc<MirrorSchedules>) {
        let sequence = self.next_sequence();
        self.schedules = Some(PendingEntry {
            sequence,
            value: schedules,
        });
    }

    fn make_resource_room(&mut self) {
        if self.statuses.len() + self.sizes.len() < self.limit {
            return;
        }
        enum ResourceKey {
            Status(String),
            Size(String),
        }
        let status = self
            .statuses
            .iter()
            .min_by_key(|(_, entry)| entry.sequence)
            .map(|(name, entry)| (entry.sequence, ResourceKey::Status(name.clone())));
        let size = self
            .sizes
            .iter()
            .min_by_key(|(_, entry)| entry.sequence)
            .map(|(name, entry)| (entry.sequence, ResourceKey::Size(name.clone())));
        let oldest = match (status, size) {
            (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
            (left, right) => left.or(right),
        };
        if let Some((sequence, key)) = oldest {
            let resource = match key {
                ResourceKey::Status(name) => {
                    self.statuses.remove(&name);
                    format!("status:{name}")
                }
                ResourceKey::Size(name) => {
                    self.sizes.remove(&name);
                    format!("size:{name}")
                }
            };
            tracing::warn!(
                sequence,
                %resource,
                pending_limit = self.limit,
                "evicting oldest pending report resource"
            );
        }
    }

    fn snapshot_batch(&self, limit: usize) -> Vec<PendingReport> {
        let mut batch = Vec::with_capacity(limit);
        batch.extend(
            self.statuses
                .iter()
                .map(|(name, entry)| PendingReport::Status {
                    mirror: name.clone(),
                    entry: entry.clone(),
                }),
        );
        batch.extend(self.sizes.iter().map(|(name, entry)| PendingReport::Size {
            mirror: name.clone(),
            entry: entry.clone(),
        }));
        if let Some(entry) = &self.schedules {
            batch.push(PendingReport::Schedules(entry.clone()));
        }
        batch.sort_unstable_by_key(PendingReport::sequence);
        batch.truncate(limit);
        batch
    }

    fn remove_if_same_sequence(&mut self, report: &PendingReport) {
        match report {
            PendingReport::Status { mirror, entry } => {
                if self.statuses.get(mirror).map(|current| current.sequence) == Some(entry.sequence)
                {
                    self.statuses.remove(mirror);
                }
            }
            PendingReport::Size { mirror, entry } => {
                if self.sizes.get(mirror).map(|current| current.sequence) == Some(entry.sequence) {
                    self.sizes.remove(mirror);
                }
            }
            PendingReport::Schedules(entry) => {
                if self.schedules.as_ref().map(|current| current.sequence) == Some(entry.sequence) {
                    self.schedules = None;
                }
            }
        }
    }

    fn remove_mirror(&mut self, mirror: &str) {
        self.statuses.remove(mirror);
        self.sizes.remove(mirror);
        if let Some(entry) = &mut self.schedules {
            Arc::make_mut(&mut entry.value)
                .schedules
                .retain(|schedule| schedule.mirror_name != mirror);
        }
    }
}

#[derive(Clone)]
enum PendingReport {
    Status {
        mirror: String,
        entry: PendingEntry<MirrorStatus>,
    },
    Size {
        mirror: String,
        entry: PendingEntry<String>,
    },
    Schedules(PendingEntry<Arc<MirrorSchedules>>),
}

impl PendingReport {
    fn sequence(&self) -> u64 {
        match self {
            Self::Status { entry, .. } => entry.sequence,
            Self::Size { entry, .. } => entry.sequence,
            Self::Schedules(entry) => entry.sequence,
        }
    }
}

/// Thin async client for the tunasync manager REST API.
///
/// One instance is shared across the worker via `Arc<ManagerClient>`.
///
/// Reports that fail against one or more manager bases are stashed in a pending
/// buffer and replayed by [`ManagerClient::flush_pending_batch`] (called by the
/// report actor after a successful heartbeat), so a manager outage
/// no longer permanently loses terminal sync states or traffic stats.
pub struct ManagerClient {
    connection: parking_lot::RwLock<ManagerConnection>,
    registration: parking_lot::RwLock<Option<WorkerStatus>>,
    pending: tokio::sync::Mutex<PendingReports>,
    max_schedule_entries: usize,
    /// Serialize live delivery with pending replay. Without this, a stale
    /// snapshot taken by pending replay could arrive after a newer live report
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
        Self::new_with_pending_limit(bases, client, token, DEFAULT_PENDING_LIMIT)
    }

    pub fn new_with_pending_limit(
        bases: Vec<String>,
        client: Client,
        token: String,
        pending_limit: usize,
    ) -> Self {
        let pending_limit = crate::config::effective_report_max_resources(pending_limit);
        Self {
            connection: parking_lot::RwLock::new(ManagerConnection {
                bases,
                client,
                token,
            }),
            registration: parking_lot::RwLock::new(None),
            pending: tokio::sync::Mutex::new(PendingReports::new(pending_limit)),
            max_schedule_entries: pending_limit,
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

    pub fn set_registration(&self, status: WorkerStatus) {
        *self.registration.write() = Some(status);
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
                    last_err = anyhow::anyhow!(
                        "POST {} returned {code}: {body}",
                        crate::redact_url_diagnostic(&url)
                    );
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
            .with_context(|| format!("POST {}", crate::redact_url_diagnostic(&url)))?;
        let code = resp.status();
        if !code.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "POST {} returned {code}: {body}",
                crate::redact_url_diagnostic(&url)
            );
        }
        resp.json::<WorkerStatus>().await.with_context(|| {
            format!(
                "decode registration response from {}",
                crate::redact_url_diagnostic(&url)
            )
        })
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
        if result.attempted == 0 {
            self.pending.lock().await.insert_status(status.clone());
            Err(anyhow::anyhow!("no manager URLs configured"))
        } else if result.errors.is_empty() {
            // A fresh report supersedes any stashed (older) one.
            self.pending.lock().await.statuses.remove(&status.name);
            Ok(status.clone())
        } else if result.errors.len() < result.attempted {
            // At least one base succeeded — partial failure is acceptable.
            for (url, e) in &result.errors {
                tracing::warn!(url = %crate::redact_url_diagnostic(url), error = %e, "partial status report failure");
            }
            self.pending.lock().await.insert_status(status.clone());
            Ok(status.clone())
        } else {
            // All bases failed — stash the latest status for replay once the
            // manager comes back (see flush_pending_batch). Latest-wins per mirror.
            self.pending.lock().await.insert_status(status.clone());
            Err(result.errors.into_iter().last().unwrap().1)
        }
    }

    /// Replay a bounded batch of reports that previously failed.
    ///
    /// Called by the report actor right after a successful heartbeat —
    /// i.e. the moment we KNOW the manager is reachable again. Entries are
    /// Snapshots remain in the buffer while transport is in flight. A snapshot
    /// is removed only after every configured manager accepted it and only if
    /// no newer value replaced it in the meantime.
    pub async fn flush_pending_batch(&self, worker_id: &str, limit: usize) -> bool {
        let _delivery = self.delivery.lock().await;
        let batch = {
            let p = self.pending.lock().await;
            if p.is_empty() {
                return false;
            }
            p.snapshot_batch(limit.max(1))
        };
        tracing::info!(
            worker = %worker_id,
            pending = batch.len(),
            "manager reachable again - replaying a bounded report batch"
        );
        let mut complete_success = true;
        for report in &batch {
            let result = match report {
                PendingReport::Status { entry, .. } => {
                    let path = format!("/workers/{worker_id}/jobs/{}", entry.value.name);
                    self.post_all(&path, &entry.value).await
                }
                PendingReport::Size { mirror, entry } => {
                    #[derive(serde::Serialize)]
                    struct SizeMsg<'a> {
                        name: &'a str,
                        size: &'a str,
                    }
                    let path = format!("/workers/{worker_id}/jobs/{mirror}/size");
                    self.post_all(
                        &path,
                        &SizeMsg {
                            name: mirror,
                            size: &entry.value,
                        },
                    )
                    .await
                }
                PendingReport::Schedules(entry) => {
                    let path = format!("/workers/{worker_id}/schedules");
                    self.post_all(&path, entry.value.as_ref()).await
                }
            };
            if result.attempted == 0 || !result.errors.is_empty() {
                complete_success = false;
            }
        }
        if complete_success {
            let mut pending = self.pending.lock().await;
            for report in &batch {
                pending.remove_if_same_sequence(report);
            }
        }
        !self.pending.lock().await.is_empty()
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
        if result.attempted == 0 {
            self.pending
                .lock()
                .await
                .insert_size(mirror_id.to_string(), size.to_string());
            Err(anyhow::anyhow!("no manager URLs configured"))
        } else if result.errors.is_empty() {
            self.pending.lock().await.sizes.remove(mirror_id);
            Ok(())
        } else if result.errors.len() < result.attempted {
            for (url, e) in &result.errors {
                tracing::warn!(url = %crate::redact_url_diagnostic(url), error = %e, "partial size report failure");
            }
            self.pending
                .lock()
                .await
                .insert_size(mirror_id.to_string(), size.to_string());
            Ok(())
        } else {
            self.pending
                .lock()
                .await
                .insert_size(mirror_id.to_string(), size.to_string());
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
        self.validate_schedule_count(schedules.schedules.len())?;
        let schedules = Arc::new(schedules.clone());
        let _delivery = self.delivery.lock().await;
        self.report_schedules_unlocked(worker_id, &schedules).await
    }

    pub(crate) async fn report_schedules_shared(
        &self,
        worker_id: &str,
        schedules: &Arc<MirrorSchedules>,
    ) -> Result<()> {
        self.validate_schedule_count(schedules.schedules.len())?;
        let _delivery = self.delivery.lock().await;
        self.report_schedules_unlocked(worker_id, schedules).await
    }

    fn validate_schedule_count(&self, count: usize) -> Result<()> {
        if count <= self.max_schedule_entries {
            return Ok(());
        }
        tracing::warn!(
            schedule_rows = count,
            max_schedule_entries = self.max_schedule_entries,
            "rejecting oversized complete schedule snapshot"
        );
        anyhow::bail!(
            "schedule snapshot has {count} rows, exceeding limit {}",
            self.max_schedule_entries
        )
    }

    async fn report_schedules_unlocked(
        &self,
        worker_id: &str,
        schedules: &Arc<MirrorSchedules>,
    ) -> Result<()> {
        let path = format!("/workers/{worker_id}/schedules");
        let result = self.post_all(&path, schedules.as_ref()).await;
        if result.attempted == 0 {
            self.pending
                .lock()
                .await
                .insert_schedules(Arc::clone(schedules));
            Err(anyhow::anyhow!("no manager URLs configured"))
        } else if result.errors.is_empty() {
            self.pending.lock().await.schedules = None;
            Ok(())
        } else if result.errors.len() < result.attempted {
            for (url, e) in &result.errors {
                tracing::warn!(url = %crate::redact_url_diagnostic(url), error = %e, "partial schedule report failure");
            }
            self.pending
                .lock()
                .await
                .insert_schedules(Arc::clone(schedules));
            Ok(())
        } else {
            self.pending
                .lock()
                .await
                .insert_schedules(Arc::clone(schedules));
            Err(result.errors.into_iter().last().unwrap().1)
        }
    }

    pub async fn remove_pending_mirror(&self, mirror: &str) {
        self.pending.lock().await.remove_mirror(mirror);
    }

    #[cfg(test)]
    pub(crate) async fn pending_counts(&self) -> (usize, usize, bool) {
        let pending = self.pending.lock().await;
        (
            pending.statuses.len(),
            pending.sizes.len(),
            pending.schedules.is_some(),
        )
    }

    #[cfg(test)]
    pub(crate) async fn pending_status(&self, mirror: &str) -> Option<MirrorStatus> {
        self.pending
            .lock()
            .await
            .statuses
            .get(mirror)
            .map(|entry| entry.value.clone())
    }

    #[cfg(test)]
    pub(crate) async fn pending_schedule_mirrors(&self) -> Option<Vec<String>> {
        self.pending.lock().await.schedules.as_ref().map(|entry| {
            entry
                .value
                .schedules
                .iter()
                .map(|schedule| schedule.mirror_name.clone())
                .collect()
        })
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
                    tracing::warn!(url = %crate::redact_url_diagnostic(&url), error = %e, "manager request failed");
                    errors.push((base.clone(), e.into()));
                }
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        let body = resp.text().await.unwrap_or_default();
                        errors.push((
                            base.clone(),
                            anyhow::anyhow!(
                                "POST {} returned {}: {body}",
                                crate::redact_url_diagnostic(&url),
                                status
                            ),
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

    fn status(name: &str, state: tunasync_protocol::SyncStatus) -> MirrorStatus {
        MirrorStatus {
            name: name.into(),
            worker: "w1".into(),
            status: state,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn empty_manager_bases_fail_and_retain_reports() {
        #[derive(Default)]
        struct Capture {
            statuses: parking_lot::Mutex<Vec<tunasync_protocol::SyncStatus>>,
            sizes: parking_lot::Mutex<Vec<String>>,
            schedules: AtomicBool,
        }

        async fn status_report(
            State(state): State<Arc<Capture>>,
            Json(body): Json<MirrorStatus>,
        ) -> StatusCode {
            state.statuses.lock().push(body.status);
            StatusCode::OK
        }

        async fn size_report(
            State(state): State<Arc<Capture>>,
            Json(body): Json<Value>,
        ) -> StatusCode {
            state
                .sizes
                .lock()
                .push(body["size"].as_str().unwrap().to_owned());
            StatusCode::OK
        }

        async fn schedules_report(State(state): State<Arc<Capture>>) -> StatusCode {
            state.schedules.store(true, Ordering::SeqCst);
            StatusCode::OK
        }

        let capture = Arc::new(Capture::default());
        let app = Router::new()
            .route("/workers/{id}/jobs/{mirror}", post(status_report))
            .route("/workers/{id}/jobs/{mirror}/size", post(size_report))
            .route("/workers/{id}/schedules", post(schedules_report))
            .with_state(Arc::clone(&capture));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let manager = ManagerClient::new(vec![], Client::new(), String::new());
        assert!(manager
            .report_status(
                "w1",
                &status("mirror", tunasync_protocol::SyncStatus::Success)
            )
            .await
            .is_err());
        assert!(manager.report_size("w1", "mirror", "1T").await.is_err());
        assert!(manager
            .report_schedules(
                "w1",
                &MirrorSchedules {
                    schedules: Vec::new(),
                },
            )
            .await
            .is_err());

        manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
        assert!(!manager.flush_pending_batch("w1", 8).await);
        assert_eq!(
            *capture.statuses.lock(),
            [tunasync_protocol::SyncStatus::Success]
        );
        assert_eq!(*capture.sizes.lock(), ["1T"]);
        assert!(capture.schedules.load(Ordering::SeqCst));
        server.abort();
    }

    #[tokio::test]
    async fn pending_replay_is_limited_to_requested_batch_size() {
        use std::sync::atomic::AtomicUsize;

        async fn report(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            StatusCode::OK
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/workers/{id}/jobs/{mirror}", post(report))
            .with_state(Arc::clone(&count));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let manager = ManagerClient::new(Vec::new(), Client::new(), String::new());
        for index in 0..20 {
            assert!(manager
                .report_status(
                    "w1",
                    &status(
                        &format!("mirror-{index}"),
                        tunasync_protocol::SyncStatus::Failed,
                    ),
                )
                .await
                .is_err());
        }
        manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());

        assert!(manager.flush_pending_batch("w1", 8).await);
        assert_eq!(count.load(Ordering::SeqCst), 8);
        assert!(manager.flush_pending_batch("w1", 8).await);
        assert_eq!(count.load(Ordering::SeqCst), 16);
        assert!(!manager.flush_pending_batch("w1", 8).await);
        assert_eq!(count.load(Ordering::SeqCst), 20);
        server.abort();
    }

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

    #[tokio::test]
    async fn pending_status_size_keys_and_schedule_rows_have_independent_caps() {
        let manager =
            ManagerClient::new_with_pending_limit(Vec::new(), Client::new(), String::new(), 4);
        for index in 0..20 {
            assert!(manager
                .report_status(
                    "w1",
                    &status(
                        &format!("status-{index}"),
                        tunasync_protocol::SyncStatus::Failed,
                    ),
                )
                .await
                .is_err());
            assert!(manager
                .report_size("w1", &format!("size-{index}"), &index.to_string())
                .await
                .is_err());
        }
        assert!(manager
            .report_schedules(
                "w1",
                &MirrorSchedules {
                    schedules: Vec::new(),
                },
            )
            .await
            .is_err());
        let (statuses, sizes, schedules) = manager.pending_counts().await;
        assert!(statuses + sizes <= 4);
        assert!(
            schedules,
            "the separate complete schedule snapshot is retained"
        );
    }

    #[tokio::test]
    async fn oversized_schedule_is_neither_sent_nor_stashed_over_valid_snapshot() {
        use std::sync::atomic::AtomicUsize;

        async fn schedules_report(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            StatusCode::OK
        }

        let sent = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn({
            let sent = Arc::clone(&sent);
            async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/workers/{id}/schedules", post(schedules_report))
                        .with_state(sent),
                )
                .await
                .unwrap();
            }
        });

        let manager =
            ManagerClient::new_with_pending_limit(Vec::new(), Client::new(), String::new(), 2);
        assert!(manager
            .report_schedules(
                "w1",
                &MirrorSchedules {
                    schedules: vec![tunasync_protocol::MirrorSchedule {
                        mirror_name: "retained".into(),
                        next_schedule: tunasync_protocol::zero_time(),
                    }],
                },
            )
            .await
            .is_err());
        let (before_sequence, before_ptr) = {
            let pending = manager.pending.lock().await;
            let entry = pending.schedules.as_ref().unwrap();
            (entry.sequence, Arc::as_ptr(&entry.value))
        };

        manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
        let error = manager
            .report_schedules(
                "w1",
                &MirrorSchedules {
                    schedules: (0..3)
                        .map(|index| tunasync_protocol::MirrorSchedule {
                            mirror_name: format!("oversized-{index}"),
                            next_schedule: tunasync_protocol::zero_time(),
                        })
                        .collect(),
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("3 rows"));
        assert_eq!(sent.load(Ordering::SeqCst), 0);

        let pending = manager.pending.lock().await;
        let retained = pending.schedules.as_ref().unwrap();
        assert_eq!(retained.sequence, before_sequence);
        assert_eq!(Arc::as_ptr(&retained.value), before_ptr);
        assert_eq!(retained.value.schedules[0].mirror_name, "retained");
        drop(pending);
        server.abort();
    }

    #[tokio::test]
    async fn cancelled_pending_replay_leaves_snapshot_intact() {
        use axum::{extract::State, routing::post, Router};
        use tokio::sync::{mpsc, Semaphore};

        struct HangingReplay {
            started: mpsc::Sender<()>,
            gate: Semaphore,
        }

        async fn hang(State(state): State<Arc<HangingReplay>>) -> StatusCode {
            let _ = state.started.try_send(());
            let permit = state.gate.acquire().await.unwrap();
            permit.forget();
            StatusCode::OK
        }

        let (started_tx, mut started_rx) = mpsc::channel(1);
        let state = Arc::new(HangingReplay {
            started: started_tx,
            gate: Semaphore::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/workers/{id}/jobs/{mirror}", post(hang))
                        .with_state(state),
                )
                .await
                .unwrap();
            }
        });

        let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
        assert!(manager
            .report_status(
                "w1",
                &status("mirror", tunasync_protocol::SyncStatus::Failed),
            )
            .await
            .is_err());
        manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
        let replay = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.flush_pending_batch("w1", 8).await }
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("replay did not start")
            .expect("replay signal channel closed");
        replay.abort();
        let _ = replay.await;
        assert_eq!(manager.pending_counts().await, (1, 0, false));
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_after_earlier_success_keeps_entire_snapshot() {
        use axum::{extract::Path, extract::State, routing::post, Router};
        use tokio::sync::{mpsc, Semaphore};

        struct PartialReplay {
            second_started: mpsc::Sender<()>,
            gate: Semaphore,
        }

        async fn report(
            Path((_worker, mirror)): Path<(String, String)>,
            State(state): State<Arc<PartialReplay>>,
        ) -> StatusCode {
            if mirror == "second" {
                let _ = state.second_started.try_send(());
                let permit = state.gate.acquire().await.unwrap();
                permit.forget();
            }
            StatusCode::OK
        }

        let (started_tx, mut started_rx) = mpsc::channel(1);
        let state = Arc::new(PartialReplay {
            second_started: started_tx,
            gate: Semaphore::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/workers/{worker}/jobs/{mirror}", post(report))
                        .with_state(state),
                )
                .await
                .unwrap();
            }
        });

        let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
        for mirror in ["first", "second"] {
            assert!(manager
                .report_status("w1", &status(mirror, tunasync_protocol::SyncStatus::Failed),)
                .await
                .is_err());
        }
        manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
        let replay = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { manager.flush_pending_batch("w1", 2).await }
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .expect("second replay did not start")
            .expect("second replay signal channel closed");
        replay.abort();
        let _ = replay.await;
        assert_eq!(manager.pending_counts().await, (2, 0, false));
        server.abort();
    }

    #[tokio::test]
    async fn remove_pending_mirror_purges_status_size_and_schedule_entry() {
        let manager = ManagerClient::new(Vec::new(), Client::new(), String::new());
        assert!(manager
            .report_status(
                "w1",
                &status("deleted", tunasync_protocol::SyncStatus::Failed),
            )
            .await
            .is_err());
        assert!(manager.report_size("w1", "deleted", "1T").await.is_err());
        assert!(manager
            .report_schedules(
                "w1",
                &MirrorSchedules {
                    schedules: vec![tunasync_protocol::MirrorSchedule {
                        mirror_name: "deleted".into(),
                        next_schedule: tunasync_protocol::zero_time(),
                    }],
                },
            )
            .await
            .is_err());

        manager.remove_pending_mirror("deleted").await;
        let pending = manager.pending.lock().await;
        assert!(pending.statuses.is_empty());
        assert!(pending.sizes.is_empty());
        assert!(pending
            .schedules
            .as_ref()
            .is_some_and(|entry| entry.value.schedules.is_empty()));
    }

    #[test]
    fn stale_snapshot_cannot_remove_newer_pending_value() {
        let mut pending = PendingReports::new(4);
        pending.insert_status(status("mirror", tunasync_protocol::SyncStatus::Failed));
        let snapshot = pending.snapshot_batch(1).pop().unwrap();
        pending.insert_status(status("mirror", tunasync_protocol::SyncStatus::Success));
        pending.remove_if_same_sequence(&snapshot);
        assert_eq!(
            pending.statuses["mirror"].value.status,
            tunasync_protocol::SyncStatus::Success
        );
    }
}
