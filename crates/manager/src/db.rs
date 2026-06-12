//! Manager persistence layer.
//!
//! Implements the `DbAdapter` trait for **redb**, **sqlite**, and **redis** backends.
//! Values are JSON bytes (same as the Go `kvDBAdapter` approach).
//!
//! Mirror-status key: `"{mirror_id}/{worker_id}"` — matches Go exactly.

pub mod redb_adapter;
pub mod redis_adapter;
pub mod sqlite_adapter;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tunasync_protocol::{MirrorStatus, SyncStatus, WorkerStatus};

// ---------------------------------------------------------------------------
// Sync history
// ---------------------------------------------------------------------------

/// One completed sync run, recorded when a mirror transitions from an
/// active state into a terminal one. Served via `GET /jobs/:name/history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncHistoryEntry {
    pub mirror: String,
    pub worker: String,
    pub status: SyncStatus,
    pub started: chrono::DateTime<chrono::Utc>,
    pub ended: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub transferred_bytes: u64,
    #[serde(default)]
    pub error_msg: String,
}

/// How many history rows to retain per mirror (pruned on insert).
pub const SYNC_HISTORY_KEEP_PER_MIRROR: usize = 100;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum DbError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("codec error: {0}")]
    Codec(#[from] serde_json::Error),
}

macro_rules! impl_from_db_error {
    ($t:ty) => {
        impl From<$t> for DbError {
            fn from(e: $t) -> Self {
                Self::Storage(e.to_string())
            }
        }
    };
}

impl_from_db_error!(redb::Error);
impl_from_db_error!(redb::DatabaseError);
impl_from_db_error!(redb::TransactionError);
impl_from_db_error!(redb::TableError);
impl_from_db_error!(redb::StorageError);
impl_from_db_error!(redb::CommitError);
impl_from_db_error!(rusqlite::Error);
impl_from_db_error!(redis::RedisError);

pub type DbResult<T> = Result<T, DbError>;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

pub trait DbAdapter: Send + Sync {
    fn list_workers(&self) -> DbResult<Vec<WorkerStatus>>;
    fn get_worker(&self, id: &str) -> DbResult<WorkerStatus>;
    fn delete_worker(&self, id: &str) -> DbResult<()>;
    fn create_worker(&self, w: WorkerStatus) -> DbResult<WorkerStatus>;
    fn refresh_worker(&self, id: &str) -> DbResult<WorkerStatus>;
    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> DbResult<MirrorStatus>;
    fn get_mirror_status(&self, worker_id: &str, mirror_id: &str) -> DbResult<MirrorStatus>;
    fn list_mirror_status(&self, worker_id: &str) -> DbResult<Vec<MirrorStatus>>;
    fn list_all_mirror_status(&self) -> DbResult<Vec<MirrorStatus>>;
    fn flush_disabled_jobs(&self) -> DbResult<()>;

    /// Record one completed sync run. Default: no-op — history is an
    /// OPTIONAL capability; currently only the sqlite backend implements
    /// it (redb/redis deployments get an empty history, never an error).
    fn record_sync_history(&self, _entry: &SyncHistoryEntry) -> DbResult<()> {
        Ok(())
    }

    /// Most-recent-first history for one mirror (across workers).
    /// Default: empty for backends without history support.
    fn get_sync_history(&self, _mirror: &str, _limit: usize) -> DbResult<Vec<SyncHistoryEntry>> {
        Ok(Vec::new())
    }

    fn close(&self) -> DbResult<()>;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub fn open(db_type: &str, db_path: &std::path::Path) -> DbResult<Box<dyn DbAdapter>> {
    match db_type {
        "redb" => Ok(Box::new(redb_adapter::RedbAdapter::open(db_path)?)),
        "sqlite" => Ok(Box::new(sqlite_adapter::SqliteAdapter::open(db_path)?)),
        "redis" => {
            let url = db_path.to_str().ok_or_else(|| {
                DbError::Storage("db_file for redis must be a valid UTF-8 URL".into())
            })?;
            Ok(Box::new(redis_adapter::RedisAdapter::open(url)?))
        }
        other => Err(DbError::Storage(format!(
            "unsupported db_type {other:?}; valid values: \"redb\", \"sqlite\", \"redis\""
        ))),
    }
}

// ---------------------------------------------------------------------------
// Key helpers (shared by both adapters)
// ---------------------------------------------------------------------------

/// Composite key for mirror_status: `"{mirror_id}/{worker_id}"`.
pub(crate) fn status_key(mirror_id: &str, worker_id: &str) -> String {
    format!("{mirror_id}/{worker_id}")
}

pub(crate) fn worker_id_from_key(key: &str) -> &str {
    key.split_once('/').map(|x| x.1).unwrap_or("")
}
