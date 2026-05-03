//! Manager persistence layer.
//!
//! Implements the `DbAdapter` trait for **redb** and **sqlite** backends.
//! Values are JSON bytes (same as the Go `kvDBAdapter` approach).
//!
//! Mirror-status key: `"{mirror_id}/{worker_id}"` — matches Go exactly.

pub mod redb_adapter;
pub mod sqlite_adapter;

use thiserror::Error;
use tunasync_protocol::{MirrorStatus, WorkerStatus};

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
            fn from(e: $t) -> Self { Self::Storage(e.to_string()) }
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
    fn update_mirror_status(&self, worker_id: &str, mirror_id: &str, status: MirrorStatus) -> DbResult<MirrorStatus>;
    fn get_mirror_status(&self, worker_id: &str, mirror_id: &str) -> DbResult<MirrorStatus>;
    fn list_mirror_status(&self, worker_id: &str) -> DbResult<Vec<MirrorStatus>>;
    fn list_all_mirror_status(&self) -> DbResult<Vec<MirrorStatus>>;
    fn flush_disabled_jobs(&self) -> DbResult<()>;
    fn close(&self) -> DbResult<()>;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub fn open(db_type: &str, db_path: &std::path::Path) -> DbResult<Box<dyn DbAdapter>> {
    match db_type {
        "redb"   => Ok(Box::new(redb_adapter::RedbAdapter::open(db_path)?)),
        "sqlite" => Ok(Box::new(sqlite_adapter::SqliteAdapter::open(db_path)?)),
        other    => Err(DbError::Storage(format!(
            "unsupported db_type {other:?}; valid values: \"redb\", \"sqlite\""
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
    key.splitn(2, '/').nth(1).unwrap_or("")
}
