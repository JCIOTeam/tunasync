//! redb storage adapter.
//!
//! Uses two [`redb::TableDefinition`]s:
//! - `workers` — key: `worker_id`, value: JSON bytes of [`WorkerStatus`]
//! - `mirror_status` — key: `"{mirror_id}/{worker_id}"`, value: JSON bytes of [`MirrorStatus`]
//!
//! This mirrors Go's `boltAdapter` / `kvDBAdapter` design exactly.

use std::path::Path;

use chrono::Utc;
use redb::{Database, ReadableTable, TableDefinition};
use tunasync_protocol::{MirrorStatus, SyncStatus, WorkerStatus};

use super::{status_key, worker_id_from_key, DbAdapter, DbError, DbResult};

// ---------------------------------------------------------------------------
// Table definitions
// ---------------------------------------------------------------------------

const WORKERS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("workers");
const MIRROR_STATUS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("mirror_status");

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// redb-backed storage adapter.
pub struct RedbAdapter {
    db: Database,
}

impl RedbAdapter {
    /// Open (or create) a redb database at `path` and initialise tables.
    pub fn open(path: &Path) -> DbResult<Self> {
        let db = Database::create(path)?;

        // Ensure both tables exist.
        let tx = db.begin_write()?;
        {
            tx.open_table(WORKERS)?;
            tx.open_table(MIRROR_STATUS)?;
        }
        tx.commit()?;

        Ok(Self { db })
    }
}

impl DbAdapter for RedbAdapter {
    // ------------------------------------------------------------------
    // Workers
    // ------------------------------------------------------------------

    fn list_workers(&self) -> DbResult<Vec<WorkerStatus>> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(WORKERS)?;
        let mut workers = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let w: WorkerStatus = serde_json::from_slice(v.value())?;
            workers.push(w);
        }
        Ok(workers)
    }

    fn get_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(WORKERS)?;
        match table.get(id)? {
            Some(v) => Ok(serde_json::from_slice(v.value())?),
            None => Err(DbError::NotFound(format!("worker {id:?}"))),
        }
    }

    fn delete_worker(&self, id: &str) -> DbResult<()> {
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(WORKERS)?;
            if table.get(id)?.is_none() {
                return Err(DbError::NotFound(format!("worker {id:?}")));
            }
            table.remove(id)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn create_worker(&self, w: WorkerStatus) -> DbResult<WorkerStatus> {
        let bytes = serde_json::to_vec(&w)?;
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(WORKERS)?;
            table.insert(w.id.as_str(), bytes.as_slice())?;
        }
        tx.commit()?;
        Ok(w)
    }

    fn refresh_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        let mut w = self.get_worker(id)?;
        w.last_online = Utc::now();
        self.create_worker(w)
    }

    // ------------------------------------------------------------------
    // Mirror status
    // ------------------------------------------------------------------

    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> DbResult<MirrorStatus> {
        let key = status_key(mirror_id, worker_id);
        let bytes = serde_json::to_vec(&status)?;
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(MIRROR_STATUS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        tx.commit()?;
        Ok(status)
    }

    fn get_mirror_status(&self, worker_id: &str, mirror_id: &str) -> DbResult<MirrorStatus> {
        let key = status_key(mirror_id, worker_id);
        let tx = self.db.begin_read()?;
        let table = tx.open_table(MIRROR_STATUS)?;
        match table.get(key.as_str())? {
            Some(v) => Ok(serde_json::from_slice(v.value())?),
            None => Err(DbError::NotFound(format!(
                "mirror {mirror_id:?} on worker {worker_id:?}"
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> DbResult<Vec<MirrorStatus>> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(MIRROR_STATUS)?;
        let mut statuses = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            if worker_id_from_key(k.value()) == worker_id {
                let m: MirrorStatus = serde_json::from_slice(v.value())?;
                statuses.push(m);
            }
        }
        Ok(statuses)
    }

    fn list_all_mirror_status(&self) -> DbResult<Vec<MirrorStatus>> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(MIRROR_STATUS)?;
        let mut statuses = Vec::new();
        for entry in table.iter()? {
            let (_, v) = entry?;
            let m: MirrorStatus = serde_json::from_slice(v.value())?;
            statuses.push(m);
        }
        Ok(statuses)
    }

    fn flush_disabled_jobs(&self) -> DbResult<()> {
        // Collect keys to delete first (can't mutate while iterating in redb).
        let keys_to_delete: Vec<String> = {
            let tx = self.db.begin_read()?;
            let table = tx.open_table(MIRROR_STATUS)?;
            let mut keys = Vec::new();
            for entry in table.iter()? {
                let (k, v) = entry?;
                let m: MirrorStatus = match serde_json::from_slice(v.value()) {
                    Ok(m) => m,
                    Err(_) => continue, // corrupt row — leave it, log in production
                };
                if m.status == SyncStatus::Disabled || m.name.is_empty() {
                    keys.push(k.value().to_owned());
                }
            }
            keys
        };

        if keys_to_delete.is_empty() {
            return Ok(());
        }

        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(MIRROR_STATUS)?;
            for key in &keys_to_delete {
                table.remove(key.as_str())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn close(&self) -> DbResult<()> {
        // redb::Database is closed on Drop; no explicit close method.
        Ok(())
    }
}
