//! SQLite storage adapter.
//!
//! Uses two tables:
//! - `workers(id TEXT PRIMARY KEY, data BLOB)` — JSON [`WorkerStatus`]
//! - `mirror_status(key TEXT PRIMARY KEY, data BLOB)` — JSON [`MirrorStatus`],
//!   key = `"{mirror_id}/{worker_id}"`
//!
//! `rusqlite::Connection` is `!Sync`, so we hold it inside a `Mutex`.
//! All callers use `&self` and obtain the lock per-call.

use std::path::Path;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::{params, Connection};
use tunasync_protocol::{MirrorStatus, SyncStatus, WorkerStatus};

use super::{status_key, DbAdapter, DbError, DbResult};

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// SQLite-backed storage adapter.
pub struct SqliteAdapter {
    conn: Mutex<Connection>,
}

impl SqliteAdapter {
    /// Open (or create) the SQLite database at `path` and run schema migrations.
    pub fn open(path: &Path) -> DbResult<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workers (
                id   TEXT NOT NULL PRIMARY KEY,
                data BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS mirror_status (
                key  TEXT NOT NULL PRIMARY KEY,
                data BLOB NOT NULL
             );",
        )?;

        Ok(Self { conn: Mutex::new(conn) })
    }
}

impl DbAdapter for SqliteAdapter {
    // ------------------------------------------------------------------
    // Workers
    // ------------------------------------------------------------------

    fn list_workers(&self) -> DbResult<Vec<WorkerStatus>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT data FROM workers")?;
        let rows: Vec<Vec<u8>> = stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        drop(conn);
        Ok(rows
            .into_iter()
            .filter_map(|bytes| serde_json::from_slice::<WorkerStatus>(&bytes).ok())
            .collect())
    }

    fn get_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        let conn = self.conn.lock().unwrap();
        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM workers WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        match result {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(DbError::NotFound(format!("worker {id:?}"))),
        }
    }

    fn delete_worker(&self, id: &str) -> DbResult<()> {
        let conn = self.conn.lock().unwrap();
        // Check existence first (same as Go's kvDBAdapter).
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM workers WHERE id = ?1)",
                params![id],
                |row| row.get(0),
            )?;
        if !exists {
            return Err(DbError::NotFound(format!("worker {id:?}")));
        }
        conn.execute("DELETE FROM workers WHERE id = ?1", params![id])?;
        Ok(())
    }

    fn create_worker(&self, w: WorkerStatus) -> DbResult<WorkerStatus> {
        let bytes = serde_json::to_vec(&w)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO workers (id, data) VALUES (?1, ?2)",
            params![w.id, bytes],
        )?;
        Ok(w)
    }

    fn refresh_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        // Get, update last_online, put — but we need the lock only once.
        // Pattern: read outside lock, then write.  Here we use a single lock
        // scope because `&self` methods share the same Mutex.
        let bytes: Vec<u8> = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT data FROM workers WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| DbError::NotFound(format!("worker {id:?}")))?
        };
        let mut w: WorkerStatus = serde_json::from_slice(&bytes)?;
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
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO mirror_status (key, data) VALUES (?1, ?2)",
            params![key, bytes],
        )?;
        Ok(status)
    }

    fn get_mirror_status(&self, worker_id: &str, mirror_id: &str) -> DbResult<MirrorStatus> {
        let key = status_key(mirror_id, worker_id);
        let conn = self.conn.lock().unwrap();
        let result: Option<Vec<u8>> = conn
            .query_row(
                "SELECT data FROM mirror_status WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        match result {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(DbError::NotFound(format!(
                "mirror {mirror_id:?} on worker {worker_id:?}"
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> DbResult<Vec<MirrorStatus>> {
        // Suffix-match: key schema is "{mirror}/{worker_id}".
        // We use a LIKE pattern; exact match is guaranteed since worker_ids
        // cannot contain '/'.
        let suffix = format!("/{worker_id}");
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT data FROM mirror_status WHERE key LIKE '%' || ?1",
        )?;
        let rows: Vec<Vec<u8>> = stmt
            .query_map(params![suffix], |row| row.get::<_, Vec<u8>>(0))?
            .filter_map(|r| r.ok())
            .collect();
        // Drop stmt + conn before processing.
        drop(stmt);
        drop(conn);
        Ok(rows
            .into_iter()
            .filter_map(|bytes| serde_json::from_slice::<MirrorStatus>(&bytes).ok())
            .collect())
    }

    fn list_all_mirror_status(&self) -> DbResult<Vec<MirrorStatus>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT data FROM mirror_status")?;
        let rows: Vec<Vec<u8>> = stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        drop(conn);
        Ok(rows
            .into_iter()
            .filter_map(|bytes| serde_json::from_slice::<MirrorStatus>(&bytes).ok())
            .collect())
    }

    fn flush_disabled_jobs(&self) -> DbResult<()> {
        // Collect keys to delete first, fully materialising before we drop
        // `stmt` and `conn` so the borrow checker is happy.
        let to_delete: Vec<String> = {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare("SELECT key, data FROM mirror_status")?;
            let rows: Vec<(String, Vec<u8>)> = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .filter_map(|r| r.ok())
                .collect();
            // `stmt` and `conn` drop here; now we process the owned Vec.
            rows.into_iter()
                .filter_map(|(key, bytes)| {
                    let m: MirrorStatus = serde_json::from_slice(&bytes).ok()?;
                    if m.status == SyncStatus::Disabled || m.name.is_empty() {
                        Some(key)
                    } else {
                        None
                    }
                })
                .collect()
        };

        if to_delete.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().unwrap();
        for key in to_delete {
            conn.execute("DELETE FROM mirror_status WHERE key = ?1", params![key])?;
        }
        Ok(())
    }

    fn close(&self) -> DbResult<()> {
        // Connection is closed on Drop.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// rusqlite optional-row helper (not in old rusqlite re-exports)
// ---------------------------------------------------------------------------

trait OptionalExt<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}

impl<T> OptionalExt<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
