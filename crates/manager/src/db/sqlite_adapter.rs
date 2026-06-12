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

use super::{status_key, worker_id_from_key, DbAdapter, DbError, DbResult};

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
             );
             CREATE TABLE IF NOT EXISTS sync_history (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                mirror TEXT NOT NULL,
                data   BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_sync_history_mirror
                ON sync_history (mirror, id DESC);",
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
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
        let exists: bool = conn.query_row(
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
        // Key schema is "{mirror_id}/{worker_id}". An earlier version used
        //
        //   SELECT data FROM mirror_status WHERE key LIKE '%' || ?1
        //
        // with `?1 = "/{worker_id}"`, but LIKE treats `_` and `%` in the
        // pattern as single-char and multi-char wildcards. Worker IDs come
        // from hostnames and almost always contain underscores in real
        // deployments (e.g. `db_node_1`), so that query would silently
        // return mirrors belonging to other workers whose IDs differ only
        // in characters covered by `_`.
        //
        // Match the redb/redis adapters: scan the whole table and filter
        // by exact suffix in Rust using `worker_id_from_key`. This is also
        // robust to any future change in key encoding.
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT key, data FROM mirror_status")?;
        let rows: Vec<(String, Vec<u8>)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        drop(conn);
        Ok(rows
            .into_iter()
            .filter(|(k, _)| worker_id_from_key(k) == worker_id)
            .filter_map(|(_, bytes)| serde_json::from_slice::<MirrorStatus>(&bytes).ok())
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

    // ------------------------------------------------------------------
    // Sync history
    // ------------------------------------------------------------------

    fn record_sync_history(&self, entry: &crate::db::SyncHistoryEntry) -> DbResult<()> {
        let data = serde_json::to_vec(entry)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sync_history (mirror, data) VALUES (?1, ?2)",
            params![entry.mirror, data],
        )?;
        // Prune: keep only the most recent N rows per mirror so the table
        // stays bounded without a background vacuum task.
        conn.execute(
            "DELETE FROM sync_history
             WHERE mirror = ?1
               AND id NOT IN (
                   SELECT id FROM sync_history
                   WHERE mirror = ?1
                   ORDER BY id DESC
                   LIMIT ?2
               )",
            params![entry.mirror, crate::db::SYNC_HISTORY_KEEP_PER_MIRROR as i64],
        )?;
        Ok(())
    }

    fn get_sync_history(
        &self,
        mirror: &str,
        limit: usize,
    ) -> DbResult<Vec<crate::db::SyncHistoryEntry>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT data FROM sync_history WHERE mirror = ?1 ORDER BY id DESC LIMIT ?2")?;
        let rows = stmt.query_map(params![mirror, limit as i64], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(serde_json::from_slice(&row?)?);
        }
        Ok(out)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tunasync_protocol::{MirrorStatus, SyncStatus};

    fn mk_status(name: &str, worker: &str) -> MirrorStatus {
        MirrorStatus {
            name: name.into(),
            worker: worker.into(),
            is_master: true,
            status: SyncStatus::Success,
            ..Default::default()
        }
    }

    /// Regression test for the SQL LIKE wildcard bug.
    ///
    /// Worker IDs that share a prefix and differ only in characters covered
    /// by `_` (single-char wildcard) or `%` (multi-char wildcard) used to
    /// leak into each other's `list_mirror_status` results.
    #[test]
    fn list_mirror_status_does_not_bleed_across_workers_with_underscores() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let adapter = SqliteAdapter::open(&db_path).unwrap();

        // `db_1` and `dbX1` collide under the buggy `LIKE '%/' || ?1` query
        // because `_` is a single-char wildcard in LIKE patterns.
        adapter
            .update_mirror_status("db_1", "ubuntu", mk_status("ubuntu", "db_1"))
            .unwrap();
        adapter
            .update_mirror_status("dbX1", "debian", mk_status("debian", "dbX1"))
            .unwrap();
        adapter
            .update_mirror_status("db01", "fedora", mk_status("fedora", "db01"))
            .unwrap();

        let for_db_1 = adapter.list_mirror_status("db_1").unwrap();
        assert_eq!(
            for_db_1.len(),
            1,
            "list_mirror_status('db_1') leaked rows belonging to other workers: {:?}",
            for_db_1
                .iter()
                .map(|m| (&m.name, &m.worker))
                .collect::<Vec<_>>()
        );
        assert_eq!(for_db_1[0].name, "ubuntu");
        assert_eq!(for_db_1[0].worker, "db_1");

        // Sanity: the other workers' rows are still individually retrievable.
        assert_eq!(adapter.list_mirror_status("dbX1").unwrap().len(), 1);
        assert_eq!(adapter.list_mirror_status("db01").unwrap().len(), 1);
    }

    /// The `%` wildcard variant of the same bug.
    #[test]
    fn list_mirror_status_treats_percent_in_worker_id_literally() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let adapter = SqliteAdapter::open(&db_path).unwrap();

        // A worker_id literally containing '%' would, under the buggy query,
        // be interpreted as a multi-char wildcard and match everything.
        adapter
            .update_mirror_status("weird%", "ubuntu", mk_status("ubuntu", "weird%"))
            .unwrap();
        adapter
            .update_mirror_status("normal", "debian", mk_status("debian", "normal"))
            .unwrap();

        let for_weird = adapter.list_mirror_status("weird%").unwrap();
        assert_eq!(for_weird.len(), 1);
        assert_eq!(for_weird[0].worker, "weird%");
    }
}
