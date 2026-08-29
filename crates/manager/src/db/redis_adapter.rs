//! Redis storage adapter.
//!
//! Uses two Redis HASH keys (matching Go's `redisAdapter` + `kvDBAdapter`):
//! - `workers`       — field: `worker_id`,             value: JSON WorkerStatus
//! - `mirror_status` — field: `{mirror_id}/{worker_id}`, value: JSON MirrorStatus
//!
//! `redis::Connection` is `!Sync`, so we wrap it in a `Mutex` — same pattern
//! as `SqliteAdapter`.

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::Utc;
use redis::{Client, Commands, Connection};
use tunasync_protocol::{MirrorStatus, SyncStatus, WorkerStatus};

use super::{status_key, worker_id_from_key, DbAdapter, DbError, DbResult};

const WORKERS_KEY: &str = "workers";
const MIRROR_STATUS_KEY: &str = "mirror_status";

/// Redis-backed storage adapter.
///
/// Data is stored in two HASH keys, exactly matching Go's `redisAdapter`.
/// Wire-compatible: Go and Rust can share the same Redis instance.
pub struct RedisAdapter {
    conn: Mutex<Connection>,
}

impl RedisAdapter {
    /// Connect to Redis using a standard URL (e.g. `redis://localhost:6379/0`).
    ///
    /// The `url` parameter is the Redis connection URL, matching Go's behaviour
    /// where `db_file` holds the Redis URL.
    pub fn open(url: &str) -> DbResult<Self> {
        let client = Client::open(url)
            .map_err(|e| DbError::Storage(format!("bad redis URL: {:?}", e.kind())))?;
        let conn = client
            .get_connection()
            .map_err(|e| DbError::Storage(format!("redis connect failed: {:?}", e.kind())))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl DbAdapter for RedisAdapter {
    fn list_workers(&self) -> DbResult<Vec<WorkerStatus>> {
        let mut conn = self.conn.lock().unwrap();
        let map: HashMap<String, String> = conn.hgetall(WORKERS_KEY)?;
        drop(conn);
        Ok(map
            .into_values()
            .filter_map(|v| serde_json::from_str::<WorkerStatus>(&v).ok())
            .collect())
    }

    fn get_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        let mut conn = self.conn.lock().unwrap();
        let val: Option<String> = conn.hget(WORKERS_KEY, id)?;
        drop(conn);
        match val {
            Some(v) => Ok(serde_json::from_str(&v)?),
            None => Err(DbError::NotFound(format!("worker {id:?}"))),
        }
    }

    fn delete_worker(&self, id: &str) -> DbResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let exists: bool = conn.hexists(WORKERS_KEY, id)?;
        if !exists {
            return Err(DbError::NotFound(format!("worker {id:?}")));
        }
        conn.hdel::<_, _, ()>(WORKERS_KEY, id)?;
        Ok(())
    }

    fn create_worker(&self, w: WorkerStatus) -> DbResult<WorkerStatus> {
        let json = serde_json::to_string(&w)?;
        let mut conn = self.conn.lock().unwrap();
        conn.hset::<_, _, _, ()>(WORKERS_KEY, &w.id, &json)?;
        Ok(w)
    }

    fn refresh_worker(&self, id: &str) -> DbResult<WorkerStatus> {
        let mut w = self.get_worker(id)?;
        w.last_online = Utc::now();
        self.create_worker(w)
    }

    fn update_mirror_status(
        &self,
        worker_id: &str,
        mirror_id: &str,
        status: MirrorStatus,
    ) -> DbResult<MirrorStatus> {
        let key = status_key(mirror_id, worker_id);
        let json = serde_json::to_string(&status)?;
        let mut conn = self.conn.lock().unwrap();
        conn.hset::<_, _, _, ()>(MIRROR_STATUS_KEY, &key, &json)?;
        Ok(status)
    }

    fn get_mirror_status(&self, worker_id: &str, mirror_id: &str) -> DbResult<MirrorStatus> {
        let key = status_key(mirror_id, worker_id);
        let mut conn = self.conn.lock().unwrap();
        let val: Option<String> = conn.hget(MIRROR_STATUS_KEY, &key)?;
        drop(conn);
        match val {
            Some(v) => Ok(serde_json::from_str(&v)?),
            None => Err(DbError::NotFound(format!(
                "mirror {mirror_id:?} on worker {worker_id:?}"
            ))),
        }
    }

    fn list_mirror_status(&self, worker_id: &str) -> DbResult<Vec<MirrorStatus>> {
        let mut conn = self.conn.lock().unwrap();
        let map: HashMap<String, String> = conn.hgetall(MIRROR_STATUS_KEY)?;
        drop(conn);
        Ok(map
            .into_iter()
            .filter(|(k, _)| worker_id_from_key(k) == worker_id)
            .filter_map(|(_, v)| serde_json::from_str::<MirrorStatus>(&v).ok())
            .collect())
    }

    fn list_all_mirror_status(&self) -> DbResult<Vec<MirrorStatus>> {
        let mut conn = self.conn.lock().unwrap();
        let map: HashMap<String, String> = conn.hgetall(MIRROR_STATUS_KEY)?;
        drop(conn);
        Ok(map
            .into_values()
            .filter_map(|v| serde_json::from_str::<MirrorStatus>(&v).ok())
            .collect())
    }

    fn flush_disabled_jobs(&self) -> DbResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let map: HashMap<String, String> = conn.hgetall(MIRROR_STATUS_KEY)?;

        let to_delete: Vec<String> = map
            .into_iter()
            .filter_map(|(k, v)| {
                let m: MirrorStatus = serde_json::from_str(&v).ok()?;
                if m.status == SyncStatus::Disabled || m.name.is_empty() {
                    Some(k)
                } else {
                    None
                }
            })
            .collect();

        for key in &to_delete {
            conn.hdel::<_, _, ()>(MIRROR_STATUS_KEY, key)?;
        }
        Ok(())
    }

    fn close(&self) -> DbResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RedisAdapter;

    #[test]
    fn invalid_url_error_does_not_echo_credentials() {
        let error = RedisAdapter::open("redis://:manager-secret@")
            .err()
            .expect("invalid URL must fail")
            .to_string();
        assert!(!error.contains("manager-secret"));
    }
}
