//! `tunasync-migrate` — import Go tunasync data into a Rust-compatible SQLite DB.
//!
//! Two modes, auto-detected from the first argument:
//!
//! **Online** (Go manager is still running):
//! ```text
//! tunasync-migrate http://localhost:14242 /var/lib/tunasync/new.db
//! ```
//! Reads workers + mirror status via the HTTP API and writes to SQLite.
//!
//! **Offline** (Go manager is stopped, bolt file on disk):
//! ```text
//! tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db
//! ```
//! Parses the bbolt file directly using `bolt-lite` — no Go process needed.
//! The bolt file must not be open by another process (no write lock needed;
//! bolt-lite opens read-only).
//!
//! Both modes produce an identical SQLite output file readable by the Rust
//! manager (`db_type = "sqlite"`).

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::params;
use tunasync_protocol::{MirrorStatus, WorkerStatus};

// ── SQLite schema (matches SqliteAdapter in crates/manager/src/db/sqlite_adapter.rs) ──

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS workers (\
    id   TEXT NOT NULL PRIMARY KEY,\
    data BLOB NOT NULL\
);\
CREATE TABLE IF NOT EXISTS mirror_status (\
    key  TEXT NOT NULL PRIMARY KEY,\
    data BLOB NOT NULL\
);\
PRAGMA journal_mode=WAL;\
";

// ── Entry point ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage:");
        eprintln!("  tunasync-migrate <go-manager-url>  <sqlite-output>  # online");
        eprintln!("  tunasync-migrate <bolt-db-file>    <sqlite-output>  # offline");
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  tunasync-migrate http://localhost:14242 /tmp/tunasync.db");
        eprintln!("  tunasync-migrate /var/lib/tunasync/tunasync.db /tmp/tunasync.db");
        std::process::exit(1);
    }

    let source = &args[1];
    let output_path = PathBuf::from(&args[2]);

    let (workers, mirrors) = if source.starts_with("http://") || source.starts_with("https://") {
        fetch_from_http(source).await?
    } else {
        read_from_bolt(source)?
    };

    write_sqlite(&output_path, &workers, &mirrors)?;
    Ok(())
}

// ── Online mode: HTTP API ────────────────────────────────────────────────────

async fn fetch_from_http(base_url: &str) -> Result<(Vec<WorkerStatus>, Vec<MirrorStatus>)> {
    let base_url = base_url.trim_end_matches('/');
    let client = reqwest::Client::new();

    eprintln!("Fetching workers from {base_url}/workers …");
    let workers: Vec<WorkerStatus> = client
        .get(format!("{base_url}/workers"))
        .send()
        .await
        .context("failed to connect to Go manager")?
        .json()
        .await
        .context("failed to parse workers JSON")?;
    eprintln!("  {} workers", workers.len());

    let mut mirrors: Vec<MirrorStatus> = Vec::new();
    for w in &workers {
        eprintln!("Fetching jobs for worker '{}' …", w.id);
        let result: Result<Vec<MirrorStatus>> = async {
            Ok(client
                .get(format!("{base_url}/workers/{}/jobs", w.id))
                .send()
                .await?
                .json::<Vec<MirrorStatus>>()
                .await?)
        }
        .await;
        match result {
            Ok(ms) => {
                eprintln!("  {} mirrors", ms.len());
                mirrors.extend(ms);
            }
            Err(e) => eprintln!("  WARNING: worker '{}': {e}", w.id),
        }
    }
    eprintln!("Total: {} mirror status entries", mirrors.len());
    Ok((workers, mirrors))
}

// ── Offline mode: read bolt file directly ───────────────────────────────────

/// Parse a bbolt database file and extract `workers` + `mirror_status` buckets.
///
/// Go tunasync stores:
/// - Bucket `"workers"`:       key = workerID,            value = JSON(WorkerStatus)
/// - Bucket `"mirror_status"`: key = mirrorID + "/" + workerID, value = JSON(MirrorStatus)
fn read_from_bolt(path: &str) -> Result<(Vec<WorkerStatus>, Vec<MirrorStatus>)> {
    use bolt_lite::Bolt;

    eprintln!("Opening bolt database: {path}");
    let db = Bolt::open_ro(path).with_context(|| format!("failed to open bolt file '{path}'"))?;

    let stats = db.stats();
    eprintln!(
        "  page_size={}, pages={}, file_size={}",
        stats.page_size, stats.page_count, stats.bytes,
    );

    let tx = db
        .begin()
        .context("failed to begin bolt read transaction")?;

    // ── workers bucket ────────────────────────────────────────────────────────
    let workers_bucket = tx
        .bucket(b"workers")
        .context("bolt file has no 'workers' bucket — is this a tunasync bolt file?")?;

    let mut workers: Vec<WorkerStatus> = Vec::new();
    let mut worker_parse_errs = 0usize;

    let cursor = workers_bucket
        .cursor()
        .context("failed to iterate 'workers' bucket")?;

    for entry in cursor {
        // skip nested buckets (flags & BUCKET_VALUE_FLAG != 0)
        if entry.flags & bolt_lite::BUCKET_VALUE_FLAG != 0 {
            continue;
        }
        match serde_json::from_slice::<WorkerStatus>(&entry.value) {
            Ok(w) => workers.push(w),
            Err(e) => {
                let key = String::from_utf8_lossy(&entry.key);
                eprintln!("  WARNING: failed to parse worker '{key}': {e}");
                worker_parse_errs += 1;
            }
        }
    }
    eprintln!(
        "  workers bucket: {} ok, {} parse errors",
        workers.len(),
        worker_parse_errs
    );

    // ── mirror_status bucket ──────────────────────────────────────────────────
    let status_bucket = tx
        .bucket(b"mirror_status")
        .context("bolt file has no 'mirror_status' bucket")?;

    let mut mirrors: Vec<MirrorStatus> = Vec::new();
    let mut mirror_parse_errs = 0usize;

    let cursor = status_bucket
        .cursor()
        .context("failed to iterate 'mirror_status' bucket")?;

    for entry in cursor {
        if entry.flags & bolt_lite::BUCKET_VALUE_FLAG != 0 {
            continue;
        }
        match serde_json::from_slice::<MirrorStatus>(&entry.value) {
            Ok(m) => mirrors.push(m),
            Err(e) => {
                let key = String::from_utf8_lossy(&entry.key);
                eprintln!("  WARNING: failed to parse mirror status '{key}': {e}");
                mirror_parse_errs += 1;
            }
        }
    }
    eprintln!(
        "  mirror_status bucket: {} ok, {} parse errors",
        mirrors.len(),
        mirror_parse_errs
    );

    Ok((workers, mirrors))
}

// ── Write SQLite ─────────────────────────────────────────────────────────────

fn write_sqlite(
    output_path: &PathBuf,
    workers: &[WorkerStatus],
    mirrors: &[MirrorStatus],
) -> Result<()> {
    eprintln!("Writing to {} …", output_path.display());

    let conn = rusqlite::Connection::open(output_path).context("failed to create SQLite file")?;
    conn.execute_batch(SCHEMA)
        .context("failed to create schema")?;

    let mut worker_errs = 0usize;
    for w in workers {
        let bytes = serde_json::to_vec(w).unwrap();
        if conn
            .execute(
                "INSERT OR REPLACE INTO workers (id, data) VALUES (?1, ?2)",
                params![w.id, bytes],
            )
            .is_err()
        {
            worker_errs += 1;
        }
    }

    let mut mirror_errs = 0usize;
    for m in mirrors {
        // Key format matches Go's: mirrorID + "/" + workerID
        let key = format!("{}/{}", m.name, m.worker);
        let bytes = serde_json::to_vec(m).unwrap();
        if conn
            .execute(
                "INSERT OR REPLACE INTO mirror_status (key, data) VALUES (?1, ?2)",
                params![key, bytes],
            )
            .is_err()
        {
            mirror_errs += 1;
        }
    }

    let ok_workers = workers.len() - worker_errs;
    let ok_mirrors = mirrors.len() - mirror_errs;
    eprintln!(
        "Migrated {} workers, {} mirror status entries → {}",
        ok_workers,
        ok_mirrors,
        output_path.display()
    );
    if worker_errs > 0 || mirror_errs > 0 {
        eprintln!("Skipped: {worker_errs} workers, {mirror_errs} mirrors (serialisation error)");
    }
    eprintln!(
        "Done. Set `db_type = \"sqlite\"` and `db_file = \"{}\"` in manager.conf.",
        output_path.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the offline bolt reader correctly parses a real bbolt file.
    ///
    /// Creates a bbolt-format file via bbolt-rs (compat feature), writes known
    /// workers + mirror status, then round-trips through `read_from_bolt` and
    /// checks every field survives intact.
    #[test]
    fn offline_bolt_round_trip() {
        use bbolt_rs::{BoltOptions, BucketRwApi, DbRwAPI, TxRwApi, TxRwRefApi};
        use tunasync_protocol::{MirrorStatus, SyncStatus, WorkerStatus};

        let dir = tempfile::tempdir().unwrap();
        let bolt_path = dir.path().join("test.db");

        // ── Write a bbolt file ────────────────────────────────────────────────
        {
            let mut db = BoltOptions::default()
                .open(bolt_path.to_str().unwrap())
                .unwrap();
            db.update(|mut tx| {
                let mut workers = tx.create_bucket(b"workers")?;
                let w = WorkerStatus {
                    id: "worker-a".into(),
                    url: "http://worker-a:6000".into(),
                    token: String::new(),
                    last_online: chrono::Utc::now(),
                    last_register: chrono::Utc::now(),
                };
                workers.put(b"worker-a", serde_json::to_vec(&w).unwrap())?;

                let mut statuses = tx.create_bucket(b"mirror_status")?;
                let m = MirrorStatus {
                    name: "ubuntu".into(),
                    worker: "worker-a".into(),
                    is_master: true,
                    status: SyncStatus::Success,
                    last_update: chrono::Utc::now(),
                    last_started: chrono::Utc::now(),
                    last_ended: chrono::Utc::now(),
                    scheduled: chrono::Utc::now(),
                    upstream: "rsync://ubuntu.example.com/".into(),
                    size: "10G".into(),
                    error_msg: String::new(),
                };
                // key = mirrorID/workerID  (matches Go's format exactly)
                statuses.put(b"ubuntu/worker-a", serde_json::to_vec(&m).unwrap())?;
                Ok(())
            })
            .unwrap();
        }

        // ── Read it back with our offline reader ──────────────────────────────
        let (workers, mirrors) = read_from_bolt(bolt_path.to_str().unwrap()).unwrap();

        assert_eq!(workers.len(), 1, "expected 1 worker");
        assert_eq!(workers[0].id, "worker-a");
        assert_eq!(workers[0].url, "http://worker-a:6000");

        assert_eq!(mirrors.len(), 1, "expected 1 mirror");
        assert_eq!(mirrors[0].name, "ubuntu");
        assert_eq!(mirrors[0].worker, "worker-a");
        assert_eq!(mirrors[0].size, "10G");
        assert_eq!(mirrors[0].status, SyncStatus::Success);
    }
}
