//! Migrate data from a running Go tunasync manager to a Rust-compatible SQLite DB.
//!
//! Usage: `tunasync-migrate <go-manager-url> <sqlite-output-file>`
//!
//! The tool fetches workers and mirror statuses via the Go manager's HTTP API
//! and writes them into SQLite using the same schema as `SqliteAdapter`, so
//! the Rust manager can read the output file directly.

use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::params;
use tunasync_protocol::{MirrorStatus, WorkerStatus};

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS workers (
    id   TEXT NOT NULL PRIMARY KEY,
    data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS mirror_status (
    key  TEXT NOT NULL PRIMARY KEY,
    data BLOB NOT NULL
);
PRAGMA journal_mode=WAL;
";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: tunasync-migrate <go-manager-url> <sqlite-output-file>");
        eprintln!("Example: tunasync-migrate http://localhost:14242 /tmp/tunasync.db");
        std::process::exit(1);
    }

    let base_url = args[1].trim_end_matches('/').to_owned();
    let output_path = PathBuf::from(&args[2]);

    let client = reqwest::Client::new();

    // --- Fetch workers ---
    eprintln!("Fetching workers from {base_url}/workers …");
    let workers: Vec<WorkerStatus> = client
        .get(format!("{base_url}/workers"))
        .send()
        .await
        .context("failed to connect to Go manager")?
        .json()
        .await
        .context("failed to parse workers JSON")?;
    eprintln!("  Found {} workers", workers.len());

    // --- Fetch mirror status per worker ---
    let mut all_mirrors: Vec<MirrorStatus> = Vec::new();
    for w in &workers {
        eprintln!("Fetching jobs for worker '{}' …", w.id);
        let url = format!("{base_url}/workers/{}/jobs", w.id);
        let resp = client.get(&url).send().await;
        match resp {
            Ok(r) => match r.json::<Vec<MirrorStatus>>().await {
                Ok(mirrors) => {
                    eprintln!("  {} mirrors", mirrors.len());
                    all_mirrors.extend(mirrors);
                }
                Err(e) => eprintln!(
                    "  WARNING: failed to parse mirrors for worker '{}': {e}",
                    w.id
                ),
            },
            Err(e) => eprintln!(
                "  WARNING: failed to fetch mirrors for worker '{}': {e}",
                w.id
            ),
        }
    }
    eprintln!("Total: {} mirror status entries", all_mirrors.len());

    // --- Write SQLite ---
    eprintln!("Writing to {} …", output_path.display());
    let conn = rusqlite::Connection::open(&output_path).context("failed to create SQLite file")?;
    conn.execute_batch(SCHEMA)
        .context("failed to create schema")?;

    let mut worker_errs = 0;
    for w in &workers {
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

    let mut mirror_errs = 0;
    for m in &all_mirrors {
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

    eprintln!(
        "Migrated {} workers, {} mirror status entries → {}",
        workers.len() - worker_errs,
        all_mirrors.len() - mirror_errs,
        output_path.display()
    );
    if worker_errs > 0 || mirror_errs > 0 {
        eprintln!("Errors: {worker_errs} worker, {mirror_errs} mirror");
    }
    eprintln!("Done.");

    Ok(())
}
