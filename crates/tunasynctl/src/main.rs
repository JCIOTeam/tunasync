//! `tunasynctl` — CLI control tool for a tunasync manager.
//!
//! Wire-compatible with Go's `cmd/tunasynctl/tunasynctl.go`.
//! All subcommands use the same JSON API that the Go manager exposes.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tunasync_protocol::{ClientCmd, CmdVerb, MirrorStatus, WebMirrorStatus, WorkerStatus};

// ---------------------------------------------------------------------------
// CLI shape
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "tunasynctl",
    version,
    about = "Control a tunasync manager",
    long_about = None,
)]
struct Cli {
    /// Manager HTTP(S) base URL, e.g. `https://manager.example.com:14242`.
    #[arg(long, env = "TUNASYNC_MANAGER_URL", global = true)]
    manager: Option<String>,

    /// Manager port (combined with --manager host when manager has no port).
    #[arg(
        long,
        env = "TUNASYNC_MANAGER_PORT",
        global = true,
        default_value = "14242"
    )]
    port: u16,

    /// CA cert for pinning the manager's TLS certificate.
    #[arg(long, global = true)]
    ca_cert: Option<PathBuf>,

    /// Verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List all mirror jobs (WebMirrorStatus, same output as status page).
    List {
        /// List jobs of a specific worker; omit for all workers.
        #[arg(short, long)]
        worker: Option<String>,
        /// Filter by status (comma-separated: syncing,failed,success,…).
        #[arg(long)]
        status: Option<String>,
        /// Output format: `json` (default) or `table`.
        #[arg(long, default_value = "json")]
        format: String,
    },
    /// List all registered workers.
    Workers,
    /// Flush all disabled job rows from the manager DB.
    Flush,
    /// Remove a worker from the manager.
    RmWorker {
        /// Worker ID.
        worker: String,
    },
    /// Update the size of a mirror (operator override).
    SetSize {
        /// Mirror name.
        mirror: String,
        /// Human-readable size string, e.g. `1.2T`.
        size: String,
        /// Restrict to a specific worker.
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Start a mirror job.
    Start {
        /// Mirror name, or `all` to broadcast.
        mirror: String,
        /// Restrict to a specific worker.
        #[arg(short, long)]
        worker: Option<String>,
        /// Ignore concurrency limit (force-start).
        #[arg(long)]
        force: bool,
    },
    /// Stop a running mirror job.
    Stop {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Disable a mirror job.
    Disable {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Restart a mirror job.
    Restart {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Tell a worker to reload its config from disk.
    Reload {
        /// Worker ID.
        worker: String,
    },
}

// ---------------------------------------------------------------------------
// Manager API client
// ---------------------------------------------------------------------------

struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    fn new(base_url: String, ca_cert: Option<&PathBuf>) -> Result<Self> {
        let http = if let Some(path) = ca_cert {
            tunasync_common::http::HttpClientBuilder::new()
                .ca_cert_pem_from_path(path)?
                .build()?
        } else {
            tunasync_common::http::HttpClientBuilder::new().build()?
        };
        Ok(Self { base_url, http })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.http
            .get(self.url(path))
            .send()
            .await
            .context("GET request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        self.http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .context("POST request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    async fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.http
            .delete(self.url(path))
            .send()
            .await
            .context("DELETE request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    // ------------------------------------------------------------------
    // Manager API calls
    // ------------------------------------------------------------------

    async fn list_all_jobs(&self) -> Result<Vec<WebMirrorStatus>> {
        self.get("/jobs").await
    }

    async fn list_jobs_of_worker(&self, worker_id: &str) -> Result<Vec<MirrorStatus>> {
        self.get(&format!("/workers/{worker_id}/jobs")).await
    }

    async fn list_workers(&self) -> Result<Vec<WorkerStatus>> {
        self.get("/workers").await
    }

    async fn flush_disabled(&self) -> Result<serde_json::Value> {
        self.delete("/jobs/disabled").await
    }

    async fn remove_worker(&self, worker_id: &str) -> Result<serde_json::Value> {
        self.delete(&format!("/workers/{worker_id}")).await
    }

    async fn set_size(&self, worker_id: &str, mirror: &str, size: &str) -> Result<MirrorStatus> {
        #[derive(serde::Serialize)]
        struct SizeMsg<'a> {
            name: &'a str,
            size: &'a str,
        }
        self.post(
            &format!("/workers/{worker_id}/jobs/{mirror}/size"),
            &SizeMsg { name: mirror, size },
        )
        .await
    }

    async fn send_cmd(&self, cmd: ClientCmd) -> Result<serde_json::Value> {
        self.post("/cmd", &cmd).await
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_base_url(cli: &Cli) -> String {
    if let Some(m) = &cli.manager {
        // If manager already contains a port, use as-is.
        if m.starts_with("http://") || m.starts_with("https://") {
            return m.trim_end_matches('/').to_owned();
        }
        return format!("http://{}:{}", m, cli.port);
    }
    format!("http://127.0.0.1:{}", cli.port)
}

/// Resolve the worker ID for a per-mirror command.
///
/// If `--worker` is specified → use it directly.
/// Otherwise, look up the mirror across all workers and return the first match.
async fn resolve_worker(
    client: &Client,
    mirror: &str,
    explicit_worker: Option<&str>,
) -> Result<String> {
    if let Some(w) = explicit_worker {
        return Ok(w.to_owned());
    }
    // Auto-discover which worker owns this mirror.
    let workers = client.list_workers().await?;
    for w in &workers {
        let jobs = client.list_jobs_of_worker(&w.id).await.unwrap_or_default();
        if jobs.iter().any(|j| j.name == mirror) {
            return Ok(w.id.clone());
        }
    }
    anyhow::bail!(
        "mirror {mirror:?} not found on any worker; use --worker to specify one explicitly"
    )
}

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(v).context("serialize output")?
    );
    Ok(())
}

fn print_table_jobs(jobs: &[WebMirrorStatus]) {
    println!("{:<30} {:<12} {:<20} Size", "Name", "Status", "Last Update");
    println!("{}", "-".repeat(80));
    for j in jobs {
        println!(
            "{:<30} {:<12} {:<20} {}",
            j.name,
            j.status.to_string(),
            j.last_update.format("%Y-%m-%d %H:%M:%S").to_string(),
            j.size,
        );
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    tunasync_common::logger::init(cli.verbose, false);

    let base_url = build_base_url(&cli);
    let client = Client::new(base_url, cli.ca_cert.as_ref())?;

    match &cli.command {
        // ── list ──────────────────────────────────────────────────────────
        Command::List {
            worker,
            status,
            format,
        } => {
            let jobs = if let Some(w) = worker {
                // List jobs of one worker (MirrorStatus → convert to WebMirrorStatus).
                client
                    .list_jobs_of_worker(w)
                    .await?
                    .iter()
                    .map(tunasync_protocol::WebMirrorStatus::from_mirror_status)
                    .collect::<Vec<_>>()
            } else {
                client.list_all_jobs().await?
            };

            // Optional status filter.
            let jobs = if let Some(status_filter) = status {
                let allowed: Vec<tunasync_protocol::SyncStatus> = status_filter
                    .split(',')
                    .map(|s| s.trim().parse())
                    .collect::<Result<_, _>>()
                    .context("parse --status filter")?;
                jobs.into_iter()
                    .filter(|j| allowed.contains(&j.status))
                    .collect()
            } else {
                jobs
            };

            if format == "table" {
                print_table_jobs(&jobs);
            } else {
                print_json(&jobs)?;
            }
        }

        // ── workers ───────────────────────────────────────────────────────
        Command::Workers => {
            let workers = client.list_workers().await?;
            print_json(&workers)?;
        }

        // ── flush ─────────────────────────────────────────────────────────
        Command::Flush => {
            let resp = client.flush_disabled().await?;
            tracing::info!("flush response: {resp}");
            println!("Flushed disabled jobs.");
        }

        // ── rm-worker ─────────────────────────────────────────────────────
        Command::RmWorker { worker } => {
            client.remove_worker(worker).await?;
            println!("Removed worker {worker}.");
        }

        // ── set-size ──────────────────────────────────────────────────────
        Command::SetSize {
            mirror,
            size,
            worker,
        } => {
            let worker_id = resolve_worker(&client, mirror, worker.as_deref()).await?;
            let updated = client.set_size(&worker_id, mirror, size).await?;
            println!(
                "Updated size of mirror {:?} on worker {:?}: {}",
                updated.name, updated.worker, updated.size
            );
        }

        // ── job control commands ───────────────────────────────────────────
        Command::Start {
            mirror,
            worker,
            force,
        } => {
            let worker_id = resolve_worker(&client, mirror, worker.as_deref()).await?;
            let mut options = HashMap::new();
            if *force {
                options.insert("force".into(), true);
            }
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Start,
                    mirror_id: mirror.clone(),
                    worker_id,
                    args: vec![],
                    options,
                })
                .await?;
            println!("Sent start command for mirror {mirror:?}.");
        }

        Command::Stop { mirror, worker } => {
            let worker_id = resolve_worker(&client, mirror, worker.as_deref()).await?;
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Stop,
                    mirror_id: mirror.clone(),
                    worker_id,
                    args: vec![],
                    options: HashMap::new(),
                })
                .await?;
            println!("Sent stop command for mirror {mirror:?}.");
        }

        Command::Disable { mirror, worker } => {
            let worker_id = resolve_worker(&client, mirror, worker.as_deref()).await?;
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Disable,
                    mirror_id: mirror.clone(),
                    worker_id,
                    args: vec![],
                    options: HashMap::new(),
                })
                .await?;
            println!("Sent disable command for mirror {mirror:?}.");
        }

        Command::Restart { mirror, worker } => {
            let worker_id = resolve_worker(&client, mirror, worker.as_deref()).await?;
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Restart,
                    mirror_id: mirror.clone(),
                    worker_id,
                    args: vec![],
                    options: HashMap::new(),
                })
                .await?;
            println!("Sent restart command for mirror {mirror:?}.");
        }

        Command::Reload { worker } => {
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Reload,
                    mirror_id: String::new(),
                    worker_id: worker.clone(),
                    args: vec![],
                    options: HashMap::new(),
                })
                .await?;
            println!("Sent reload command to worker {worker:?}.");
        }
    }

    Ok(())
}
