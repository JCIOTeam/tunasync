//! `tunasync` — manager + worker dispatcher.
//!
//! Mirrors the subcommand structure of Go's `cmd/tunasync/tunasync.go`:
//!
//! ```text
//! tunasync manager [--config FILE] [--addr ADDR] [--port PORT] ...
//! tunasync worker  [--config FILE] ...
//! ```

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "tunasync",
    version,
    about = "Mirror job management tool (Rust port of tuna/tunasync)",
    long_about = None,
)]
struct Cli {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,

    /// Verbose logging (lifts default level from info to debug).
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run as a manager server.
    Manager {
        /// Path to manager.conf (TOML).
        #[arg(short, long, default_value = "/etc/tunasync/manager.conf")]
        config: PathBuf,
    },
    /// Run as a worker.
    Worker {
        /// Path to worker.conf (TOML).
        #[arg(short, long, default_value = "/etc/tunasync/worker.conf")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    tunasync_common::logger::init(cli.verbose);

    match cli.command {
        Command::Manager { config } => {
            tracing::info!(?config, "starting tunasync manager");
            tunasync_manager::run(config).await
        }
        Command::Worker { config } => {
            tracing::info!(?config, "starting tunasync worker");
            tunasync_worker::run(config).await
        }
    }
}
