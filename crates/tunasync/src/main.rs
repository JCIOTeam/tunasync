//! `tunasync` — manager + worker dispatcher.
//!
//! Mirrors the subcommand structure of Go's `cmd/tunasync/tunasync.go`:
//!
//! ```text
//! tunasync manager [--config FILE] [--addr ADDR] [--port PORT] \
//!                  [--cert FILE] [--key FILE] [--db-file FILE] \
//!                  [--db-type TYPE] [--debug] [--with-systemd] ...
//! tunasync worker  [--config FILE] [--with-systemd] ...
//! ```

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "tunasync",
    version = tunasync_common::VERSION,
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

    /// Adapt logging for systemd: suppress timestamps and ANSI colours
    /// (systemd journal adds its own timestamps).
    #[arg(long, global = true)]
    with_systemd: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run as a manager server.
    Manager {
        /// Path to manager.conf (TOML).
        #[arg(short, long, default_value = "/etc/tunasync/manager.conf")]
        config: PathBuf,

        // CLI overrides — when provided these take precedence over the config file values.
        /// Override manager listen address.
        #[arg(long)]
        addr: Option<String>,

        /// Override manager listen port.
        #[arg(long)]
        port: Option<u16>,

        /// SSL certificate file (enables HTTPS).
        #[arg(long)]
        cert: Option<PathBuf>,

        /// SSL key file (enables HTTPS).
        #[arg(long)]
        key: Option<PathBuf>,

        /// Override database file path.
        #[arg(long)]
        db_file: Option<PathBuf>,

        /// Override database type: redb, sqlite, redis.
        #[arg(long)]
        db_type: Option<String>,

        /// Enable debug-level logging.
        #[arg(long)]
        debug: bool,

        /// PID file path.
        #[arg(long, default_value = "/run/tunasync/tunasync.manager.pid")]
        pidfile: Option<PathBuf>,
    },
    /// Run as a worker.
    Worker {
        /// Path to worker.conf (TOML).
        #[arg(short, long, default_value = "/etc/tunasync/worker.conf")]
        config: PathBuf,

        /// PID file path.
        #[arg(long, default_value = "/run/tunasync/tunasync.worker.pid")]
        pidfile: Option<PathBuf>,

        /// Validate the config and exit without starting the worker.
        /// Exit code 0 = OK (warnings allowed), 1 = errors found.
        /// Intended for `ExecStartPre=` and pre-reload checks.
        #[arg(long)]
        check: bool,

        /// Generate deterministic root broker policy and exit.
        #[arg(long, value_name = "PATH", conflicts_with = "check")]
        emit_netns_policy: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install ring as the rustls crypto provider.  Must happen before any TLS
    // operation.  We use axum-server's `tls-rustls-no-provider` feature so
    // aws-lc-rs is never compiled; ring is the sole backend.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install ring crypto provider");

    let cli = Cli::parse();

    match &cli.command {
        Command::Manager {
            config,
            addr,
            port,
            cert,
            key,
            db_file,
            db_type,
            debug,
            pidfile,
        } => {
            // debug flag elevates to trace-level; we pass it to the logger.
            tunasync_common::logger::init(cli.verbose || *debug, cli.with_systemd, true);
            tracing::info!(?config, "starting tunasync manager");

            // Load config file, then apply CLI overrides — mirrors Go's LoadConfig
            // which patches the struct with cli.Context values after TOML decode.
            //
            // Only fall back to defaults if the file does not exist. Any other
            // error (parse, permission, etc.) is fatal — silently swallowing a
            // typo in the config and starting with defaults is a much worse
            // operator experience than a clear "fix your config" message.
            let mut cfg: tunasync_manager::config::ManagerConfig = if config.exists() {
                tunasync_common::config::load_toml(config)?
            } else {
                tracing::warn!(
                    path = %config.display(),
                    "config file not found — using defaults"
                );
                Default::default()
            };

            if let Some(a) = addr {
                cfg.server.addr = a.clone();
            }
            if let Some(p) = port {
                cfg.server.port = *p;
            }
            if let (Some(c), Some(k)) = (cert, key) {
                cfg.server.ssl_cert = c.to_string_lossy().into();
                cfg.server.ssl_key = k.to_string_lossy().into();
            }
            if let Some(f) = db_file {
                cfg.files.db_file = f.clone();
            }
            if let Some(t) = db_type {
                cfg.files.db_type = t.clone();
            }

            // Write PID file if requested (best-effort, non-fatal).
            if let Some(pf) = pidfile {
                write_pidfile(pf);
            }

            tunasync_manager::run_with_config(cfg).await
        }

        Command::Worker {
            config,
            pidfile,
            check,
            emit_netns_policy,
        } => {
            tunasync_common::logger::init(cli.verbose, cli.with_systemd, true);

            if *check {
                // Validate-only mode: never touches PID files, never starts
                // any task. Prints a human-readable report and sets the exit
                // code for scripting.
                let report = tunasync_worker::check_config(config)?;
                for w in &report.warnings {
                    eprintln!("warning: {w}");
                }
                for e in &report.errors {
                    eprintln!("error: {e}");
                }
                if report.errors.is_empty() {
                    println!(
                        "{}: OK — {} mirror(s), {} warning(s)",
                        config.display(),
                        report.mirrors,
                        report.warnings.len()
                    );
                    return Ok(());
                }
                eprintln!(
                    "{}: {} error(s), {} warning(s)",
                    config.display(),
                    report.errors.len(),
                    report.warnings.len()
                );
                std::process::exit(1);
            }

            if let Some(output) = emit_netns_policy {
                tunasync_worker::netns_policy::emit_policy(config, output)?;
                println!("wrote network namespace policy to {}", output.display());
                return Ok(());
            }

            tracing::info!(?config, "starting tunasync worker");

            // Write PID file if requested (best-effort, non-fatal).
            if let Some(pf) = pidfile {
                write_pidfile(pf);
            }

            tunasync_worker::run(config.clone()).await
        }
    }
}

/// Write the current PID to `path`, creating parent directories as needed.
/// Logs a warning on failure but does not abort — matching Go's behaviour
/// where a missing pidfile is not fatal.
fn write_pidfile(path: &PathBuf) {
    let pid = std::process::id().to_string();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(path, pid) {
        tracing::warn!(path = %path.display(), error = %e, "failed to write pidfile");
    }
}
