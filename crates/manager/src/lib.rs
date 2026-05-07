//! tunasync manager server.
//!
//! Entry-point: [`run`] (config file only) or [`run_with_config`] (pre-built
//! config, used by the CLI when flags override config-file values).

#![warn(rust_2018_idioms)]

pub mod config;
pub mod db;
pub mod server;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::config::ManagerConfig;
use crate::db::open as open_db;
use crate::server::{build_router, AppState};

/// Start the manager from a config file path, blocking until the server exits.
pub async fn run(config_path: std::path::PathBuf) -> Result<()> {
    let cfg: ManagerConfig =
        tunasync_common::config::load_toml(&config_path).unwrap_or_else(|_| {
            tracing::warn!(
                path = %config_path.display(),
                "config file not found or unreadable — using defaults"
            );
            ManagerConfig::default()
        });
    run_with_config(cfg).await
}

/// Start the manager from a fully-constructed [`ManagerConfig`].
///
/// Used by the CLI to apply command-line overrides after loading the config
/// file, matching Go's `LoadConfig` which accepts a `*cli.Context` and
/// patches the struct fields with CLI flag values.
pub async fn run_with_config(cfg: ManagerConfig) -> Result<()> {
    tracing::info!(
        addr = %cfg.server.addr,
        port = cfg.server.port,
        db_type = %cfg.files.db_type,
        db_file = %cfg.files.db_file.display(),
        "starting tunasync manager"
    );

    // Open DB adapter.
    let db = open_db(&cfg.files.db_type, &cfg.files.db_file).with_context(|| {
        format!(
            "open {} DB at {}",
            cfg.files.db_type,
            cfg.files.db_file.display()
        )
    })?;

    // Build HTTP client (used by manager to forward commands to workers).
    let http_client = if cfg.files.ca_cert.is_empty() {
        tunasync_common::http::HttpClientBuilder::new().build()?
    } else {
        tunasync_common::http::HttpClientBuilder::new()
            .ca_cert_pem_from_path(std::path::Path::new(&cfg.files.ca_cert))?
            .build()?
    };

    // Wrap in Arc so both the router and background tasks share the same state.
    let state = std::sync::Arc::new(AppState { db, http_client });

    // Spawn status-file writer if the parent directory exists.
    let status_file = cfg.files.status_file.clone();
    if let Some(parent) = status_file.parent() {
        if parent.exists() {
            let state_clone = std::sync::Arc::clone(&state);
            tokio::spawn(async move {
                status_file_writer(state_clone, status_file).await;
            });
        } else {
            tracing::warn!(
                path = %status_file.display(),
                "status_file parent directory does not exist — status file will not be written"
            );
        }
    }

    let router = build_router(state);
    let bind_addr = cfg.server.bind_addr();

    // Graceful shutdown signal (SIGTERM or SIGINT).
    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("received SIGTERM"),
                _ = sigint.recv()  => tracing::info!("received SIGINT"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };

    if cfg.server.tls_enabled() {
        tracing::info!(%bind_addr, "binding HTTPS listener");
        axum_server::bind_rustls(
            bind_addr,
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &cfg.server.ssl_cert,
                &cfg.server.ssl_key,
            )
            .await
            .context("load TLS cert/key")?,
        )
        .serve(router.into_make_service())
        .await
        .context("HTTPS server error")?;
    } else {
        tracing::info!(%bind_addr, "binding HTTP listener");
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("bind {bind_addr}"))?;
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
            .context("HTTP server error")?;
    }

    Ok(())
}

/// Background task: write a JSON snapshot of all mirror statuses to
/// `status_file` every 30 seconds.
///
/// Writes atomically via a `.tmp` file + rename so readers never see a
/// partially-written file.  Errors are logged but never fatal — a transient
/// write failure will retry on the next tick.
async fn status_file_writer(state: std::sync::Arc<AppState>, path: std::path::PathBuf) {
    use std::time::Duration;
    use tokio::time;

    tracing::info!(path = %path.display(), interval_s = 30, "status file writer started");

    let mut interval = time::interval(Duration::from_secs(30));
    // Skip the first tick (fires immediately) to avoid a write at t=0 before
    // the manager has received any status updates.
    interval.tick().await;

    loop {
        interval.tick().await;

        let mirrors = match state.db.list_all_mirror_status() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "status_file: failed to read mirror status");
                continue;
            }
        };

        let web: Vec<tunasync_protocol::WebMirrorStatus> = mirrors
            .iter()
            .map(tunasync_protocol::WebMirrorStatus::from_mirror_status)
            .collect();

        let json = match serde_json::to_string_pretty(&web) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(error = %e, "status_file: JSON serialization failed");
                continue;
            }
        };

        // Atomic write: write to <path>.tmp then rename.
        let tmp_path = path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp_path, &json) {
            tracing::warn!(
                path = %tmp_path.display(),
                error = %e,
                "status_file: failed to write tmp file"
            );
            continue;
        }
        if let Err(e) = std::fs::rename(&tmp_path, &path) {
            tracing::warn!(
                src = %tmp_path.display(),
                dst = %path.display(),
                error = %e,
                "status_file: rename failed"
            );
            let _ = std::fs::remove_file(&tmp_path);
            continue;
        }

        tracing::debug!(path = %path.display(), mirrors = mirrors.len(), "status file updated");
    }
}
