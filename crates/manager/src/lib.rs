//! tunasync manager server.
//!
//! Entry-point: [`run`].  Called by the `tunasync manager` subcommand.

#![warn(rust_2018_idioms)]

pub mod config;
pub mod db;
pub mod server;

use anyhow::{Context, Result};
use tokio::net::TcpListener;

use crate::config::ManagerConfig;
use crate::db::open as open_db;
use crate::server::{build_router, AppState};

/// Start the manager and block until the server exits.
pub async fn run(config_path: std::path::PathBuf) -> Result<()> {
    let cfg: ManagerConfig =
        tunasync_common::config::load_toml(&config_path).unwrap_or_else(|_| {
            tracing::warn!(
                path = %config_path.display(),
                "config file not found or unreadable — using defaults"
            );
            ManagerConfig::default()
        });

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

    let state = AppState { db, http_client };
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
