//! tunasync manager server.
//!
//! Entry-point: [`run`] (config file only) or [`run_with_config`] (pre-built
//! config, used by the CLI when flags override config-file values).

#![warn(rust_2018_idioms)]

pub mod config;
pub mod db;
pub mod server;
pub mod webhook;

use anyhow::{Context, Result};
use tokio::net::TcpListener;

use crate::config::ManagerConfig;
use crate::db::open as open_db;
use crate::server::{build_router, AppState};

pub struct ConfigCheckReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn check_config(cfg: &ManagerConfig) -> ConfigCheckReport {
    let mut errors = Vec::new();
    let warnings = Vec::new();

    if !cfg.files.db_type_explicitly_configured {
        errors.push(
            "manager files.db_type must be set explicitly before migration: the Go manager defaults to bolt while tunasync-rs defaults to redb; migrate existing data, then select redb, sqlite, or redis"
                .into(),
        );
    }

    if let Err(error) = cfg.server.validate_tls() {
        errors.push(error);
    }
    if let Err(error) = cfg.server.bind_addr() {
        errors.push(error);
    }
    match cfg.files.db_type.as_str() {
        "redb" | "sqlite" | "redis" => {}
        "bolt" | "badger" | "leveldb" => errors.push(format!(
            "manager files.db_type {:?} is supported by the Go manager but not tunasync-rs; migrate the data and use redb, sqlite, or redis",
            cfg.files.db_type
        )),
        other => errors.push(format!(
            "unsupported manager files.db_type {other:?}; valid values are redb, sqlite, or redis"
        )),
    }
    if cfg.files.db_type == "redis" {
        match cfg.files.db_file.to_str() {
            Some(url) if url.starts_with("redis://") || url.starts_with("rediss://") => {}
            _ => errors.push(
                "manager files.db_file must be a redis:// or rediss:// URL when files.db_type is redis"
                    .into(),
            ),
        }
    }

    ConfigCheckReport { errors, warnings }
}

/// Start the manager from a config file path, blocking until the server exits.
///
/// Only falls back to defaults when the file does not exist. Parse errors,
/// permission errors, etc. are fatal — silently swallowing a typo in the
/// config and starting with defaults is a much worse operator experience
/// than a clear "fix your config" message.
pub async fn run(config_path: std::path::PathBuf) -> Result<()> {
    let cfg: ManagerConfig = if config_path.exists() {
        tunasync_common::config::load_toml(&config_path)?
    } else {
        tracing::warn!(
            path = %config_path.display(),
            "config file not found — using defaults"
        );
        ManagerConfig::default()
    };
    run_with_config(cfg).await
}

/// Start the manager from a fully-constructed [`ManagerConfig`].
///
/// Used by the CLI to apply command-line overrides after loading the config
/// file, matching Go's `LoadConfig` which accepts a `*cli.Context` and
/// patches the struct fields with CLI flag values.
pub async fn run_with_config(cfg: ManagerConfig) -> Result<()> {
    let report = check_config(&cfg);
    if !report.errors.is_empty() {
        anyhow::bail!("{}", report.errors.join("; "));
    }
    let db_target = db_target_for_diagnostic(&cfg);
    tracing::info!(
        addr = %cfg.server.addr,
        port = cfg.server.port,
        db_type = %cfg.files.db_type,
        db_file = %db_target,
        "starting tunasync manager"
    );

    // Open DB adapter.
    let db = open_db(&cfg.files.db_type, &cfg.files.db_file)
        .with_context(|| format!("open {} DB at {db_target}", cfg.files.db_type))?;

    // Build HTTP client (used by manager to forward commands to workers).
    let http_client = if cfg.files.ca_cert.is_empty() {
        tunasync_common::http::HttpClientBuilder::new().build()?
    } else {
        tunasync_common::http::HttpClientBuilder::new()
            .ca_cert_pem_from_path(std::path::Path::new(&cfg.files.ca_cert))?
            .build()?
    };

    // Streaming client for proxying worker SSE log streams (no total
    // timeout — see `GET /jobs/:name/log/stream`). Shares the CA pin.
    let sse_client = if cfg.files.ca_cert.is_empty() {
        tunasync_common::http::HttpClientBuilder::new()
            .streaming()
            .build()?
    } else {
        tunasync_common::http::HttpClientBuilder::new()
            .ca_cert_pem_from_path(std::path::Path::new(&cfg.files.ca_cert))?
            .streaming()
            .build()?
    };

    // Wrap in Arc so both the router and background tasks share the same state.
    let state = std::sync::Arc::new(AppState {
        db,
        mirror_update_lock: tokio::sync::Mutex::new(()),
        http_client,
        sse_client,
        api_token: cfg.server.api_token.clone(),
        maintenance: std::sync::atomic::AtomicBool::new(false),
        notify: cfg.notify.clone(),
    });

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

    // Spawn stale detector if configured.
    if !cfg.notify.stale_after.is_empty() {
        if let Some(stale_secs) =
            tunasync_common::util::parse_duration_secs(&cfg.notify.stale_after)
        {
            let state_clone = std::sync::Arc::clone(&state);
            tokio::spawn(async move {
                stale_detector(state_clone, stale_secs).await;
            });
            tracing::info!(
                stale_after = %cfg.notify.stale_after,
                webhook_enabled = !cfg.notify.webhook_url.is_empty(),
                "stale detector started"
            );
        } else {
            tracing::warn!(
                stale_after = %cfg.notify.stale_after,
                "invalid stale_after duration — stale detector not started"
            );
        }
    }

    let router = build_router(state);
    let bind_addr = cfg.server.bind_addr().map_err(anyhow::Error::msg)?;

    if cfg.server.tls_enabled() {
        tracing::info!(%bind_addr, "binding HTTPS listener");
        let handle = axum_server::Handle::new();
        tokio::spawn(shutdown_axum_server(handle.clone()));
        axum_server::bind_rustls(
            bind_addr,
            axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &cfg.server.ssl_cert,
                &cfg.server.ssl_key,
            )
            .await
            .context("load TLS cert/key")?,
        )
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .context("HTTPS server error")?;
    } else {
        tracing::info!(%bind_addr, "binding HTTP listener");
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("bind {bind_addr}"))?;
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("HTTP server error")?;
    }

    Ok(())
}

fn db_target_for_diagnostic(cfg: &ManagerConfig) -> String {
    if cfg.files.db_type != "redis" {
        return cfg.files.db_file.display().to_string();
    }

    let raw = cfg.files.db_file.to_string_lossy();
    let Ok(mut url) = url::Url::parse(&raw) else {
        return "<invalid Redis URL>".into();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

async fn shutdown_signal() {
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
}

async fn shutdown_axum_server(handle: axum_server::Handle<std::net::SocketAddr>) {
    shutdown_signal().await;
    handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
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

        let mirrors = match state.db_call(|db| db.list_all_mirror_status()).await {
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

        // Atomic write: write to <path>.tmp then rename. Filesystem I/O is
        // blocking too, so keep it off the async executor alongside the DB.
        let tmp_path = path.with_extension("json.tmp");
        let write_tmp = tmp_path.clone();
        let write_json = json.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || std::fs::write(write_tmp, write_json))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())))
        {
            tracing::warn!(
                path = %tmp_path.display(),
                error = %e,
                "status_file: failed to write tmp file"
            );
            continue;
        }
        let rename_src = tmp_path.clone();
        let rename_dst = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || std::fs::rename(rename_src, rename_dst))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e.to_string())))
        {
            tracing::warn!(
                src = %tmp_path.display(),
                dst = %path.display(),
                error = %e,
                "status_file: rename failed"
            );
            let cleanup = tmp_path.clone();
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(cleanup)).await;
            continue;
        }

        tracing::debug!(path = %path.display(), mirrors = mirrors.len(), "status file updated");
    }
}

/// Background task: check all mirrors every 5 minutes and mark as stale
/// any mirror whose `last_update` is older than `stale_secs` seconds ago.
/// Fires a webhook when a mirror transitions into or out of stale state.
async fn stale_detector(state: std::sync::Arc<AppState>, stale_secs: u64) {
    use std::time::Duration;
    use tokio::time;

    let stale_duration = chrono::Duration::seconds(stale_secs as i64);

    let mut interval = time::interval(Duration::from_secs(300)); // 5 minutes
    interval.tick().await; // skip immediate first tick

    loop {
        interval.tick().await;

        let mirrors = match state.db_call(|db| db.list_all_mirror_status()).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "stale_detector: failed to read mirror status");
                continue;
            }
        };

        let now = chrono::Utc::now();

        for mirror in mirrors {
            let age = now - mirror.last_update;
            let was_stale = mirror.stale;
            let is_stale = !tunasync_protocol::is_zero_time(&mirror.last_update)
                && age > stale_duration
                && !matches!(
                    mirror.status,
                    tunasync_protocol::SyncStatus::Disabled | tunasync_protocol::SyncStatus::Paused
                );

            if is_stale != was_stale {
                // Re-fetch the current row immediately before writing so we
                // merge our stale-flag change onto the latest worker telemetry
                // rather than overwriting it. Without this, the sequence
                //
                //   t0  stale_detector reads mirror (Failed, stale=false)
                //   t1  worker reports Success → last_update bumped, stale
                //       implicitly cleared by the new last_update
                //   t2  stale_detector writes its stale=true plus the OLD
                //       (Failed, old last_update) row, losing the Success.
                //
                // is a real data-loss bug. The narrow window (read → write)
                // is on the order of hundreds of microseconds, but a 5-minute
                // scan over thousands of mirrors will hit it eventually.
                //
                // Strategy: fetch the latest row. If `last_update` has moved
                // forward since our scan, re-evaluate `is_stale` against the
                // fresh row — the worker has reported new data and our scan's
                // verdict is potentially stale itself. Then write only if the
                // verdict still differs from the persisted `stale` flag.
                let fresh_worker = mirror.worker.clone();
                let fresh_name = mirror.name.clone();
                let _update_guard = state.mirror_update_lock.lock().await;
                let fresh = match state
                    .db_call(move |db| db.get_mirror_status(&fresh_worker, &fresh_name))
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            mirror = %mirror.name,
                            error = %e,
                            "stale_detector: failed to re-fetch row; skipping"
                        );
                        continue;
                    }
                };

                let fresh_is_stale = !tunasync_protocol::is_zero_time(&fresh.last_update)
                    && (now - fresh.last_update) > stale_duration
                    && !matches!(
                        fresh.status,
                        tunasync_protocol::SyncStatus::Disabled
                            | tunasync_protocol::SyncStatus::Paused
                    );

                if fresh_is_stale == fresh.stale {
                    // Either a worker write since our scan already updated
                    // last_update past the threshold (clearing stale) or
                    // another stale_detector pass already wrote the flag.
                    // Nothing to do.
                    continue;
                }

                // Merge: take the fresh row and only flip stale.
                let mut to_write = fresh.clone();
                to_write.stale = fresh_is_stale;

                let update_worker = mirror.worker.clone();
                let update_name = mirror.name.clone();
                let update_value = to_write.clone();
                if let Err(e) = state
                    .db_call(move |db| {
                        db.update_mirror_status(&update_worker, &update_name, update_value)
                    })
                    .await
                {
                    tracing::warn!(
                        mirror = %mirror.name,
                        error = %e,
                        "stale_detector: failed to update stale flag"
                    );
                    continue;
                }

                // Use the actual written value for downstream notification.
                let is_stale = fresh_is_stale;
                let mirror = to_write;

                // Webhook notification.
                if !state.notify.webhook_url.is_empty() {
                    let text = if is_stale {
                        format!(
                            "⏰ Mirror {} on worker {} is now STALE — last successful sync was {}",
                            mirror.name,
                            mirror.worker,
                            mirror.last_update.format("%Y-%m-%d %H:%M UTC")
                        )
                    } else {
                        format!(
                            "✅ Mirror {} on worker {} is no longer stale",
                            mirror.name, mirror.worker
                        )
                    };
                    let client = state.http_client.clone();
                    let url = state.notify.webhook_url.clone();
                    tokio::spawn(async move {
                        crate::webhook::send(&client, &url, &text).await;
                    });
                }

                tracing::info!(
                    mirror = %mirror.name,
                    worker = %mirror.worker,
                    stale = is_stale,
                    last_update = %mirror.last_update,
                    "stale_detector: stale state changed"
                );
            }
        }
    }
}

#[cfg(test)]
mod config_check_tests {
    use super::{check_config, db_target_for_diagnostic};
    use crate::config::ManagerConfig;

    #[test]
    fn rejects_go_only_database_backends_with_migration_guidance() {
        for backend in ["bolt", "badger", "leveldb"] {
            let mut cfg = ManagerConfig::default();
            cfg.files.db_type = backend.into();
            cfg.files.db_type_explicitly_configured = true;
            let errors = check_config(&cfg).errors.join(" ");
            assert!(errors.contains(backend));
            assert!(errors.contains("migrate"));
            assert!(errors.contains("sqlite"));
        }
    }

    #[test]
    fn accepts_supported_backends_and_validates_redis_url() {
        for backend in ["redb", "sqlite"] {
            let mut cfg = ManagerConfig::default();
            cfg.files.db_type = backend.into();
            cfg.files.db_type_explicitly_configured = true;
            assert!(check_config(&cfg).errors.is_empty());
        }

        let mut redis = ManagerConfig::default();
        redis.files.db_type = "redis".into();
        redis.files.db_type_explicitly_configured = true;
        redis.files.db_file = "redis://127.0.0.1:6379/0".into();
        assert!(check_config(&redis).errors.is_empty());
        redis.files.db_file = "/var/lib/tunasync/redis".into();
        assert!(check_config(&redis)
            .errors
            .iter()
            .any(|error| error.contains("redis://")));
    }

    #[test]
    fn rejects_omitted_database_type_before_migration() {
        let cfg: ManagerConfig = tunasync_common::config::parse_toml(
            r#"
[files]
db_file = "/var/lib/tunasync/tunasync.db"
"#,
        )
        .unwrap();

        let errors = check_config(&cfg).errors.join(" ");
        assert!(errors.contains("must be set explicitly"));
        assert!(errors.contains("Go manager defaults to bolt"));
    }

    #[test]
    fn redis_diagnostic_target_redacts_credentials_and_tokens() {
        let mut cfg = ManagerConfig::default();
        cfg.files.db_type = "redis".into();
        cfg.files.db_file =
            "rediss://user:manager-secret@redis.example.invalid:6380/0?token=query-secret#fragment"
                .into();

        let target = db_target_for_diagnostic(&cfg);
        assert!(target.contains("redis.example.invalid:6380/0"));
        assert!(!target.contains("user"));
        assert!(!target.contains("manager-secret"));
        assert!(!target.contains("query-secret"));
        assert!(!target.contains("fragment"));
    }
}
