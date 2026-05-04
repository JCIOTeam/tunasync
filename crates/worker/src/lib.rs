//! tunasync worker.
//!
//! Stage 3: config, job state machine, scheduler, manager client, HTTP server.
//! Stage 4: CmdProvider, RsyncProvider, TwoStageRsyncProvider, exec_post+loglimit hooks.
//! Stage 5: cgroup, docker, zfs, btrfs hooks.

#![warn(rust_2018_idioms)]

pub mod config;
pub mod diff_config;
pub mod hooks;
pub mod http_server;
pub mod job;
pub mod manager_client;
pub mod provider;
pub mod providers;
pub mod runner;
pub mod schedule;
pub mod worker;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use config::ProviderKind;
#[cfg(target_os = "linux")]
use hooks::CgroupHook;
use hooks::{
    BtrfsSnapshotHook, DockerConfig, DockerHook, ExecOn, ExecPostHook, JobHook, LogLimitHook,
    ZfsHook,
};
use providers::{CmdProvider, RsyncProvider, TwoStageRsyncProvider};

/// Expand Go template syntax in `log_dir`.
///
/// Go's `formatLogDir` uses `html/template` with `{{.Name}}` resolving
/// to the mirror name. We support `{{.Name}}` only since that is the
/// only template variable used in practice.
fn expand_log_dir_template(log_dir: &str, mirror_name: &str) -> String {
    log_dir.replace("{{.Name}}", mirror_name)
}

/// Merge Go's `[include]` section glob into `global.include` array, then
/// expand all glob patterns and merge the resulting mirror configs
/// into `cfg.mirrors_conf`.  Called at startup and on hot-reload.
pub fn load_include_mirrors(cfg: &mut config::WorkerConfig) {
    // Merge Go-style [include] section into global.include array.
    if !cfg.include.include_mirrors.is_empty() {
        cfg.global.include.push(cfg.include.include_mirrors.clone());
    }

    for pattern in cfg.global.include.clone() {
        let entries = match glob::glob(&pattern) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(pattern = %pattern, error = %e, "invalid include glob");
                continue;
            }
        };
        for entry in entries {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "error reading include glob entry");
                    continue;
                }
            };
            match tunasync_common::config::load_toml::<config::WorkerConfig>(&path) {
                Ok(extra) => {
                    tracing::debug!(file = %path.display(), mirrors = extra.mirrors_conf.len(), "loaded include file");
                    cfg.mirrors_conf.extend(extra.mirrors_conf);
                }
                Err(e) => {
                    tracing::warn!(file = %path.display(), error = %e, "failed to load include file");
                }
            }
        }
    }
}

/// Entry point invoked by `tunasync worker`.
pub async fn run(config_path: std::path::PathBuf) -> Result<()> {
    let mut cfg: config::WorkerConfig = tunasync_common::config::load_toml(&config_path)
        .unwrap_or_else(|_| {
            tracing::warn!(
                path = %config_path.display(),
                "config file not found or unreadable — using defaults"
            );
            config::WorkerConfig::default()
        });

    if cfg.global.retry == 0 {
        cfg.global.retry = 3;
    }

    // Merge include files before building the mirror list.
    load_include_mirrors(&mut cfg);
    cfg.mirrors = cfg.mirrors_conf.clone();

    tracing::info!(
        worker = %cfg.global.name,
        mirrors = cfg.mirrors.len(),
        "starting tunasync worker"
    );

    let http_client = if cfg.manager.ca_cert.is_empty() {
        tunasync_common::http::HttpClientBuilder::new().build()?
    } else {
        tunasync_common::http::HttpClientBuilder::new()
            .ca_cert_pem_from_path(std::path::Path::new(&cfg.manager.ca_cert))?
            .build()?
    };

    let w = worker::Worker::new(cfg, config_path, build_providers, http_client);
    w.run().await.context("worker run failed")
}

/// Build (provider, hooks) pairs from the worker config.
#[allow(clippy::type_complexity)]
fn build_providers(
    cfg: &config::WorkerConfig,
) -> Vec<(Box<dyn provider::MirrorProvider>, Vec<Box<dyn JobHook>>)> {
    let mut out = Vec::new();

    for mc in &cfg.mirrors {
        match build_one_provider(mc, cfg) {
            Ok((provider, hooks)) => out.push((provider, hooks)),
            Err(e) => {
                tracing::error!(mirror = %mc.name, error = %e, "failed to build provider — skipping");
            }
        }
    }

    out
}

/// Build a single mirror's (provider, hooks) pair.
///
/// Called at startup (via `build_providers`) and on hot-reload for
/// new/modified mirrors.
#[allow(clippy::type_complexity)]
pub fn build_one_provider(
    mc: &config::MirrorConfig,
    cfg: &config::WorkerConfig,
) -> Result<(Box<dyn provider::MirrorProvider>, Vec<Box<dyn JobHook>>)> {
    let result: Result<Box<dyn provider::MirrorProvider>> = match mc.provider {
        ProviderKind::Command => CmdProvider::from_config(mc, &cfg.global)
            .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>),
        ProviderKind::Rsync => RsyncProvider::from_config(mc, &cfg.global)
            .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>),
        ProviderKind::TwoStageRsync => TwoStageRsyncProvider::from_config(mc, &cfg.global)
            .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>),
    };

    let mut provider = match result {
        Ok(p) => p,
        Err(e) => {
            return Err(e.context(format!("build provider for mirror {:?}", mc.name)));
        }
    };

    // Build hooks for this mirror.
    // Go supports {{.Name}} template syntax in log_dir — we expand it here.
    let log_dir_raw = if mc.log_dir.is_empty() {
        cfg.global.log_dir.clone()
    } else {
        mc.log_dir.clone()
    };
    let log_dir = PathBuf::from(expand_log_dir_template(&log_dir_raw, &mc.name));
    let working_dir = mc.effective_mirror_dir(&cfg.global);

    let mut hooks: Vec<Box<dyn JobHook>> = Vec::new();

    // loglimit hook (always enabled if log_dir is set).
    // Wire up shared log path: LogLimitHook sets the path in PreExec,
    // the provider reads it in run() so stdout/stderr go to the rotated log.
    // The shared Arc<Mutex<PathBuf>> is also passed to ExecPostHook and
    // DockerConfig so they read the current rotated path dynamically.
    let mut log_path_shared: Option<Arc<Mutex<PathBuf>>> = None;
    if !log_dir.to_string_lossy().is_empty() {
        let ll_hook = LogLimitHook::new(mc.name.clone(), log_dir.clone());
        log_path_shared = Some(ll_hook.current_log_shared());
        hooks.push(Box::new(ll_hook));
    }
    // Fallback if no LogLimitHook — create a standalone default path.
    let log_path_arc =
        log_path_shared.unwrap_or_else(|| Arc::new(Mutex::new(log_dir.join("latest.log"))));
    provider.set_log_path_shared(Arc::clone(&log_path_arc));

    // Docker hook — also wires up argv wrapping on the provider.
    // Docker and cgroup are mutually exclusive — matches Go:
    //   if docker.Enable && image != "" { DockerHook }
    //   else if cgroup.Enable { CgroupHook }
    if cfg.docker.enable && !mc.docker_image.is_empty() {
        let mut volumes = cfg.docker.volumes.clone();
        volumes.extend(mc.docker_volumes.iter().cloned());
        if !mc.exclude_file.is_empty() {
            volumes.push(format!("{}:{}:ro", mc.exclude_file, mc.exclude_file));
        }
        let mut options = cfg.docker.options.clone();
        options.extend(mc.docker_options.iter().cloned());
        let mem_limit = mc.memory_limit.map(|m| m.0).unwrap_or(0);

        // Compute docker env: TUNASYNC_LOG_FILE is set dynamically from
        // the shared Arc when wrap_argv() is called.
        let docker_env = compute_docker_env(mc, &cfg.global, &working_dir, &log_dir);

        let docker_config = DockerConfig {
            mirror_name: mc.name.clone(),
            image: mc.docker_image.clone(),
            volumes,
            options,
            memory_limit_bytes: mem_limit,
            working_dir: working_dir.clone(),
            log_dir: log_dir.clone(),
            log_file: Arc::clone(&log_path_arc),
            env: docker_env,
        };

        // Wire up argv wrapping on the provider.
        provider.set_docker_config(docker_config.clone());

        // Create DockerHook from the same config for lifecycle hooks.
        hooks.push(Box::new(DockerHook::new(docker_config)));
    } else {
        // cgroup (Linux only) — only when Docker is not active for this mirror.
        #[cfg(target_os = "linux")]
        if cfg.cgroup.enable {
            let mem_limit = mc.memory_limit.map(|m| m.0).unwrap_or(0);
            hooks.push(Box::new(CgroupHook::new(
                mc.name.clone(),
                &cfg.cgroup.base_path,
                &cfg.cgroup.group,
                mem_limit,
            )));
        }
    }

    // zfs
    if cfg.zfs.enable {
        hooks.push(Box::new(ZfsHook::new(
            mc.name.clone(),
            cfg.zfs.zpool.clone(),
            working_dir.to_owned(),
        )));
    }

    // btrfs snapshot (Linux only)
    #[cfg(target_os = "linux")]
    if cfg.btrfs_snapshot.enable {
        hooks.push(Box::new(BtrfsSnapshotHook::new(
            mc.name.clone(),
            working_dir.to_owned(),
            &cfg.btrfs_snapshot.snapshot_path,
            "", // mirror-level snapshot path override not yet in MirrorConfig
        )));
    }

    // Effective exec_on_success: mirror overrides, then append extras to globals.
    let exec_on_success = if !mc.exec_on_success.is_empty() {
        let mut v = mc.exec_on_success.clone();
        v.extend(mc.exec_on_success_extra.iter().cloned());
        v
    } else {
        let mut v = cfg.global.exec_on_success.clone();
        v.extend(mc.exec_on_success_extra.iter().cloned());
        v
    };
    for cmd in &exec_on_success {
        match ExecPostHook::new(
            cmd,
            ExecOn::Success,
            mc.name.clone(),
            working_dir.clone(),
            mc.upstream.clone(),
            log_dir.clone(),
            Arc::clone(&log_path_arc),
        ) {
            Ok(h) => hooks.push(Box::new(h)),
            Err(e) => {
                tracing::warn!(mirror = %mc.name, error = %e, "skip exec_on_success hook")
            }
        }
    }

    let exec_on_failure = if !mc.exec_on_failure.is_empty() {
        let mut v = mc.exec_on_failure.clone();
        v.extend(mc.exec_on_failure_extra.iter().cloned());
        v
    } else {
        let mut v = cfg.global.exec_on_failure.clone();
        v.extend(mc.exec_on_failure_extra.iter().cloned());
        v
    };
    for cmd in &exec_on_failure {
        match ExecPostHook::new(
            cmd,
            ExecOn::Failure,
            mc.name.clone(),
            working_dir.clone(),
            mc.upstream.clone(),
            log_dir.clone(),
            Arc::clone(&log_path_arc),
        ) {
            Ok(h) => hooks.push(Box::new(h)),
            Err(e) => {
                tracing::warn!(mirror = %mc.name, error = %e, "skip exec_on_failure hook")
            }
        }
    }

    Ok((provider, hooks))
}

/// Compute the full environment map for Docker `-e` flags.
///
/// Includes provider-specific env vars (TUNASYNC_* for all providers,
/// USER/RSYNC_PASSWORD for rsync providers) plus mirror-level env overrides.
/// TUNASYNC_LOG_FILE is NOT included here — it is set dynamically from
/// the shared `Arc<Mutex<PathBuf>>` each time `wrap_argv()` is called,
/// so it always reflects the rotated timestamped log path.
fn compute_docker_env(
    mc: &config::MirrorConfig,
    _global: &config::GlobalConfig,
    working_dir: &std::path::Path,
    log_dir: &std::path::Path,
) -> HashMap<String, String> {
    let mut env = HashMap::new();

    // TUNASYNC_* env vars (needed by all providers).
    env.insert("TUNASYNC_MIRROR_NAME".into(), mc.name.clone());
    env.insert(
        "TUNASYNC_WORKING_DIR".into(),
        working_dir.to_string_lossy().into(),
    );
    env.insert("TUNASYNC_UPSTREAM_URL".into(), mc.upstream.clone());
    env.insert("TUNASYNC_LOG_DIR".into(), log_dir.to_string_lossy().into());
    // TUNASYNC_LOG_FILE is injected dynamically in wrap_argv() from the
    // shared Arc<Mutex<PathBuf>> — not baked into this static env map.

    // Rsync-specific env vars for Rsync and TwoStageRsync providers.
    match mc.provider {
        ProviderKind::Rsync | ProviderKind::TwoStageRsync => {
            if !mc.username.is_empty() {
                env.insert("USER".into(), mc.username.clone());
            }
            if !mc.password.is_empty() {
                env.insert("RSYNC_PASSWORD".into(), mc.password.clone());
            }
        }
        _ => {}
    }

    // Mirror-level env overrides.
    env.extend(mc.env.iter().map(|(k, v)| (k.clone(), v.clone())));

    env
}
