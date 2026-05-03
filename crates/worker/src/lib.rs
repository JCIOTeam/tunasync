//! tunasync worker.
//!
//! Stage 3: config, job state machine, scheduler, manager client, HTTP server.
//! Stage 4: CmdProvider, RsyncProvider, TwoStageRsyncProvider, exec_post+loglimit hooks.
//! Stage 5: cgroup, docker, zfs, btrfs hooks.

#![warn(rust_2018_idioms)]

pub mod config;
pub mod context;
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

use anyhow::{Context, Result};
use config::ProviderKind;
use hooks::{ExecOn, ExecPostHook, JobHook, LogLimitHook, DockerHook, ZfsHook, BtrfsSnapshotHook};
#[cfg(target_os = "linux")]
use hooks::CgroupHook;
use providers::{CmdProvider, RsyncProvider, TwoStageRsyncProvider};

/// Expand `global.include` glob patterns and merge the resulting mirror configs
/// into `cfg.mirrors_conf`.  Called at startup and on hot-reload.
pub fn load_include_mirrors(cfg: &mut config::WorkerConfig) {
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
fn build_providers(
    cfg: &config::WorkerConfig,
) -> Vec<(Box<dyn provider::MirrorProvider>, Vec<Box<dyn JobHook>>)> {
    let mut out = Vec::new();

    for mc in &cfg.mirrors {
        let result: Result<Box<dyn provider::MirrorProvider>> = match mc.provider {
            ProviderKind::Command => {
                CmdProvider::from_config(mc, &cfg.global)
                    .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>)
            }
            ProviderKind::Rsync => {
                RsyncProvider::from_config(mc, &cfg.global)
                    .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>)
            }
            ProviderKind::TwoStageRsync => {
                TwoStageRsyncProvider::from_config(mc, &cfg.global)
                    .map(|p| Box::new(p) as Box<dyn provider::MirrorProvider>)
            }
        };

        let provider = match result {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(mirror = %mc.name, error = %e, "failed to build provider — skipping");
                continue;
            }
        };

        // Build hooks for this mirror.
        let log_dir = if mc.log_dir.is_empty() {
            std::path::PathBuf::from(&cfg.global.log_dir)
        } else {
            std::path::PathBuf::from(&mc.log_dir)
        };
        let working_dir = mc.effective_mirror_dir(&cfg.global);
        let log_file = log_dir.join(format!("{}.log", mc.name));

        let mut hooks: Vec<Box<dyn JobHook>> = Vec::new();

        // loglimit hook (always enabled if log_dir is set).
        if !log_dir.to_string_lossy().is_empty() {
            hooks.push(Box::new(LogLimitHook::new(mc.name.clone(), log_dir.clone())));
        }

        // System-level hooks (cgroup, docker, zfs, btrfs).
        add_system_hooks(&mut hooks, mc, cfg, &working_dir, &log_dir, &log_file);

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
                cmd, ExecOn::Success,
                mc.name.clone(), working_dir.clone(),
                mc.upstream.clone(), log_dir.clone(), log_file.clone(),
            ) {
                Ok(h) => hooks.push(Box::new(h)),
                Err(e) => tracing::warn!(mirror = %mc.name, error = %e, "skip exec_on_success hook"),
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
                cmd, ExecOn::Failure,
                mc.name.clone(), working_dir.clone(),
                mc.upstream.clone(), log_dir.clone(), log_file.clone(),
            ) {
                Ok(h) => hooks.push(Box::new(h)),
                Err(e) => tracing::warn!(mirror = %mc.name, error = %e, "skip exec_on_failure hook"),
            }
        }

        out.push((provider, hooks));
    }

    out
}

/// Wire system-level hooks (cgroup, docker, zfs, btrfs) for one mirror.
fn add_system_hooks(
    hooks: &mut Vec<Box<dyn JobHook>>,
    mc: &config::MirrorConfig,
    cfg: &config::WorkerConfig,
    working_dir: &std::path::Path,
    log_dir: &std::path::Path,
    log_file: &std::path::Path,
) {
    // cgroup (Linux only)
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

    // docker
    if cfg.docker.enable && !mc.docker_image.is_empty() {
        let mut volumes = cfg.docker.volumes.clone();
        volumes.extend(mc.docker_volumes.iter().cloned());
        if !mc.exclude_file.is_empty() {
            volumes.push(format!("{}:{}:ro", mc.exclude_file, mc.exclude_file));
        }
        let mut options = cfg.docker.options.clone();
        options.extend(mc.docker_options.iter().cloned());
        let mem_limit = mc.memory_limit.map(|m| m.0).unwrap_or(0);
        hooks.push(Box::new(DockerHook::new(
            mc.name.clone(),
            mc.docker_image.clone(),
            volumes,
            options,
            mem_limit,
            working_dir.to_owned(),
            log_dir.to_owned(),
            log_file.to_owned(),
            mc.env.clone(), // pass env so DockerHook can emit -e flags
        )));
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
}
