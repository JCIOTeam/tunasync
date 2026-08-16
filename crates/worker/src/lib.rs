//! tunasync worker.
//!
//! Stage 3: config, job state machine, scheduler, manager client, HTTP server.
//! Stage 4: CmdProvider, RsyncProvider, TwoStageRsyncProvider, exec_post+loglimit hooks.
//! Stage 5: cgroup, docker, zfs, btrfs hooks.

#![warn(rust_2018_idioms)]

pub mod blackout;
pub mod config;
pub mod diff_config;
pub mod hooks;
pub mod http_server;
pub mod job;
pub mod log_stream;
pub mod manager_client;
pub mod netns_policy;
pub mod priority_semaphore;
pub mod provider;
pub mod providers;
pub mod report_actor;
pub mod runner;
pub mod schedule;
pub mod scheduling;
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
/// Go's `formatLogDir` uses `html/template` with fields like `{{.Name}}`,
/// `{{.Provider}}`, `{{.Upstream}}`, `{{.Role}}`, `{{.MirrorSubDir}}` etc.
/// We support the same template variables since Go exposes all MirrorConfig
/// fields in the template context.
fn expand_log_dir_template(log_dir: &str, mc: &config::MirrorConfig) -> String {
    let mut result = log_dir.to_string();
    result = result.replace("{{.Name}}", &mc.name);
    result = result.replace(
        "{{.Provider}}",
        match mc.provider {
            config::ProviderKind::Command => "command",
            config::ProviderKind::Rsync => "rsync",
            config::ProviderKind::TwoStageRsync => "two-stage-rsync",
        },
    );
    result = result.replace("{{.Upstream}}", &mc.upstream);
    result = result.replace("{{.Role}}", &mc.role);
    result = result.replace("{{.MirrorSubDir}}", &mc.mirror_subdir);
    result
}

/// Merge Go's `[include]` section glob into `global.include` array, then
/// expand all glob patterns and merge the resulting mirror configs
/// into `cfg.mirrors_conf`.  Called at startup and on hot-reload.
pub fn load_include_mirrors(cfg: &mut config::WorkerConfig) -> Vec<String> {
    let mut errors = Vec::new();
    // Merge Go-style [include] section into global.include array.
    if !cfg.include.include_mirrors.is_empty() {
        cfg.global.include.push(cfg.include.include_mirrors.clone());
    }

    for pattern in cfg.global.include.clone() {
        let entries = match glob::glob(&pattern) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(pattern = %pattern, error = %e, "invalid include glob");
                errors.push(format!("invalid include glob {pattern:?}: {e}"));
                continue;
            }
        };
        for entry in entries {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "error reading include glob entry");
                    errors.push(format!("error reading include glob {pattern:?}: {e}"));
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
                    errors.push(format!(
                        "failed to load include file {}: {e}",
                        path.display()
                    ));
                }
            }
        }
    }
    errors
}

fn validate_path_component(value: &str, field: &str, mirror: &str) -> Option<String> {
    let mut components = std::path::Path::new(value).components();
    let valid = matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none();
    (!valid).then(|| {
        format!("mirror {mirror:?}: {field} must be a single safe path component, got {value:?}")
    })
}

fn validate_relative_path(value: &str, field: &str, mirror: &str) -> Option<String> {
    if value.is_empty() {
        return None;
    }
    let valid = std::path::Path::new(value)
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)));
    (!valid).then(|| {
        format!(
            "mirror {mirror:?}: {field} must be a relative path without '.' or '..', got {value:?}"
        )
    })
}

pub(crate) fn validate_worker_config(cfg: &config::WorkerConfig) -> Vec<String> {
    let mut errors = Vec::new();

    let report_limit = cfg.global.effective_report_max_resources();
    if cfg.mirrors.len() > report_limit {
        errors.push(format!(
            "configured mirror count {} exceeds effective global.report_max_resources limit {}; increase report_max_resources (maximum {}) or reduce mirrors",
            cfg.mirrors.len(),
            report_limit,
            config::MAX_REPORT_RESOURCES
        ));
    }

    if let Err(e) = cfg.server.validate_tls() {
        errors.push(e);
    }
    if let Err(e) = cfg.server.bind_addr() {
        errors.push(e);
    }
    if let Err(e) = tunasync_netns::validate_absolute_normalized_path(
        &cfg.netns_broker.socket,
        "netns_broker.socket",
    ) {
        errors.push(e);
    }
    if cfg.netns_broker.generation.is_empty() || cfg.netns_broker.generation.len() > 128 {
        errors.push("netns_broker.generation must contain 1..=128 characters".into());
    }

    if !cfg.global.timezone.is_empty() {
        if let Err(e) = cfg.global.timezone.parse::<chrono_tz::Tz>() {
            errors.push(format!(
                "global.timezone {:?} is not a valid IANA timezone name: {e}",
                cfg.global.timezone
            ));
        }
    }
    match cfg.global.interval_mode {
        config::IntervalMode::FixedDelay => {
            if !cfg.global.fixed_rate_anchor.is_empty() {
                errors.push("global.fixed_rate_anchor is invalid with fixed-delay mode".into());
            }
        }
        config::IntervalMode::FixedRate => {
            validate_fixed_rate(
                "global",
                cfg.global.interval,
                &cfg.global.fixed_rate_anchor,
                &mut errors,
            );
        }
    }

    let mut seen = std::collections::HashSet::new();
    let has_network_namespace = cfg
        .mirrors
        .iter()
        .any(|mc| !mc.network_namespace.is_empty());
    if has_network_namespace && cfg.cgroup.enable {
        errors.push("cgroup.enable cannot be combined with network_namespace in phase 2".into());
    }
    for mc in &cfg.mirrors {
        if !seen.insert(mc.name.as_str()) {
            errors.push(format!("duplicate mirror name {:?}", mc.name));
        }
        if let Some(e) = validate_path_component(&mc.name, "name", &mc.name) {
            errors.push(e);
        }
        if let Some(e) = validate_relative_path(&mc.mirror_subdir, "mirror_subdir", &mc.name) {
            errors.push(e);
        }
        if !mc.network_namespace.is_empty() {
            #[cfg(not(target_os = "linux"))]
            errors.push(format!(
                "mirror {:?}: network_namespace is only supported on Linux",
                mc.name
            ));
            if let Err(e) = tunasync_netns::validate_namespace_name(&mc.network_namespace) {
                errors.push(format!("mirror {:?}: {e}", mc.name));
            }
            if cfg.docker.enable && !mc.docker_image.is_empty() {
                errors.push(format!(
                    "mirror {:?}: Docker and network_namespace cannot both be active",
                    mc.name
                ));
            }
            for (field, upstream) in std::iter::once(("upstream", &mc.upstream)).chain(
                mc.upstream_fallback
                    .iter()
                    .map(|url| ("upstream_fallback", url)),
            ) {
                if upstream.is_empty() {
                    continue;
                }
                match url::Url::parse(upstream) {
                    Ok(url) if url_has_forbidden_credentials(upstream, &url) => {
                        errors.push(format!(
                            "mirror {:?}: network namespace {field} URL must not contain userinfo, password, query, or fragment: {:?}",
                            mc.name,
                            redact_url_diagnostic(upstream)
                        ));
                    }
                    Ok(_) => {}
                    Err(_) if upstream.contains(['?', '#']) => errors.push(format!(
                        "mirror {:?}: network namespace {field} URL must not contain userinfo, password, query, or fragment: {:?}",
                        mc.name,
                        redact_url_diagnostic(upstream)
                    )),
                    Err(_) => {}
                }
            }
            for key in mc.env.keys() {
                if let Err(e) = tunasync_netns::validate_env_key(key) {
                    errors.push(format!("mirror {:?}: {e}", mc.name));
                }
            }
            let cwd = mc.effective_mirror_dir(&cfg.global);
            if let Err(e) = tunasync_netns::validate_absolute_normalized_path(
                &cwd.to_string_lossy(),
                "mirror working directory",
            ) {
                errors.push(format!("mirror {:?}: {e}", mc.name));
            }
            if mc.provider == ProviderKind::Command {
                match shell_words::split(&mc.command) {
                    Ok(argv)
                        if argv
                            .first()
                            .is_some_and(|program| std::path::Path::new(program).is_absolute()) => {}
                    Ok(_) => errors.push(format!(
                        "mirror {:?}: network namespace command executable must be an absolute path",
                        mc.name
                    )),
                    Err(e) => errors.push(format!(
                        "mirror {:?}: cannot parse network namespace command: {e}",
                        mc.name
                    )),
                }
            }
            if matches!(
                mc.provider,
                ProviderKind::Rsync | ProviderKind::TwoStageRsync
            ) && !mc.command.is_empty()
                && !std::path::Path::new(&mc.command).is_absolute()
            {
                errors.push(format!(
                    "mirror {:?}: custom network namespace rsync command must be an absolute path",
                    mc.name
                ));
            }
            let log_dir = if mc.log_dir.is_empty() {
                &cfg.global.log_dir
            } else {
                &mc.log_dir
            };
            if mc.provider == ProviderKind::Command && log_dir.is_empty() {
                errors.push(format!(
                    "mirror {:?}: network namespace command provider requires an absolute log directory",
                    mc.name
                ));
            }
            if !log_dir.is_empty() {
                if let Err(e) = tunasync_netns::validate_absolute_normalized_path(
                    log_dir,
                    "mirror log directory",
                ) {
                    errors.push(format!("mirror {:?}: {e}", mc.name));
                }
            }
            if mc.check_upstream {
                for upstream in std::iter::once(&mc.upstream).chain(&mc.upstream_fallback) {
                    if !matches!(
                        url::Url::parse(upstream)
                            .ok()
                            .map(|url| url.scheme().to_owned())
                            .as_deref(),
                        Some("rsync" | "http" | "https")
                    ) {
                        errors.push(format!(
                            "mirror {:?}: network namespace command probes only support rsync/http/https URLs, got {:?}",
                            mc.name,
                            redact_url_diagnostic(upstream)
                        ));
                    }
                }
            }
        }
        if !mc.disk_quota.is_empty()
            && tunasync_common::util::parse_size_bytes(&mc.disk_quota).is_none()
        {
            errors.push(format!(
                "mirror {:?}: invalid disk_quota {:?}",
                mc.name, mc.disk_quota
            ));
        }
        for spec in &mc.blackout {
            if crate::blackout::BlackoutWindow::parse(spec).is_none() {
                errors.push(format!(
                    "mirror {:?}: unparseable blackout window {:?}",
                    mc.name, spec
                ));
            }
        }
        if !mc.timezone.is_empty() {
            if let Err(e) = mc.timezone.parse::<chrono_tz::Tz>() {
                errors.push(format!(
                    "mirror {:?}: timezone {:?} is not a valid IANA timezone name: {e}",
                    mc.name, mc.timezone
                ));
            }
        }
        if !mc.cron.is_empty() {
            if let Err(e) = crate::worker::parse_cron_lenient(&mc.cron) {
                errors.push(format!(
                    "mirror {:?}: invalid cron expression {:?}: {e}",
                    mc.name, mc.cron
                ));
            }
        }

        let mode = mc.effective_interval_mode(&cfg.global);
        let anchor = mc.effective_fixed_rate_anchor(&cfg.global);
        match mode {
            config::IntervalMode::FixedDelay => {
                if !anchor.is_empty() {
                    errors.push(format!(
                        "mirror {:?}: fixed_rate_anchor is invalid with fixed-delay mode",
                        mc.name
                    ));
                }
            }
            config::IntervalMode::FixedRate => {
                if !mc.cron.is_empty() {
                    errors.push(format!(
                        "mirror {:?}: cron conflicts with fixed-rate interval_mode",
                        mc.name
                    ));
                }
                let interval_minutes = if mc.interval > 0 {
                    mc.interval
                } else {
                    cfg.global.interval
                };
                validate_fixed_rate(
                    &format!("mirror {:?}", mc.name),
                    interval_minutes,
                    anchor,
                    &mut errors,
                );
            }
        }
    }
    errors
}

fn validate_fixed_rate(scope: &str, interval: u64, anchor: &str, errors: &mut Vec<String>) {
    if !(1..=1440).contains(&interval) {
        errors.push(format!(
            "{scope}: fixed-rate interval must be in 1..=1440 minutes"
        ));
    } else if 1440 % interval != 0 {
        errors.push(format!(
            "{scope}: fixed-rate interval {interval} must divide 1440 minutes"
        ));
    }
    if let Err(e) = crate::scheduling::parse_fixed_rate_anchor(anchor) {
        errors.push(format!(
            "{scope}: invalid fixed_rate_anchor {anchor:?}: {e}"
        ));
    }
}

/// Entry point invoked by `tunasync worker`.
pub async fn run(config_path: std::path::PathBuf) -> Result<()> {
    // Only fall back to defaults if the file does not exist. Any other
    // error (parse, permission, etc.) is fatal — a typo in worker.conf
    // must not silently start a worker with no mirrors configured.
    let mut cfg: config::WorkerConfig = if config_path.exists() {
        tunasync_common::config::load_toml(&config_path)?
    } else {
        tracing::warn!(
            path = %config_path.display(),
            "config file not found — using defaults"
        );
        config::WorkerConfig::default()
    };

    if cfg.global.retry == 0 {
        cfg.global.retry = 3;
    }

    // Merge include files before building the mirror list.
    let include_errors = load_include_mirrors(&mut cfg);
    if !include_errors.is_empty() {
        anyhow::bail!("{}", include_errors.join("; "));
    }

    // Flatten nested mirror configs (Go's recursiveMirrors).
    cfg.mirrors = config::flatten_mirrors(&cfg.mirrors_conf);

    let config_errors = validate_worker_config(&cfg);
    if !config_errors.is_empty() {
        anyhow::bail!("{}", config_errors.join("; "));
    }

    // Do not start a partially configured worker. Every mirror must build
    // successfully so a healthy process cannot silently omit broken jobs.
    for mc in &cfg.mirrors {
        build_one_provider(mc, &cfg)?;
    }

    verify_netns_broker_for_config(&cfg).await?;

    let listen_ip = cfg.server.bind_addr().map_err(anyhow::Error::msg)?.ip();
    if !listen_ip.is_loopback() && cfg.manager.api_token.is_empty() {
        tracing::warn!(
            listen_addr = %listen_ip,
            "worker HTTP API is reachable beyond loopback without api_token authentication"
        );
    }

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

pub(crate) async fn verify_netns_broker_for_config(cfg: &config::WorkerConfig) -> Result<()> {
    if cfg
        .mirrors
        .iter()
        .all(|mirror| mirror.network_namespace.is_empty())
    {
        return Ok(());
    }
    runner::verify_broker_ready(
        std::path::Path::new(&cfg.netns_broker.socket),
        &cfg.netns_broker.generation,
    )
    .await
    .context("network namespace broker is not ready for this worker configuration")
}

pub(crate) fn redact_url_diagnostic(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return value
            .split(['?', '#'])
            .next()
            .unwrap_or_default()
            .to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn url_has_forbidden_credentials(value: &str, url: &url::Url) -> bool {
    let has_raw_userinfo = value
        .split_once("://")
        .and_then(|(_, rest)| rest.split(['/', '?', '#']).next())
        .is_some_and(|authority| authority.contains('@'));
    has_raw_userinfo
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
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
    let log_dir = PathBuf::from(expand_log_dir_template(&log_dir_raw, mc));
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
    configure_provider_broker(provider.as_mut(), mc, cfg);

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
            // Share the same CgroupHook between the provider (PID placement) and
            // the hooks vec (PreExec cgroup creation, PostExec cleanup).
            let hook = std::sync::Arc::new(CgroupHook::new(
                mc.name.clone(),
                &cfg.cgroup.base_path,
                &cfg.cgroup.group,
                mem_limit,
            ));
            provider.set_cgroup_hook(std::sync::Arc::clone(&hook));
            hooks.push(Box::new(hook));
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
            &mc.snapshot_path, // per-mirror override (Go: mirrorConfig.SnapshotPath)
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

fn configure_provider_broker(
    provider: &mut dyn provider::MirrorProvider,
    mc: &config::MirrorConfig,
    cfg: &config::WorkerConfig,
) {
    if mc.network_namespace.is_empty() {
        return;
    }
    provider.set_broker_config(runner::BrokerConfig {
        socket: PathBuf::from(&cfg.netns_broker.socket),
        generation: cfg.netns_broker.generation.clone(),
        mirror: mc.name.clone(),
        namespace: mc.network_namespace.clone(),
    });
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

// ---------------------------------------------------------------------------
// Config check mode (`tunasync worker --check`)
// ---------------------------------------------------------------------------

/// Result of a `--check` validation pass.
pub struct ConfigCheckReport {
    /// Fatal problems — the worker would refuse to start, or a mirror would
    /// silently misbehave (e.g. fall back from cron to interval scheduling).
    pub errors: Vec<String>,
    /// Non-fatal observations worth an operator's attention.
    pub warnings: Vec<String>,
    /// Number of mirrors after include-merging and flattening.
    pub mirrors: usize,
}

/// Validate a worker config file WITHOUT starting anything.
///
/// Designed for `ExecStartPre=` in systemd units and pre-reload CI checks:
/// it runs the exact same parsing/flattening pipeline as `run()` plus the
/// checks that are only *warnings* at runtime (blackout expressions,
/// provider construction), and collects ALL problems instead of bailing on
/// the first — so one check run shows everything that needs fixing.
///
/// Returns `Err` only when the file itself cannot be read or parsed;
/// semantic problems are reported through [`ConfigCheckReport::errors`].
pub fn check_config(config_path: &std::path::Path) -> Result<ConfigCheckReport> {
    let mut cfg: config::WorkerConfig = tunasync_common::config::load_toml(config_path)?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    if cfg.global.retry == 0 {
        cfg.global.retry = 3;
    }
    errors.extend(load_include_mirrors(&mut cfg));
    cfg.mirrors = config::flatten_mirrors(&cfg.mirrors_conf);

    // Global-level checks.
    if cfg.manager.api_base_list().iter().all(|b| b.is_empty()) {
        warnings.push(
            "[manager] api_base is empty — the worker will not be able to \
             report status to any manager"
                .to_string(),
        );
    }

    errors.extend(validate_worker_config(&cfg));

    // Per-mirror provider checks. Scheduling validation is centralized above.
    for mc in &cfg.mirrors {
        // Try to actually construct the provider + hooks: catches bad
        // provider options, unsupported combinations, bad template syntax.
        if let Err(e) = build_one_provider(mc, &cfg) {
            errors.push(format!("mirror {:?}: {e:#}", mc.name));
        }
    }

    Ok(ConfigCheckReport {
        errors,
        warnings,
        mirrors: cfg.mirrors.len(),
    })
}

#[cfg(test)]
mod tests {
    use crate::config::{IntervalMode, MirrorConfig, ProviderKind, WorkerConfig};

    fn validated_config(mirror: MirrorConfig) -> WorkerConfig {
        let mut cfg = WorkerConfig::default();
        cfg.global.interval = 60;
        cfg.mirrors = vec![mirror];
        cfg
    }

    #[test]
    fn scheduling_validation_is_centralized() {
        let mut fixed_rate = MirrorConfig {
            name: "rate".into(),
            interval: 60,
            interval_mode: Some(IntervalMode::FixedRate),
            fixed_rate_anchor: Some("03:15".into()),
            ..Default::default()
        };
        assert!(crate::validate_worker_config(&validated_config(fixed_rate.clone())).is_empty());

        fixed_rate.interval = 7;
        let errors = crate::validate_worker_config(&validated_config(fixed_rate.clone()));
        assert!(errors.iter().any(|e| e.contains("divide 1440")));

        fixed_rate.interval = 60;
        fixed_rate.cron = "0 3 * * *".into();
        let errors = crate::validate_worker_config(&validated_config(fixed_rate));
        assert!(errors.iter().any(|e| e.contains("conflicts")));

        let fixed_delay_with_anchor = MirrorConfig {
            name: "delay".into(),
            fixed_rate_anchor: Some("03:15".into()),
            ..Default::default()
        };
        let errors = crate::validate_worker_config(&validated_config(fixed_delay_with_anchor));
        assert!(errors.iter().any(|e| e.contains("fixed-delay")));

        let invalid_cron_and_timezone = MirrorConfig {
            name: "invalid".into(),
            cron: "not cron".into(),
            timezone: "Not/AZone".into(),
            ..Default::default()
        };
        let errors = crate::validate_worker_config(&validated_config(invalid_cron_and_timezone));
        assert!(errors.iter().any(|e| e.contains("invalid cron")));
        assert!(errors.iter().any(|e| e.contains("timezone")));
    }

    #[test]
    fn flattened_mirror_count_must_fit_complete_schedule_snapshot_limit() {
        let mut cfg = WorkerConfig::default();
        cfg.global.report_max_resources = 1;
        cfg.mirrors = vec![
            MirrorConfig {
                name: "first".into(),
                ..Default::default()
            },
            MirrorConfig {
                name: "second".into(),
                ..Default::default()
            },
        ];

        let errors = crate::validate_worker_config(&cfg);
        assert!(errors.iter().any(|error| {
            error.contains("configured mirror count 2")
                && error.contains("report_max_resources limit 1")
        }));

        cfg.global.report_max_resources = 0;
        assert!(crate::validate_worker_config(&cfg)
            .iter()
            .all(|error| !error.contains("report_max_resources")));
    }

    #[test]
    fn network_namespace_validation_rejects_conflicts_and_bad_probe_schemes() {
        let mirror = MirrorConfig {
            name: "isolated".into(),
            provider: ProviderKind::Command,
            command: "/bin/true".into(),
            upstream: "ftp://example.invalid/pub".into(),
            check_upstream: true,
            network_namespace: "warp0".into(),
            docker_image: "example/image".into(),
            ..Default::default()
        };
        let mut cfg = validated_config(mirror);
        cfg.global.mirror_dir = "/srv/mirrors".into();
        cfg.global.log_dir = "/var/log/tunasync".into();
        cfg.docker.enable = true;
        cfg.cgroup.enable = true;
        let errors = crate::validate_worker_config(&cfg);
        assert!(errors.iter().any(|e| e.contains("cgroup.enable")));
        assert!(errors.iter().any(|e| e.contains("Docker")));
        assert!(errors.iter().any(|e| e.contains("rsync/http/https")));
    }

    #[test]
    fn network_namespace_validation_rejects_credential_bearing_url_components() {
        for upstream in [
            "https://@example.invalid/data",
            "https://user@example.invalid/data",
            "https://user:password@example.invalid/data",
            "https://example.invalid/data?token=secret",
            "https://example.invalid/data#secret",
        ] {
            let mirror = MirrorConfig {
                name: "isolated".into(),
                provider: ProviderKind::Command,
                command: "/bin/true".into(),
                upstream: upstream.into(),
                network_namespace: "warp0".into(),
                ..Default::default()
            };
            let mut cfg = validated_config(mirror);
            cfg.global.mirror_dir = "/srv/mirrors".into();
            cfg.global.log_dir = "/var/log/tunasync".into();
            let errors = crate::validate_worker_config(&cfg);
            assert!(
                errors.iter().any(|e| e.contains("must not contain")),
                "accepted {upstream:?}: {errors:?}"
            );
        }

        let mirror = MirrorConfig {
            name: "isolated".into(),
            provider: ProviderKind::Command,
            command: "/bin/true".into(),
            upstream: "https://example.invalid/data".into(),
            upstream_fallback: vec!["https://user:secret@example.invalid/fallback".into()],
            network_namespace: "warp0".into(),
            ..Default::default()
        };
        let mut cfg = validated_config(mirror);
        cfg.global.mirror_dir = "/srv/mirrors".into();
        cfg.global.log_dir = "/var/log/tunasync".into();
        let diagnostic = crate::validate_worker_config(&cfg).join(" ");
        assert!(diagnostic.contains("upstream_fallback"));
        assert!(!diagnostic.contains("secret"));
    }

    #[test]
    fn inherited_network_namespace_is_flattened() {
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(
            r#"
[[mirrors]]
name = "parent"
network_namespace = "warp0"

[[mirrors.mirrors]]
name = "child"
upstream = "rsync://example.invalid/module/"
"#,
        )
        .unwrap();
        let flat = crate::config::flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat[0].network_namespace, "warp0");
    }

    #[test]
    fn expand_log_dir_name() {
        let mc = MirrorConfig::default();
        let result = crate::expand_log_dir_template("/var/log/{{.Name}}", &mc);
        assert_eq!(result, "/var/log/");
        let mc = MirrorConfig {
            name: "ubuntu".into(),
            ..Default::default()
        };
        let result = crate::expand_log_dir_template("/var/log/{{.Name}}", &mc);
        assert_eq!(result, "/var/log/ubuntu");
    }

    #[test]
    fn expand_log_dir_provider() {
        let mc = MirrorConfig {
            name: "debian".into(),
            provider: ProviderKind::Rsync,
            ..Default::default()
        };
        let result = crate::expand_log_dir_template("/var/log/{{.Provider}}/{{.Name}}", &mc);
        assert_eq!(result, "/var/log/rsync/debian");
    }

    #[test]
    fn expand_log_dir_multiple_vars() {
        let mc = MirrorConfig {
            name: "archlinux".into(),
            provider: ProviderKind::TwoStageRsync,
            upstream: "rsync://rsync.archlinux.org/archlinux/".into(),
            role: "master".into(),
            ..Default::default()
        };
        let result =
            crate::expand_log_dir_template("/var/log/{{.Provider}}/{{.Name}}/{{.Role}}", &mc);
        assert_eq!(result, "/var/log/two-stage-rsync/archlinux/master");
    }

    #[test]
    fn url_diagnostics_remove_secrets_and_nonessential_components() {
        assert_eq!(
            crate::redact_url_diagnostic(
                "rsync://user:password@example.invalid/module?token=secret#fragment"
            ),
            "rsync://example.invalid/module"
        );
        assert_eq!(
            crate::redact_url_diagnostic("not-a-url?token=secret#fragment"),
            "not-a-url"
        );
    }
}
