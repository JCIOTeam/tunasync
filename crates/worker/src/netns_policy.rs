use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use anyhow::{Context, Result};
use tunasync_netns::{LaunchPlan, Policy, PolicyLaunch, DYNAMIC_LOG_ENV};

pub fn generate_policy(cfg: &crate::config::WorkerConfig) -> Result<Policy> {
    let errors = crate::validate_worker_config(cfg);
    if !errors.is_empty() {
        anyhow::bail!(errors.join("; "));
    }
    let mut launch = Vec::new();
    for mirror in cfg
        .mirrors
        .iter()
        .filter(|mirror| !mirror.network_namespace.is_empty())
    {
        let (provider, _) = crate::build_one_provider(mirror, cfg)?;
        let log_root = effective_log_root(mirror, cfg);
        for spec in provider.launch_plan_specs() {
            let env: BTreeMap<String, String> = spec.env.into_iter().collect();
            let plan = LaunchPlan {
                argv: spec.argv,
                cwd: spec.cwd.to_string_lossy().into_owned(),
                env,
            };
            launch.push(PolicyLaunch {
                mirror: mirror.name.clone(),
                operation: spec.operation,
                namespace: mirror.network_namespace.clone(),
                cwd: plan.cwd.clone(),
                plan_sha256: plan.normalized_hash().map_err(anyhow::Error::msg)?,
                secret_env: plan.secret_env_keys(),
                allow_concurrency: false,
                log_root: plan
                    .env
                    .contains_key(DYNAMIC_LOG_ENV)
                    .then(|| log_root.to_string_lossy().into_owned()),
            });
        }
    }
    launch.sort_by(|a, b| (&a.mirror, &a.operation).cmp(&(&b.mirror, &b.operation)));
    launch.dedup_by(|right, left| left == right);
    let policy = Policy {
        generation: cfg.netns_broker.generation.clone(),
        launch,
    };
    policy.validate().map_err(anyhow::Error::msg)?;
    Ok(policy)
}

pub(crate) fn policy_content_changed(
    old: &crate::config::WorkerConfig,
    new: &crate::config::WorkerConfig,
) -> Result<bool> {
    let mut old_policy = generate_policy(old)?;
    let mut new_policy = generate_policy(new)?;
    old_policy.generation.clear();
    new_policy.generation.clear();
    Ok(old_policy != new_policy)
}

pub fn emit_policy(config_path: &Path, output_path: &Path) -> Result<()> {
    let mut cfg: crate::config::WorkerConfig = tunasync_common::config::load_toml(config_path)?;
    if cfg.global.retry == 0 {
        cfg.global.retry = 3;
    }
    let include_errors = crate::load_include_mirrors(&mut cfg);
    if !include_errors.is_empty() {
        anyhow::bail!(include_errors.join("; "));
    }
    cfg.mirrors = crate::config::flatten_mirrors(&cfg.mirrors_conf);
    let policy = generate_policy(&cfg)?;
    let mut bytes = serde_json::to_vec_pretty(&policy)?;
    bytes.push(b'\n');
    write_policy_atomic(output_path, &bytes)
}

fn write_policy_atomic(output_path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = output_path
        .parent()
        .context("netns policy output must have a parent directory")?;
    let output_name = output_path
        .file_name()
        .context("netns policy output must have a file name")?;
    reject_symlink_components(parent)?;
    if let Ok(metadata) = std::fs::symlink_metadata(output_path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            anyhow::bail!(
                "refusing unsafe netns policy output target {}",
                output_path.display()
            );
        }
    }
    let mut attempt = 0_u32;
    let (temp_path, mut temp) = loop {
        let candidate = parent.join(format!(
            ".{}.{}.{}.tmp",
            output_name.to_string_lossy(),
            std::process::id(),
            attempt
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && attempt < 100 => {
                attempt += 1;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create netns policy temp in {}", parent.display()));
            }
        }
    };
    let result = (|| -> Result<()> {
        temp.write_all(bytes)?;
        temp.sync_all()?;
        drop(temp);
        std::fs::rename(&temp_path, output_path).with_context(|| {
            format!("atomically replace netns policy {}", output_path.display())
        })?;
        std::fs::set_permissions(output_path, std::fs::Permissions::from_mode(0o600))?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = if path.is_absolute() {
        std::path::PathBuf::from("/")
    } else {
        std::path::PathBuf::new()
    };
    for component in path.components() {
        match component {
            std::path::Component::RootDir => continue,
            std::path::Component::Normal(component) => current.push(component),
            _ => anyhow::bail!("netns policy output parent must be normalized"),
        }
        if std::fs::symlink_metadata(&current)?
            .file_type()
            .is_symlink()
        {
            anyhow::bail!(
                "refusing netns policy output through symlink component {}",
                current.display()
            );
        }
    }
    Ok(())
}

fn effective_log_root(
    mirror: &crate::config::MirrorConfig,
    cfg: &crate::config::WorkerConfig,
) -> std::path::PathBuf {
    let raw = if mirror.log_dir.is_empty() {
        &cfg.global.log_dir
    } else {
        &mirror.log_dir
    };
    std::path::PathBuf::from(crate::expand_log_dir_template(raw, mirror))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tunasync_netns::{LaunchRequest, DYNAMIC_LOG_ENV};

    fn assert_runtime_specs_authorized(cfg: &crate::config::WorkerConfig, policy: &Policy) {
        for mirror in &cfg.mirrors {
            let (provider, _) = crate::build_one_provider(mirror, cfg).unwrap();
            for spec in provider.launch_plan_specs() {
                let request = LaunchRequest {
                    generation: cfg.netns_broker.generation.clone(),
                    mirror: mirror.name.clone(),
                    operation: spec.operation,
                    namespace: mirror.network_namespace.clone(),
                    plan: LaunchPlan {
                        argv: spec.argv,
                        cwd: spec.cwd.to_string_lossy().into_owned(),
                        env: spec.env.into_iter().collect(),
                    },
                };
                policy.authorize(&request).unwrap_or_else(|error| {
                    panic!(
                        "runtime plan {}/{} was not authorized: {error}",
                        request.mirror, request.operation
                    )
                });
            }
        }
    }

    #[test]
    fn policy_is_deterministic_and_does_not_contain_secret_values() {
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.mirror_dir = "/srv/mirrors".into();
        cfg.global.log_dir = "/var/log/tunasync".into();
        cfg.netns_broker.generation = "test-1".into();
        cfg.mirrors = vec![crate::config::MirrorConfig {
            name: "debian".into(),
            upstream: "rsync://example.invalid/debian/".into(),
            network_namespace: "warp0".into(),
            password: "do-not-emit".into(),
            check_upstream: true,
            ..Default::default()
        }];
        let first = generate_policy(&cfg).unwrap();
        let second = generate_policy(&cfg).unwrap();
        assert_eq!(first, second);
        let json = serde_json::to_string(&first).unwrap();
        assert!(!json.contains("do-not-emit"));
        assert!(json.contains("probe-rsync-"));
    }

    #[test]
    fn policy_contains_every_provider_operation() {
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.mirror_dir = "/srv/mirrors".into();
        cfg.global.log_dir = "/var/log/tunasync".into();
        cfg.mirrors = vec![
            crate::config::MirrorConfig {
                name: "two-stage".into(),
                provider: crate::config::ProviderKind::TwoStageRsync,
                upstream: "rsync://example.invalid/module/".into(),
                upstream_fallback: vec!["https://example.invalid/health".into()],
                check_upstream: true,
                stage1_profile: "debian".into(),
                network_namespace: "warp0".into(),
                ..Default::default()
            },
            crate::config::MirrorConfig {
                name: "command".into(),
                provider: crate::config::ProviderKind::Command,
                upstream: "https://example.invalid/data".into(),
                command: "/bin/true".into(),
                check_upstream: true,
                network_namespace: "warp0".into(),
                ..Default::default()
            },
        ];
        let policy = generate_policy(&cfg).unwrap();
        let operations: std::collections::BTreeSet<_> = policy
            .launch
            .iter()
            .map(|entry| (entry.mirror.as_str(), entry.operation.as_str()))
            .collect();
        assert!(operations.contains(&("two-stage", "stage1")));
        assert!(operations.contains(&("two-stage", "stage2")));
        assert!(operations
            .iter()
            .any(|(mirror, operation)| *mirror == "two-stage"
                && operation.starts_with("probe-rsync-")));
        assert!(operations
            .iter()
            .any(|(mirror, operation)| *mirror == "two-stage"
                && operation.starts_with("probe-http-")));
        assert!(operations.contains(&("command", "sync")));
        assert!(operations.iter().any(
            |(mirror, operation)| *mirror == "command" && operation.starts_with("probe-http-")
        ));
    }

    #[test]
    fn policy_authorizes_exact_runtime_specs_across_provider_modes() {
        let mut cfg = crate::config::WorkerConfig::default();
        cfg.global.mirror_dir = "/srv/mirrors".into();
        cfg.global.log_dir = "/var/log/tunasync/{{.Name}}".into();
        cfg.global.staging_dir = "/srv/staging".into();
        cfg.netns_broker.generation = "parity-1".into();
        cfg.mirrors = vec![
            crate::config::MirrorConfig {
                name: "default-rsync".into(),
                upstream: "rsync://example.invalid/module/".into(),
                upstream_fallback: vec!["https://example.invalid/health".into()],
                password: "initial-secret".into(),
                check_upstream: true,
                network_namespace: "warp0".into(),
                ..Default::default()
            },
            crate::config::MirrorConfig {
                name: "two-stage".into(),
                provider: crate::config::ProviderKind::TwoStageRsync,
                upstream: "rsync://example.invalid/two-stage/".into(),
                stage1_profile: "debian".into(),
                network_namespace: "warp0".into(),
                ..Default::default()
            },
            crate::config::MirrorConfig {
                name: "custom-rsync".into(),
                upstream: "rsync://example.invalid/custom/".into(),
                command: "/opt/tunasync/bin/rsync".into(),
                network_namespace: "warp0".into(),
                ..Default::default()
            },
            crate::config::MirrorConfig {
                name: "command".into(),
                provider: crate::config::ProviderKind::Command,
                command: "/usr/bin/true".into(),
                upstream: "https://example.invalid/data".into(),
                env: std::collections::HashMap::from([(
                    "SERVICE_TOKEN".into(),
                    "initial-token".into(),
                )]),
                network_namespace: "warp0".into(),
                ..Default::default()
            },
            crate::config::MirrorConfig {
                name: "atomic".into(),
                provider: crate::config::ProviderKind::Command,
                command: "/usr/bin/true".into(),
                upstream: "https://example.invalid/atomic".into(),
                atomic_publish: true,
                network_namespace: "warp0".into(),
                ..Default::default()
            },
        ];

        let policy = generate_policy(&cfg).unwrap();
        assert_runtime_specs_authorized(&cfg, &policy);

        let default_sync = policy
            .launch
            .iter()
            .find(|entry| entry.mirror == "default-rsync" && entry.operation == "sync")
            .unwrap();
        assert_eq!(default_sync.secret_env, vec!["RSYNC_PASSWORD"]);
        assert!(policy.launch.iter().any(|entry| {
            entry.mirror == "default-rsync" && entry.operation.starts_with("probe-rsync-")
        }));
        assert!(policy.launch.iter().any(|entry| {
            entry.mirror == "default-rsync" && entry.operation.starts_with("probe-http-")
        }));

        let (provider, _) = crate::build_one_provider(&cfg.mirrors[0], &cfg).unwrap();
        let spec = provider
            .launch_plan_specs()
            .into_iter()
            .find(|spec| spec.operation == "sync")
            .unwrap();
        assert_eq!(spec.argv[0], "/usr/bin/rsync");
        let mut rotated_secret_plan = LaunchPlan {
            argv: spec.argv,
            cwd: spec.cwd.to_string_lossy().into_owned(),
            env: spec.env.into_iter().collect(),
        };
        rotated_secret_plan
            .env
            .insert("RSYNC_PASSWORD".into(), "rotated-secret".into());
        policy
            .authorize(&LaunchRequest {
                generation: cfg.netns_broker.generation.clone(),
                mirror: "default-rsync".into(),
                operation: "sync".into(),
                namespace: "warp0".into(),
                plan: rotated_secret_plan,
            })
            .unwrap();

        let (provider, _) = crate::build_one_provider(&cfg.mirrors[3], &cfg).unwrap();
        let spec = provider
            .launch_plan_specs()
            .into_iter()
            .find(|spec| spec.operation == "sync")
            .unwrap();
        let mut rotated_log_plan = LaunchPlan {
            argv: spec.argv,
            cwd: spec.cwd.to_string_lossy().into_owned(),
            env: spec.env.into_iter().collect(),
        };
        rotated_log_plan.env.insert(
            DYNAMIC_LOG_ENV.into(),
            "/var/log/tunasync/command/2026-08-16.log".into(),
        );
        rotated_log_plan
            .env
            .insert("SERVICE_TOKEN".into(), "rotated-token".into());
        policy
            .authorize(&LaunchRequest {
                generation: cfg.netns_broker.generation.clone(),
                mirror: "command".into(),
                operation: "sync".into(),
                namespace: "warp0".into(),
                plan: rotated_log_plan,
            })
            .unwrap();
    }

    #[test]
    fn policy_content_change_detection_ignores_generation_only() {
        let mut old = crate::config::WorkerConfig::default();
        old.global.mirror_dir = "/srv/mirrors".into();
        old.global.log_dir = "/var/log/tunasync".into();
        old.netns_broker.generation = "g1".into();
        old.mirrors = vec![crate::config::MirrorConfig {
            name: "mirror".into(),
            command: "/bin/true".into(),
            provider: crate::config::ProviderKind::Command,
            network_namespace: "warp0".into(),
            ..Default::default()
        }];
        let mut changed_generation = old.clone();
        changed_generation.netns_broker.generation = "g2".into();
        assert!(!policy_content_changed(&old, &changed_generation).unwrap());

        let mut changed_plan = changed_generation;
        changed_plan.mirrors[0].command = "/bin/false".into();
        assert!(policy_content_changed(&old, &changed_plan).unwrap());
    }

    #[test]
    fn emit_policy_does_not_create_mirror_directories() {
        let temp = tempfile::tempdir().unwrap();
        let mirror_dir = temp.path().join("not-created/mirrors");
        let log_dir = temp.path().join("not-created/logs");
        let config_path = temp.path().join("worker.conf");
        let output_path = temp.path().join("policy.json");
        std::fs::write(
            &config_path,
            format!(
                r#"
[global]
mirror_dir = {:?}
log_dir = {:?}

[netns_broker]
generation = "g1"

[[mirrors]]
name = "debian"
upstream = "rsync://example.invalid/debian/"
network_namespace = "warp0"
password = "never-write-this"
"#,
                mirror_dir.to_string_lossy(),
                log_dir.to_string_lossy()
            ),
        )
        .unwrap();
        emit_policy(&config_path, &output_path).unwrap();
        assert!(!mirror_dir.exists());
        assert!(!log_dir.exists());
        let output = std::fs::read_to_string(output_path).unwrap();
        assert!(!output.contains("never-write-this"));
        assert_eq!(
            std::fs::metadata(temp.path().join("policy.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn emit_policy_refuses_symlink_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("worker.conf");
        let real_path = temp.path().join("real.json");
        let output_path = temp.path().join("policy.json");
        std::fs::write(&config_path, "[netns_broker]\ngeneration = \"g1\"\n").unwrap();
        std::fs::write(&real_path, "unchanged").unwrap();
        symlink(&real_path, &output_path).unwrap();
        assert!(emit_policy(&config_path, &output_path).is_err());
        assert_eq!(std::fs::read_to_string(real_path).unwrap(), "unchanged");
    }
}
