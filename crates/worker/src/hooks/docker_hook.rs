//! Docker hook — wraps a sync job inside a Docker container.
//!
//! Mirrors Go's `dockerHook` / `docker.go`.
//!
//! `DockerConfig` holds all the data needed to build `docker run …` argv.
//! It is shared between `DockerHook` (which runs lifecycle hooks) and
//! providers (which call `wrap_argv()` inside `run()` to wrap their command).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::process::Command;

use crate::hooks::{HookPhase, JobHook};

// ---------------------------------------------------------------------------
// DockerConfig — shared between DockerHook and providers
// ---------------------------------------------------------------------------

/// Configuration for Docker wrapping — contains all the data needed to build
/// `docker run …` argv. Created in `build_providers()` and shared with both
/// the provider (for argv wrapping) and the DockerHook (for lifecycle hooks).
#[derive(Clone)]
pub struct DockerConfig {
    pub mirror_name: String,
    pub image: String,
    pub volumes: Vec<String>,
    pub options: Vec<String>,
    pub memory_limit_bytes: i64,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Shared log path — read dynamically so `-e TUNASYNC_LOG_FILE`
    /// reflects the rotated timestamped file from LogLimitHook.
    pub log_file: Arc<Mutex<PathBuf>>,
    /// Environment variables to pass to the container via `-e` flags.
    /// The TUNASYNC_LOG_FILE entry is rebuilt dynamically from log_file
    /// each time wrap_argv() is called.
    pub env: HashMap<String, String>,
}

impl DockerConfig {
    /// Container name — `tunasync-job-{mirror_name}`.
    pub fn container_name(&self) -> String {
        format!("tunasync-job-{}", self.mirror_name)
    }

    /// Build the full `docker run …` argv by prepending docker flags to the
    /// provider's original command.
    /// Build the full `docker run …` argv by prepending docker flags to the
    /// provider's original command. Uses `self.working_dir` for `-w`, `-v`,
    /// and `TUNASYNC_WORKING_DIR`.
    pub fn wrap_argv(&self, inner_argv: &[String]) -> Vec<String> {
        self.wrap_argv_for(inner_argv, None)
    }

    /// Like `wrap_argv` but with an optional working-directory override.
    ///
    /// When `working_dir_override` is `Some(p)`, the container's `-w`,
    /// the working-dir bind mount, and the `TUNASYNC_WORKING_DIR` env var
    /// all use `p` instead of `self.working_dir`. This is essential for
    /// atomic_publish: rsync writes to a staging directory and the swap
    /// happens host-side after the sync completes. If the container were
    /// still pointed at the publish dir, the user's mirror script would
    /// write directly there and bypass the staging mechanism entirely.
    ///
    /// The mount is added *in addition* to the publish-dir mount (the
    /// container may legitimately need to read the publish dir, e.g. for
    /// delta uploads), so we still mount `self.working_dir` as well.
    pub fn wrap_argv_for(
        &self,
        inner_argv: &[String],
        working_dir_override: Option<&std::path::Path>,
    ) -> Vec<String> {
        let active_wd: PathBuf = working_dir_override
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.working_dir.clone());

        let mut argv = vec!["docker".into(), "run".into(), "--rm".into()];

        // Attach stdout/stderr.
        argv.extend(["-a".into(), "STDOUT".into(), "-a".into(), "STDERR".into()]);
        // Container name.
        argv.extend(["--name".into(), self.container_name()]);
        // Working directory inside container — staging dir when atomic_publish
        // is active so user scripts can't bypass the swap.
        argv.extend(["-w".into(), active_wd.to_string_lossy().into()]);
        // Run as current user.
        argv.extend([
            "-u".into(),
            format!("{}:{}", unsafe_getuid(), unsafe_getgid()),
        ]);

        // Environment variables via `-e` flags.
        // Read log_file dynamically to get the rotated timestamped path.
        let log_file_path = self.log_file.lock().unwrap().clone();
        for (k, v) in &self.env {
            if k == "TUNASYNC_LOG_FILE" {
                argv.extend([
                    "-e".into(),
                    format!("{k}={}", log_file_path.to_string_lossy()),
                ]);
            } else if k == "TUNASYNC_WORKING_DIR" {
                // Override with the active (possibly staging) dir.
                argv.extend([
                    "-e".into(),
                    format!("{k}={}", active_wd.to_string_lossy()),
                ]);
            } else {
                argv.extend(["-e".into(), format!("{k}={v}")]);
            }
        }

        // Configured volume mounts.
        for vol in &self.volumes {
            argv.extend(["-v".into(), vol.clone()]);
        }
        // Runtime volume mounts: log dir, publish dir, and (if different) the
        // active working dir. We mount the publish dir even under atomic_publish
        // so the container can read previous content for delta uploads.
        let mut runtime_vols = vec![
            format!("{}:{}", self.log_dir.display(), self.log_dir.display()),
            format!(
                "{}:{}",
                self.working_dir.display(),
                self.working_dir.display()
            ),
        ];
        if working_dir_override.is_some() && active_wd != self.working_dir {
            runtime_vols.push(format!(
                "{}:{}",
                active_wd.display(),
                active_wd.display()
            ));
        }
        for vol in &runtime_vols {
            argv.extend(["-v".into(), vol.clone()]);
        }

        // Memory limit.
        if self.memory_limit_bytes != 0 {
            argv.extend(["-m".into(), self.memory_limit_bytes.to_string()]);
        }

        // Extra options (e.g. --cpus).
        argv.extend(self.options.iter().cloned());
        // Image.
        argv.push(self.image.clone());
        // Actual sync command.
        argv.extend(inner_argv.iter().cloned());

        argv
    }
}

// ---------------------------------------------------------------------------
// DockerHook — lifecycle hooks using DockerConfig
// ---------------------------------------------------------------------------

pub struct DockerHook {
    config: DockerConfig,
}

impl DockerHook {
    pub fn new(config: DockerConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &DockerConfig {
        &self.config
    }

    /// Container name — delegates to DockerConfig.
    pub fn container_name(&self) -> String {
        self.config.container_name()
    }

    /// Build wrapped argv — delegates to DockerConfig.
    pub fn wrap_argv(&self, inner_argv: &[String]) -> Vec<String> {
        self.config.wrap_argv(inner_argv)
    }

    /// Build wrapped argv with an explicit working-dir override (atomic_publish).
    pub fn wrap_argv_for(
        &self,
        inner_argv: &[String],
        working_dir_override: Option<&std::path::Path>,
    ) -> Vec<String> {
        self.config.wrap_argv_for(inner_argv, working_dir_override)
    }

    /// Send `docker stop -t 2 {container}` to gracefully stop the container.
    pub async fn terminate_container(&self) {
        let name = self.config.container_name();
        let out = Command::new("docker")
            .args(["stop", "-t", "2", &name])
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => {
                tracing::debug!(container = %name, "docker stop succeeded");
            }
            Ok(o) => {
                tracing::warn!(container = %name, status = %o.status, "docker stop failed");
            }
            Err(e) => {
                tracing::warn!(error = %e, container = %name, "docker stop failed");
            }
        }
    }

    /// Poll `docker ps` until the container is gone, or warn after 10 s.
    async fn wait_container_gone(&self) {
        let name = self.config.container_name();
        for _ in 0..10 {
            let out = Command::new("docker")
                .args([
                    "ps",
                    "-a",
                    "--filter",
                    &format!("name=^{name}$"),
                    "--format",
                    "{{.Status}}",
                ])
                .output()
                .await;
            match out {
                Ok(o) if o.stdout.is_empty() => return,
                Ok(o) => {
                    let status = String::from_utf8_lossy(&o.stdout);
                    tracing::debug!(container = %name, status = %status.trim(), "waiting for container exit");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "docker ps failed");
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::warn!(
            container = %name,
            "container not removed automatically — next sync may fail"
        );
    }

    async fn ensure_working_dir(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.config.working_dir).await?;
        Ok(())
    }
}

#[async_trait]
impl JobHook for DockerHook {
    fn name(&self) -> &str {
        "docker"
    }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match phase {
            HookPhase::PreExec => self.ensure_working_dir().await,
            HookPhase::PostExec => {
                self.wait_container_gone().await;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn unsafe_getuid() -> u32 {
    unsafe { libc::getuid() }
}
#[cfg(unix)]
fn unsafe_getgid() -> u32 {
    unsafe { libc::getgid() }
}
#[cfg(not(unix))]
fn unsafe_getuid() -> u32 {
    0
}
#[cfg(not(unix))]
fn unsafe_getgid() -> u32 {
    0
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Verify that `wrap_argv_for` correctly retargets `-w`, the working-dir
    //! bind mount, and the `TUNASYNC_WORKING_DIR` env when atomic_publish
    //! provides a staging-dir override.

    use super::*;
    use std::collections::HashMap;

    fn make_config() -> DockerConfig {
        let mut env = HashMap::new();
        env.insert(
            "TUNASYNC_WORKING_DIR".to_string(),
            "/srv/mirrors/debian".to_string(),
        );
        env.insert(
            "TUNASYNC_MIRROR_NAME".to_string(),
            "debian".to_string(),
        );
        DockerConfig {
            mirror_name: "debian".into(),
            image: "rsync:latest".into(),
            volumes: vec![],
            options: vec![],
            memory_limit_bytes: 0,
            working_dir: PathBuf::from("/srv/mirrors/debian"),
            log_dir: PathBuf::from("/var/log/tunasync"),
            log_file: Arc::new(Mutex::new(PathBuf::from("/var/log/tunasync/debian.log"))),
            env,
        }
    }

    /// Without an override, wrap_argv uses the publish dir for -w, the
    /// working-dir mount, and TUNASYNC_WORKING_DIR. This is the unchanged
    /// pre-fix behaviour (and the correct behaviour when atomic_publish=false).
    #[test]
    fn wrap_argv_default_uses_publish_dir() {
        let cfg = make_config();
        let argv = cfg.wrap_argv(&["rsync".into(), "src/".into(), "dst/".into()]);
        let joined = argv.join(" ");
        assert!(joined.contains("-w /srv/mirrors/debian"));
        assert!(joined.contains("-v /srv/mirrors/debian:/srv/mirrors/debian"));
        assert!(joined.contains("TUNASYNC_WORKING_DIR=/srv/mirrors/debian"));
    }

    /// With a staging-dir override, -w, TUNASYNC_WORKING_DIR, and an
    /// *additional* -v all point at the staging dir. The publish-dir mount
    /// is kept for delta reads.
    #[test]
    fn wrap_argv_for_with_override_targets_staging() {
        let cfg = make_config();
        let staging = PathBuf::from("/var/log/tunasync/staging/debian");
        let argv = cfg.wrap_argv_for(
            &["rsync".into(), "src/".into(), "dst/".into()],
            Some(staging.as_path()),
        );
        let joined = argv.join(" ");

        // -w must point at staging.
        assert!(
            joined.contains("-w /var/log/tunasync/staging/debian"),
            "argv missing staging -w: {joined}"
        );

        // TUNASYNC_WORKING_DIR must be overridden in the -e flags.
        assert!(
            joined.contains("TUNASYNC_WORKING_DIR=/var/log/tunasync/staging/debian"),
            "argv missing overridden TUNASYNC_WORKING_DIR: {joined}"
        );
        // The old publish-dir value must NOT appear in any TUNASYNC_WORKING_DIR -e flag.
        assert!(
            !joined.contains("TUNASYNC_WORKING_DIR=/srv/mirrors/debian"),
            "argv still has stale TUNASYNC_WORKING_DIR=publish: {joined}"
        );

        // Staging volume must be mounted in addition to the publish-dir mount.
        assert!(
            joined.contains("-v /var/log/tunasync/staging/debian:/var/log/tunasync/staging/debian"),
            "argv missing staging volume mount: {joined}"
        );
        assert!(
            joined.contains("-v /srv/mirrors/debian:/srv/mirrors/debian"),
            "argv missing publish-dir volume mount (needed for delta reads): {joined}"
        );
    }

    /// Other env vars unrelated to working dir must not be perturbed.
    #[test]
    fn wrap_argv_for_preserves_other_env() {
        let cfg = make_config();
        let argv = cfg.wrap_argv_for(
            &["rsync".into()],
            Some(std::path::Path::new("/tmp/staging")),
        );
        let joined = argv.join(" ");
        assert!(joined.contains("TUNASYNC_MIRROR_NAME=debian"));
    }
}
