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
    pub fn wrap_argv(&self, inner_argv: &[String]) -> Vec<String> {
        let mut argv = vec!["docker".into(), "run".into(), "--rm".into()];

        // Attach stdout/stderr.
        argv.extend(["-a".into(), "STDOUT".into(), "-a".into(), "STDERR".into()]);
        // Container name.
        argv.extend(["--name".into(), self.container_name()]);
        // Working directory inside container.
        argv.extend(["-w".into(), self.working_dir.to_string_lossy().into()]);
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
            } else {
                argv.extend(["-e".into(), format!("{k}={v}")]);
            }
        }

        // Configured volume mounts.
        for vol in &self.volumes {
            argv.extend(["-v".into(), vol.clone()]);
        }
        // Runtime volume mounts: log dir and working dir.
        // Note: log_dir already contains all log files including the rotated
        // ones, so no separate log_file mount is needed (avoids Docker
        // creating an unwanted directory if the file doesn't exist yet).
        let runtime_vols = [
            format!("{}:{}", self.log_dir.display(), self.log_dir.display()),
            format!(
                "{}:{}",
                self.working_dir.display(),
                self.working_dir.display()
            ),
        ];
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
