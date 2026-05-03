//! Docker hook — wraps a sync job inside a Docker container.
//!
//! Mirrors Go's `dockerHook` / `docker.go`.
//!
//! Go's runner (`newCmdJob`) injects the docker wrapper at spawn time when
//! `provider.Docker() != nil`. In our Rust architecture the hook exposes
//! `DockerHook::wrap_argv()` which providers call inside `run()` to build the
//! `docker run …` command line before passing it to `runner::spawn`.
//!
//! Lifecycle:
//!   `PreExec`  → ensure working dir exists
//!   `PostExec` → poll until the container is gone (docker ps), timeout warn

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::process::Command;

use crate::hooks::{HookPhase, JobHook};

pub struct DockerHook {
    mirror_name: String,
    pub image: String,
    pub volumes: Vec<String>,
    pub options: Vec<String>,
    pub memory_limit_bytes: i64,
    working_dir: PathBuf,
    log_dir: PathBuf,
    log_file: PathBuf,
    /// Environment variables to pass to the container via `-e` flags.
    env: HashMap<String, String>,
}

impl DockerHook {
    pub fn new(
        mirror_name: String,
        image: String,
        volumes: Vec<String>,         // global + mirror volumes
        options: Vec<String>,         // global + mirror options
        memory_limit_bytes: i64,
        working_dir: PathBuf,
        log_dir: PathBuf,
        log_file: PathBuf,
        env: HashMap<String, String>, // environment variables passed via `-e`
    ) -> Self {
        Self { mirror_name, image, volumes, options, memory_limit_bytes, working_dir, log_dir, log_file, env }
    }

    /// Container name — `tunasync-job-{mirror_name}`.
    pub fn container_name(&self) -> String {
        format!("tunasync-job-{}", self.mirror_name)
    }

    /// Build the full `docker run …` argv by prepending docker flags to the
    /// provider's original command.
    ///
    /// Called by providers to wrap their command inside a container.
    pub fn wrap_argv(&self, inner_argv: &[String]) -> Vec<String> {
        let mut argv = vec!["docker".into(), "run".into(), "--rm".into()];

        // Attach stdout/stderr.
        argv.extend(["-a".into(), "STDOUT".into(), "-a".into(), "STDERR".into()]);
        // Container name.
        argv.extend(["--name".into(), self.container_name()]);
        // Working directory inside container.
        argv.extend(["-w".into(), self.working_dir.to_string_lossy().into()]);
        // Run as current user.
        argv.extend(["-u".into(), format!("{}:{}", unsafe_getuid(), unsafe_getgid())]);

        // Environment variables via `-e` flags (matches Go's newCmdJob).
        for (k, v) in &self.env {
            argv.extend(["-e".into(), format!("{k}={v}")]);
        }

        // Always-needed volume mounts: log dir, log file, working dir.
        let runtime_vols = [
            format!("{}:{}", self.log_dir.display(), self.log_dir.display()),
            format!("{}:{}", self.log_file.display(), self.log_file.display()),
            format!("{}:{}", self.working_dir.display(), self.working_dir.display()),
        ];
        for vol in &self.volumes {
            argv.extend(["-v".into(), vol.clone()]);
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

    // ------------------------------------------------------------------
    // Lifecycle helpers
    // ------------------------------------------------------------------

    async fn ensure_working_dir(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.working_dir).await?;
        Ok(())
    }

    /// Send `docker stop -t 2 {container}` to gracefully stop the container.
    /// Called by the provider's terminate() when Docker wrapping is active.
    pub async fn terminate_container(&self) {
        let name = self.container_name();
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
        let name = self.container_name();
        for _ in 0..10 {
            let out = Command::new("docker")
                .args([
                    "ps", "-a",
                    "--filter", &format!("name=^{name}$"),
                    "--format", "{{.Status}}",
                ])
                .output()
                .await;
            match out {
                Ok(o) if o.stdout.is_empty() => return, // container gone
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
}

#[async_trait]
impl JobHook for DockerHook {
    fn name(&self) -> &str { "docker" }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match phase {
            HookPhase::PreExec  => self.ensure_working_dir().await,
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
fn unsafe_getuid() -> u32 { unsafe { libc::getuid() } }
#[cfg(unix)]
fn unsafe_getgid() -> u32 { unsafe { libc::getgid() } }
#[cfg(not(unix))]
fn unsafe_getuid() -> u32 { 0 }
#[cfg(not(unix))]
fn unsafe_getgid() -> u32 { 0 }
