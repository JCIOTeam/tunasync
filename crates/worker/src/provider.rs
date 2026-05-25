//! Mirror provider abstraction.
//!
//! Stage 4 adds: `CmdProvider`, `RsyncProvider`, `TwoStageRsyncProvider`.
//! Stage 5 will add hooks as a dependency injected via the trait.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::hooks::DockerConfig;

/// What the worker scheduler needs from a provider.
#[async_trait]
pub trait MirrorProvider: Send + Sync {
    fn name(&self) -> &str;
    fn upstream(&self) -> &str;
    fn is_master(&self) -> bool;
    fn interval(&self) -> Duration;
    fn retry(&self) -> u32;
    fn timeout(&self) -> Duration;
    async fn run(&self) -> anyhow::Result<()>;
    async fn terminate(&self) -> anyhow::Result<()>;
    /// Human-readable data size after a successful run. Empty = unknown.
    fn data_size(&self) -> String {
        String::new()
    }
    /// Bytes transferred during the most recent sync. 0 = unknown.
    /// Parsed from rsync `--stats` output or equivalent.
    fn transferred_bytes(&self) -> u64 {
        0
    }
    /// Working directory for this mirror — used for disk quota and atomic
    /// publish staging directory checks. Empty = no working directory.
    fn working_dir(&self) -> &std::path::Path {
        std::path::Path::new("")
    }
    /// Disk quota threshold in bytes (minimum free space required before sync).
    /// 0 = no quota check.
    fn disk_quota_bytes(&self) -> u64 {
        0
    }
    /// Whether this provider uses atomic-publish staging (staging dir → rename).
    /// When true, the runtime should expect sync to write to `<working_dir>/.staging`
    /// and rename to the final dir on success.
    fn atomic_publish(&self) -> bool {
        false
    }
    /// Pre-sync upstream connectivity check. Returns Err if upstream is unreachable
    /// (rsync --list-only timeout, HTTP HEAD failure, etc).
    /// Default no-op = no probe. Providers that opt in (`check_upstream = true`)
    /// override this; failure causes the sync to be skipped, not retried.
    async fn probe_upstream(&self) -> anyhow::Result<()> {
        Ok(())
    }
    /// Wire up Docker wrapping — called in `build_providers()` when Docker is
    /// active for this mirror. The provider uses the config to wrap its argv
    /// with `docker run …` inside `run()` and set the container name for
    /// `terminate()`.
    fn set_docker_config(&mut self, config: DockerConfig);
    /// Wire up log path coordination with `LogLimitHook`. The shared
    /// `Arc<Mutex<PathBuf>>` is set by `LogLimitHook::preExec` and read
    /// by the provider in `run()` so stdout/stderr go to the rotated log.
    fn set_log_path_shared(&mut self, path: Arc<Mutex<PathBuf>>);
    /// Wire up a `LogPublisher` so the runner can push each stdout/stderr
    /// line into the per-mirror replay buffer + live broadcast channel that
    /// back the `GET /jobs/<mirror>/log/stream` SSE endpoint.
    ///
    /// Default no-op — only providers that drive a real `runner::spawn` need
    /// to forward this through.
    fn set_log_publisher(&mut self, _pub: crate::log_stream::LogPublisher) {}
    /// Wire up a `CgroupHook` so the provider can place the spawned child PID
    /// into the cgroup between `spawn()` and `wait()`.
    /// Only called on Linux when `[cgroup] enable = true` and Docker is off.
    /// Default no-op keeps non-Linux builds and cgroup-off configs compiling.
    #[cfg(target_os = "linux")]
    fn set_cgroup_hook(&mut self, _hook: std::sync::Arc<crate::hooks::CgroupHook>) {}
}
