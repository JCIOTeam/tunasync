//! Job lifecycle hooks.

pub mod exec_post_hook;
pub mod loglimit_hook;
pub mod docker_hook;
pub mod zfs_hook;
pub mod btrfs_hook;
#[cfg(target_os = "linux")]
pub mod cgroup_hook;

pub use exec_post_hook::{ExecOn, ExecPostHook};
pub use loglimit_hook::LogLimitHook;
pub use docker_hook::DockerHook;
pub use zfs_hook::ZfsHook;
pub use btrfs_hook::BtrfsSnapshotHook;
#[cfg(target_os = "linux")]
pub use cgroup_hook::CgroupHook;

use async_trait::async_trait;

/// Lifecycle phase for hook invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookPhase {
    PreJob,
    PreExec,
    PostExec,
    PostSuccess,
    PostFail,
}

/// A hook that observes / modifies job lifecycle phases.
#[async_trait]
pub trait JobHook: Send + Sync {
    fn name(&self) -> &str;
    async fn on_phase(&self, _phase: HookPhase) -> anyhow::Result<()> {
        Ok(())
    }
}
