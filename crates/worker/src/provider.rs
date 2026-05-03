//! Mirror provider abstraction.
//!
//! Stage 4 adds: `CmdProvider`, `RsyncProvider`, `TwoStageRsyncProvider`.
//! Stage 5 will add hooks as a dependency injected via the trait.

use std::time::Duration;

use async_trait::async_trait;

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
}
