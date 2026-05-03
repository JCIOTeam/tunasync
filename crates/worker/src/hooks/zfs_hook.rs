//! ZFS hook — verifies the mirror's working directory is a ZFS dataset.
//!
//! Mirrors Go's `zfsHook` / `zfs_hook.go`.
//!
//! `PreJob` runs `mountpoint -q {working_dir}` and fails if the directory
//! is not a mount point (i.e. no ZFS dataset is mounted there).
//! On failure it logs helpful hints for creating the dataset.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use tokio::process::Command;

use crate::hooks::{HookPhase, JobHook};

pub struct ZfsHook {
    mirror_name: String,
    zpool: String,
    working_dir: PathBuf,
}

impl ZfsHook {
    pub fn new(mirror_name: String, zpool: String, working_dir: PathBuf) -> Self {
        Self {
            mirror_name,
            zpool,
            working_dir,
        }
    }

    fn dataset_name(&self) -> String {
        format!("{}/{}", self.zpool, self.mirror_name).to_lowercase()
    }

    fn print_help(&self) {
        let ds = self.dataset_name();
        let wd = self.working_dir.display();
        tracing::info!("You may create the ZFS dataset with:");
        tracing::info!("    zfs create '{ds}'");
        tracing::info!("    zfs set mountpoint='{wd}' '{ds}'");
    }

    async fn check_mountpoint(&self) -> Result<()> {
        if !self.working_dir.exists() {
            self.print_help();
            anyhow::bail!("working dir {} does not exist", self.working_dir.display());
        }

        let status = Command::new("mountpoint")
            .arg("-q")
            .arg(&self.working_dir)
            .status()
            .await;

        match status {
            Ok(s) if s.success() => Ok(()),
            Ok(_) => {
                self.print_help();
                anyhow::bail!("{} is not a mount point", self.working_dir.display())
            }
            Err(e) => {
                tracing::warn!(error = %e, "mountpoint command not available — skipping ZFS check");
                Ok(())
            }
        }
    }
}

#[async_trait]
impl JobHook for ZfsHook {
    fn name(&self) -> &str {
        "zfs"
    }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        if phase == HookPhase::PreJob {
            self.check_mountpoint().await
        } else {
            Ok(())
        }
    }
}
