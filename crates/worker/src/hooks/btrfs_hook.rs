//! Btrfs snapshot hook — manages btrfs subvolumes and snapshots.
//!
//! Mirrors Go's `btrfsSnapshotHook` / `btrfs_snapshot_hook.go`.
//! Linux-only (uses `btrfs` subcommands from btrfs-progs).
//!
//! Lifecycle:
//!   `PreJob`      → ensure working dir is a btrfs subvolume (create if absent)
//!   `PostSuccess` → delete old snapshot, create new writable snapshot

use std::path::PathBuf;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::process::Command;

use crate::hooks::{HookPhase, JobHook};

pub struct BtrfsSnapshotHook {
    #[allow(dead_code)] // used for logging in future
    mirror_name: String,
    working_dir: PathBuf,
    snapshot_path: PathBuf,
}

impl BtrfsSnapshotHook {
    /// `snapshot_path` is the path where the snapshot is created.
    /// If empty, defaults to `{global_snapshot_dir}/{mirror_name}`.
    pub fn new(
        mirror_name: String,
        working_dir: PathBuf,
        global_snapshot_dir: &str,
        mirror_snapshot_path: &str,
    ) -> Self {
        let snapshot_path = if !mirror_snapshot_path.is_empty() {
            PathBuf::from(mirror_snapshot_path)
        } else {
            PathBuf::from(global_snapshot_dir).join(&mirror_name)
        };
        Self {
            mirror_name,
            working_dir,
            snapshot_path,
        }
    }

    // ------------------------------------------------------------------
    // Helpers: wrap btrfs-progs CLI
    // ------------------------------------------------------------------

    /// Check if `path` is a btrfs subvolume via `btrfs subvolume show`.
    async fn is_subvolume(path: &PathBuf) -> bool {
        Command::new("btrfs")
            .args(["subvolume", "show"])
            .arg(path)
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// `btrfs subvolume create {path}`
    async fn create_subvolume(path: &PathBuf) -> Result<()> {
        let status = Command::new("btrfs")
            .args(["subvolume", "create"])
            .arg(path)
            .status()
            .await
            .context("btrfs subvolume create")?;
        if !status.success() {
            anyhow::bail!("btrfs subvolume create {} failed", path.display());
        }
        tracing::info!(path = %path.display(), "created btrfs subvolume");
        Ok(())
    }

    /// `btrfs subvolume delete {path}`
    async fn delete_subvolume(path: &PathBuf) -> Result<()> {
        let status = Command::new("btrfs")
            .args(["subvolume", "delete"])
            .arg(path)
            .status()
            .await
            .context("btrfs subvolume delete")?;
        if !status.success() {
            anyhow::bail!("btrfs subvolume delete {} failed", path.display());
        }
        tracing::info!(path = %path.display(), "deleted btrfs subvolume/snapshot");
        Ok(())
    }

    /// `btrfs subvolume snapshot {src} {dst}` (writable).
    async fn create_snapshot(src: &PathBuf, dst: &PathBuf) -> Result<()> {
        let status = Command::new("btrfs")
            .args(["subvolume", "snapshot"])
            .arg(src)
            .arg(dst)
            .status()
            .await
            .context("btrfs subvolume snapshot")?;
        if !status.success() {
            anyhow::bail!(
                "btrfs subvolume snapshot {} → {} failed",
                src.display(),
                dst.display()
            );
        }
        tracing::info!(
            src = %src.display(),
            dst = %dst.display(),
            "created btrfs snapshot"
        );
        Ok(())
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// PreJob: ensure working_dir is a btrfs subvolume.
    async fn pre_job(&self) -> Result<()> {
        if !self.working_dir.exists() {
            Self::create_subvolume(&self.working_dir).await?;
        } else if !Self::is_subvolume(&self.working_dir).await {
            anyhow::bail!(
                "{} exists but is not a btrfs subvolume",
                self.working_dir.display()
            );
        }
        Ok(())
    }

    /// PostSuccess: rotate snapshot (delete old, create new).
    async fn post_success(&self) -> Result<()> {
        // Delete old snapshot if it exists.
        if self.snapshot_path.exists() {
            if !Self::is_subvolume(&self.snapshot_path).await {
                anyhow::bail!(
                    "{} exists but is not a btrfs snapshot",
                    self.snapshot_path.display()
                );
            }
            Self::delete_subvolume(&self.snapshot_path).await?;
        }
        // Create snapshot parent dir if needed.
        if let Some(parent) = self.snapshot_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        Self::create_snapshot(&self.working_dir, &self.snapshot_path).await
    }
}

#[async_trait]
impl JobHook for BtrfsSnapshotHook {
    fn name(&self) -> &str {
        "btrfs_snapshot"
    }

    async fn on_phase(&self, phase: HookPhase) -> Result<()> {
        match phase {
            HookPhase::PreJob => self.pre_job().await,
            HookPhase::PostSuccess => self.post_success().await,
            _ => Ok(()),
        }
    }
}
