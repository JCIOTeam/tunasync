//! Worker configuration.
//!
//! Wire-compatible with Go tunasync's `worker/config.go`. TOML field names
//! are preserved exactly so existing `worker.conf` files work without change.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Provider enum
// ---------------------------------------------------------------------------

/// Sync provider type for a mirror.
///
/// Matches Go's `providerEnum` with identical TOML text representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// Raw shell command. Default.
    #[default]
    Command,
    /// `rsync` provider.
    Rsync,
    /// Two-stage rsync (skeleton + full).
    TwoStageRsync,
}

impl FromStr for ProviderKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "command" => Ok(Self::Command),
            "rsync" => Ok(Self::Rsync),
            "two-stage-rsync" => Ok(Self::TwoStageRsync),
            other => Err(format!("invalid provider: {other:?}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Memory bytes — matches Go's units.RAMInBytes semantics
// ---------------------------------------------------------------------------

/// A memory limit expressed as bytes, parseable from human strings like `"512M"`.
///
/// Matches Go's `MemBytes` type (backed by `docker/go-units`).
/// We support the same suffixes: `K`, `M`, `G`, `T`, `P` (base-1024).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct MemBytes(pub i64);

impl<'de> Deserialize<'de> for MemBytes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        // TOML represents this as a string (e.g. `memory_limit = "512M"`).
        let s = String::deserialize(d)?;
        parse_mem_bytes(&s).map_err(Error::custom)
    }
}

fn parse_mem_bytes(s: &str) -> Result<MemBytes, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(MemBytes(0));
    }
    let (num_part, suffix) = s
        .find(|c: char| c.is_ascii_alphabetic())
        .map(|i| s.split_at(i))
        .unwrap_or((s, ""));
    let base: f64 = num_part
        .parse()
        .map_err(|_| format!("invalid number in memory limit: {s:?}"))?;
    let mult = match suffix.to_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" => 1_024,
        "M" | "MB" => 1_024 * 1_024,
        "G" | "GB" => 1_024 * 1_024 * 1_024,
        "T" | "TB" => 1_024_i64.pow(4),
        "P" | "PB" => 1_024_i64.pow(5),
        other => return Err(format!("unknown memory suffix: {other:?}")),
    };
    Ok(MemBytes((base * mult as f64) as i64))
}

// ---------------------------------------------------------------------------
// Top-level Config
// ---------------------------------------------------------------------------

/// Top-level worker configuration.
///
/// Corresponds to Go's `worker.Config`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerConfig {
    /// Worker-wide defaults.
    #[serde(default)]
    pub global: GlobalConfig,

    /// Manager API settings.
    #[serde(default)]
    pub manager: ManagerApiConfig,

    /// This worker's HTTP server.
    #[serde(default)]
    pub server: ServerConfig,

    /// Linux cgroup resource limits.
    #[serde(default)]
    pub cgroup: CgroupConfig,

    /// ZFS snapshot hooks.
    #[serde(default)]
    pub zfs: ZfsConfig,

    /// Btrfs snapshot hooks.
    #[serde(default, rename = "btrfs_snapshot")]
    pub btrfs_snapshot: BtrfsSnapshotConfig,

    /// Docker hooks.
    #[serde(default)]
    pub docker: DockerConfig,

    /// Go-compatible include section.
    /// Maps Go's `[include]` with `include_mirrors = "/path/*.conf"`
    /// into the same `global.include` list.
    #[serde(default)]
    pub include: IncludeConfig,

    /// `[[mirrors]]` table — inline mirror definitions.
    #[serde(default, rename = "mirrors")]
    pub mirrors_conf: Vec<MirrorConfig>,

    /// Resolved mirrors (include files merged in). Populated at runtime.
    #[serde(skip)]
    pub mirrors: Vec<MirrorConfig>,
}

// ---------------------------------------------------------------------------
// Include section (Go-compatible)
// ---------------------------------------------------------------------------

/// Go-compatible `[include]` section.
///
/// Go's config uses a separate `[include]` section with a single glob string:
/// ```toml
/// [include]
/// include_mirrors = "/etc/tunasync/mirrors.d/*.conf"
/// ```
///
/// This struct captures that format. After loading, the glob is merged into
/// `global.include` (our array-based format), so both styles work.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IncludeConfig {
    #[serde(default)]
    pub include_mirrors: String,
}

// ---------------------------------------------------------------------------
// Global section
// ---------------------------------------------------------------------------

/// Worker-wide defaults — inherited by all mirrors unless overridden.
///
/// Corresponds to Go's `globalConfig`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalConfig {
    /// Worker identifier (used as `worker_id` when registering with manager).
    #[serde(default)]
    pub name: String,

    /// Directory for per-mirror log files.
    #[serde(default)]
    pub log_dir: String,

    /// Base directory for mirror storage.
    #[serde(default)]
    pub mirror_dir: String,

    /// Max concurrent syncing jobs. 0 = unlimited.
    #[serde(default)]
    pub concurrent: usize,

    /// Default interval between syncs, in minutes.
    #[serde(default)]
    pub interval: u64,

    /// Default max retry count.
    #[serde(default)]
    pub retry: u32,

    /// Default per-sync timeout, in seconds. 0 = no timeout.
    #[serde(default)]
    pub timeout: u64,

    /// Glob patterns for additional mirror config files to include.
    /// Mirrors Go's `include` field in `[global]`.
    #[serde(default)]
    pub include: Vec<String>,

    /// Extra `rsync` options appended to every rsync provider job.
    #[serde(default)]
    pub rsync_options: Vec<String>,

    /// Commands to run on successful sync.
    #[serde(default)]
    pub exec_on_success: Vec<String>,

    /// Commands to run on failed sync.
    #[serde(default)]
    pub exec_on_failure: Vec<String>,

    /// Extra exit codes to treat as success for all mirrors.
    #[serde(default)]
    pub dangerous_global_success_exit_codes: Vec<i32>,

    /// Extra rsync exit codes to treat as success for all mirrors.
    #[serde(default)]
    pub dangerous_global_rsync_success_exit_codes: Vec<i32>,
}

impl GlobalConfig {
    pub fn interval_duration(&self) -> Duration {
        Duration::from_secs(self.interval * 60)
    }
    pub fn timeout_duration(&self) -> Option<Duration> {
        if self.timeout == 0 {
            None
        } else {
            Some(Duration::from_secs(self.timeout))
        }
    }
}

// ---------------------------------------------------------------------------
// Manager API section
// ---------------------------------------------------------------------------

/// How the worker reaches the manager.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ManagerApiConfig {
    /// Single manager API base URL.
    #[serde(default)]
    pub api_base: String,

    /// Multiple manager URLs (round-robin fallback). Overrides `api_base`.
    #[serde(default)]
    pub api_base_list: Vec<String>,

    /// CA cert to pin when connecting to manager.
    #[serde(default)]
    pub ca_cert: String,
}

impl ManagerApiConfig {
    /// Returns the effective list of manager API base URLs.
    pub fn api_base_list(&self) -> Vec<&str> {
        if !self.api_base_list.is_empty() {
            self.api_base_list.iter().map(|s| s.as_str()).collect()
        } else {
            vec![self.api_base.as_str()]
        }
    }
}

// ---------------------------------------------------------------------------
// Server section (this worker's own HTTP listener)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerConfig {
    #[serde(default)]
    pub hostname: String,
    #[serde(default, rename = "listen_addr")]
    pub addr: String,
    #[serde(default, rename = "listen_port")]
    pub port: u16,
    #[serde(default)]
    pub ssl_cert: String,
    #[serde(default)]
    pub ssl_key: String,
}

impl ServerConfig {
    pub fn bind_addr(&self) -> std::net::SocketAddr {
        let ip: std::net::IpAddr = if self.addr.is_empty() {
            "0.0.0.0".parse().unwrap()
        } else {
            self.addr.parse().unwrap_or("0.0.0.0".parse().unwrap())
        };
        std::net::SocketAddr::new(ip, if self.port == 0 { 6000 } else { self.port })
    }

    pub fn tls_enabled(&self) -> bool {
        !self.ssl_cert.is_empty() && !self.ssl_key.is_empty()
    }

    /// Public URL the manager uses to reach this worker.
    pub fn public_url(&self, cfg: &WorkerConfig) -> String {
        let proto = if self.tls_enabled() { "https" } else { "http" };
        let host = if !self.hostname.is_empty() {
            self.hostname.clone()
        } else {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "localhost".into())
        };
        let port = if self.port == 0 { 6000 } else { self.port };
        let _ = cfg; // reserved for future use
        format!("{proto}://{host}:{port}")
    }
}

// ---------------------------------------------------------------------------
// System integration hook configs
// ---------------------------------------------------------------------------

/// Linux cgroup resource limits for sync jobs.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CgroupConfig {
    #[serde(default)]
    pub enable: bool,
    /// cgroup base path (v1: `/sys/fs/cgroup`, v2: varies).
    #[serde(default)]
    pub base_path: String,
    /// cgroup group/slice name.
    #[serde(default)]
    pub group: String,
    /// v1 subsystem (e.g. `"memory"`). Unused for v2.
    #[serde(default)]
    pub subsystem: String,
}

/// ZFS snapshot hooks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ZfsConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub zpool: String,
}

/// Btrfs snapshot hooks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BtrfsSnapshotConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub snapshot_path: String,
}

/// Docker container hooks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DockerConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(default)]
    pub options: Vec<String>,
}

// ---------------------------------------------------------------------------
// Mirror config
// ---------------------------------------------------------------------------

/// Per-mirror configuration block.
///
/// Corresponds to Go's `mirrorConfig`. Inherits from [`GlobalConfig`] where
/// fields are zero/empty.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MirrorConfig {
    pub name: String,

    #[serde(default)]
    pub provider: ProviderKind,

    #[serde(default)]
    pub upstream: String,

    /// Sync interval in minutes. 0 = inherit from global.
    #[serde(default)]
    pub interval: u64,

    /// Max retries. 0 = inherit from global.
    #[serde(default)]
    pub retry: u32,

    /// Timeout in seconds. 0 = inherit from global.
    #[serde(default)]
    pub timeout: u64,

    #[serde(default)]
    pub mirror_dir: String,

    #[serde(default)]
    pub mirror_subdir: String,

    #[serde(default)]
    pub log_dir: String,

    #[serde(default)]
    pub env: HashMap<String, String>,

    /// `"master"` | `"slave"` | `""` (default = master).
    #[serde(default)]
    pub role: String,

    // exec hooks
    #[serde(default)]
    pub exec_on_success: Vec<String>,
    #[serde(default)]
    pub exec_on_failure: Vec<String>,
    #[serde(default)]
    pub exec_on_success_extra: Vec<String>,
    #[serde(default)]
    pub exec_on_failure_extra: Vec<String>,

    // exit codes
    #[serde(default)]
    pub success_exit_codes: Vec<i32>,
    #[serde(default)]
    pub rsync_success_exit_codes: Vec<i32>,

    // rsync-specific
    #[serde(default)]
    pub command: String,
    #[serde(default)]
    pub fail_on_match: String,
    #[serde(default)]
    pub size_pattern: String,
    #[serde(default)]
    pub use_ipv6: bool,
    #[serde(default)]
    pub use_ipv4: bool,
    #[serde(default)]
    pub exclude_file: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub rsync_no_timeout: bool,
    #[serde(default)]
    pub rsync_timeout: u32,
    #[serde(default)]
    pub rsync_options: Vec<String>,
    #[serde(default)]
    pub rsync_override: Vec<String>,
    #[serde(default)]
    pub rsync_override_only: bool,
    #[serde(default)]
    pub stage1_profile: String,

    #[serde(default)]
    pub memory_limit: Option<MemBytes>,

    // docker-specific
    #[serde(default)]
    pub docker_image: String,
    #[serde(default)]
    pub docker_volumes: Vec<String>,
    #[serde(default)]
    pub docker_options: Vec<String>,
}

impl MirrorConfig {
    /// Effective interval, falling back to global default.
    pub fn effective_interval(&self, global: &GlobalConfig) -> Duration {
        let mins = if self.interval > 0 {
            self.interval
        } else {
            global.interval.max(1)
        };
        Duration::from_secs(mins * 60)
    }

    /// Effective retry count, falling back to global default.
    pub fn effective_retry(&self, global: &GlobalConfig) -> u32 {
        if self.retry > 0 {
            self.retry
        } else {
            global.retry.max(1)
        }
    }

    /// Effective timeout, falling back to global default (`None` = no timeout).
    pub fn effective_timeout(&self, global: &GlobalConfig) -> Option<Duration> {
        let secs = if self.timeout > 0 {
            self.timeout
        } else {
            global.timeout
        };
        if secs == 0 {
            None
        } else {
            Some(Duration::from_secs(secs))
        }
    }

    /// Resolved mirror storage directory.
    /// Matches Go: `Join(MirrorDir, MirrorSubDir, Name)`.
    pub fn effective_mirror_dir(&self, global: &GlobalConfig) -> PathBuf {
        if !self.mirror_dir.is_empty() {
            let base = PathBuf::from(&self.mirror_dir);
            if !self.mirror_subdir.is_empty() {
                base.join(&self.mirror_subdir)
            } else {
                base
            }
        } else if !self.mirror_subdir.is_empty() {
            PathBuf::from(&global.mirror_dir)
                .join(&self.mirror_subdir)
                .join(&self.name)
        } else {
            PathBuf::from(&global.mirror_dir).join(&self.name)
        }
    }

    /// Whether this worker acts as master for the mirror.
    pub fn is_master(&self) -> bool {
        self.role.is_empty() || self.role == "master"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mem_bytes_units() {
        assert_eq!(parse_mem_bytes("512M").unwrap().0, 512 * 1024 * 1024);
        assert_eq!(parse_mem_bytes("2G").unwrap().0, 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_mem_bytes("1K").unwrap().0, 1024);
        assert_eq!(parse_mem_bytes("0").unwrap().0, 0);
    }

    #[test]
    fn provider_kind_roundtrip() {
        let toml = r#"
[[mirrors]]
name = "ubuntu"
provider = "rsync"
upstream = "rsync://archive.ubuntu.com/ubuntu/"
interval = 720
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        assert_eq!(cfg.mirrors_conf[0].provider, ProviderKind::Rsync);
        assert_eq!(cfg.mirrors_conf[0].interval, 720);
    }

    #[test]
    fn two_stage_rsync_provider_name() {
        let toml = r#"
[[mirrors]]
name = "archlinux"
provider = "two-stage-rsync"
upstream = "rsync://rsync.archlinux.org/archlinux/"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        assert_eq!(cfg.mirrors_conf[0].provider, ProviderKind::TwoStageRsync);
    }
}
