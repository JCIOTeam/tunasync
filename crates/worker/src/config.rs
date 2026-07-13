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
///
/// **Default is `Rsync`** — matching Go's `iota` ordering where `provRsync == 0`
/// is the zero value.  A mirror config that omits `provider` is treated as
/// `rsync`, exactly as the Go implementation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderKind {
    /// `rsync` provider. Default (matches Go's `provRsync = iota = 0`).
    #[default]
    Rsync,
    /// Two-stage rsync (skeleton + full).
    TwoStageRsync,
    /// Raw shell command.
    Command,
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

    /// Per-upstream host concurrency limits.
    ///
    /// Maps upstream hostname (e.g. `"rsync.kernel.org"`) to the maximum number
    /// of mirrors that may sync from that host simultaneously.  Hosts not listed
    /// here have no extra limit beyond the global `concurrent` setting.
    ///
    /// Example in TOML:
    /// ```toml
    /// [global.per_upstream_concurrent]
    /// "rsync.kernel.org" = 2
    /// ```
    #[serde(default)]
    pub per_upstream_concurrent: std::collections::HashMap<String, usize>,

    /// IANA timezone name used as the default for cron and blackout windows.
    ///
    /// When empty, `UTC` is used — preserving the existing wire/schedule
    /// behaviour. Individual mirrors can override this via `MirrorConfig::timezone`.
    ///
    /// Valid examples:
    /// - `""` (default, treated as UTC)
    /// - `"UTC"`
    /// - `"Asia/Shanghai"`
    /// - `"America/New_York"`
    /// - `"Europe/Berlin"`
    ///
    /// Invalid names cause the worker to fail at startup so misconfiguration
    /// is caught immediately rather than producing silently-wrong schedules.
    #[serde(default)]
    pub timezone: String,

    /// Base directory for atomic-publish staging, used when a mirror has
    /// `atomic_publish = true` set.
    ///
    /// When set, each mirror's staging directory is `<staging_dir>/<name>`.
    /// When empty (default), staging falls back to `<log_dir>/staging/<name>`
    /// — which only works when `log_dir` and `mirror_dir` are on the same
    /// filesystem. Most production deployments put logs on the OS disk and
    /// mirror data on a separate large data disk, so this fallback fails
    /// the same-filesystem check; setting `staging_dir` explicitly is the
    /// right answer for those layouts.
    ///
    /// REQUIREMENT: the staging directory must be on the same filesystem
    /// as `mirror_dir` (rename(2) is only atomic within a single mount).
    /// The worker checks device IDs at sync time and refuses to start if
    /// they differ. Individual mirrors can override with
    /// `MirrorConfig::staging_dir`.
    ///
    /// Recommended layout:
    /// ```toml
    /// [global]
    /// mirror_dir  = "/srv/mirrors"
    /// staging_dir = "/srv/mirrors/.staging"   # hidden from nginx by leading dot
    /// ```
    /// Note: anything under `mirror_dir` is served by nginx by default;
    /// a leading dot in the directory name (`.staging`) blocks the default
    /// nginx auto-index, but for stricter setups consider a sibling path
    /// outside the document root entirely.
    #[serde(default)]
    pub staging_dir: String,
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

    /// Shared API token. When non-empty:
    /// - attached as `Authorization: Bearer` to every manager request, and
    /// - REQUIRED on this worker's own command endpoint (`POST /`) and SSE
    ///   log stream (commands/streams come from the manager or operators).
    ///
    /// Set the SAME value in the manager's `[server] api_token`.
    #[serde(default)]
    pub api_token: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub hostname: String,
    #[serde(default = "ServerConfig::default_addr", rename = "listen_addr")]
    pub addr: String,
    #[serde(default = "ServerConfig::default_port", rename = "listen_port")]
    pub port: u16,
    #[serde(default)]
    pub ssl_cert: String,
    #[serde(default)]
    pub ssl_key: String,
}

impl ServerConfig {
    fn default_addr() -> String {
        "127.0.0.1".into()
    }

    fn default_port() -> u16 {
        6000
    }

    pub fn bind_addr(&self) -> Result<std::net::SocketAddr, String> {
        let ip = self
            .addr
            .parse()
            .map_err(|e| format!("invalid worker listen_addr {:?}: {e}", self.addr))?;
        Ok(std::net::SocketAddr::new(ip, self.port))
    }

    pub fn tls_enabled(&self) -> bool {
        !self.ssl_cert.is_empty() && !self.ssl_key.is_empty()
    }

    pub fn validate_tls(&self) -> Result<(), String> {
        if self.ssl_cert.is_empty() == self.ssl_key.is_empty() {
            Ok(())
        } else {
            Err("worker TLS requires both ssl_cert and ssl_key".into())
        }
    }

    /// Public URL the manager uses to reach this worker.
    pub fn public_url(&self, cfg: &WorkerConfig) -> String {
        let proto = if self.tls_enabled() { "https" } else { "http" };
        let host = if !self.hostname.is_empty() {
            self.hostname.clone()
        } else if self
            .addr
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| !ip.is_unspecified())
        {
            self.addr.clone()
        } else {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "localhost".into())
        };
        let port = self.port;
        let _ = cfg; // reserved for future use
        format!("{proto}://{host}:{port}")
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            hostname: String::new(),
            addr: Self::default_addr(),
            port: Self::default_port(),
            ssl_cert: String::new(),
            ssl_key: String::new(),
        }
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

    /// Per-mirror btrfs snapshot path override.
    #[serde(default)]
    pub snapshot_path: String,

    // ── Extension fields (tunasync-rs only, opt-in, backwards compatible) ──
    /// Disk quota for this mirror (e.g. "2TB", "500GB").
    /// If set, the worker checks available space before syncing and skips
    /// the sync if available space is below the threshold.
    #[serde(default)]
    pub disk_quota: String,

    /// Cron expression for scheduling (e.g. "0 3 * * *").
    /// When set, overrides `interval` and schedules syncs at the specified times.
    #[serde(default)]
    pub cron: String,

    /// Blackout windows — list of time ranges during which no new syncs
    /// are started (e.g. ["08:00-18:00 Mon-Fri"]).
    #[serde(default)]
    pub blackout: Vec<String>,

    /// IANA timezone name used to interpret this mirror's `cron` expression
    /// and `blackout` windows. When empty, the worker falls back to the
    /// global `timezone` field; when that's also empty, UTC is used.
    ///
    /// Valid examples: `"Asia/Shanghai"`, `"America/New_York"`,
    /// `"Europe/Berlin"`, `"UTC"`. Invalid names cause the worker to fail
    /// at startup.
    ///
    /// Cron expressions like `cron = "0 3 * * *"` and blackout windows like
    /// `blackout = ["08:00-18:00 Mon-Fri"]` are interpreted in this timezone.
    /// For example, with `timezone = "Asia/Shanghai"` and `cron = "0 3 * * *"`,
    /// syncs fire at 03:00 CST (= 19:00 UTC the previous day).
    #[serde(default)]
    pub timezone: String,

    /// Job priority (higher = more important, default 50).
    /// Higher-priority jobs get to run first when the semaphore is contended.
    #[serde(default = "default_priority")]
    pub priority: i32,

    /// When true, sync to a staging directory first, then atomically
    /// rename to the publish directory on success.
    #[serde(default)]
    pub atomic_publish: bool,

    /// Per-mirror override for the atomic-publish staging directory.
    ///
    /// When non-empty, this mirror's staging path is `<staging_dir>/<name>`,
    /// overriding `GlobalConfig::staging_dir` for this mirror only. Useful
    /// when a single worker serves mirrors across multiple data filesystems.
    ///
    /// When empty, falls back to `[global].staging_dir`; when *that* is also
    /// empty, falls back to `<log_dir>/staging/<name>`.
    #[serde(default)]
    pub staging_dir: String,

    /// Fallback upstream URLs for health probing. Does NOT change the
    /// actual sync data source — only used for pre-sync connectivity checks.
    #[serde(default)]
    pub upstream_fallback: Vec<String>,

    /// Whether to probe upstream availability before syncing.
    /// When true, does a quick `rsync --list-only --timeout=10` (or HTTP HEAD
    /// for non-rsync) against `upstream` before starting the real sync.
    #[serde(default)]
    pub check_upstream: bool,

    /// Nested child mirrors — Go's `[[mirrors.mirrors]]` inheritance.
    #[serde(default, rename = "mirrors")]
    pub child_mirrors: Vec<MirrorConfig>,
}

fn default_priority() -> i32 {
    50
}

/// Recursively flatten nested mirror configs.
///
/// Mirrors Go's `recursiveMirrors()`: start with parent (or empty), merge
/// child's non-default fields over parent, then if the merged result has
/// children, recurse; otherwise append to the flat output list.
pub fn flatten_mirrors(nested: &[MirrorConfig]) -> Vec<MirrorConfig> {
    let mut flat = Vec::new();
    for m in nested {
        recurse_flatten(None, m, &mut flat);
    }
    flat
}

fn recurse_flatten(
    parent: Option<&MirrorConfig>,
    child: &MirrorConfig,
    out: &mut Vec<MirrorConfig>,
) {
    let base = parent.cloned().unwrap_or_default();
    let merged = merge_mirror(base, child.clone());

    if merged.child_mirrors.is_empty() {
        let mut final_m = merged;
        final_m.child_mirrors = Vec::new(); // clear for cleanliness
        out.push(final_m);
    } else {
        let children = merged.child_mirrors.clone();
        let parent_for_children = {
            let mut p = merged;
            p.child_mirrors = Vec::new();
            p
        };
        for grandchild in &children {
            recurse_flatten(Some(&parent_for_children), grandchild, out);
        }
    }
}

/// Merge child into parent: for each field, child's non-default value
/// overrides parent's value. Matches Go's `mergo.Merge` with override.
fn merge_mirror(parent: MirrorConfig, child: MirrorConfig) -> MirrorConfig {
    MirrorConfig {
        name: if child.name.is_empty() {
            parent.name
        } else {
            child.name
        },
        provider: if child.provider == ProviderKind::default() {
            parent.provider
        } else {
            child.provider
        },
        upstream: if child.upstream.is_empty() {
            parent.upstream
        } else {
            child.upstream
        },
        interval: if child.interval == 0 {
            parent.interval
        } else {
            child.interval
        },
        retry: if child.retry == 0 {
            parent.retry
        } else {
            child.retry
        },
        timeout: if child.timeout == 0 {
            parent.timeout
        } else {
            child.timeout
        },
        mirror_dir: if child.mirror_dir.is_empty() {
            parent.mirror_dir
        } else {
            child.mirror_dir
        },
        mirror_subdir: if child.mirror_subdir.is_empty() {
            parent.mirror_subdir
        } else {
            child.mirror_subdir
        },
        log_dir: if child.log_dir.is_empty() {
            parent.log_dir
        } else {
            child.log_dir
        },
        env: if child.env.is_empty() {
            parent.env
        } else {
            child.env
        },
        role: if child.role.is_empty() {
            parent.role
        } else {
            child.role
        },
        exec_on_success: if child.exec_on_success.is_empty() {
            parent.exec_on_success
        } else {
            child.exec_on_success
        },
        exec_on_failure: if child.exec_on_failure.is_empty() {
            parent.exec_on_failure
        } else {
            child.exec_on_failure
        },
        exec_on_success_extra: if child.exec_on_success_extra.is_empty() {
            parent.exec_on_success_extra
        } else {
            child.exec_on_success_extra
        },
        exec_on_failure_extra: if child.exec_on_failure_extra.is_empty() {
            parent.exec_on_failure_extra
        } else {
            child.exec_on_failure_extra
        },
        success_exit_codes: if child.success_exit_codes.is_empty() {
            parent.success_exit_codes
        } else {
            child.success_exit_codes
        },
        rsync_success_exit_codes: if child.rsync_success_exit_codes.is_empty() {
            parent.rsync_success_exit_codes
        } else {
            child.rsync_success_exit_codes
        },
        command: if child.command.is_empty() {
            parent.command
        } else {
            child.command
        },
        fail_on_match: if child.fail_on_match.is_empty() {
            parent.fail_on_match
        } else {
            child.fail_on_match
        },
        size_pattern: if child.size_pattern.is_empty() {
            parent.size_pattern
        } else {
            child.size_pattern
        },
        use_ipv6: if !child.use_ipv6 {
            parent.use_ipv6
        } else {
            child.use_ipv6
        },
        use_ipv4: if !child.use_ipv4 {
            parent.use_ipv4
        } else {
            child.use_ipv4
        },
        exclude_file: if child.exclude_file.is_empty() {
            parent.exclude_file
        } else {
            child.exclude_file
        },
        username: if child.username.is_empty() {
            parent.username
        } else {
            child.username
        },
        password: if child.password.is_empty() {
            parent.password
        } else {
            child.password
        },
        rsync_no_timeout: if !child.rsync_no_timeout {
            parent.rsync_no_timeout
        } else {
            child.rsync_no_timeout
        },
        rsync_timeout: if child.rsync_timeout == 0 {
            parent.rsync_timeout
        } else {
            child.rsync_timeout
        },
        rsync_options: if child.rsync_options.is_empty() {
            parent.rsync_options
        } else {
            child.rsync_options
        },
        rsync_override: if child.rsync_override.is_empty() {
            parent.rsync_override
        } else {
            child.rsync_override
        },
        rsync_override_only: if !child.rsync_override_only {
            parent.rsync_override_only
        } else {
            child.rsync_override_only
        },
        stage1_profile: if child.stage1_profile.is_empty() {
            parent.stage1_profile
        } else {
            child.stage1_profile
        },
        memory_limit: if child.memory_limit.is_none() {
            parent.memory_limit
        } else {
            child.memory_limit
        },
        docker_image: if child.docker_image.is_empty() {
            parent.docker_image
        } else {
            child.docker_image
        },
        docker_volumes: if child.docker_volumes.is_empty() {
            parent.docker_volumes
        } else {
            child.docker_volumes
        },
        docker_options: if child.docker_options.is_empty() {
            parent.docker_options
        } else {
            child.docker_options
        },
        snapshot_path: if child.snapshot_path.is_empty() {
            parent.snapshot_path
        } else {
            child.snapshot_path
        },
        disk_quota: if child.disk_quota.is_empty() {
            parent.disk_quota
        } else {
            child.disk_quota
        },
        cron: if child.cron.is_empty() {
            parent.cron
        } else {
            child.cron
        },
        blackout: if child.blackout.is_empty() {
            parent.blackout
        } else {
            child.blackout
        },
        timezone: if child.timezone.is_empty() {
            parent.timezone
        } else {
            child.timezone
        },
        priority: if child.priority == default_priority() {
            parent.priority
        } else {
            child.priority
        },
        atomic_publish: if !child.atomic_publish {
            parent.atomic_publish
        } else {
            child.atomic_publish
        },
        staging_dir: if child.staging_dir.is_empty() {
            parent.staging_dir
        } else {
            child.staging_dir
        },
        upstream_fallback: if child.upstream_fallback.is_empty() {
            parent.upstream_fallback
        } else {
            child.upstream_fallback
        },
        check_upstream: if !child.check_upstream {
            parent.check_upstream
        } else {
            child.check_upstream
        },
        child_mirrors: child.child_mirrors, // always take child's children
    }
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

    /// Effective IANA timezone name for this mirror's cron / blackout windows.
    /// Falls back to the global `timezone`; if both are empty, returns `"UTC"`.
    /// The returned string is guaranteed non-empty but is NOT validated here —
    /// callers should `.parse::<chrono_tz::Tz>()` and handle errors. Validation
    /// happens once at config load time in `lib.rs::run`.
    pub fn effective_timezone(&self, global: &GlobalConfig) -> String {
        if !self.timezone.is_empty() {
            return self.timezone.clone();
        }
        if !global.timezone.is_empty() {
            return global.timezone.clone();
        }
        "UTC".to_owned()
    }

    /// Resolved mirror storage directory.
    /// Matches Go: `Join(MirrorDir, MirrorSubDir, Name)`.
    pub fn effective_mirror_dir(&self, global: &GlobalConfig) -> PathBuf {
        if !self.mirror_dir.is_empty() {
            // Go: when mirror_dir is explicitly set, use it as-is.
            // mirror_subdir and name are NOT appended (matches Go's provider.go
            // `if mirrorDir == "" { … } else { use mirrorDir directly }`).
            PathBuf::from(&self.mirror_dir)
        } else if !self.mirror_subdir.is_empty() {
            // mirror_dir not set, but mirror_subdir is: join global/subdir/name.
            PathBuf::from(&global.mirror_dir)
                .join(&self.mirror_subdir)
                .join(&self.name)
        } else {
            // Neither set: join global/name.
            PathBuf::from(&global.mirror_dir).join(&self.name)
        }
    }

    /// Resolved atomic-publish staging directory for this mirror.
    ///
    /// Resolution order:
    ///   1. `MirrorConfig::staging_dir` (this mirror's per-mirror override)
    ///   2. `GlobalConfig::staging_dir` (the worker-wide base)
    ///   3. `<log_dir>/staging/<name>` (legacy fallback)
    ///
    /// For options 1 and 2, the mirror name is appended; option 3 produces a
    /// complete per-mirror path on its own. Returns `None` when atomic_publish
    /// is disabled — callers should handle that case before calling.
    pub fn effective_staging_dir(&self, global: &GlobalConfig) -> PathBuf {
        if !self.staging_dir.is_empty() {
            return PathBuf::from(&self.staging_dir).join(&self.name);
        }
        if !global.staging_dir.is_empty() {
            return PathBuf::from(&global.staging_dir).join(&self.name);
        }
        // Legacy fallback: under log_dir. Only works when log_dir and
        // mirror_dir share a filesystem.
        PathBuf::from(&global.log_dir)
            .join("staging")
            .join(&self.name)
    }

    /// Whether this worker acts as master for the mirror.
    pub fn is_master(&self) -> bool {
        self.role.is_empty() || self.role == "master"
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
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

    #[test]
    fn default_public_url_uses_loopback_listener() {
        let cfg = WorkerConfig::default();
        assert_eq!(cfg.server.public_url(&cfg), "http://127.0.0.1:6000");
    }

    #[test]
    fn flatten_nested_mirrors_basic() {
        // Parent is an inheritance container — only leaf mirrors appear in
        // the flat list (matches Go's recursiveMirrors).
        let toml = r#"
[[mirrors]]
name = "debian"
provider = "rsync"
upstream = "rsync://ftp.debian.org/debian/"
interval = 600

[[mirrors.mirrors]]
name = "debian-security"
upstream = "rsync://security.debian.org/debian-security/"

[[mirrors.mirrors]]
name = "debian-backports"
upstream = "rsync://ftp.debian.org/debian-backports/"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 2);
        // Children inherit parent's provider and interval.
        assert_eq!(flat[0].name, "debian-security");
        assert_eq!(flat[0].provider, ProviderKind::Rsync);
        assert_eq!(flat[0].interval, 600);
        assert_eq!(
            flat[0].upstream,
            "rsync://security.debian.org/debian-security/"
        );
        assert_eq!(flat[1].name, "debian-backports");
        assert_eq!(flat[1].provider, ProviderKind::Rsync);
        assert_eq!(flat[1].interval, 600);
    }

    #[test]
    fn flatten_nested_mirrors_override() {
        // Child's non-default fields override parent.
        //
        // Note (issue 16): since `Rsync` is now the *default* variant (matching
        // Go's `provRsync = iota = 0`), a child that explicitly writes
        // `provider = "rsync"` is indistinguishable from "not set" in
        // `merge_mirror`.  This means a child cannot override a parent's
        // `provider = "command"` with `provider = "rsync"` — the parent value
        // wins.  This is intentional and mirrors Go's zero-value semantics.
        //
        // To override a parent's provider a child must use a non-default variant
        // such as `two-stage-rsync` or `command`.
        let toml = r#"
[[mirrors]]
name = "centos"
provider = "command"
interval = 300

[[mirrors.mirrors]]
name = "centos-stream"
provider = "two-stage-rsync"
upstream = "rsync://mirror.centos.org/centos-stream/"
interval = 120
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].name, "centos-stream");
        assert_eq!(flat[0].provider, ProviderKind::TwoStageRsync);
        assert_eq!(flat[0].interval, 120);
        assert_eq!(flat[0].upstream, "rsync://mirror.centos.org/centos-stream/");
    }

    /// Verify that `provider = "rsync"` in a child (== default) does NOT
    /// override a parent's `provider = "command"` (issue 16, intentional).
    #[test]
    fn flatten_rsync_child_does_not_override_command_parent() {
        let toml = r#"
[[mirrors]]
name = "base"
provider = "command"
interval = 300

[[mirrors.mirrors]]
name = "child"
provider = "rsync"
upstream = "rsync://example.com/"
interval = 60
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 1);
        // Child's `provider = "rsync"` == default → parent's `command` wins.
        assert_eq!(flat[0].provider, ProviderKind::Command);
        assert_eq!(flat[0].interval, 60); // non-default numeric field still overrides
    }

    #[test]
    fn flatten_deeply_nested() {
        // Two-level nesting: grandparent → parent → leaf.
        let toml = r#"
[[mirrors]]
name = "fedora"
provider = "rsync"
interval = 900
use_ipv6 = true

[[mirrors.mirrors]]
name = "fedora-epel"
mirror_subdir = "epel"

[[mirrors.mirrors]]
name = "fedora-updates"
upstream = "rsync://ftp.fedora.org/updates/"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 2);
        assert_eq!(flat[0].name, "fedora-epel");
        assert_eq!(flat[0].provider, ProviderKind::Rsync);
        assert_eq!(flat[0].interval, 900);
        assert!(flat[0].use_ipv6);
        assert_eq!(flat[0].mirror_subdir, "epel");
        assert_eq!(flat[1].name, "fedora-updates");
        assert_eq!(flat[1].provider, ProviderKind::Rsync);
        assert!(flat[1].use_ipv6);
    }

    #[test]
    fn flatten_leaf_only() {
        // A mirror with no children is a leaf and gets added directly.
        let toml = r#"
[[mirrors]]
name = "ubuntu"
provider = "rsync"
upstream = "rsync://archive.ubuntu.com/ubuntu/"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].name, "ubuntu");
        assert_eq!(flat[0].provider, ProviderKind::Rsync);
    }

    // ── effective_mirror_dir ─────────────────────────────────────────────────

    fn global_cfg() -> GlobalConfig {
        tunasync_common::config::parse_toml::<WorkerConfig>(
            r#"
[global]
name = "w"
log_dir = "/log"
mirror_dir = "/srv/mirrors"
concurrent = 1
"#,
        )
        .unwrap()
        .global
    }

    #[test]
    fn effective_mirror_dir_explicit_mirror_dir_ignores_subdir() {
        let global = global_cfg();
        let mc = MirrorConfig {
            name: "ubuntu".into(),
            mirror_dir: "/data/ubuntu".into(),
            mirror_subdir: "should-be-ignored".into(),
            ..MirrorConfig::default()
        };
        let got = mc.effective_mirror_dir(&global);
        assert_eq!(got, std::path::PathBuf::from("/data/ubuntu"));
    }

    #[test]
    fn effective_mirror_dir_fallback_to_global() {
        let global = global_cfg();
        let mc = MirrorConfig {
            name: "debian".into(),
            ..MirrorConfig::default()
        };
        let got = mc.effective_mirror_dir(&global);
        assert_eq!(got, std::path::PathBuf::from("/srv/mirrors/debian"));
    }

    #[test]
    fn effective_mirror_dir_with_subdir_only() {
        let global = global_cfg();
        let mc = MirrorConfig {
            name: "fedora-epel".into(),
            mirror_subdir: "epel".into(),
            ..MirrorConfig::default()
        };
        let got = mc.effective_mirror_dir(&global);
        assert_eq!(
            got,
            std::path::PathBuf::from("/srv/mirrors/epel/fedora-epel")
        );
    }

    /// `snapshot_path` propagates through `merge_mirror` inheritance.
    #[test]
    fn snapshot_path_inherited_and_overridden() {
        let toml = r#"
[[mirrors]]
name = "base"
provider = "rsync"
snapshot_path = "/snapshots/base"

[[mirrors.mirrors]]
name = "child-inherit"
upstream = "rsync://example.com/inherit/"

[[mirrors.mirrors]]
name = "child-override"
upstream = "rsync://example.com/override/"
snapshot_path = "/snapshots/override"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);
        assert_eq!(flat.len(), 2);

        let inherit = flat.iter().find(|m| m.name == "child-inherit").unwrap();
        assert_eq!(
            inherit.snapshot_path, "/snapshots/base",
            "child without snapshot_path should inherit parent's"
        );

        let overridden = flat.iter().find(|m| m.name == "child-override").unwrap();
        assert_eq!(
            overridden.snapshot_path, "/snapshots/override",
            "child with explicit snapshot_path should override parent's"
        );
    }

    // ── effective_staging_dir resolution ──────────────────────────────────

    /// Per-mirror staging_dir takes precedence over the global one.
    #[test]
    fn effective_staging_dir_mirror_overrides_global() {
        let mut g = GlobalConfig::default();
        g.staging_dir = "/srv/global-staging".into();
        g.log_dir = "/var/log/tunasync".into();
        g.mirror_dir = "/srv/mirrors".into();

        let mut mc = MirrorConfig::default();
        mc.name = "debian".into();
        mc.staging_dir = "/srv/mirror-specific-staging".into();

        assert_eq!(
            mc.effective_staging_dir(&g),
            std::path::PathBuf::from("/srv/mirror-specific-staging/debian")
        );
    }

    /// When per-mirror is empty, fall back to global.
    #[test]
    fn effective_staging_dir_falls_back_to_global() {
        let mut g = GlobalConfig::default();
        g.staging_dir = "/srv/mirrors/.staging".into();
        g.log_dir = "/var/log/tunasync".into();
        g.mirror_dir = "/srv/mirrors".into();

        let mut mc = MirrorConfig::default();
        mc.name = "ubuntu".into();

        assert_eq!(
            mc.effective_staging_dir(&g),
            std::path::PathBuf::from("/srv/mirrors/.staging/ubuntu")
        );
    }

    /// When both are empty, fall back to legacy <log_dir>/staging/<name>.
    #[test]
    fn effective_staging_dir_legacy_fallback() {
        let mut g = GlobalConfig::default();
        g.log_dir = "/var/log/tunasync".into();
        g.mirror_dir = "/srv/mirrors".into();

        let mut mc = MirrorConfig::default();
        mc.name = "arch".into();

        assert_eq!(
            mc.effective_staging_dir(&g),
            std::path::PathBuf::from("/var/log/tunasync/staging/arch")
        );
    }

    /// Verify staging_dir flows through child-mirror inheritance via merge_mirror.
    #[test]
    fn staging_dir_inherited_through_merge() {
        let toml = r#"
[[mirrors]]
name = "parent"
provider = "rsync"
staging_dir = "/data/staging"

[[mirrors.mirrors]]
name = "child-inherit"
upstream = "rsync://example.com/c/"

[[mirrors.mirrors]]
name = "child-override"
upstream = "rsync://example.com/o/"
staging_dir = "/other/staging"
"#;
        let cfg: WorkerConfig = tunasync_common::config::parse_toml(toml).unwrap();
        let flat = flatten_mirrors(&cfg.mirrors_conf);

        let inherit = flat.iter().find(|m| m.name == "child-inherit").unwrap();
        assert_eq!(inherit.staging_dir, "/data/staging");

        let overridden = flat.iter().find(|m| m.name == "child-override").unwrap();
        assert_eq!(overridden.staging_dir, "/other/staging");
    }
}
