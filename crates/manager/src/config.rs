//! Manager configuration.
//!
//! Wire-compatible with Go tunasync's `manager/config.go`. TOML field names
//! are preserved exactly so existing `manager.conf` files work without change.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level manager configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManagerConfig {
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub files: FilesConfig,
    /// Webhook and alerting configuration.
    #[serde(default)]
    pub notify: NotifyConfig,
}

/// Webhook notification and stale-detection configuration.
///
/// All fields default to disabled / empty, so existing configs without
/// a `[notify]` section are unaffected.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotifyConfig {
    /// Webhook URL to POST events to (Slack, Discord, Feishu, WeChat Work,
    /// or any service accepting `{ "text": "..." }` payloads).
    /// Empty string disables webhook notifications.
    #[serde(default)]
    pub webhook_url: String,

    /// Human-readable duration after which a mirror is considered stale if
    /// it hasn't had a successful sync (e.g. "48h", "7d").
    /// Empty string disables stale detection. Checked every 5 minutes.
    #[serde(default)]
    pub stale_after: String,

    /// Fire a webhook alert after this many consecutive sync failures.
    /// 0 = disabled (only stale triggers alerts).
    #[serde(default)]
    pub alert_after_failures: u32,
}

/// HTTP server bind settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "ServerConfig::default_addr")]
    pub addr: String,
    #[serde(default = "ServerConfig::default_port")]
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
        14242
    }

    pub fn bind_addr(&self) -> std::net::SocketAddr {
        let ip: IpAddr = self.addr.parse().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        std::net::SocketAddr::new(ip, self.port)
    }

    pub fn tls_enabled(&self) -> bool {
        !self.ssl_cert.is_empty() && !self.ssl_key.is_empty()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: Self::default_addr(),
            port: Self::default_port(),
            ssl_cert: String::new(),
            ssl_key: String::new(),
        }
    }
}

/// Filesystem paths owned by the manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesConfig {
    /// Path to the JSON status file written by the manager every 30 seconds.
    ///
    /// Matches Go's `FileConfig.StatusFile` (`toml:"status_file"`).
    /// Default: `/var/lib/tunasync/tunasync.json`.
    ///
    /// Written atomically (via a `.tmp` rename).  Skipped silently if the
    /// parent directory does not exist (e.g. dev setups without
    /// `/var/lib/tunasync/`).  Set to `""` to disable entirely.
    #[serde(default = "FilesConfig::default_status_file")]
    pub status_file: PathBuf,

    #[serde(default = "FilesConfig::default_db_file")]
    pub db_file: PathBuf,
    /// "redb" (default), "sqlite", or "redis".
    ///
    /// When db_type = "redis", db_file is a Redis URL:
    ///   redis://localhost:6379/0
    ///   redis://:password@redis.example.com:6379/1
    #[serde(default = "FilesConfig::default_db_type")]
    pub db_type: String,
    #[serde(default)]
    pub ca_cert: String,
}

impl FilesConfig {
    fn default_status_file() -> PathBuf {
        PathBuf::from("/var/lib/tunasync/tunasync.json")
    }
    fn default_db_file() -> PathBuf {
        PathBuf::from("/var/lib/tunasync/tunasync.db")
    }
    fn default_db_type() -> String {
        "redb".into()
    }
}

impl Default for FilesConfig {
    fn default() -> Self {
        Self {
            status_file: Self::default_status_file(),
            db_file: Self::default_db_file(),
            db_type: Self::default_db_type(),
            ca_cert: String::new(),
        }
    }
}
