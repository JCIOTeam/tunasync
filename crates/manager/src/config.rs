//! Manager configuration.
//!
//! Wire-compatible with Go tunasync's `manager/config.go`. TOML field names
//! are preserved exactly so existing `manager.conf` files work without change.

use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Top-level manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagerConfig {
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub files: FilesConfig,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            debug: false,
            server: ServerConfig::default(),
            files: FilesConfig::default(),
        }
    }
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
    fn default_addr() -> String { "127.0.0.1".into() }
    fn default_port() -> u16 { 14242 }

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
    #[serde(default = "FilesConfig::default_db_file")]
    pub db_file: PathBuf,
    /// "redb" (default) or "sqlite"
    #[serde(default = "FilesConfig::default_db_type")]
    pub db_type: String,
    #[serde(default)]
    pub ca_cert: String,
}

impl FilesConfig {
    fn default_db_file() -> PathBuf { PathBuf::from("/var/lib/tunasync/tunasync.db") }
    fn default_db_type() -> String { "redb".into() }
}

impl Default for FilesConfig {
    fn default() -> Self {
        Self {
            db_file: Self::default_db_file(),
            db_type: Self::default_db_type(),
            ca_cert: String::new(),
        }
    }
}
