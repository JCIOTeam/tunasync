//! TOML configuration file loader.
//!
//! Go tunasync uses `BurntSushi/toml` with custom merging via `dario.cat/mergo`.
//! We replace that with `serde` + `toml`, leaving merge logic to the caller
//! since `mergo`'s deep-merge semantics don't have a direct Rust equivalent
//! and Manager/Worker have only a handful of nested fields each.

use std::path::Path;

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;

/// Load a TOML config file from `path` and deserialise into `T`.
///
/// Errors include the file path in their context, so the resulting message is
/// directly useful in operator-facing logs.
pub fn load_toml<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("read config from {}", path.display()))?;
    toml::from_str(&content).with_context(|| format!("parse TOML config from {}", path.display()))
}

/// Parse a TOML config from an in-memory string.
///
/// Useful for tests and for embedded defaults.
pub fn parse_toml<T: DeserializeOwned>(content: &str) -> Result<T> {
    toml::from_str(content).context("parse TOML config")
}
