//! Shared utilities used by tunasync-rs binaries and libraries.
//!
//! This crate is a small grab-bag — it exists to avoid duplicating logger
//! setup, HTTP client configuration, and config-file loading between the
//! manager, worker, and CLI.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod config;
pub mod http;
pub mod logger;
pub mod util;
