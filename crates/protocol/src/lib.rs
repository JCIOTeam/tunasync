//! Wire-compatible protocol types for tunasync.
//!
//! Every type in this crate is byte-for-byte JSON-compatible with the
//! corresponding type in the upstream Go implementation
//! (<https://github.com/tuna/tunasync>, package `internal`).
//!
//! Compatibility is verified by the `wire_compat` integration test, which
//! round-trips reference JSON payloads captured from the Go implementation.
//!
//! # Module layout
//!
//! - [`status`] — [`SyncStatus`] enum
//! - [`msg`]    — wire messages exchanged between manager, worker, and CLI
//! - [`time`]   — helpers for Go's `time.Time{}` zero-value semantics

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod msg;
pub mod status;
pub mod time;
pub mod web;

pub use msg::{
    ClientCmd, CmdVerb, MirrorSchedule, MirrorSchedules, MirrorStatus, WorkerCmd, WorkerStatus,
};
pub use status::SyncStatus;
pub use time::{is_zero_time, zero_time};
pub use web::WebMirrorStatus;
