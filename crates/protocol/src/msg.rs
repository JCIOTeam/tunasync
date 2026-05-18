//! Wire messages exchanged between manager, worker, and CLI.
//!
//! Field names and JSON encoding match Go's `internal/msg.go` exactly.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

use crate::status::SyncStatus;

/// Deserialise `null` as `T::default()` instead of erroring.
///
/// Go's `encoding/json` marshals `nil` slices and maps as JSON `null`, not
/// `[]` / `{}`. With plain `#[serde(default)]` we accept *absent* fields but
/// not explicit `null`, so a Go worker/manager sending `"args": null` would
/// fail to deserialise on the Rust side.
///
/// Apply this with:
/// ```ignore
/// #[serde(default, deserialize_with = "deserialize_null_default")]
/// pub args: Vec<String>,
/// ```
pub(crate) fn deserialize_null_default<'de, D, T>(des: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Option::<T>::deserialize(des).map(|o| o.unwrap_or_default())
}

// Status messages (worker → manager → client)

/// A sync status update reported by a worker for one of its mirrors.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorStatus {
    /// Mirror name (e.g. `"ubuntu"`).
    pub name: String,
    /// ID of the worker reporting this status.
    pub worker: String,
    /// Whether this worker is the master for the mirror (relevant for
    /// multi-worker setups where one is canonical).
    pub is_master: bool,
    /// Current state of the job.
    pub status: SyncStatus,
    /// Timestamp of the last status update.
    pub last_update: DateTime<Utc>,
    /// Timestamp the most recent sync started.
    pub last_started: DateTime<Utc>,
    /// Timestamp the most recent sync ended (success or failure).
    pub last_ended: DateTime<Utc>,
    /// Next scheduled run.
    ///
    /// Note: the JSON key is `next_schedule` (matching Go), not `scheduled`.
    #[serde(rename = "next_schedule")]
    pub scheduled: DateTime<Utc>,
    /// Upstream URL being mirrored.
    pub upstream: String,
    /// Human-readable size of the mirror (e.g. `"1.2T"`); the worker is free
    /// to format this however it wants.
    pub size: String,
    /// Last error message, or empty string if none.
    pub error_msg: String,

    // ── Optional extension fields ─────────────────────────────────────────
    //
    // Each is `#[serde(default)]` (absent JSON → zero/false/empty) and
    // `skip_serializing_if = is_default` (zero/false/empty → absent JSON),
    // so a status reported or stored without these fields is byte-for-byte
    // identical to the legacy Go-compatible shape. A Go worker reporting
    // status to a tunasync-rs manager works unchanged.
    /// Bytes transferred during the most recent sync.
    /// Worker parses this from rsync output; 0 means unknown.
    #[serde(default, skip_serializing_if = "crate::is_default")]
    pub last_transferred_bytes: u64,

    /// Running total of bytes transferred across all syncs of this mirror
    /// on this worker. Accumulated by the manager when a worker reports a
    /// new `last_started` value.
    #[serde(default, skip_serializing_if = "crate::is_default")]
    pub total_transferred_bytes: u64,

    /// Number of consecutive sync failures since the last success.
    /// Reset to 0 by manager on each Success. Used by stale-detection /
    /// webhook alerting (`notify_url`) to fire alerts.
    #[serde(default, skip_serializing_if = "crate::is_default")]
    pub consecutive_failures: u32,

    /// Whether the mirror is currently considered stale by the manager.
    /// Set by the manager's stale detector when `last_update` is older than
    /// the configured `stale_after`. Cleared automatically on next Success.
    #[serde(default, skip_serializing_if = "crate::is_default")]
    pub stale: bool,
}

impl Default for MirrorStatus {
    /// Build a `MirrorStatus` with Go's zero-time semantics on all timestamps
    /// (`0001-01-01T00:00:00Z`), empty strings, and zeroed extension fields.
    /// Matches the `zero_mirror_status(name, worker)` helper in the manager.
    fn default() -> Self {
        Self {
            name: String::new(),
            worker: String::new(),
            is_master: false,
            status: SyncStatus::None,
            last_update: crate::time::zero_time(),
            last_started: crate::time::zero_time(),
            last_ended: crate::time::zero_time(),
            scheduled: crate::time::zero_time(),
            upstream: String::new(),
            size: String::new(),
            error_msg: String::new(),
            last_transferred_bytes: 0,
            total_transferred_bytes: 0,
            consecutive_failures: 0,
            stale: false,
        }
    }
}

/// A worker registered with the manager.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// Worker ID (unique across the deployment).
    pub id: String,
    /// HTTP(S) URL where the manager can reach this worker.
    pub url: String,
    /// Session token (used for worker → manager authentication).
    pub token: String,
    /// Last time the worker pinged in.
    pub last_online: DateTime<Utc>,
    /// Last time the worker registered (or re-registered).
    pub last_register: DateTime<Utc>,
}

// Schedules (worker → manager)

/// A batch of mirror schedules announced by a worker on startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorSchedules {
    /// Per-mirror next-run schedule.
    ///
    /// Go marshals `nil` slices as JSON `null`, so we tolerate either an absent
    /// field, `[]`, or `null` here for full wire-compat.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub schedules: Vec<MirrorSchedule>,
}

/// One mirror's next scheduled run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorSchedule {
    /// Mirror name.
    ///
    /// Note: the JSON key is `name` (matching Go's struct tag), not
    /// `mirror_name`.
    #[serde(rename = "name")]
    pub mirror_name: String,
    /// When this mirror is next due to sync.
    pub next_schedule: DateTime<Utc>,
}

// Commands (manager → worker, client → manager)

/// Action verb for a job/worker command.
/// JSON encoding is the
/// lower-case verb string. Note that Go uses `iota` for the in-memory
/// representation (so `CmdStart == 0`), but the wire format is always the
/// string — which is what we serialise here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CmdVerb {
    /// Start a job.
    #[serde(rename = "start")]
    Start,
    /// Stop syncing, but keep the job scheduled.
    #[serde(rename = "stop")]
    Stop,
    /// Disable the job (kills its goroutine on Go, its task on Rust).
    #[serde(rename = "disable")]
    Disable,
    /// Restart a syncing job.
    #[serde(rename = "restart")]
    Restart,
    /// Liveness check.
    #[serde(rename = "ping")]
    Ping,
    /// Tell a worker to reload its mirror config from disk.
    #[serde(rename = "reload")]
    Reload,
}

impl std::fmt::Display for CmdVerb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Disable => "disable",
            Self::Restart => "restart",
            Self::Ping => "ping",
            Self::Reload => "reload",
        };
        f.write_str(s)
    }
}

/// A command sent from manager → worker.
///
/// Go's struct allows `args` and `options` to be omitted; we mirror that with
/// `#[serde(default)]` so payloads without those fields decode cleanly.
/// We also tolerate explicit `null` for both fields because Go's
/// `encoding/json` marshals `nil` slices and maps as `null`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCmd {
    /// Action to perform.
    pub cmd: CmdVerb,
    /// Target mirror ID (empty when the command is worker-wide, e.g. `reload`).
    pub mirror_id: String,
    /// Optional positional arguments.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub args: Vec<String>,
    /// Optional boolean flags (e.g. `force: true`).
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub options: HashMap<String, bool>,
}

impl std::fmt::Display for WorkerCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.args.is_empty() {
            write!(f, "{} ({})", self.cmd, self.mirror_id)
        } else {
            write!(f, "{} ({}, {:?})", self.cmd, self.mirror_id, self.args)
        }
    }
}

/// A command sent from CLI → manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCmd {
    /// Action to perform.
    pub cmd: CmdVerb,
    /// Target mirror ID.
    pub mirror_id: String,
    /// Target worker ID (empty for "all workers" or when looking up by mirror).
    pub worker_id: String,
    /// Optional positional arguments.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub args: Vec<String>,
    /// Optional boolean flags.
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub options: HashMap<String, bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmd_verb_json_round_trip() {
        for verb in [
            CmdVerb::Start,
            CmdVerb::Stop,
            CmdVerb::Disable,
            CmdVerb::Restart,
            CmdVerb::Ping,
            CmdVerb::Reload,
        ] {
            let s = serde_json::to_string(&verb).unwrap();
            let back: CmdVerb = serde_json::from_str(&s).unwrap();
            assert_eq!(verb, back);
        }
    }

    #[test]
    fn worker_cmd_omits_default_fields_decodes() {
        // Go often sends `{"cmd":"ping","mirror_id":""}` without args/options.
        let json = r#"{"cmd":"ping","mirror_id":""}"#;
        let cmd: WorkerCmd = serde_json::from_str(json).unwrap();
        assert_eq!(cmd.cmd, CmdVerb::Ping);
        assert!(cmd.args.is_empty());
        assert!(cmd.options.is_empty());
    }

    #[test]
    fn worker_cmd_display() {
        let cmd = WorkerCmd {
            cmd: CmdVerb::Start,
            mirror_id: "ubuntu".into(),
            args: vec![],
            options: HashMap::new(),
        };
        assert_eq!(cmd.to_string(), "start (ubuntu)");

        let cmd_with_args = WorkerCmd {
            cmd: CmdVerb::Restart,
            mirror_id: "ubuntu".into(),
            args: vec!["--force".into()],
            options: HashMap::new(),
        };
        assert_eq!(
            cmd_with_args.to_string(),
            r#"restart (ubuntu, ["--force"])"#
        );
    }

    // ── Wire-compat: Go marshals nil slice/map as JSON null ────────────────

    /// Go's `encoding/json` writes `nil` slices and maps as JSON `null`.
    /// `#[serde(default)]` alone accepts an *absent* field but rejects
    /// explicit `null`, so we need `deserialize_null_default` for parity.
    #[test]
    fn worker_cmd_accepts_null_args_and_options() {
        let json = r#"{"cmd":"start","mirror_id":"ubuntu","args":null,"options":null}"#;
        let cmd: WorkerCmd = serde_json::from_str(json).expect("null collections must decode");
        assert_eq!(cmd.cmd, CmdVerb::Start);
        assert_eq!(cmd.mirror_id, "ubuntu");
        assert!(cmd.args.is_empty());
        assert!(cmd.options.is_empty());
    }

    #[test]
    fn client_cmd_accepts_null_args_and_options() {
        let json =
            r#"{"cmd":"start","mirror_id":"ubuntu","worker_id":"w1","args":null,"options":null}"#;
        let cmd: ClientCmd = serde_json::from_str(json).expect("null collections must decode");
        assert_eq!(cmd.worker_id, "w1");
        assert!(cmd.args.is_empty());
        assert!(cmd.options.is_empty());
    }

    /// Go's MirrorSchedules with no entries marshals as `{"schedules":null}`.
    #[test]
    fn mirror_schedules_accepts_null_schedules() {
        let json = r#"{"schedules":null}"#;
        let s: MirrorSchedules = serde_json::from_str(json).expect("null schedules must decode");
        assert!(s.schedules.is_empty());
    }

    /// Sanity: present-but-empty arrays still work.
    #[test]
    fn collections_accept_empty_arrays() {
        let json = r#"{"cmd":"start","mirror_id":"x","args":[],"options":{}}"#;
        let cmd: WorkerCmd = serde_json::from_str(json).expect("empty arrays decode");
        assert!(cmd.args.is_empty());
        assert!(cmd.options.is_empty());
    }

    /// Sanity: absent fields still default correctly.
    #[test]
    fn collections_accept_absent_fields() {
        let json = r#"{"cmd":"start","mirror_id":"x"}"#;
        let cmd: WorkerCmd = serde_json::from_str(json).expect("absent fields decode");
        assert!(cmd.args.is_empty());
        assert!(cmd.options.is_empty());
    }
}
