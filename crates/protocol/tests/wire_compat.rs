//! Wire-compatibility tests against reference JSON payloads.
//!
//! These payloads represent the exact bytes produced by Go's
//! `encoding/json` package on the corresponding `internal.*` structs.
//! Each test parses the JSON, asserts the parsed value, and re-serialises
//! it to confirm we produce the same field set Go does (within JSON's
//! key-ordering tolerance — we compare *parsed* values, not byte strings).

use std::collections::HashMap;

use serde_json::Value;
use tunasync_protocol::{
    is_zero_time, ClientCmd, CmdVerb, MirrorSchedules, MirrorStatus, SyncStatus,
    WorkerCmd, WorkerStatus,
};

/// Re-serialise a value and assert that the resulting JSON, when parsed back
/// into a generic [`Value`], equals the parsed expected JSON. This sidesteps
/// the fact that Go and `serde_json` may emit fields in different orders.
fn assert_serialises_to<T: serde::Serialize>(value: &T, expected_json: &str) {
    let actual: Value = serde_json::from_str(&serde_json::to_string(value).unwrap()).unwrap();
    let expected: Value = serde_json::from_str(expected_json).unwrap();
    assert_eq!(actual, expected, "serialised JSON does not match expected");
}

#[test]
fn mirror_status_round_trip() {
    // Exact-shape payload that a Go worker would produce.
    let json = r#"{
        "name": "ubuntu",
        "worker": "worker-1",
        "is_master": true,
        "status": "syncing",
        "last_update": "2024-06-15T10:30:45.123456789Z",
        "last_started": "2024-06-15T10:00:00Z",
        "last_ended": "0001-01-01T00:00:00Z",
        "next_schedule": "2024-06-15T16:00:00Z",
        "upstream": "rsync://archive.ubuntu.com/ubuntu/",
        "size": "1.2T",
        "error_msg": ""
    }"#;

    let status: MirrorStatus = serde_json::from_str(json).unwrap();
    assert_eq!(status.name, "ubuntu");
    assert_eq!(status.worker, "worker-1");
    assert!(status.is_master);
    assert_eq!(status.status, SyncStatus::Syncing);
    assert!(is_zero_time(&status.last_ended));
    assert_eq!(status.upstream, "rsync://archive.ubuntu.com/ubuntu/");
    assert_eq!(status.size, "1.2T");

    // Re-serialise and compare structurally.
    assert_serialises_to(&status, json);
}

#[test]
fn worker_status_with_zero_last_online() {
    // A freshly-registered worker has last_online == time.Time{}.
    let json = r#"{
        "id": "worker-1",
        "url": "https://worker-1.example.com:6000",
        "token": "deadbeef",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "2024-06-15T10:00:00Z"
    }"#;

    let ws: WorkerStatus = serde_json::from_str(json).unwrap();
    assert_eq!(ws.id, "worker-1");
    assert!(is_zero_time(&ws.last_online));
    assert!(!is_zero_time(&ws.last_register));

    assert_serialises_to(&ws, json);
}

#[test]
fn mirror_schedules_uses_name_key() {
    // Go's struct tag on MirrorSchedule.MirrorName is `json:"name"`.
    let json = r#"{
        "schedules": [
            {"name": "ubuntu", "next_schedule": "2024-06-15T16:00:00Z"},
            {"name": "debian", "next_schedule": "2024-06-15T17:00:00Z"}
        ]
    }"#;

    let schedules: MirrorSchedules = serde_json::from_str(json).unwrap();
    assert_eq!(schedules.schedules.len(), 2);
    assert_eq!(schedules.schedules[0].mirror_name, "ubuntu");
    assert_eq!(schedules.schedules[1].mirror_name, "debian");

    assert_serialises_to(&schedules, json);
}

#[test]
fn worker_cmd_with_args_and_options() {
    let json = r#"{
        "cmd": "restart",
        "mirror_id": "ubuntu",
        "args": ["--force"],
        "options": {"dry_run": false}
    }"#;

    let cmd: WorkerCmd = serde_json::from_str(json).unwrap();
    assert_eq!(cmd.cmd, CmdVerb::Restart);
    assert_eq!(cmd.mirror_id, "ubuntu");
    assert_eq!(cmd.args, vec!["--force"]);
    assert_eq!(cmd.options.get("dry_run"), Some(&false));

    // Round trip via Value comparison handles map key ordering.
    assert_serialises_to(&cmd, json);
}

#[test]
fn worker_cmd_minimal_payload() {
    // Common case: ping with no args/options. Go encoders may still include
    // empty `args:[]` and `options:{}`, but our `#[serde(default)]` accepts
    // payloads without them.
    let json = r#"{"cmd":"ping","mirror_id":""}"#;
    let cmd: WorkerCmd = serde_json::from_str(json).unwrap();
    assert_eq!(cmd.cmd, CmdVerb::Ping);
    assert!(cmd.args.is_empty());
    assert!(cmd.options.is_empty());
}

#[test]
fn client_cmd_full_shape() {
    let json = r#"{
        "cmd": "start",
        "mirror_id": "ubuntu",
        "worker_id": "worker-1",
        "args": [],
        "options": {}
    }"#;

    let cmd: ClientCmd = serde_json::from_str(json).unwrap();
    assert_eq!(cmd.cmd, CmdVerb::Start);
    assert_eq!(cmd.mirror_id, "ubuntu");
    assert_eq!(cmd.worker_id, "worker-1");

    assert_serialises_to(&cmd, json);
}

#[test]
fn all_sync_status_variants_round_trip_as_strings() {
    let cases = [
        (SyncStatus::None, "none"),
        (SyncStatus::Failed, "failed"),
        (SyncStatus::Success, "success"),
        (SyncStatus::Syncing, "syncing"),
        (SyncStatus::PreSyncing, "pre-syncing"),
        (SyncStatus::Paused, "paused"),
        (SyncStatus::Disabled, "disabled"),
    ];
    for (variant, expected) in cases {
        let json = serde_json::to_string(&variant).unwrap();
        assert_eq!(json, format!(r#""{expected}""#));
        let back: SyncStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, variant);
    }
}

#[test]
fn all_cmd_verb_variants_round_trip_as_strings() {
    let cases = [
        (CmdVerb::Start, "start"),
        (CmdVerb::Stop, "stop"),
        (CmdVerb::Disable, "disable"),
        (CmdVerb::Restart, "restart"),
        (CmdVerb::Ping, "ping"),
        (CmdVerb::Reload, "reload"),
    ];
    for (variant, expected) in cases {
        let json = serde_json::to_string(&variant).unwrap();
        assert_eq!(json, format!(r#""{expected}""#));
        let back: CmdVerb = serde_json::from_str(&json).unwrap();
        assert_eq!(back, variant);
    }
}

#[test]
fn build_a_default_mirror_status_using_zero_time() {
    // Smoke test: a worker that's never reported can be constructed with
    // sensible zero-value defaults that match Go semantics.
    let status = MirrorStatus {
        name: "fresh".into(),
        worker: "worker-1".into(),
        is_master: true,
        ..Default::default()
    };
    let json = serde_json::to_string(&status).unwrap();
    // last_ended must serialise as Go's zero time string.
    assert!(json.contains(r#""last_ended":"0001-01-01T00:00:00Z""#));
}

#[test]
fn unknown_options_keys_preserved() {
    // Go's `map[string]bool` is loose about which keys appear. Make sure we
    // round-trip arbitrary keys without losing them.
    let mut options = HashMap::new();
    options.insert("force".to_string(), true);
    options.insert("incremental".to_string(), false);

    let cmd = WorkerCmd {
        cmd: CmdVerb::Start,
        mirror_id: "ubuntu".into(),
        args: vec![],
        options,
    };

    let json = serde_json::to_string(&cmd).unwrap();
    let back: WorkerCmd = serde_json::from_str(&json).unwrap();
    assert_eq!(back.options.get("force"), Some(&true));
    assert_eq!(back.options.get("incremental"), Some(&false));
}
