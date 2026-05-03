# Wire compatibility with Go tunasync

This document captures the field-by-field mapping from Go tunasync's
`internal/` package to `tunasync-protocol`. The goal is byte-for-byte JSON
compatibility — a Rust component can talk to a Go component (and vice versa)
without any translation layer.

## `SyncStatus` (`internal/status.go`)

| Go `iota` | Rust variant         | JSON string     |
|-----------|----------------------|-----------------|
| `None`        | `SyncStatus::None`        | `"none"`        |
| `Failed`      | `SyncStatus::Failed`      | `"failed"`      |
| `Success`     | `SyncStatus::Success`     | `"success"`     |
| `Syncing`     | `SyncStatus::Syncing`     | `"syncing"`     |
| `PreSyncing`  | `SyncStatus::PreSyncing`  | `"pre-syncing"` |
| `Paused`      | `SyncStatus::Paused`      | `"paused"`      |
| `Disabled`    | `SyncStatus::Disabled`    | `"disabled"`    |

Note that the in-memory `iota` ordering is irrelevant — both sides only
exchange the string form.

## `CmdVerb` (`internal/msg.go`)

| Go constant   | Rust variant     | JSON string |
|---------------|------------------|-------------|
| `CmdStart`    | `CmdVerb::Start`   | `"start"`    |
| `CmdStop`     | `CmdVerb::Stop`    | `"stop"`     |
| `CmdDisable`  | `CmdVerb::Disable` | `"disable"`  |
| `CmdRestart`  | `CmdVerb::Restart` | `"restart"`  |
| `CmdPing`     | `CmdVerb::Ping`    | `"ping"`     |
| `CmdReload`   | `CmdVerb::Reload`  | `"reload"`   |

## `MirrorStatus`

| Go field       | JSON key         | Rust field                      | Notes                        |
|----------------|------------------|---------------------------------|------------------------------|
| `Name`         | `name`           | `name: String`                  |                              |
| `Worker`       | `worker`         | `worker: String`                |                              |
| `IsMaster`     | `is_master`      | `is_master: bool`               |                              |
| `Status`       | `status`         | `status: SyncStatus`            |                              |
| `LastUpdate`   | `last_update`    | `last_update: DateTime<Utc>`    | RFC 3339, ns precision       |
| `LastStarted`  | `last_started`   | `last_started: DateTime<Utc>`   | Same                         |
| `LastEnded`    | `last_ended`     | `last_ended: DateTime<Utc>`     | Same                         |
| `Scheduled`    | **`next_schedule`** | `scheduled: DateTime<Utc>`   | **Rust field name differs**  |
| `Upstream`     | `upstream`       | `upstream: String`              |                              |
| `Size`         | `size`           | `size: String`                  | Worker formats ad-hoc        |
| `ErrorMsg`     | `error_msg`      | `error_msg: String`             |                              |

## `WorkerStatus`

| Go field       | JSON key         | Rust field                      |
|----------------|------------------|---------------------------------|
| `ID`           | `id`             | `id: String`                    |
| `URL`          | `url`            | `url: String`                   |
| `Token`        | `token`          | `token: String`                 |
| `LastOnline`   | `last_online`    | `last_online: DateTime<Utc>`    |
| `LastRegister` | `last_register`  | `last_register: DateTime<Utc>`  |

## `MirrorSchedule` / `MirrorSchedules`

| Go field       | JSON key         | Rust field                          | Notes                       |
|----------------|------------------|-------------------------------------|-----------------------------|
| `MirrorName`   | **`name`**       | `mirror_name: String`               | **Rust field name differs** |
| `NextSchedule` | `next_schedule`  | `next_schedule: DateTime<Utc>`      |                             |

`MirrorSchedules` is a single-field wrapper: `{ "schedules": [...] }`.

## `WorkerCmd` / `ClientCmd`

| Go field    | JSON key     | Rust field                              | Notes                          |
|-------------|--------------|-----------------------------------------|--------------------------------|
| `Cmd`       | `cmd`        | `cmd: CmdVerb`                          |                                |
| `MirrorID`  | `mirror_id`  | `mirror_id: String`                     |                                |
| `WorkerID`† | `worker_id`† | `worker_id: String`†                    | †`ClientCmd` only              |
| `Args`      | `args`       | `args: Vec<String>`                     | `serde(default)` accepts omit  |
| `Options`   | `options`    | `options: HashMap<String, bool>`        | `serde(default)` accepts omit  |

## Time format

Both sides use **RFC 3339** with sub-second precision suppressed when zero:

- `2024-06-15T10:30:00Z` (no fractional)
- `2024-06-15T10:30:00.123Z` (millisecond)
- `2024-06-15T10:30:00.123456789Z` (nanosecond)

Go's `time.Time{}` zero value (`0001-01-01T00:00:00Z`) is a meaningful
sentinel for "never set". Use `tunasync_protocol::zero_time()` to construct
it; check with `tunasync_protocol::is_zero_time(&t)`. **Do not** use
`chrono::DateTime::default()` — that's the Unix epoch, which is a different
sentinel.

## Verification

The integration test `crates/protocol/tests/wire_compat.rs` round-trips
reference payloads and asserts structural equality (parsing the produced
JSON back into a generic `serde_json::Value` to ignore key-ordering
differences between Go's `encoding/json` and `serde_json`).
