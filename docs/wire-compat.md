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
| `URL`          | `url`            | `url: String`                    |
| `Token`        | `token`          | `token: String`                  |
| `LastOnline`   | `last_online`    | `last_online: DateTime<Utc>`     |
| `LastRegister` | `last_register`  | `last_register: DateTime<Utc>`   |

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

## `WebMirrorStatus`

The `/jobs` API returns `WebMirrorStatus` (not `MirrorStatus`). Each timestamp
field appears twice: a human-readable text form and a Unix-epoch integer.

| Go field        | JSON key            | Rust field             | Format              |
|-----------------|----------------------|------------------------|---------------------|
| `Name`          | `name`               | `name`                 |                     |
| `IsMaster`      | `is_master`          | `is_master`            |                     |
| `Status`        | `status`             | `status`               | SyncStatus string   |
| `LastUpdate`    | `last_update`        | `last_update`          | `"2024-06-15 10:30:45 +0000"` |
| `LastUpdateTs`  | `last_update_ts`     | `last_update_ts`       | integer seconds     |
| `LastStarted`   | `last_started`       | `last_started`         | same format         |
| `LastStartedTs` | `last_started_ts`    | `last_started_ts`      | integer seconds     |
| `LastEnded`     | `last_ended`         | `last_ended`           | same format         |
| `LastEndedTs`   | `last_ended_ts`      | `last_ended_ts`        | integer seconds     |
| `Scheduled`     | `next_schedule`      | `scheduled`            | same format         |
| `ScheduledTs`   | `next_schedule_ts`   | `scheduled_ts`         | integer seconds     |
| `Upstream`      | `upstream`           | `upstream`             |                     |
| `Size`          | `size`               | `size`                 |                     |

## Manager HTTP API

All endpoints match Go's `manager/server.go` route table exactly unless
marked **Rust addition**:

| Path                          | Method | Purpose                                          |
|-------------------------------|--------|--------------------------------------------------|
| `/ping`                       | GET    | Health check                                     |
| `/jobs`                       | GET    | List all mirror statuses (Web format)            |
| `/jobs`                       | HEAD   | **Rust addition** — same as GET, headers only    |
| `/jobs/disabled`              | DELETE | Flush disabled mirror statuses                   |
| `/jobs/:name`                 | GET    | **Rust addition** — `MirrorStatus[]` for one mirror across all workers (includes `error_msg`) |
| `/workers`                    | GET    | List registered workers                          |
| `/workers`                    | POST   | Register a new worker                            |
| `/workers/:id`                | DELETE | Delete a worker                                  |
| `/workers/:id/heartbeat`      | POST   | **Rust addition** — explicit heartbeat           |
| `/workers/:id/jobs`           | GET    | List mirror statuses for a worker                |
| `/workers/:id/jobs/:job`      | POST   | Update a mirror's status                         |
| `/workers/:id/jobs/:job/size` | POST   | Update a mirror's size                           |
| `/workers/:id/schedules`      | POST   | Update scheduling info                           |
| `/cmd`                        | POST   | Send command to a worker                         |
| `/metrics`                    | GET    | **Rust addition** — Prometheus metrics           |

Response bodies use `{"message": "..."}` for info and `{"error": "..."}` for errors,
matching Go's `_infoKey` / `_errorKey` convention.

### `GET /jobs/:name` response

Returns `MirrorStatus[]` (not `WebMirrorStatus[]`), so it includes `worker`
and `error_msg` fields. Multiple entries are returned when the same mirror
name runs on multiple workers.

### `GET /metrics` — Prometheus exposition

Format: text 0.0.4 (`Content-Type: text/plain; version=0.0.4; charset=utf-8`).

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `tunasync_workers_total` | gauge | — | Number of registered workers |
| `tunasync_mirrors_total` | gauge | `status` | Mirror count per status (all 7 values always exported) |
| `tunasync_mirror_status` | gauge | `mirror`, `worker` | Status code: 0=none 1=pre-syncing 2=syncing 3=success 4=failed 5=paused 6=disabled |
| `tunasync_mirror_size_bytes` | gauge | `mirror`, `worker` | Mirror size in bytes; −1 if unknown or unparseable |
| `tunasync_mirror_last_success_timestamp_seconds` | gauge | `mirror`, `worker` | Unix timestamp of last successful sync; 0 if never |
| `tunasync_mirror_last_sync_duration_seconds` | gauge | `mirror`, `worker` | Duration of most recent completed sync in seconds; 0 if never run |

Size string parsing accepts `K/M/G/T/P` suffixes (base-1024), with or
without trailing `B` (e.g. `1.5G`, `500M`, `1GB`). Returns −1 for `""`
or `"unknown"`.

### `status_file`

When `files.status_file` is set (default `/var/lib/tunasync/tunasync.json`)
and the parent directory exists, the manager writes a `WebMirrorStatus[]`
JSON snapshot to that path every 30 seconds via an atomic `.tmp` → rename.
Skipped silently if the parent directory does not exist.

## Worker HTTP API

The worker exposes a single command endpoint matching Go:

| Path | Method | Purpose                       |
|------|--------|-------------------------------|
| `/`  | POST   | Receive `WorkerCmd` from manager |

Additional Rust-only endpoint (not in Go, harmless addition):

| Path   | Method | Purpose         |
|--------|--------|-----------------|
| `/jobs` | GET   | Job introspection |

### Command mapping (`WorkerCmd.cmd` → `CtrlAction`)

| `WorkerCmd.cmd` | `options`          | `CtrlAction`     | Go equivalent    |
|------------------|--------------------|------------------|------------------|
| `"start"`        | `{}`               | `Start`          | `jobStart`       |
| `"start"`        | `{"force": true}`  | `ForceStart`     | `jobForceStart`  |
| `"stop"`         | `{}`               | `Stop`           | `jobStop`        |
| `"disable"`      | `{}`               | `Disable`        | `jobDisable`     |
| `"restart"`      | `{}`               | `Restart`        | `jobRestart`     |
| `"ping"`         | `{}`               | `Ping`           | `jobPing`        |
| `"reload"`       | `{}`               | Reload (worker)  | SIGHUP           |

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

The manager DB adapters (`redb`, `sqlite`) are tested identically via the
`db_tests!` macro in `crates/manager/tests/integration.rs`.
## New optional MirrorStatus fields (Rust port)

The following fields were added to `MirrorStatus` in the Rust port.  They are
all tagged `#[serde(default)]` (deserialise absent → zero/false) and
`#[serde(skip_serializing_if = "is_default")]` (omit from JSON when zero).
This means:

- A Go manager receiving a Rust worker's status update simply ignores the
  new fields (Go's `encoding/json` ignores unknown keys by default).
- A Rust manager receiving status from an old Go worker gets the zero value
  for each field, which is the correct "unknown / not reported" sentinel.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `last_transferred_bytes` | `u64` | `0` | Bytes transferred in the last sync |
| `total_transferred_bytes` | `u64` | `0` | Cumulative bytes transferred |
| `consecutive_failures` | `u32` | `0` | Failures since last success |
| `stale` | `bool` | `false` | Manager has not seen a success in > stale_after |

### Serialisation contract

When all four fields are at their zero/false values the JSON object is
identical to what a Go worker would produce — the fields are simply absent.
This preserves full wire compatibility with the Go implementation.

### Webhook events (Rust manager only)

The Rust manager fires a fire-and-forget `{"text": "…"}` POST to
`notify.webhook_url` (if configured) on these events:

- Consecutive failures reaches `notify.alert_after_failures`
- Recovery from consecutive failures (first success after failures)
- Mirror transitions to/from the `stale` state

## Timezone semantics for cron and blackout

The Rust port introduces two new optional config fields for time-of-day
scheduling: `[global].timezone` and `[[mirrors]].timezone`. Both take IANA
timezone names (e.g. `"Asia/Shanghai"`, `"America/New_York"`, `"UTC"`).

**Default is UTC.** When both fields are empty, all cron expressions and
blackout windows are evaluated in UTC. This is a deliberate, documented
choice — implicit "use the host's local timezone" defaults are convenient
on a single-admin system but a footgun in a multi-region deployment.

Operators upgrading from the Go `tunasync` worker (which uses local time
implicitly) and relying on local-time cron/blackout schedules MUST set
`timezone` explicitly in `[global]` or per-mirror in `[[mirrors]]`.

Validation happens at startup: an invalid IANA name fails the worker with
a clear message, rather than silently falling back and producing the wrong
schedule.
