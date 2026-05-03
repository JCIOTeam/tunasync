# tunasync-rs

A Rust port of [`tuna/tunasync`](https://github.com/tuna/tunasync), the mirror job management tool that powers [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) and many other open-source mirror sites.

> **Status: Stages 1–5 complete.** Wire protocol, manager, worker runtime, providers, and hooks are all implemented and wire-compatible with Go. See [Roadmap](#roadmap).

## Why

The Go implementation has served TUNA well for years. This port exists for:

- **Distribution**: a single statically-linked binary with no Go runtime / cgo concerns
- **Footprint**: lower idle memory on small mirror sites
- **Correctness**: stronger compile-time guarantees around the worker's hook composition and lifecycle state machine

It is **not** a fork. It targets full wire compatibility with Go tunasync so that, during migration, a Rust manager can drive a Go worker (or vice versa).

## Architecture

Same design as upstream — see `docs/wire-compat.md` for the protocol mapping.

```
+------------+ +---+                  +---+
| Client API | |   |    Job Status    |   |    +----------+     +----------+
+------------+ |   +----------------->|   |--->|  mirror  +---->|  mirror  |
+------------+ |   |                  | w |    |  config  |     | provider |
| Worker API | | H |                  | o |    +----------+     +----+-----+
+------------+ | T |   Job Control    | r |                          |
+------------+ | T +----------------->| k |       +------------+     |
| Job/Status | | P |                  | e |       | mirror job |<----+
| Management | | S |                  | r |       +------^-----+
+------------+ |   |   Update Status  |   |    +---------+---------+
+------------+ |   <------------------+   |    |     Scheduler     |
|  redb /    | |   |                  |   |    +-------------------+
|  sqlite    | +---+                  +---+
+------------+
```

## Workspace layout

```
crates/
├── protocol/    # Wire types — JSON-compatible with Go's internal/msg.go
├── common/      # Logging, HTTP client, config loader
├── manager/     # Manager HTTP server (axum), redb/sqlite storage
├── worker/      # Worker runtime: scheduler, job state machine, providers, hooks
├── tunasync/    # Combined manager+worker dispatcher binary
└── tunasynctl/  # CLI control tool
```

## Build

```bash
cargo build --release
# binaries land at: target/release/{tunasync,tunasynctl}
```

Run the test suite, including JSON wire-compat tests:

```bash
cargo test --workspace
```

Lint and format:

```bash
cargo clippy --workspace
cargo fmt --all
```

## Roadmap

| Stage | Scope                                                                 | Status |
|-------|-----------------------------------------------------------------------|--------|
| 1     | Workspace, wire-compat protocol types, common utilities               | ✅ done |
| 2     | Manager: HTTP routes, redb + sqlite adapters, worker lifecycle        | ✅ done |
| 3     | Worker: scheduler, job state machine, cmd_provider, manager handshake | ✅ done |
| 4     | rsync + two-stage rsync providers, exec_post & loglimit hooks         | ✅ done |
| 5     | cgroup, docker, zfs, btrfs-snapshot hooks                             | ✅ done |
| 6     | tunasynctl HTTP wiring, CI/release, production hardening              | 🔧 WIP |

## Wire compatibility

`tunasync-protocol` round-trips every JSON shape Go produces. See `crates/protocol/tests/wire_compat.rs` for the conformance suite. Notable subtleties:

- `SyncStatus::PreSyncing` serialises as `"pre-syncing"` (hyphen, not underscore) — matches Go.
- Go's `time.Time{}` zero value (`"0001-01-01T00:00:00Z"`) is preserved by `tunasync_protocol::zero_time()`. Don't use `chrono::DateTime::default()` for "unset" timestamps — that's the Unix epoch, a different sentinel.
- `MirrorStatus::scheduled` is named `next_schedule` on the wire (matching Go's struct tag).
- `MirrorSchedule::mirror_name` is named `name` on the wire.

### Known differences from Go

These are intentional fixes or minor additions that don't break wire compatibility:

| Area | Difference | Reason |
|------|-----------|--------|
| Manager: size update | Uses `&&` instead of Go's buggy `||` condition | Go bug: `len(msg.Size) > 0 || msg.Size != "unknown"` always true |
| Manager: heartbeat | Added `POST /workers/:id/heartbeat` endpoint | Go tracks liveness implicitly; explicit endpoint is more robust |
| Manager: deleteWorker | Returns 400 on invalid worker ID vs Go's 500 | More useful error response |
| Worker: HTTP response | Returns 200 with `{ "msg": "OK" }` — matches Go | Fixed: previously returned empty body |
| Worker: unknown mirror | Returns 404 — matches Go | Fixed: previously silent drop |
| Worker: unknown cmd | Returns 406 — matches Go | Fixed: previously silent drop |
| Worker: GET /jobs | Additional introspection endpoint | Not in Go, but harmless addition |

## License

GPL-3.0-or-later, same as upstream.