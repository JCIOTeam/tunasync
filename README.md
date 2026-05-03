# tunasync-rs

A Rust port of [`tuna/tunasync`](https://github.com/tuna/tunasync), the mirror job management tool that powers the [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) and many other open-source mirror sites.

> **Status: WIP, stage 1 of 6.** Wire protocol types and crate skeleton are in place; manager / worker runtimes are stubs. See [Roadmap](#roadmap).

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
├── manager/     # Manager server (stage 2)
├── worker/      # Worker runtime (stages 3–5)
├── tunasync/    # Combined manager+worker dispatcher binary
└── tunasynctl/  # CLI control tool (stage 6)
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

## Roadmap

| Stage | Scope                                                                 | Status |
|-------|-----------------------------------------------------------------------|--------|
| 1     | Workspace, wire-compat protocol types, common utilities               | ✅ done |
| 2     | Manager: HTTP routes, redb + sqlite adapters, worker lifecycle        | ⏳ next |
| 3     | Worker: scheduler, job state machine, `cmd_provider`, manager handshake | ⏳     |
| 4     | rsync + two-stage rsync providers, `exec_post` & `loglimit` hooks     | ⏳     |
| 5     | cgroup, docker, zfs, btrfs-snapshot hooks                             | ⏳     |
| 6     | `tunasynctl` HTTP wiring, integration tests, release packaging        | ⏳     |

## Wire compatibility

`tunasync-protocol` round-trips every JSON shape Go produces. See `crates/protocol/tests/wire_compat.rs` for the conformance suite. Notable subtleties:

- `SyncStatus::PreSyncing` serialises as `"pre-syncing"` (hyphen, not underscore) — matches Go.
- Go's `time.Time{}` zero value (`"0001-01-01T00:00:00Z"`) is preserved by `tunasync_protocol::zero_time()`. Don't use `chrono::DateTime::default()` for "unset" timestamps — that's the Unix epoch, a different sentinel.
- `MirrorStatus::scheduled` is named `next_schedule` on the wire (matching Go's struct tag).
- `MirrorSchedule::mirror_name` is named `name` on the wire.

## License

GPL-3.0-or-later, same as upstream.
