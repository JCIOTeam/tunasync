# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

A Rust port of [`tuna/tunasync`](https://github.com/tuna/tunasync), the mirror job management tool that powers [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) and many other open-source mirror sites.

> **中文文档**: [README_zh.md](README_zh.md)

## Download

Pre-built binaries for Linux (x86_64, aarch64, armv7, riscv64, loongarch64, musl) and macOS are available at [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases).

## Design

Same architecture as upstream — see `docs/wire-compat.md` for the protocol mapping.

### Architecture

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

### Job Run Process

```
         ┌─────────────────────────────┐
         │       PreSyncing             │
         │  (pre-job → pre-exec hooks) │
         └──────────────┬──────────────┘
                        │
         ┌──────────────▼──────────────┐
         │         Syncing              │
         │   (job run → post-exec)     │
         └──────────────┬──────────────┘
                        │
              ┌─────────┴─────────┐
              │                   │
    ┌─────────▼──────┐  ┌────────▼───────┐
    │     Success     │  │     Failed      │
    │ (post-success) │  │  (post-fail)    │
    └────────────────┘  └────────────────┘
```

## Getting Started

Create directories:

```bash
mkdir -p ~/tunasync_demo /tmp/tunasync/{log,manager-db}
```

### Worker config (`~/tunasync_demo/worker.conf`)

```toml
[global]
name = "test_worker"
log_dir = "/tmp/tunasync/log"
mirror_dir = "/tmp/tunasync"
concurrent = 10
interval = 120

[manager]
api_base = "http://localhost:12345"

[server]
hostname = "localhost"
listen_addr = "127.0.0.1"
listen_port = 6000

[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv6 = false
```

### Manager config (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"
port = 12345

[files]
db_type = "sqlite"
db_file = "/tmp/tunasync/manager-db/tunasync.db"
```

Supported `db_type` values: `redb` (default), `sqlite`. Note: the Go version also supports `redis`; this port does not yet include a Redis backend.

### Running

```bash
tunasync manager -c ~/tunasync_demo/manager.conf
tunasync worker -c ~/tunasync_demo/worker.conf
```

Mirror data will be synced into `/tmp/tunasync/`.

### Control

```bash
# List all mirror statuses
tunasynctl list --all -p 12345

# Start / stop / disable a specific mirror
tunasynctl start elvish -p 12345
tunasynctl stop elvish -p 12345
tunasynctl disable elvish -p 12345
```

`tunasynctl` also reads config from `~/.config/tunasync/ctl.conf` or `/etc/tunasync/ctl.conf`:

```toml
manager_addr = "127.0.0.1"
manager_port = 12345
```

### Security

Worker–manager communication uses HTTP(S). If both run on the same machine, plain HTTP is sufficient — leave `ssl_cert` / `ssl_key` empty on the manager and `ca_cert` empty on the worker, with `api_base` using `http://`.

For encrypted communication, set `ssl_cert` and `ssl_key` on the manager, `ca_cert` on the worker, and use `https://` as the `api_base` prefix.

## Building

Requires Rust stable (≥ 1.80). See `rust-toolchain.toml`.

```bash
# Build release binaries
cargo build --release
# Binaries land at: target/release/{tunasync, tunasynctl}

# Run test suite (including wire-compat conformance)
cargo test --workspace

# Lint
cargo clippy --workspace
cargo fmt --all
```

## Workspace layout

```
crates/
├── protocol/    # Wire types — JSON-compatible with Go's internal/msg.go
├── common/      # Logging, HTTP client helpers, config loader
├── manager/     # Manager HTTP server (axum), redb/sqlite storage
├── worker/      # Worker runtime: scheduler, job state machine, providers, hooks
├── tunasync/    # Combined manager+worker dispatcher binary
└── tunasynctl/  # CLI control tool
```

### Providers

| Provider | Description |
|----------|-------------|
| `command` | Arbitrary shell command |
| `rsync` | Classic rsync mirror |
| `two-stage-rsync` | Stage-1 (quick list) + Stage-2 (full sync) |

### Hooks

| Hook | Description |
|------|-------------|
| `exec_post` | Run command after sync phases |
| `loglimit` | Rotate / truncate logs |
| `docker` | Wrap sync inside a Docker container |
| `cgroup` | Limit CPU/memory via cgroups |
| `btrfs_snapshot` | Btrfs snapshot before/after sync |
| `zfs_snapshot` | ZFS snapshot before/after sync |

## Roadmap

| Stage | Scope | Status |
|-------|-------|--------|
| 1 | Workspace, wire-compat protocol types, common utilities | ✅ |
| 2 | Manager: HTTP routes, redb + sqlite adapters, worker lifecycle | ✅ |
| 3 | Worker: scheduler, job state machine, cmd_provider, manager handshake | ✅ |
| 4 | rsync + two-stage rsync providers, exec_post & loglimit hooks | ✅ |
| 5 | cgroup, docker, zfs, btrfs hooks | ✅ |
| 6 | tunasynctl CLI, CI/release, production hardening | 🔧 |

## Wire compatibility

`tunasync-protocol` round-trips every JSON shape Go produces. See `crates/protocol/tests/wire_compat.rs` for the conformance suite. Notable subtleties:

- `SyncStatus::PreSyncing` serialises as `"pre-syncing"` (hyphen) — matches Go.
- Go's `time.Time{}` zero value (`"0001-01-01T00:00:00Z"`) is preserved by `tunasync_protocol::zero_time()`. Do **not** use `chrono::DateTime::default()` for "unset" timestamps — that's the Unix epoch, a different sentinel.
- `MirrorStatus::scheduled` → `next_schedule` on the wire (matching Go's struct tag).
- `MirrorSchedule::mirror_name` → `name` on the wire.

### Known differences from Go

| Area | Difference | Reason |
|------|-----------|--------|
| Manager: size update | `&&` instead of Go's buggy `||` | Go bug: condition always true |
| Manager: heartbeat | Added `POST /workers/:id/heartbeat` | More robust than implicit refresh |
| Manager: deleteWorker | 400 on invalid ID (Go: 500) | More useful error |
| Manager: DB | No Redis backend yet | Only redb/sqlite supported |
| Worker: GET /jobs | Additional introspection endpoint | Harmless addition |

## License

GPL-3.0-or-later, same as upstream.