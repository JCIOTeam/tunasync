# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

A Rust port of [`tuna/tunasync`](https://github.com/tuna/tunasync), the mirror job management tool that powers [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) and many other open-source mirror sites.

> **中文文档**: [README_zh.md](README_zh.md)

## Download

Pre-built binaries for Linux (x86_64, aarch64, armv7, riscv64, loongarch64, musl) and macOS are available at [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases).

## Migrating from the Go version

tunasync-rs is **wire-compatible** with the Go implementation: a Rust manager can drive Go workers and vice versa. The config file format (TOML) uses the same keys, so existing Go config files work without modification.

### Migration steps

1. **Stop the Go services** — `systemctl stop tunasync-manager tunasync-worker`.
2. **Install the Rust binaries** — download from [Releases](https://github.com/JCIOTeam/tunasync/releases) or build from source, then copy `tunasync` and `tunasynctl` to `/usr/bin/`.
3. **Keep the config files** — the Rust version reads the same TOML format. No changes needed.
4. **Choose a DB backend** — if the Go version uses BoltDB (the default), switch to `sqlite` or `redb`. The Rust version does **not** read BoltDB files, so you need to let it create a fresh DB. Existing mirror states will be re-populated when workers register. If the Go version uses Redis, no migration is needed — both versions can share the same Redis instance.
5. **Restart** — `systemctl start tunasync-manager tunasync-worker`.
6. **Verify** — `tunasynctl list --all -p <port>` should show all mirrors.

### Compatibility notes

| Feature | Go version | Rust version |
|---------|-----------|--------------|
| Config format | TOML, same keys | ✅ Compatible |
| `[include]` section | Glob-based mirror configs | ✅ Supported |
| `{{.Name}}` in log_dir | Template expansion | ✅ Supported |
| SIGHUP hot-reload | Reload mirror config | ✅ Supported |
| DB backends | BoltDB, Redis, MySQL | redb, sqlite, redis (no MySQL yet) |
| Docker hook | Container wrapping | ✅ Compatible |
| Cgroup hook | v1/v2 memory limit | ✅ Compatible |
| Btrfs/ZFS hooks | Snapshot before/after | ✅ Compatible |
| Wire protocol | JSON REST API | ✅ Fully compatible |
| `tunasynctl` CLI | Same commands | ✅ Compatible (`-p`, `-w` short flags) |

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
|  redis     |                          |
+------------+                          +
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
name = "test_worker"                     # Worker identity (used as worker_id)
log_dir = "/tmp/tunasync/log"            # Default log directory
mirror_dir = "/tmp/tunasync"             # Default mirror data directory
concurrent = 10                          # Max concurrent sync jobs (0 = unlimited)
interval = 120                           # Default sync interval in minutes
retry = 3                                # Default retry count on failure
timeout = 600                            # Default timeout in seconds (0 = no timeout)
# rsync_options = ["--no-motd"]          # Global rsync options appended to every rsync job
# exec_on_success = []                   # Global post-success commands
# exec_on_failure = []                   # Global post-failure commands

[manager]
api_base = "http://localhost:14242"      # Manager URL (single)
# api_base_list = [                      # Multiple managers (overrides api_base)
#     "http://mgr1:14242",
#     "http://mgr2:14242",
# ]
# ca_cert = "/etc/tunasync/ca.crt"       # CA cert for TLS

[server]
hostname = "localhost"                   # Public hostname for worker URL
listen_addr = "127.0.0.1"               # Worker HTTP listener address
listen_port = 6000                       # Worker HTTP listener port
# ssl_cert = ""                          # Worker TLS cert
# ssl_key = ""                           # Worker TLS key

# Docker hook — wraps sync jobs in containers (mutually exclusive with cgroup)
[docker]
enable = false
# volumes = ["/data:/data"]              # Global Docker volumes
# options = ["--network=host"]           # Global Docker options

# Cgroup hook — limits CPU/memory per job (Linux only, mutually exclusive with docker)
[cgroup]
enable = false
# base_path = "/sys/fs/cgroup"          # Cgroup mount point
# group = "tunasync"                    # Cgroup slice name

# ZFS snapshot hook
[zfs]
enable = false
# zpool = "tank"                        # ZFS pool name

# Btrfs snapshot hook (Linux only)
[btrfs_snapshot]
enable = false
# snapshot_path = "/snapshots"          # Btrfs snapshot directory

# Include additional mirror configs via glob
[include]
# include_mirrors = "/etc/tunasync/mirrors.d/*.conf"   # Go-compatible [include] section

# --- Mirror definitions ---
# provider types: "command", "rsync", "two-stage-rsync"

# Simple rsync mirror
[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv4 = true
# interval = 60                          # Override global interval (minutes)
# retry = 3                              # Override global retry count
# timeout = 600                          # Override global timeout (seconds)
# mirror_dir = "/data/elvish"            # Override global mirror_dir
# log_dir = "/var/log/tunasync/elvish"   # Override global log_dir
# username = "mirror"                    # Rsync username
# password = "secret"                    # Rsync password (env: RSYNC_PASSWORD)
# exclude_file = "/etc/tunasync/exclude.txt"  # Rsync exclude-from file

# Command mirror (arbitrary shell command)
[[mirrors]]
name = "myrepo"
provider = "command"
upstream = "https://example.com/repo/"
command = "wget -m -np -nd {{upstream}} -P {{working_dir}}"
# fail_on_match = "error|failed"         # Fail if regex matches log output
# size_pattern = "Total size: ([\\d.]+[KMG])"  # Extract size from log
# success_exit_codes = [0, 1, 2]         # Treat these exit codes as success
# env = { "MY_VAR" = "value" }           # Extra environment variables

# Two-stage rsync (for large repos like Debian)
[[mirrors]]
name = "debian"
provider = "two-stage-rsync"
upstream = "rsync://ftp.debian.org/debian/"
stage1_profile = "debian"                # "debian" or "debian-oldstyle"
use_ipv4 = true
# command = "/usr/local/bin/rsync"       # Override rsync binary path

# Docker-wrapped mirror
# [[mirrors]]
# name = "docker-mirror"
# provider = "command"
# upstream = "https://example.com/"
# command = "sync-script {{upstream}}"
# docker_image = "sync-runner:latest"    # Docker image (enables docker hook)
# docker_volumes = ["/data:/data"]       # Per-mirror Docker volumes
# docker_options = ["--network=host"]    # Per-mirror Docker options
# memory_limit = "512M"                  # Memory limit (K/M/G suffix)

# Mirror with custom role and hooks
# [[mirrors]]
# name = "slave-mirror"
# provider = "rsync"
# upstream = "rsync://master.example.com/mirror/"
# role = "slave"                         # "master" (default) or "slave"
# exec_on_success = ["curl -s http://notify/success"]
# exec_on_failure = ["curl -s http://notify/failure"]
```

### Manager config (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"                       # Listen address
port = 14242                             # Listen port (default: 14242)
# ssl_cert = "/etc/tunasync/server.crt"  # TLS certificate
# ssl_key = "/etc/tunasync/server.key"   # TLS private key
# debug = true                           # Enable debug logging

[files]
db_type = "sqlite"                       # "redb" (default), "sqlite", or "redis"
db_file = "/tmp/tunasync/manager-db/tunasync.db"  # DB file path
# ca_cert = ""                           # CA cert for worker TLS verification
```

Supported `db_type` values: `redb` (default), `sqlite`, `redis`. When using Redis, set `db_file` to a Redis URL (e.g. `redis://localhost:6379/0`). Data is wire-compatible with Go — both versions can share the same Redis instance.

### tunasynctl config (`~/.config/tunasync/ctl.conf`)

```toml
manager_addr = "127.0.0.1"
manager_port = 14242
```

Or specify on the command line:

```bash
tunasynctl list --all -p 14242
tunasynctl start elvish -p 14242 -w test_worker
```

### Running

```bash
tunasync manager -c ~/tunasync_demo/manager.conf
tunasync worker -c ~/tunasync_demo/worker.conf
```

Mirror data will be synced into `/tmp/tunasync/`.

### Control

```bash
# List all mirror statuses
tunasynctl list --all -p 14242

# Start / stop / disable a specific mirror
tunasynctl start elvish -p 14242
tunasynctl stop elvish -p 14242
tunasynctl disable elvish -p 14242

# Reload worker config (hot-reload without restart)
tunasynctl reload -p 14242
```

### Security

Worker-manager communication uses HTTP(S). If both run on the same machine, plain HTTP is sufficient — leave `ssl_cert` / `ssl_key` empty on the manager and `ca_cert` empty on the worker, with `api_base` using `http://`.

For encrypted communication, set `ssl_cert` and `ssl_key` on the manager, `ca_cert` on the worker, and use `https://` as the `api_base` prefix.

### Running as a systemd service

Example service files are provided in the `systemd/` directory.

```bash
# Create tunasync user
sudo useradd -r -s /bin/false tunasync

# Install binaries
sudo cp target/release/tunasync /usr/bin/
sudo cp target/release/tunasynctl /usr/bin/

# Install config and service files
sudo mkdir -p /etc/tunasync /var/lib/tunasync
sudo cp systemd/tunasync-manager.service /etc/systemd/system/
sudo cp systemd/tunasync-worker.service /etc/systemd/system/
sudo cp manager.conf /etc/tunasync/
sudo cp worker.conf /etc/tunasync/

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable --now tunasync-manager
sudo systemctl enable --now tunasync-worker

# Hot-reload worker config (reads config from disk, applies diff)
sudo systemctl reload tunasync-worker
```

The `--with-systemd` flag in the service files suppresses timestamps and ANSI colours in log output, since systemd journal already adds timestamps.

## Building

Requires Rust stable (>= 1.80). See `rust-toolchain.toml`.

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
+-- protocol/    # Wire types -- JSON-compatible with Go's internal/msg.go
+-- common/      # Logging, HTTP client helpers, config loader
+-- manager/     # Manager HTTP server (axum), redb/sqlite storage
+-- worker/      # Worker runtime: scheduler, job state machine, providers, hooks
+-- tunasync/    # Combined manager+worker dispatcher binary
+-- tunasynctl/  # CLI control tool
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
| 1 | Workspace, wire-compat protocol types, common utilities | Done |
| 2 | Manager: HTTP routes, redb + sqlite adapters, worker lifecycle | Done |
| 3 | Worker: scheduler, job state machine, cmd_provider, manager handshake | Done |
| 4 | rsync + two-stage rsync providers, exec_post & loglimit hooks | Done |
| 5 | cgroup, docker, zfs, btrfs hooks | Done |
| 6 | tunasynctl CLI, CI/release, production hardening | In progress |

## Wire compatibility

`tunasync-protocol` round-trips every JSON shape Go produces. See `crates/protocol/tests/wire_compat.rs` for the conformance suite. Notable subtleties:

- `SyncStatus::PreSyncing` serialises as `"pre-syncing"` (hyphen) -- matches Go.
- Go's `time.Time{}` zero value (`"0001-01-01T00:00:00Z"`) is preserved by `tunasync_protocol::zero_time()`. Do **not** use `chrono::DateTime::default()` for "unset" timestamps -- that's the Unix epoch, a different sentinel.
- `MirrorStatus::scheduled` -> `next_schedule` on the wire (matching Go's struct tag).
- `MirrorSchedule::mirror_name` -> `name` on the wire.

### Known differences from Go

| Area | Difference | Reason |
|------|-----------|--------|
| Manager: size update | `&&` instead of Go's buggy `||` | Go bug: condition always true |
| Manager: heartbeat | Added `POST /workers/:id/heartbeat` | More robust than implicit refresh |
| Manager: deleteWorker | 400 on invalid ID (Go: 500) | More useful error |
| Manager: DB | Redis backend added | redb, sqlite, redis all supported |
| Worker: GET /jobs | Additional introspection endpoint | Harmless addition |

## License

GPL-3.0-or-later, same as upstream.