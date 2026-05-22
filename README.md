# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://img.shields.io/github/v/release/JCIOTeam/tunasync?label=release)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

A Rust port of [`tuna/tunasync`](https://github.com/tuna/tunasync), the mirror job management tool that powers [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) and many other open-source mirror sites.

> **中文文档**: [README_zh.md](README_zh.md)

## Download

Pre-built binaries for Linux (x86_64, aarch64, armv7, riscv64, loongarch64, x86_64-musl, aarch64-musl) are available at [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases). Each archive contains `tunasync` and `tunasynctl` only.

`tunasync-migrate` is not included in the release archives — it is a one-time migration tool that most users won't need after switching to the Rust version. You can obtain it by:

1. Building from source: `cargo build --release -p tunasync-migrate`
2. Downloading from the [CI artifacts](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml) of any successful release build

## Migrating from the Go version

tunasync-rs is **wire-compatible** with the Go implementation: a Rust manager can drive Go workers and vice versa. The config file format (TOML) uses the same keys, so existing Go config files work without modification.

> **Note:** The Go version defaults to port **12345**, while the Rust version defaults to port **14242**. If you are migrating, either change the Rust config to match the Go port, or update the worker `api_base` and `tunasynctl` config accordingly.

### Migration steps

1. **Install the Rust binaries** — download `tunasync` and `tunasynctl` from [Releases](https://github.com/JCIOTeam/tunasync/releases) or build from source, then copy to `/usr/bin/`. If you need `tunasync-migrate`, build it separately: `cargo build --release -p tunasync-migrate`.
2. **Keep the config files** — the Rust version reads the same TOML format. No changes needed.
3. **Migrate data** — the Rust version uses a different default DB backend (redb instead of BoltDB). If the Go version uses BoltDB (the default), you need to export the data with `tunasync-migrate`. Two modes are available:

   **Option A — Offline migration (recommended, no downtime constraint):**
   Stop the Go manager first, then read the bolt file directly:
   ```bash
   systemctl stop tunasync-manager tunasync-worker

   # Point at the Go bolt file (default: /var/lib/tunasync/tunasync.db)
   tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db
   ```

   **Option B — Online migration (Go manager still running):**
   Useful when you want to prepare the new DB while the Go version is still serving:
   ```bash
   # Go manager defaults to port 12345
   tunasync-migrate http://localhost:12345 /var/lib/tunasync/new.db
   ```

   After either option, update the Rust manager config:
   ```toml
   [files]
   db_type = "sqlite"
   db_file = "/var/lib/tunasync/new.db"
   ```

   **If the Go version uses Redis**, no migration is needed — both versions can share the same Redis instance directly.

4. **Stop the Go services** (if you haven't already) — `systemctl stop tunasync-manager tunasync-worker`.
5. **Start the Rust services** — `systemctl start tunasync-manager tunasync-worker`.
6. **Verify** — `tunasynctl list` should show all mirrors with their last-sync timestamps preserved.

### Compatibility notes

| Feature | Go version | Rust version |
|---------|-----------|--------------|
| Config format | TOML, same keys | ✅ Compatible |
| `[include]` section | Glob-based mirror configs | ✅ Supported |
| `{{.Name}}` in log_dir | Template expansion | ✅ Supported |
| SIGHUP hot-reload | Reload mirror config | ✅ Supported |
| DB backends | BoltDB, LevelDB, Badger, Redis | redb, sqlite, redis |
| Docker hook | Container wrapping | ✅ Compatible |
| Cgroup hook | v1/v2 memory limit | ✅ Compatible |
| Btrfs/ZFS hooks | Snapshot before/after | ✅ Compatible |
| Wire protocol | JSON REST API | ✅ Fully compatible |
| Default port | 12345 | 14242 |
| `tunasynctl` CLI | Same commands | ✅ Compatible (`-p`, `-w` short flags) |

## New features in the Rust port

The following features are unique to tunasync-rs and have no Go equivalent. All are opt-in via config and default to off, so existing Go configs work unchanged.

### Disk quota pre-sync check

Skip a sync if free disk space falls below a threshold. The mirror stays in the scheduler queue and retries at its next interval.

```toml
[[mirrors]]
name = "debian"
disk_quota = "100G"   # skip sync if < 100 GiB free in mirror dir
```

### Cron-style scheduling

Schedule syncs with a standard 5-field cron expression instead of a fixed interval. Overrides the `interval` field when set.

```toml
[[mirrors]]
name = "kernel"
cron = "0 3 * * *"   # daily at 03:00 (in the mirror's effective timezone)
```

Accepts both 5-field POSIX (`minute hour dom month dow`) and the cron-crate 6/7-field format. Invalid expressions cause the worker to fail at startup with a clear error.

### Timezone-aware scheduling

Cron expressions and blackout windows are interpreted in the **UTC timezone by default**. Set `timezone` to use a local time:

```toml
[global]
timezone = "Asia/Shanghai"   # all mirrors default to CST

[[mirrors]]
name = "euromirror"
timezone = "Europe/Berlin"   # per-mirror override
cron = "0 3 * * *"           # fires at 03:00 CET/CEST, not 03:00 UTC
```

Valid values are IANA timezone names (e.g. `"Asia/Shanghai"`, `"America/New_York"`, `"Europe/Berlin"`, `"UTC"`). Invalid names cause the worker to fail at startup. The timezone applies to both `cron` and `blackout` windows for that mirror.

> **Upgrading from Go:** The Go worker uses the host's local timezone implicitly. If your cron/blackout schedules relied on local time, set `timezone` explicitly when switching to the Rust version.

### Blackout windows

Suppress new syncs during busy periods. In-progress syncs are never interrupted — only new starts are gated. When the scheduler fires a job during a blackout, it defers by 5 minutes and tries again.

```toml
[[mirrors]]
name = "debian"
# Interpreted in the mirror's effective timezone (see above — defaults to UTC).
blackout = ["08:00-18:00 Mon-Fri", "22:00-04:00"]
```

Format: `"HH:MM-HH:MM [<days>]"`. The optional day specifier accepts weekday ranges (`Mon-Fri`, `Sat-Sun`), single days (`Mon`), or `daily` (equivalent to omitting the field). Midnight wraparound is supported: `"22:00-04:00"` covers 22:00–23:59 and 00:00–04:00.

### Job priority

Higher-priority mirrors get a concurrency slot ahead of lower-priority ones when many jobs are waiting.

```toml
[[mirrors]]
name = "critical"
priority = 90   # default is 50; higher = runs first
```

### Per-upstream concurrency limit

Limit how many mirrors may sync from the same upstream host simultaneously, independent of the global `concurrent` setting.

```toml
[global.per_upstream_concurrent]
"rsync.kernel.org" = 2
"ftp.debian.org"   = 1
```

Changes to this map take effect on the next SIGHUP / `tunasynctl reload` (hot-reload supported). The global `concurrent` limit still applies; the per-upstream limit adds an additional constraint.

### Upstream probe with fallback

Check upstream reachability before starting a sync. If the primary upstream is unreachable, the fallback list is tried concurrently with a 15-second timeout per URL. The sync is skipped (not failed permanently) if all URLs are unreachable.

```toml
[[mirrors]]
name = "kernel"
check_upstream = true
upstream_fallback = ["rsync://mirror.example.com/kernel/"]
```

The fallback list is for health-checking only — the actual sync data source is always `upstream`.

### Atomic publish

Rsync into a staging directory first, then atomically swap it with the publish directory using `renameat2(RENAME_EXCHANGE)`. Users always see either the complete old or complete new content; there is no window where the publish path is missing or half-synced.

```toml
[[mirrors]]
name = "debian"
atomic_publish = true
```

**Staging directory location**, resolved in this order:

1. `[[mirrors]].staging_dir` (per-mirror override)
2. `[global].staging_dir` (worker-wide base)
3. `<log_dir>/staging/<name>` (legacy fallback, requires `log_dir` and `mirror_dir` to share a filesystem)

For options 1 and 2, the mirror name is appended automatically (so `staging_dir = "/srv/mirrors/.staging"` produces `/srv/mirrors/.staging/<name>/` per mirror).

**Requirement:** the staging directory and the publish directory (`mirror_dir`) must be on the **same filesystem** — `rename(2)` is only atomic within a single mount. The worker checks device IDs at sync time and refuses to start if they differ.

Most production deployments put logs on the OS disk and mirror data on a separate data disk, so the legacy fallback under `<log_dir>` doesn't work. Set `[global].staging_dir` to a path on the same filesystem as `mirror_dir`:

```toml
[global]
log_dir     = "/var/log/tunasync"      # OS disk
mirror_dir  = "/srv/mirrors"           # data disk
staging_dir = "/srv/mirrors/.staging"  # same fs as mirror_dir
```

The leading dot in `.staging` blocks nginx's default auto-index from serving in-progress contents; for stricter setups use a sibling path outside the document root entirely.

For mirrors served from a different data disk, override per-mirror:

```toml
[[mirrors]]
name        = "huge-archive"
mirror_dir  = "/data2/archive"
staging_dir = "/data2/staging"   # follows the data disk
atomic_publish = true
```

Falls back to a two-step rename (brief 404 window) only on kernels/filesystems that don't support `RENAME_EXCHANGE` (pre-3.15 kernels or some FUSE mounts); a warning is logged.

### Maintenance mode

Put the manager into read-only mode. All mutating API calls (sync updates, commands) return 503 until maintenance mode is disabled.

```bash
tunasynctl maintenance enable
tunasynctl maintenance status
tunasynctl maintenance disable
```

### Glob expansion in tunasynctl

`stop`, `disable`, `restart`, and `start` accept glob patterns to operate on multiple mirrors at once:

```bash
tunasynctl disable "debian-*"
tunasynctl restart "ubuntu-*" -w worker1
tunasynctl stop "*"
```

Exact names skip the glob fetch entirely (same performance as before).

### Flush stale mirrors

```bash
tunasynctl flush --stale-only    # flush only mirrors marked stale by the manager
tunasynctl stale                 # list all stale mirrors
```

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
|  redis     |
+------------+
```

### Job run process

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
concurrent = 10                          # Max concurrent sync jobs
interval = 120                           # Default sync interval in minutes
retry = 3                                # Default retry count on failure
timeout = 600                            # Default timeout in seconds (0 = no timeout)

# Timezone for cron expressions and blackout windows.
# Defaults to UTC when unset. Use IANA names: "Asia/Shanghai", "Europe/Berlin", etc.
# timezone = "UTC"

# Base staging directory for mirrors with atomic_publish = true.
# Must share a filesystem with mirror_dir. When unset, falls back to
# <log_dir>/staging/<name>. Most production deployments need this set
# explicitly because logs and mirror data are usually on different disks.
# staging_dir = "/srv/mirrors/.staging"

# Per-upstream host concurrency limits (optional).
# [global.per_upstream_concurrent]
# "rsync.kernel.org" = 2

[manager]
api_base = "http://localhost:14242"

[server]
hostname = "localhost"
listen_addr = "127.0.0.1"
listen_port = 6000

# --- Mirror definitions ---

[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv4 = true
# interval = 60           # minutes
# cron = "0 3 * * *"     # alternative: cron schedule (overrides interval)
# timezone = "Asia/Shanghai"  # per-mirror timezone (overrides [global].timezone)
# blackout = ["08:00-18:00 Mon-Fri"]  # suppress new syncs during these windows
# disk_quota = "100G"     # skip sync if free space < threshold
# priority = 50           # higher = runs first when semaphore contested
# check_upstream = false  # probe upstream before syncing
# upstream_fallback = []  # fallback URLs for the probe (health-check only)
# atomic_publish = false  # use staging dir + renameat2 swap
# staging_dir = ""        # per-mirror staging override (else uses [global].staging_dir)

[[mirrors]]
name = "myrepo"
provider = "command"
upstream = "https://example.com/repo/"
command = "wget -m -np -nd https://example.com/repo/ -P /path/to/mirror"
# The following env vars are always injected:
#   TUNASYNC_MIRROR_NAME, TUNASYNC_WORKING_DIR, TUNASYNC_UPSTREAM_URL,
#   TUNASYNC_LOG_DIR, TUNASYNC_LOG_FILE

[[mirrors]]
name = "debian"
provider = "two-stage-rsync"
upstream = "rsync://ftp.debian.org/debian/"
stage1_profile = "debian"
use_ipv4 = true
```

A fully-commented reference config is available at [`examples/worker.conf`](examples/worker.conf).

### Manager config (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"
port = 14242

[files]
db_type = "sqlite"     # "redb" (default), "sqlite", or "redis"
db_file = "/tmp/tunasync/manager-db/tunasync.db"
```

A fully-commented reference config is available at [`examples/manager.conf`](examples/manager.conf).

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
tunasync worker  -c ~/tunasync_demo/worker.conf
```

Mirror data will be synced into `/tmp/tunasync/`.

### Control

```bash
# List all mirror statuses
tunasynctl list --all -p 14242

# Start / stop / disable / restart a mirror (exact name or glob)
tunasynctl start  elvish   -p 14242
tunasynctl stop   "debian-*" -p 14242
tunasynctl disable elvish  -p 14242
tunasynctl restart "ubuntu-*" -p 14242

# Reload worker config (hot-reload without restart)
tunasynctl reload test_worker -p 14242

# Flush disabled mirrors; --stale-only limits to stale ones
tunasynctl flush              -p 14242
tunasynctl flush --stale-only -p 14242

# Maintenance mode
tunasynctl maintenance enable  -p 14242
tunasynctl maintenance status  -p 14242
tunasynctl maintenance disable -p 14242
```

### Shell completion

Pre-generated completion scripts for bash, zsh, and fish are in the `completions/` directory.

```bash
# bash (per user)
tunasynctl completion bash >> ~/.bashrc && source ~/.bashrc

# zsh
tunasynctl completion zsh >> ~/.zshrc && source ~/.zshrc

# fish
tunasynctl completion fish > ~/.config/fish/completions/tunasynctl.fish
```

### Language / 语言

`tunasynctl` detects the system locale and outputs Chinese when `LANG` starts with `zh`. Override with `TUNASYNCTL_LANG`:

```bash
TUNASYNCTL_LANG=zh tunasynctl list --format table
TUNASYNCTL_LANG=en tunasynctl list --format table
```

### Security

For encrypted worker-manager communication, set `ssl_cert` and `ssl_key` on the manager, `ca_cert` on the worker, and use `https://` in `api_base`. For same-host deployments, plain HTTP is sufficient.

### Running as a systemd service

Service files are provided in the `initscripts/` directory.

```bash
sudo useradd -r -s /bin/false tunasync
sudo cp target/release/{tunasync,tunasynctl} /usr/bin/
sudo cp initscripts/tunasync-manager.service /etc/systemd/system/
sudo cp initscripts/tunasync-worker.service  /etc/systemd/system/
sudo cp manager.conf worker.conf /etc/tunasync/
sudo systemctl daemon-reload
sudo systemctl enable --now tunasync-manager tunasync-worker

# Hot-reload worker config (reads from disk, diffs against running state)
sudo systemctl reload tunasync-worker
```

The `--with-systemd` flag suppresses timestamps and ANSI colours since systemd journal adds its own timestamps.

### Running with SysVinit (init.d)

```bash
sudo cp initscripts/tunasync-manager.initd /etc/init.d/tunasync-manager
sudo cp initscripts/tunasync-worker.initd  /etc/init.d/tunasync-worker
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker
sudo update-rc.d tunasync-manager defaults && sudo service tunasync-manager start
sudo update-rc.d tunasync-worker  defaults && sudo service tunasync-worker  start
sudo service tunasync-worker reload   # hot-reload
```

### Running with OpenRC (Alpine, Gentoo)

```bash
sudo cp initscripts/tunasync-manager.openrc /etc/init.d/tunasync-manager
sudo cp initscripts/tunasync-worker.openrc  /etc/init.d/tunasync-worker
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker
sudo rc-update add tunasync-manager default && sudo rc-service tunasync-manager start
sudo rc-update add tunasync-worker  default && sudo rc-service tunasync-worker  start
sudo rc-service tunasync-worker reload   # hot-reload
```

## Building

Requires Rust stable (≥ 1.80). See `rust-toolchain.toml`.

```bash
cargo build --release
# Binaries: target/release/{tunasync, tunasynctl}

cargo test --workspace       # full suite including wire-compat conformance
cargo clippy --workspace
cargo fmt --all
```

## Workspace layout

```
crates/
├── protocol/    # Wire types — JSON-compatible with Go's internal/msg.go
├── common/      # Logging, HTTP client helpers, config loader, util
├── manager/     # Manager HTTP server (axum), redb/sqlite/redis storage
├── worker/      # Worker runtime: scheduler, job FSM, providers, hooks
├── tunasync/    # Combined manager+worker dispatcher binary
├── tunasynctl/  # CLI control tool
└── migrate/     # Go→Rust data migration tool
```

### Providers

| Provider | Description |
|----------|-------------|
| `command` | Arbitrary shell command (env vars injected) |
| `rsync` | Classic rsync mirror |
| `two-stage-rsync` | Stage-1 (quick list) + Stage-2 (full sync), for large repos like Debian |

All three providers support `disk_quota`, `atomic_publish`, `check_upstream`, `upstream_fallback`, `cron`, `timezone`, `blackout`, and `priority`.

### Mirror sync status

| Status | Wire value | Meaning |
|--------|-----------|---------|
| `None` | `"none"` | Registered but never run |
| `PreSyncing` | `"pre-syncing"` | Pre-sync hooks running |
| `Syncing` | `"syncing"` | Main sync command running |
| `Success` | `"success"` | Last sync succeeded |
| `Failed` | `"failed"` | Last sync failed; `error_msg` has the reason |
| `Paused` | `"paused"` | Paused by operator (`tunasynctl stop`) |
| `Disabled` | `"disabled"` | Disabled; will not run until re-enabled |

Filter by status:

```bash
tunasynctl list --status failed
tunasynctl list --status syncing,pre-syncing
```

### Hooks

| Hook | Description |
|------|-------------|
| `exec_post` | Run command after sync phases |
| `loglimit` | Rotate / truncate logs |
| `docker` | Wrap sync inside a Docker container |
| `cgroup` | Limit CPU/memory via cgroups (Linux) |
| `btrfs_snapshot` | Btrfs snapshot before/after sync |
| `zfs_snapshot` | ZFS snapshot before/after sync |

## CLI Reference

### `tunasync`

```
tunasync manager [OPTIONS]
  -c, --config <CONFIG>    Config file [default: /etc/tunasync/manager.conf]
      --addr <ADDR>        Override listen address
      --port <PORT>        Override listen port (default 14242)
      --cert / --key       TLS certificate/key (enables HTTPS)
      --db-file / --db-type  Override DB path / type

tunasync worker [OPTIONS]
  -c, --config <CONFIG>    Config file [default: /etc/tunasync/worker.conf]
```

### `tunasynctl`

```
tunasynctl list       [--all] [-w WORKER] [--status STATUS] [--format json|table]
tunasynctl workers
tunasynctl start      <MIRROR|GLOB> [-w WORKER] [-f]
tunasynctl stop       <MIRROR|GLOB> [-w WORKER]
tunasynctl disable    <MIRROR|GLOB> [-w WORKER]
tunasynctl restart    <MIRROR|GLOB> [-w WORKER]
tunasynctl reload     <WORKER>
tunasynctl set-size   <MIRROR> <SIZE> [-w WORKER]
tunasynctl rm-worker  <WORKER>
tunasynctl flush      [--stale-only]
tunasynctl stale      [-w WORKER]
tunasynctl maintenance enable | disable | status
tunasynctl completion bash | zsh | fish | powershell
```

`MIRROR|GLOB` accepts exact names or glob patterns (`*`, `?`, `[…]`). Patterns are expanded by querying the manager; exact names skip the query for zero overhead.

Config file priority: `/etc/tunasync/ctl.conf` → `~/.config/tunasync/ctl.conf` → `--config FILE` → CLI flags.

### `tunasync-migrate`

```
tunasync-migrate <go-manager-url-or-bolt-file> <sqlite-output-file>

# Offline (Go manager stopped)
tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db

# Online (Go manager running)
tunasync-migrate http://localhost:12345 /var/lib/tunasync/new.db
```

## Wire compatibility

`tunasync-protocol` round-trips every JSON shape Go produces. See `crates/protocol/tests/wire_compat.rs` for the conformance suite.

Notable subtleties:

- `SyncStatus::PreSyncing` serialises as `"pre-syncing"` (hyphen) — matches Go.
- Go's `time.Time{}` zero value (`"0001-01-01T00:00:00Z"`) is preserved by `tunasync_protocol::zero_time()`. Do **not** use `chrono::DateTime::default()` for "unset" timestamps — that's the Unix epoch.
- `MirrorStatus::scheduled` → `next_schedule` on the wire (matching Go's struct tag).
- `MirrorStatus` extension fields (`last_transferred_bytes`, `total_transferred_bytes`, `consecutive_failures`, `stale`) use `#[serde(default, skip_serializing_if = "is_default")]` so a Go manager or worker sees plain Go-compatible JSON when all fields are zero/false.

### Known differences from Go

| Area | Difference | Reason |
|------|-----------|--------|
| Manager: size update | `&&` instead of Go's buggy `||` | Go bug: condition always true |
| Manager: heartbeat | Added `POST /workers/:id/heartbeat` | More robust than implicit refresh |
| Manager: deleteWorker | 400 on invalid ID (Go: 500) | More useful error |
| Manager: DB | redb, sqlite, redis | BoltDB/LevelDB/Badger not supported |
| Manager: GET /jobs/:name | Mirror detail with `error_msg` | New endpoint for frontends |
| Manager: maintenance mode | `POST/DELETE/GET /maintenance` | No Go equivalent |
| Worker: scheduling | Cron + timezone + blackout | No Go equivalent |
| Worker: disk quota | Pre-sync free space check | No Go equivalent |
| Worker: priority | `PrioritySemaphore` for job ordering | No Go equivalent |
| Worker: atomic publish | `renameat2(RENAME_EXCHANGE)` swap | No Go equivalent |
| Worker: upstream probe | Concurrent probe with 15s timeout | No Go equivalent |
| Default port | 14242 (Go: 12345) | Intentional, avoids conflict |

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/metrics` | Prometheus metrics |
| GET | `/jobs` | List all mirrors (summary) |
| GET | `/jobs/:name` | Mirror detail across all workers (includes `error_msg`) |
| DELETE | `/jobs/disabled` | Flush disabled mirror rows |
| GET | `/workers` | List registered workers (tokens redacted) |
| POST | `/workers` | Register a worker |
| DELETE | `/workers/:id` | Remove a worker |
| POST | `/workers/:id/heartbeat` | Worker heartbeat |
| GET | `/workers/:id/jobs` | List mirrors of one worker |
| POST | `/workers/:id/jobs/:job` | Update mirror status |
| POST | `/workers/:id/jobs/:job/size` | Update mirror size |
| POST | `/workers/:id/schedules` | Update schedules |
| POST | `/cmd` | Control command (start/stop/disable/reload) |
| POST | `/maintenance` | Enable maintenance mode |
| DELETE | `/maintenance` | Disable maintenance mode |
| GET | `/maintenance` | Get maintenance mode status |

### Prometheus metrics (`GET /metrics`)

The manager exposes a Prometheus-format scrape endpoint at `/metrics`. All per-mirror metrics carry `mirror="<name>",worker="<worker_id>"` labels.

| Metric | Type | Description |
|---|---|---|
| `tunasync_workers_total` | gauge | Number of currently registered workers |
| `tunasync_mirrors_total{status="..."}` | gauge | Count of mirrors in each status. The `status` label is one of `none`, `pre-syncing`, `syncing`, `success`, `failed`, `paused`, `disabled` |
| `tunasync_mirror_status{mirror,worker}` | gauge | Sync status code per mirror: `0=none 1=pre-syncing 2=syncing 3=success 4=failed 5=paused 6=disabled` |
| `tunasync_mirror_size_bytes{mirror,worker}` | gauge | Mirror size in bytes (parsed from rsync stats; `-1` if unknown/unparseable) |
| `tunasync_mirror_last_success_timestamp_seconds{mirror,worker}` | gauge | Unix timestamp of the last `last_update` write. `0` if the mirror has never reported a successful sync |
| `tunasync_mirror_last_sync_duration_seconds{mirror,worker}` | gauge | `last_ended - last_started` for the most recent completed sync (`0` if never run) |
| `tunasync_mirror_last_transferred_bytes{mirror,worker}` | gauge | Bytes transferred during the most recent sync (`0` if unknown). Resets on each new sync |
| `tunasync_mirror_total_transferred_bytes{mirror,worker}` | counter | Cumulative bytes transferred across all syncs of this `(mirror, worker)` pair. Monotonically non-decreasing — use `rate()` for traffic-per-second |

#### Recipes

Traffic over the last hour (bytes / sec):
```promql
rate(tunasync_mirror_total_transferred_bytes[1h])
```

Per-mirror sync duration percentile:
```promql
quantile by (mirror) (0.95, tunasync_mirror_last_sync_duration_seconds)
```

Mirrors that have failed at least one consecutive sync (the manager flips `mirror_status` from `3=success` to `4=failed` while preserving `last_success_timestamp_seconds`):
```promql
tunasync_mirror_status == 4
```

Mirrors which have not synced in 48 hours (use the timestamp metric and the Prometheus server's `time()`):
```promql
time() - tunasync_mirror_last_success_timestamp_seconds > 48 * 3600
```

#### How `total_transferred_bytes` accumulates

The worker reads `Total transferred file size` from each rsync `--stats` log (`extract_transferred_bytes_from_rsync_log` in `crates/common/src/util.rs`) and reports it as `last_transferred_bytes` in the status update. On the manager side (`update_job_of_worker` in `crates/manager/src/server.rs`), whenever `last_started` changes and `last_transferred_bytes > 0` the manager adds the new value to the running total:

```rust
if incoming.last_transferred_bytes > 0 && incoming.last_started != cur.last_started {
    incoming.total_transferred_bytes =
        cur.total_transferred_bytes + incoming.last_transferred_bytes;
}
```

This means the counter advances exactly once per completed sync. It does **not** include hook-uploaded data (e.g. `exec_post` scripts that publish to a CDN), and it does **not** reset when you `flush` a disabled mirror — the row is deleted entirely and the counter starts at zero if the mirror is later re-registered.

Non-rsync providers (`command`, `two-stage-rsync`) only report `last_transferred_bytes` when their `exec_log_file` matches the rsync stats format; otherwise both gauges stay at `0`.

## License

GPL-3.0-or-later, same as upstream.
