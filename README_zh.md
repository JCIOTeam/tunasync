# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://img.shields.io/github/v/release/JCIOTeam/tunasync?label=release)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

[`tuna/tunasync`](https://github.com/tuna/tunasync) 的 Rust 移植版。tunasync 是驱动 [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) 及众多开源镜像站的镜像同步管理工具。

> **English**: [README.md](README.md)

## 下载

Linux（x86_64、aarch64、armv7、riscv64、loongarch64、x86_64-musl、aarch64-musl）的预编译二进制文件可在 [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases) 下载。每个压缩包仅包含 `tunasync` 和 `tunasynctl`。

`tunasync-migrate` 不包含在发布包中 — 它是一次性迁移工具，大多数用户在切换到 Rust 版本后不再需要。获取方式：

1. 从源码构建：`cargo build --release -p tunasync-migrate`
2. 从任意成功的 release 构建 [CI artifacts](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml) 中下载

## 从 Go 版本迁移

tunasync-rs 与 Go 实现**线路兼容**：Rust manager 可以驱动 Go worker，反之亦然。配置文件格式（TOML）使用相同字段名，现有 Go 配置文件无需修改即可使用。

> **注意：** Go 版本默认端口为 **12345**，而 Rust 版本默认端口为 **14242**。迁移时请将 Rust 配置中的端口改为 Go 使用的端口，或者相应更新 worker 的 `api_base` 和 `tunasynctl` 配置。

### 迁移步骤

1. **安装 Rust 二进制** — 从 [Releases](https://github.com/JCIOTeam/tunasync/releases) 下载 `tunasync` 和 `tunasynctl` 或从源码编译，复制到 `/usr/bin/`。如需 `tunasync-migrate`，单独构建：`cargo build --release -p tunasync-migrate`
2. **保留配置文件** — Rust 版本读取相同 TOML 格式，无需修改
3. **迁移数据** — Rust 版本默认使用 redb 作为数据库后端（Go 默认 BoltDB）。如果 Go 版本使用 BoltDB（默认），需要通过 `tunasync-migrate` 导出数据。支持两种方式：

   **方式 A — 离线迁移（推荐，无停机时间要求）：**
   先停止 Go manager，直接读取 bolt 数据库文件：
   ```bash
   systemctl stop tunasync-manager tunasync-worker

   # 指向 Go 的 bolt 文件（默认路径：/var/lib/tunasync/tunasync.db）
   tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db
   ```

   **方式 B — 在线迁移（Go manager 仍在运行）：**
   适合在 Go 版本继续提供服务的同时准备好新数据库：
   ```bash
   # Go manager 默认端口为 12345
   tunasync-migrate http://localhost:12345 /var/lib/tunasync/new.db
   ```

   两种方式完成后，修改 Rust manager 配置：
   ```toml
   [files]
   db_type = "sqlite"
   db_file = "/var/lib/tunasync/new.db"
   ```

   **如果 Go 版本使用 Redis**，则无需迁移 — 两个版本可以直接共享同一个 Redis 实例。

4. **停止 Go 服务**（如果尚未停止） — `systemctl stop tunasync-manager tunasync-worker`
5. **启动 Rust 服务** — `systemctl start tunasync-manager tunasync-worker`
6. **验证** — `tunasynctl list` 应显示所有镜像及其上次同步时间

### 兼容性对照

| 功能 | Go 版本 | Rust 版本 |
|------|---------|----------|
| 配置格式 | TOML，相同字段名 | 兼容 |
| `[include]` 段 | Glob 匹配加载子配置 | 兼容 |
| `{{.Name}}` 模板 | log_dir 中模板展开 | 兼容 |
| SIGHUP 热重载 | 重新加载镜像配置 | 兼容 |
| 数据库后端 | BoltDB、LevelDB、Badger、Redis | redb / sqlite / redis |
| Docker hook | 容器包装 | 兼容 |
| Cgroup hook | v1/v2 内存限制 | 兼容 |
| Btrfs/ZFS hook | 同步前后快照 | 兼容 |
| 线路协议 | JSON REST API | 完全兼容 |
| 默认端口 | 12345 | 14242 |
| `tunasynctl` CLI | 相同命令 | 兼容（支持 `-p`、`-w` 短参数） |

## 设计

架构与上游一致，协议映射详见 `docs/wire-compat.md`。

### 整体架构

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

### Job 运行流程

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

## 快速入门

创建目录：

```bash
mkdir -p ~/tunasync_demo /tmp/tunasync/{log,manager-db}
```

### Worker 配置 (`~/tunasync_demo/worker.conf`)

```toml
[global]
name = "test_worker"                     # Worker 名称（用作 worker_id）
log_dir = "/tmp/tunasync/log"            # 默认日志目录
mirror_dir = "/tmp/tunasync"             # 默认镜像数据目录
concurrent = 10                          # 最大并发同步数（0 = 不限）
interval = 120                           # 默认同步间隔（分钟）
retry = 3                                # 默认失败重试次数
timeout = 600                            # 默认超时时间（秒，0 = 不限）
# rsync_options = ["--no-motd"]          # 全局 rsync 选项，追加到每个 rsync 任务
# exec_on_success = []                   # 全局同步成功后执行的命令
# exec_on_failure = []                   # 全局同步失败后执行的命令

[manager]
api_base = "http://localhost:14242"      # Manager URL（单个）
# api_base_list = [                      # 多个 Manager（覆盖 api_base）
#     "http://mgr1:14242",
#     "http://mgr2:14242",
# ]
# ca_cert = "/etc/tunasync/ca.crt"       # TLS CA 证书

[server]
hostname = "localhost"                   # Worker 公网主机名
listen_addr = "127.0.0.1"               # Worker HTTP 监听地址
listen_port = 6000                       # Worker HTTP 监听端口
# ssl_cert = ""                          # Worker TLS 证书
# ssl_key = ""                           # Worker TLS 密钥

# Docker hook — 在容器内运行同步（与 cgroup 互斥）
[docker]
enable = false
# volumes = ["/data:/data"]              # 全局 Docker 卷映射
# options = ["--network=host"]           # 全局 Docker 选项

# Cgroup hook — 限制每个任务的 CPU/内存（仅 Linux，与 docker 互斥）
[cgroup]
enable = false
# base_path = "/sys/fs/cgroup"          # Cgroup 挂载路径
# group = "tunasync"                    # Cgroup slice 名称

# ZFS 快照 hook
[zfs]
enable = false
# zpool = "tank"                        # ZFS 池名

# Btrfs 快照 hook（仅 Linux）
[btrfs_snapshot]
enable = false
# snapshot_path = "/snapshots"          # Btrfs 快照目录

# 通过 Glob 包含额外的镜像配置
[include]
# include_mirrors = "/etc/tunasync/mirrors.d/*.conf"   # Go 兼容的 [include] 段

# --- 镜像定义 ---
# provider 类型: "command"、"rsync"、"two-stage-rsync"

# 简单 rsync 镜像
[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv4 = true
# interval = 60                          # 覆盖全局间隔（分钟）
# retry = 3                              # 覆盖全局重试次数
# timeout = 600                          # 覆盖全局超时（秒）
# mirror_dir = "/data/elvish"            # 覆盖全局 mirror_dir
# log_dir = "/var/log/tunasync/elvish"   # 覆盖全局 log_dir
# username = "mirror"                    # Rsync 用户名
# password = "secret"                    # Rsync 密码（环境变量: RSYNC_PASSWORD）
# exclude_file = "/etc/tunasync/exclude.txt"  # Rsync exclude-from 文件

# 命令镜像（执行任意 shell 命令）
[[mirrors]]
name = "myrepo"
provider = "command"
upstream = "https://example.com/repo/"
command = "wget -m -np -nd https://example.com/repo/ -P /path/to/mirror"
# fail_on_match = "error|failed"         # 正则匹配日志则判定失败
# size_pattern = "Total size: ([\\d.]+[KMG])"  # 从日志提取大小
# success_exit_codes = [0, 1, 2]         # 将这些退出码视为成功
# env = { "MY_VAR" = "value" }           # 附加环境变量
# tunasync 始终注入以下环境变量供命令使用：
#   TUNASYNC_MIRROR_NAME    镜像名称（即 [[mirrors]] 的 name 字段）
#   TUNASYNC_WORKING_DIR    镜像数据目录（生效路径）
#   TUNASYNC_UPSTREAM_URL   upstream 字段值
#   TUNASYNC_LOG_DIR        日志目录（日志文件的父目录）
#   TUNASYNC_LOG_FILE       当前日志文件的完整路径

# 两阶段 rsync（适用于 Debian 等大型仓库）
[[mirrors]]
name = "debian"
provider = "two-stage-rsync"
upstream = "rsync://ftp.debian.org/debian/"
stage1_profile = "debian"                # "debian" 或 "debian-oldstyle"
use_ipv4 = true
# command = "/usr/local/bin/rsync"       # 自定义 rsync 二进制路径

# Docker 包装的镜像
# [[mirrors]]
# name = "docker-mirror"
# provider = "command"
# upstream = "https://example.com/"
# command = "sync-script $TUNASYNC_UPSTREAM_URL"
# docker_image = "sync-runner:latest"    # Docker 镜像（启用 docker hook）
# docker_volumes = ["/data:/data"]       # 每个镜像的 Docker 卷映射
# docker_options = ["--network=host"]    # 每个镜像的 Docker 选项
# memory_limit = "512M"                  # 内存限制（K/M/G 后缀）

# 自定义角色和钩子的镜像
# [[mirrors]]
# name = "slave-mirror"
# provider = "rsync"
# upstream = "rsync://master.example.com/mirror/"
# role = "slave"                         # "master"（默认）或 "slave"
# exec_on_success = ["curl -s http://notify/success"]
# exec_on_failure = ["curl -s http://notify/failure"]
```

### Manager 配置 (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"                       # 监听地址
port = 14242                             # 监听端口（默认: 14242）
# ssl_cert = "/etc/tunasync/server.crt"  # TLS 证书
# ssl_key = "/etc/tunasync/server.key"   # TLS 密钥
# debug = true                           # 启用调试日志

[files]
db_type = "sqlite"                       # "redb"（默认）、"sqlite" 或 "redis"
db_file = "/tmp/tunasync/manager-db/tunasync.db"  # 数据库文件路径
# ca_cert = ""                           # Worker TLS 验证的 CA 证书
```

支持的 `db_type`：`redb`（默认）、`sqlite`、`redis`。使用 Redis 时，`db_file` 应设为 Redis URL（如 `redis://localhost:6379/0`）。数据与 Go 版完全兼容 — 两个版本可以共享同一个 Redis 实例。

### tunasynctl 配置 (`~/.config/tunasync/ctl.conf`)

```toml
manager_addr = "127.0.0.1"
manager_port = 14242
```

或通过命令行指定：

```bash
tunasynctl list --all -p 14242
tunasynctl start elvish -p 14242 -w test_worker
```

### 启动

```bash
tunasync manager -c ~/tunasync_demo/manager.conf
tunasync worker -c ~/tunasync_demo/worker.conf
```

镜像数据将同步到 `/tmp/tunasync/`。

### 控制

```bash
# 查看所有镜像状态
tunasynctl list --all -p 14242

# 启动 / 停止 / 禁用指定镜像
tunasynctl start elvish -p 14242
tunasynctl stop elvish -p 14242
tunasynctl disable elvish -p 14242

# 热重载 Worker 配置（无需重启）
# 需要指定 worker ID，即 worker.conf 中 [global] name 字段的值
tunasynctl reload test_worker -p 14242
```

### 安全

Worker 与 Manager 之间使用 HTTP(S) 通信。如果两者运行在同一台机器上，使用普通 HTTP 即可——Manager 端留空 `ssl_cert` / `ssl_key`，Worker 端留空 `ca_cert`，`api_base` 使用 `http://`。

若需加密通信，Manager 端配置 `ssl_cert` 和 `ssl_key`，Worker 端配置 `ca_cert`，并将 `api_base` 设为 `https://`。

### 以 systemd 服务运行

示例服务文件在 `systemd/` 目录中提供。

```bash
# 创建 tunasync 用户
sudo useradd -r -s /bin/false tunasync

# 安装二进制
sudo cp target/release/tunasync /usr/bin/
sudo cp target/release/tunasynctl /usr/bin/

# 安装配置和服务文件
sudo mkdir -p /etc/tunasync /var/lib/tunasync
sudo cp systemd/tunasync-manager.service /etc/systemd/system/
sudo cp systemd/tunasync-worker.service /etc/systemd/system/
sudo cp manager.conf /etc/tunasync/
sudo cp worker.conf /etc/tunasync/

# 启用并启动
sudo systemctl daemon-reload
sudo systemctl enable --now tunasync-manager
sudo systemctl enable --now tunasync-worker

# 热重载 Worker 配置（从磁盘重新读取配置，应用差异）
sudo systemctl reload tunasync-worker
```

服务文件中的 `--with-systemd` 参数会抑制日志中的时间戳和 ANSI 颜色，因为 systemd journal 已经自带时间戳。

### 使用 SysVinit (init.d) 运行

Debian/Ubuntu 风格的 SysVinit 脚本位于 `init.d/` 目录。

```bash
sudo cp init.d/tunasync-manager /etc/init.d/
sudo cp init.d/tunasync-worker /etc/init.d/
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker

# 启用并启动
sudo update-rc.d tunasync-manager defaults
sudo update-rc.d tunasync-worker defaults
sudo service tunasync-manager start
sudo service tunasync-worker start

# 热重载 Worker 配置
sudo service tunasync-worker reload
```

### 使用 OpenRC (Alpine、Gentoo) 运行

OpenRC 脚本位于 `openrc/` 目录。

```bash
sudo cp openrc/tunasync-manager /etc/init.d/
sudo cp openrc/tunasync-worker /etc/init.d/
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker

# 启用并启动
sudo rc-update add tunasync-manager default
sudo rc-update add tunasync-worker default
sudo rc-service tunasync-manager start
sudo rc-service tunasync-worker start

# 热重载 Worker 配置
sudo rc-service tunasync-worker reload
```

## 编译

需要 Rust stable（>= 1.80），参见 `rust-toolchain.toml`。

```bash
# 编译 release 二进制
cargo build --release
# 产出: target/release/{tunasync, tunasynctl}

# 运行测试（含 wire-compat 一致性测试）
cargo test --workspace

# 代码检查
cargo clippy --workspace
cargo fmt --all
```

## 项目结构

```
crates/
+-- protocol/    # 线路类型 — JSON 格式与 Go 的 internal/msg.go 兼容
+-- common/      # 日志、HTTP 客户端、配置加载
+-- manager/     # Manager HTTP 服务 (axum)、redb/sqlite/redis 存储
+-- worker/      # Worker 运行时：调度器、任务状态机、provider、hook
+-- tunasync/    # 合并 manager+worker 的二进制入口
+-- tunasynctl/  # CLI 控制工具
+-- migrate/     # Go→Rust 数据迁移工具
```

### Provider（同步提供者）

| Provider | 说明 |
|----------|------|
| `command` | 执行任意 shell 命令 |
| `rsync` | 经典 rsync 镜像同步 |
| `two-stage-rsync` | 两阶段 rsync：第一阶段快速列表，第二阶段完整同步 |

### 镜像同步状态

每个镜像任务在任意时刻只有一种状态，通过 `tunasynctl list` 显示，也可通过 manager HTTP API 查询。

| 状态 | 线路值 | 含义 |
|------|--------|------|
| `None` | `"none"` | 任务已注册但从未运行（如 worker 刚启动） |
| `PreSyncing` | `"pre-syncing"` | 同步前钩子正在运行（主同步命令尚未启动） |
| `Syncing` | `"syncing"` | 主同步命令正在执行（含 post-exec 钩子） |
| `Success` | `"success"` | 上次同步成功完成 |
| `Failed` | `"failed"` | 上次同步失败；`error_msg` 字段包含原因 |
| `Paused` | `"paused"` | 被运维人员暂停（`tunasynctl stop`） |
| `Disabled` | `"disabled"` | 任务被禁用（`tunasynctl disable`），重新启用前不会运行 |

正常生命周期：`None → PreSyncing → Syncing → Success / Failed → （下次调度）→ PreSyncing → …`

`Failed` 状态的镜像会保留 `error_msg`，直到下次同步成功才会清除。可通过 `--status` 过滤：

```bash
tunasynctl list --status failed
tunasynctl list --status syncing,pre-syncing
```

### Hook（钩子）

| Hook | 说明 |
|------|------|
| `exec_post` | 同步阶段结束后执行命令 |
| `loglimit` | 日志轮转 / 截断 |
| `docker` | 在 Docker 容器内执行同步 |
| `cgroup` | 通过 cgroup 限制 CPU/内存 |
| `btrfs_snapshot` | 同步前后创建 Btrfs 快照 |
| `zfs_snapshot` | 同步前后创建 ZFS 快照 |

## CLI 参考

### `tunasync`

```
tunasync — 镜像同步管理工具

Usage: tunasync [OPTIONS] <COMMAND>

Commands:
  manager  以 manager 模式运行
  worker   以 worker 模式运行

Options:
  -v, --verbose       详细日志
      --with-systemd  为 systemd 抑制时间戳和 ANSI 颜色
  -h, --help          显示帮助
  -V, --version       显示版本

tunasync manager [OPTIONS]
  -c, --config <CONFIG>    配置文件路径 [default: /etc/tunasync/manager.conf]
      --addr <ADDR>        覆盖监听地址
      --port <PORT>        覆盖监听端口（默认: 14242）
      --cert <CERT>        TLS 证书文件（启用 HTTPS）
      --key <KEY>          TLS 私钥文件（启用 HTTPS）
      --db-file <DB_FILE>  覆盖数据库文件路径
      --db-type <DB_TYPE>  覆盖数据库类型: redb, sqlite, redis
      --debug              启用 debug 级别日志
      --pidfile <PIDFILE>  PID 文件 [default: /run/tunasync/tunasync.manager.pid]
      --with-systemd       为 systemd 抑制时间戳和 ANSI 颜色

tunasync worker [OPTIONS]
  -c, --config <CONFIG>    配置文件路径 [default: /etc/tunasync/worker.conf]
      --pidfile <PIDFILE>  PID 文件 [default: /run/tunasync/tunasync.worker.pid]
      --with-systemd       为 systemd 抑制时间戳和 ANSI 颜色
```

### `tunasynctl`

```
tunasynctl — tunasync manager 控制工具

Usage: tunasynctl [OPTIONS] <COMMAND>

Commands:
  list       列出所有镜像任务
  workers    列出所有已注册 worker
  flush      清除数据库中已禁用的任务记录
  rm-worker  从 manager 中移除 worker
  set-size   更新镜像大小（手动覆盖）
  start      启动镜像同步任务
  stop       停止正在运行的镜像任务
  disable    禁用镜像任务
  restart    重启镜像任务
  reload     通知 worker 从磁盘重新加载配置

全局选项:
  -c, --config <CONFIG>     配置文件（覆盖系统/用户配置）
  -m, --manager <MANAGER>   Manager 主机/IP [env: TUNASYNC_MANAGER]
  -p, --port <PORT>         Manager 端口 [env: TUNASYNC_MANAGER_PORT]
      --ca-cert <CA_CERT>   CA 证书（启用 HTTPS）
  -v, --verbose             详细日志

tunasynctl list [OPTIONS]
  -w, --worker <WORKER>     指定 worker
      --status <STATUS>     按状态过滤（逗号分隔）
      --format <FORMAT>     输出格式: json（默认）或 table
      --all                  显示所有 worker 的任务

tunasynctl start <MIRROR> [-w <WORKER>] [-f]
  MIRROR   镜像名称，或 "all" 广播到所有 worker
  -f       强制启动（忽略并发限制）

tunasynctl stop <MIRROR> [-w <WORKER>]
tunasynctl disable <MIRROR> [-w <WORKER>]
tunasynctl restart <MIRROR> [-w <WORKER>]

tunasynctl set-size <MIRROR> <SIZE> [-w <WORKER>]
  SIZE   可读大小，如 "1.2T"

tunasynctl rm-worker <WORKER>
tunasynctl flush
tunasynctl reload <WORKER>
```

`tunasynctl` 配置文件优先级：

1. `/etc/tunasync/ctl.conf`（系统级）
2. `$HOME/.config/tunasync/ctl.conf`（用户级）
3. `--config FILE`（显式指定）
4. CLI 参数（`--manager`、`--port`、`--ca-cert`）

### `tunasync-migrate`

```
Usage: tunasync-migrate <go-manager-url-or-bolt-file> <sqlite-output-file>

Examples:
  # 离线：直接读取 Go 的 bolt 文件（Go manager 必须已停止）
  tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db

  # 在线：从运行中的 Go manager 拉取数据
  tunasync-migrate http://localhost:12345 /var/lib/tunasync/new.db
```

## 开发路线

| 阶段 | 范围 | 状态 |
|------|------|------|
| 1 | 工作空间、线路协议类型、通用工具 | 已完成 |
| 2 | Manager：HTTP 路由、redb + sqlite 存储、worker 生命周期 | 已完成 |
| 3 | Worker：调度器、任务状态机、cmd_provider、manager 注册 | 已完成 |
| 4 | rsync + 两阶段 rsync provider、exec_post & loglimit hook | 已完成 |
| 5 | cgroup、docker、zfs、btrfs hook | 已完成 |
| 6 | tunasynctl CLI、CI/release、生产加固 | 进行中 |

## 线路兼容性

`tunasync-protocol` 可与 Go 产生的所有 JSON 格式双向转换。一致性测试见 `crates/protocol/tests/wire_compat.rs`。注意要点：

- `SyncStatus::PreSyncing` 序列化为 `"pre-syncing"`（带连字符）——与 Go 一致。
- Go 的 `time.Time{}` 零值 (`"0001-01-01T00:00:00Z"`) 通过 `tunasync_protocol::zero_time()` 保留。**不要**用 `chrono::DateTime::default()` 表示"未设置"——那是 Unix epoch，是不同的哨兵值。
- `MirrorStatus::scheduled` 在线路上的字段名为 `next_schedule`（匹配 Go 的 struct tag）。
- `MirrorSchedule::mirror_name` 在线路上的字段名为 `name`。

### 与 Go 版本的已知差异

| 区域 | 差异 | 原因 |
|------|------|------|
| Manager: size 更新 | 使用 `&&` 而非 Go 的有 bug 的 `||` | Go 的条件永远为 true |
| Manager: heartbeat | 新增 `POST /workers/:id/heartbeat` | 显式心跳比隐式刷新更可靠 |
| Manager: deleteWorker | 无效 ID 返回 400（Go 返回 500） | 更有用的错误信息 |
| Manager: DB | Redis 后端已实现 | 支持 redb、sqlite、redis |
| Manager: GET /jobs/:name | 镜像详情，含 `error_msg`，跨所有 worker | 前端使用的新端点 |

## API 参考

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/ping` | 存活检查 → `{ "message": "pong" }` |
| GET | `/jobs` | 列出所有镜像（摘要，不含 `error_msg`） |
| HEAD | `/jobs` | 检查镜像可用性（同 GET，无响应体） |
| GET | `/jobs/:name` | 镜像详情，跨所有 worker（含 `error_msg`、时间戳） |
| DELETE | `/jobs/disabled` | 清除所有已禁用的镜像行 |
| GET | `/workers` | 列出已注册 worker（token 已脱敏） |
| POST | `/workers` | 注册新 worker |
| DELETE | `/workers/:id` | 删除 worker |
| POST | `/workers/:id/heartbeat` | worker 心跳 |
| GET | `/workers/:id/jobs` | 列出某个 worker 的镜像 |
| POST | `/workers/:id/jobs/:job` | 更新镜像状态（worker → manager） |
| POST | `/workers/:id/jobs/:job/size` | 更新镜像大小 |
| POST | `/workers/:id/schedules` | 更新调度计划 |
| POST | `/cmd` | 发送控制命令（start/stop/disable/reload） |

`GET /jobs/:name` 返回 `Vec<MirrorStatus>` — 完整状态对象，包含 `error_msg`。示例：

```bash
curl http://localhost:14242/jobs/ubuntu
# → [ { "name": "ubuntu", "worker": "w1", "status": "failed", "error_msg": "rsync: timeout", ... } ]

curl http://localhost:14242/jobs/nonexistent
# → []
```

## 许可证

GPL-3.0-or-later，与上游相同。