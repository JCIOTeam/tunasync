# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

[`tuna/tunasync`](https://github.com/tuna/tunasync) 的 Rust 移植版。tunasync 是驱动 [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) 及众多开源镜像站的镜像同步管理工具。

> **English**: [README.md](README.md)

## 下载

Linux（x86_64、aarch64、armv7、riscv64、loongarch64、musl 静态版）和 macOS 的预编译二进制文件可在 [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases) 下载。

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
+------------+
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

### Manager 配置 (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"
port = 12345

[files]
db_type = "sqlite"
db_file = "/tmp/tunasync/manager-db/tunasync.db"
```

支持的 `db_type`：`redb`（默认）、`sqlite`。注：Go 版本还支持 `redis`，本移植版暂未实现 Redis 后端。

### 启动

```bash
tunasync manager -c ~/tunasync_demo/manager.conf
tunasync worker -c ~/tunasync_demo/worker.conf
```

镜像数据将同步到 `/tmp/tunasync/`。

### 控制

```bash
# 查看所有镜像状态
tunasynctl list --all -p 12345

# 启动 / 停止 / 禁用指定镜像
tunasynctl start elvish -p 12345
tunasynctl stop elvish -p 12345
tunasynctl disable elvish -p 12345
```

`tunasynctl` 也可以从 `~/.config/tunasync/ctl.conf` 或 `/etc/tunasync/ctl.conf` 读取配置：

```toml
manager_addr = "127.0.0.1"
manager_port = 12345
```

### 安全

Worker 与 Manager 之间使用 HTTP(S) 通信。如果两者运行在同一台机器上，使用普通 HTTP 即可——Manager 端留空 `ssl_cert` / `ssl_key`，Worker 端留空 `ca_cert`，`api_base` 使用 `http://`。

若需加密通信，Manager 端配置 `ssl_cert` 和 `ssl_key`，Worker 端配置 `ca_cert`，并将 `api_base` 设为 `https://`。

## 编译

需要 Rust stable（≥ 1.80），参见 `rust-toolchain.toml`。

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
├── protocol/    # 线路类型 — JSON 格式与 Go 的 internal/msg.go 兼容
├── common/      # 日志、HTTP 客户端、配置加载
├── manager/     # Manager HTTP 服务 (axum)、redb/sqlite 存储
├── worker/      # Worker 运行时：调度器、任务状态机、provider、hook
├── tunasync/    # 合并 manager+worker 的二进制入口
└── tunasynctl/  # CLI 控制工具
```

### Provider（同步提供者）

| Provider | 说明 |
|----------|------|
| `command` | 执行任意 shell 命令 |
| `rsync` | 经典 rsync 镜像同步 |
| `two-stage-rsync` | 两阶段 rsync：第一阶段快速列表，第二阶段完整同步 |

### Hook（钩子）

| Hook | 说明 |
|------|------|
| `exec_post` | 同步阶段结束后执行命令 |
| `loglimit` | 日志轮转 / 截断 |
| `docker` | 在 Docker 容器内执行同步 |
| `cgroup` | 通过 cgroup 限制 CPU/内存 |
| `btrfs_snapshot` | 同步前后创建 Btrfs 快照 |
| `zfs_snapshot` | 同步前后创建 ZFS 快照 |

## 开发路线

| 阶段 | 范围 | 状态 |
|------|------|------|
| 1 | 工作空间、线路协议类型、通用工具 | ✅ |
| 2 | Manager：HTTP 路由、redb + sqlite 存储、worker 生命周期 | ✅ |
| 3 | Worker：调度器、任务状态机、cmd_provider、manager 注册 | ✅ |
| 4 | rsync + 两阶段 rsync provider、exec_post & loglimit hook | ✅ |
| 5 | cgroup、docker、zfs、btrfs hook | ✅ |
| 6 | tunasynctl CLI、CI/release、生产加固 | 🔧 |

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
| Manager: DB | 暂无 Redis 后端 | 仅支持 redb/sqlite |
| Worker: GET /jobs | 额外的自省端点 | 无害的新增功能 |

## 许可证

GPL-3.0-or-later，与上游相同。