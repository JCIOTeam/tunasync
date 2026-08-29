# tunasync-rs

[![CI](https://github.com/JCIOTeam/tunasync/actions/workflows/ci.yml/badge.svg?branch=rs)](https://github.com/JCIOTeam/tunasync/actions?query=branch%3Ars)
[![Release](https://img.shields.io/github/v/release/JCIOTeam/tunasync?label=release)](https://github.com/JCIOTeam/tunasync/releases)
[![License: GPL-3.0+](https://img.shields.io/badge/license-GPL--3.0%2B-blue.svg)](LICENSE)

[`tuna/tunasync`](https://github.com/tuna/tunasync) 的 Rust 移植版。tunasync 是驱动 [TUNA](https://mirrors.tuna.tsinghua.edu.cn/) 及众多开源镜像站的镜像同步管理工具。

> **English**: [README.md](README.md)

## 下载

Linux（x86_64、aarch64、armv7、riscv64、loongarch64、x86_64-musl、aarch64-musl）的预编译发布包可在 [GitHub Releases](https://github.com/JCIOTeam/tunasync/releases) 下载。由当前版本构建的发布包包含：

```
bin/tunasync
bin/tunasynctl
bin/tunasync-netns-broker
systemd/tunasync-manager.service
systemd/tunasync-worker.service
systemd/tunasync-netns-broker.service
systemd/tunasync-worker.service.d/netns.conf
```

`tunasync-migrate` 不包含在发布包中 — 它是一次性迁移工具，大多数用户切换到 Rust 版本后不再需要。获取方式：

1. 从源码构建：`cargo build --release -p tunasync-migrate`
2. 从任意成功的 release 构建 [CI artifacts](https://github.com/JCIOTeam/tunasync/actions/workflows/release.yml) 中下载

## 从 Go 版本迁移

tunasync-rs 与受支持的 Go API **线路兼容**：Rust manager 可以驱动 Go worker，反之亦然。Worker TOML 大体兼容，但配置语义和 manager 数据库后端并不完全相同。切换前应对实际部署文件执行两个静态检查：

```bash
tunasync manager --check -c /etc/tunasync/manager.conf
tunasync worker --check -c /etc/tunasync/worker.conf
```

当前 Go 与 Rust manager 都默认使用端口 `14242`；较旧部署可能显式配置了其他端口。应以现有 `manager.conf`、worker `api_base` 和运维工具中的实际端口为准，不要假定历史默认值。

### 迁移步骤

1. **安装 Rust 二进制** — 从 [Releases](https://github.com/JCIOTeam/tunasync/releases) 下载或从源码编译，复制到 `/usr/bin/`。如需 `tunasync-migrate`，单独构建：`cargo build --release -p tunasync-migrate`
2. **检查并迁移配置** — 复制 TOML 后运行上述两个 `--check`。worker 检查会解析 include 和嵌套镜像、构造全部 provider、拒绝有歧义的零值、检测不支持的 Go `log_dir` 模板，并在不输出密钥值的前提下报告旧 `manager.token`。manager 检查会在打开数据库前拒绝仅 Go 支持的后端，并要求显式设置 `files.db_type`：省略该字段在 Go 中表示 BoltDB，在 Rust 中表示 redb，迁移时不能静默套用 Rust 默认值。
3. **迁移数据** — Rust 版本默认使用 redb 作为数据库后端（Go 默认 BoltDB）。如果 Go 版本使用 BoltDB（默认），需要通过 `tunasync-migrate` 导出数据。支持两种方式：

   **方式 A — 离线迁移（推荐）：**
   ```bash
   systemctl stop tunasync-manager tunasync-worker
   tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db
   ```

   **方式 B — 在线迁移（Go manager 仍在运行）：**
   ```bash
   # 若现有 Go manager 使用其他端口，请替换 14242。
   tunasync-migrate http://localhost:14242 /var/lib/tunasync/new.db
   ```

   完成后修改 Rust manager 配置：
   ```toml
   [files]
   db_type = "sqlite"
   db_file = "/var/lib/tunasync/new.db"
   ```

   **如果 Go 版本使用 Redis**，无需迁移 — 两个版本可直接共享同一个 Redis 实例。

4. **停止 Go 服务**（如尚未停止）— `systemctl stop tunasync-manager tunasync-worker`
5. **启动 Rust 服务** — `systemctl start tunasync-manager tunasync-worker`
6. **验证** — `tunasynctl list` 应显示所有镜像及其上次同步时间

### 兼容性对照

| 功能 | Go 版本 | Rust 版本 |
|------|---------|----------|
| 配置格式 | TOML，大多数字段同名 | 大体兼容；必须运行两个 `--check` |
| `[include]` 段 | 基于 glob 的镜像配置 | ✅ 支持 |
| `log_dir` 模板 | 完整 Go template 上下文 | 支持 `Name`、`Provider`、`Upstream`、`Role`、`MirrorSubDir`；其他表达式由检查拒绝 |
| SIGHUP 热重载 | 重载镜像配置 | ✅ 支持 |
| 数据库后端 | BoltDB、LevelDB、Badger、Redis | redb、sqlite、redis |
| Docker 钩子 | 容器包装 | ✅ 兼容 |
| Cgroup 钩子 | v1/v2 内存限制 | 字段相同，但路径/controller 行为不同；切换前需复核 |
| Btrfs/ZFS 钩子 | 快照 | ✅ 兼容 |
| 线路协议 | JSON REST API | ✅ 完全兼容 |
| Manager 默认端口 | 当前 Go 为 14242 | 14242 |
| `tunasynctl` CLI | 相同命令 | ✅ 兼容（`-p`、`-w` 短选项） |

本版本补齐了这些兼容项：Go 风格的 `KiB`/`MiB`/`GiB` 内存单位、按键合并的嵌套 `env` 继承、向 rsync/two-stage 同步和 rsync 探针传递镜像 `env`，以及自动映射旧 `manager.token`。若同时存在 `token` 与 `api_token`，以 `api_token` 为准；`worker --check` 会要求删除旧字段，但不会输出任何 token 值。

为保证迁移安全，`worker --check` 要求 `global.concurrent` 为正数、fixed-delay 镜像的有效 interval 为正数、`server.listen_port` 为正数。原版 Go 的这些零值会分别导致所有任务被阻塞、零延迟重复调度、以及绑定临时端口却向 manager 上报端口 0；tunasync-rs 不复现这些故障模式。为保持 Go 兼容，未知 TOML 字段仍会被忽略，因此仍需检查拼写并阅读检查输出。

## Rust 版本新功能

以下功能是 tunasync-rs 独有的，Go 版本没有对应实现。它们默认关闭、按需启用，因此不会影响已经通过检查的 Go 配置。

### 磁盘配额预检

同步前检查磁盘剩余空间，不足则跳过本次同步（镜像保留在队列中，下次到期时重试）：

```toml
[[mirrors]]
name = "debian"
disk_quota = "100G"   # 镜像目录剩余空间 < 100 GiB 时跳过同步
```

### 调度模式、Cron 与时区

`interval_mode = "fixed-delay"` 是默认模式。下一次执行时间为本地完成时间加 `interval`；没有已持久化完成时间时，worker 会立即执行。因 blackout 被阻止的启动会在 5 分钟后重试。

`fixed-rate` 按镜像生效的 IANA `timezone` 中的墙上时间槽执行，要求严格的 `fixed_rate_anchor = "HH:MM"`。`interval` 必须是 1 到 1440 分钟且能整除 1440。启动和热重载都会选择下一个未来时间槽；被 blackout 阻止、重叠或错过的时间槽会跳过，不会回补。两种模式都将发生的任务提交到既有的全局及单上游并发控制，不会绕过限制。

```toml
[global]
timezone = "Asia/Shanghai"
interval = 60
interval_mode = "fixed-rate"
fixed_rate_anchor = "00:15"

[[mirrors]]
name = "completion-based"
interval_mode = "fixed-delay" # 显式清除继承的 fixed-rate anchor
interval = 120
```

Cron 同样是墙上时间调度：只选择下一个未来发生时间，不追补历史发生时间。非空 `cron` 仅在 fixed-delay/默认上下文中覆盖 `interval`；它与 `interval_mode = "fixed-rate"` 冲突，校验会拒绝该配置。

```toml
[[mirrors]]
name = "kernel"
cron = "0 3 * * *"   # 每天 03:00（在该镜像生效的时区内）
```

支持 5 字段 POSIX 格式（`分 时 日 月 周`）和 cron crate 的 6/7 字段格式。无效表达式在 worker 启动时报错。

### 时区感知调度

cron 表达式和 blackout 窗口默认解释为 **UTC 时间**。通过 `timezone` 字段指定本地时间：

```toml
[global]
timezone = "Asia/Shanghai"   # 所有镜像默认使用 CST

[[mirrors]]
name = "euromirror"
timezone = "Europe/Berlin"   # 单个镜像覆盖全局设置
cron = "0 3 * * *"           # 在柏林时间 03:00 触发，而非 UTC 03:00
```

有效值为 IANA 时区名（如 `"Asia/Shanghai"`、`"America/New_York"`、`"Europe/Berlin"`、`"UTC"`）。无效名称在 worker 启动时报错，而非静默产生错误调度。

> **从 Go 迁移注意：** Go worker 隐式使用宿主机本地时区。如果你的 cron/blackout 配置依赖本地时间，切换到 Rust 版本时必须**显式**设置 `timezone`。

### 屏蔽时间窗（Blackout）

在繁忙时段内屏蔽新同步任务的启动。**已在运行的同步不会被中断**，仅阻止新的启动。调度器在 blackout 期间将任务推迟 5 分钟后重试：

```toml
[[mirrors]]
name = "debian"
# 按该镜像生效的时区（见上方）解释，默认 UTC
blackout = ["08:00-18:00 Mon-Fri", "22:00-04:00"]
```

格式：`"HH:MM-HH:MM [<天范围>]"`。天范围支持 `Mon-Fri`、`Sat-Sun`、单个工作日、`daily`（等同于不指定）。跨午夜：`"22:00-04:00"` 覆盖 22:00–23:59 和 00:00–04:00。

### 任务优先级

多个任务争抢并发槽时，高优先级镜像优先获得执行机会：

```toml
[[mirrors]]
name = "critical"
priority = 90   # 默认 50；越大越先执行
```

### 单上游并发限制

独立于全局 `concurrent`，限制同时从同一上游主机同步的镜像数量：

```toml
[global.per_upstream_concurrent]
"rsync.kernel.org" = 2
"ftp.debian.org"   = 1
```

该配置在下一次 SIGHUP / `tunasynctl reload` 时应用。新增、提高或删除限制会立即生效；降低已有上限需要重启 worker，因为运行中的 semaphore 无法安全撤回已经发出的 permit。全局 `concurrent` 限制仍然有效，`per_upstream_concurrent` 在其基础上叠加约束。

### 上游探测与回退

同步前探测上游可达性。主上游不可达时并发探测回退列表（每个 URL 15 秒超时）；所有 URL 均不可达则跳过本次同步（非永久失败）：

```toml
[[mirrors]]
name = "kernel"
check_upstream = true
upstream_fallback = ["rsync://mirror.example.com/kernel/"]
```

回退列表**仅用于健康探测**，实际同步数据源始终是 `upstream`。

### 原子发布

先将 rsync 输出写入**暂存目录**，成功后通过 `renameat2(RENAME_EXCHANGE)` 原子交换到 publish 目录，用户始终看到完整的旧版或新版内容，不会出现 publish 路径消失或半同步状态的窗口：

```toml
[[mirrors]]
name = "debian"
atomic_publish = true
```

**暂存目录的解析顺序：**

1. `[[mirrors]].staging_dir` （单镜像覆盖）
2. `[global].staging_dir` （worker 级默认）
3. `<log_dir>/staging/<name>` （遗留回退，要求 log_dir 与 mirror_dir 同文件系统）

前两种情况会自动在路径后追加镜像名（例如 `staging_dir = "/srv/mirrors/.staging"` 实际为每个镜像生成 `/srv/mirrors/.staging/<name>/`）。

**要求：** 暂存目录与 publish 目录（即镜像的 `mirror_dir`）必须在**同一文件系统**（`rename(2)` 仅在同一挂载点内是原子的）。worker 在同步前通过 statvfs 检查设备 ID，不一致则拒绝同步。

大多数生产部署的日志放在系统盘、镜像数据放在独立数据盘，遗留回退路径 `<log_dir>/staging/` 通常无法满足同文件系统要求。设置 `[global].staging_dir` 为与 `mirror_dir` 同文件系统的路径即可：

```toml
[global]
log_dir     = "/var/log/tunasync"      # 系统盘
mirror_dir  = "/srv/mirrors"           # 数据盘
staging_dir = "/srv/mirrors/.staging"  # 与 mirror_dir 同盘
```

`.staging` 前导点可以阻止 nginx 默认 autoindex 暴露半同步内容；如果需要更严格隔离，建议用 nginx 服务根目录之外的兄弟路径（如 `/srv/staging`）。

对于跨数据盘的镜像，可在单镜像级别覆盖：

```toml
[[mirrors]]
name        = "huge-archive"
mirror_dir  = "/data2/archive"
staging_dir = "/data2/staging"   # 跟随数据盘
atomic_publish = true
```

不支持 `RENAME_EXCHANGE`（Linux 3.15 以下内核或部分 FUSE 挂载）时自动回退到两步 rename（有极短 404 窗口），并打印警告日志。

### API 令牌鉴权

在三个组件上配置同一个共享令牌后，所有变更类和 worker 相关的接口都会要求 `Authorization: Bearer <token>`（令牌为空 = 关闭鉴权，保持向下兼容的默认行为）：

```toml
# manager.conf
[server]
api_token = "use-a-long-random-string"

# worker.conf
[manager]
api_token = "use-a-long-random-string"

# ctl.conf
api_token = "use-a-long-random-string"   # 也可用 TUNASYNC_API_TOKEN / --api-token
```

manager 的公共只读接口对 Web 前端和探针保持开放：`GET /ping`、普通 `GET /jobs*` 状态查询、`GET /metrics`、`GET /maintenance`。SSE 日志代理以及其余接口——worker 注册/上报、`/cmd`、删除、维护模式开关——在没有令牌时返回 `401`。worker 自身的命令端点和 SSE 流由同一令牌守卫，manager 在转发命令或代理日志流时会自动附带该令牌。

### 配置检查模式

```console
$ tunasync worker --check -c /etc/tunasync/worker.conf
/etc/tunasync/worker.conf: OK — 42 mirror(s), 0 warning(s)
```

不启动任何服务，仅校验配置文件：TOML 解析、include 合并、监听/TLS 设置、cron 表达式、IANA 时区、屏蔽时间窗、镜像名称和路径、镜像重名、磁盘配额，并对每个镜像完成完整的 provider 构造。无效值会作为启动错误处理，不再通过告警后静默跳过。能够合并的问题会尽量一次报告，退出码为 0/1 便于脚本判断。推荐作为 systemd 单元的 `ExecStartPre=`，并在执行 `tunasynctl reload` 前先跑一遍。

### 离线上报缓冲

调度器会先提交本地状态，只把 manager I/O 放入队列。注册、持久状态恢复、上报、心跳、重放和重配置均在 report actor 中执行，因此任务发生提交、状态接收及控制命令不会等待 manager I/O。该优先可用性的启动策略意味着，本地 fixed-delay 任务可能在延迟到达的 Paused/Disabled 持久状态恢复之前运行；迟到的恢复有版本保护，不能覆盖更新的本地操作。

`[global].report_max_resources` 约束上报内存：未设置或 `0` 为 1024，生效值限制在 1–4096。它分别限制保留的状态/大小上报项，以及一份完整最新 schedule 快照中的行数。超限快照会整体拒绝，绝不截断，因此配置展开后的镜像总数必须能放入该上限。该值仅在启动时读取，修改后需重启 worker。普通状态/大小遥测在预算或 256 个 FIFO 项之前严格按 FIFO 保存，溢出后才按最新值合并并淘汰最旧项。schedule 上报独立处理：从第一次入队起始终保留一份完整的 latest-wins 快照，并受其独立行数上限约束。长时间 manager 故障时遥测可能被淘汰，但本地调度继续运行；重放是有界且公平的。关闭时最多尽力排空 5 秒。

### 同步历史

使用 **sqlite** 后端时，manager 会在每次同步从活动状态进入 Success/Failed 时记录一行历史，每个镜像保留最近 100 条：

```console
$ tunasynctl history debian -n 5         # 最新优先
$ curl http://manager:14242/jobs/debian/history?limit=5
```

每条记录包含 worker、状态、起止时间、传输字节数和错误信息。redb / redis 后端返回空列表。

### 维护模式

将 manager 置于操作员只读状态。控制命令、删除 worker、清理 disabled 任务等破坏性/操作员接口返回 503，直到关闭维护模式；worker 注册、心跳、状态、大小和调度上报仍会继续，使运行中的同步保持可观测，并避免 manager 状态与 worker 偏离：

```bash
tunasynctl maintenance enable
tunasynctl maintenance status
tunasynctl maintenance disable
```

### tunasynctl 支持 glob 匹配

`stop`、`disable`、`restart`、`start` 命令支持 glob 模式，一次操作多个镜像：

```bash
tunasynctl disable "debian-*"
tunasynctl restart "ubuntu-*" -w worker1
tunasynctl stop "*"
```

精确名称不触发 glob 查询（与之前性能一致）。

### 清除 stale 镜像

```bash
tunasynctl flush --stale-only    # 仅清除被标记为 stale 的镜像
tunasynctl stale                 # 列出所有 stale 镜像
```

## 设计

与上游架构相同，协议映射详见 `docs/wire-compat.md`。

### 整体架构

```
+------------+ +---+                  +---+
| Client API | |   |   Job Status     |   |    +----------+     +----------+
+------------+ |   +----------------->|   |--->|  mirror  +---->|  mirror  |
+------------+ |   |                  | w |    |  config  |     | provider |
| Worker API | | H |                  | o |    +----------+     +----+-----+
+------------+ | T |   Job Control    | r |                          |
+------------+ | T +----------------->| k |       +------------+     |
| Job/Status | | P |                  | e |       | mirror job |<----+
| Management | | S |                  | r |       +------^-----+
+------------+ |   |  Update Status   |   |    +---------+---------+
+------------+ |   <------------------+   |    |     Scheduler     |
|  redb /    | |   |                  |   |    +-------------------+
|  sqlite    | +---+                  +---+
|  redis     |
+------------+
```

### Job 运行流程

```
         ┌─────────────────────────────┐
         │       PreSyncing             │
         │  (pre-job → pre-exec 钩子)  │
         └──────────────┬──────────────┘
                        │
         ┌──────────────▼──────────────┐
         │         Syncing              │
         │   (job 运行 → post-exec)    │
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
retry = 3
timeout = 600

# 时区设置（可选，默认 UTC）
# timezone = "Asia/Shanghai"

# 原子发布暂存目录基路径（可选）。
# 必须与 mirror_dir 同文件系统。未设置时回退到 <log_dir>/staging/<name>。
# 大多数生产环境因日志盘与数据盘分离，需要显式设置。
# staging_dir = "/srv/mirrors/.staging"

# 单上游并发限制（可选）
# [global.per_upstream_concurrent]
# "rsync.kernel.org" = 2

[manager]
api_base = "http://localhost:14242"

[server]
hostname = "localhost"
listen_addr = "127.0.0.1"
listen_port = 6000

[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv4 = true
# cron = "0 3 * * *"
# interval = 60           # 分钟；单镜像间隔示例
# timezone = "Asia/Shanghai"
# blackout = ["08:00-18:00 Mon-Fri"]
# disk_quota = "100G"
# priority = 50
# check_upstream = false
# upstream_fallback = []
# atomic_publish = false
# staging_dir = ""        # 单镜像暂存路径覆盖（否则使用 [global].staging_dir）

[[mirrors]]
name = "myrepo"
provider = "command"
upstream = "https://example.com/repo/"
command = "wget -m -np -nd https://example.com/repo/ -P /path/to/mirror"
# 注入的环境变量：
#   TUNASYNC_MIRROR_NAME, TUNASYNC_WORKING_DIR, TUNASYNC_UPSTREAM_URL,
#   TUNASYNC_LOG_DIR, TUNASYNC_LOG_FILE

[[mirrors]]
name = "debian"
provider = "two-stage-rsync"
upstream = "rsync://ftp.debian.org/debian/"
stage1_profile = "debian"
use_ipv4 = true
```

完整注释的参考配置见 [`examples/worker.conf`](examples/worker.conf)。

### Manager 配置 (`~/tunasync_demo/manager.conf`)

```toml
[server]
addr = "127.0.0.1"
port = 14242

[files]
db_type = "sqlite"     # "redb"（默认）、"sqlite" 或 "redis"
db_file = "/tmp/tunasync/manager-db/tunasync.db"
```

完整注释的参考配置见 [`examples/manager.conf`](examples/manager.conf)。

`db_type` 可取 `redb`（默认）、`sqlite` 或 `redis`。`manager --check` 要求显式填写该字段，防止依赖 Go BoltDB 默认值的旧配置静默切换数据库格式。`db_type` 为 `redis` 时，`db_file` 填写 Redis URL（如 `redis://localhost:6379/0`）。数据格式与 Go 版本兼容，两个版本可共享同一 Redis 实例。

### tunasynctl 配置 (`~/.config/tunasync/ctl.conf`)

```toml
manager_addr = "127.0.0.1"
manager_port = 14242
```

也可以通过命令行参数指定：

```bash
tunasynctl list --all -p 14242
tunasynctl start elvish -p 14242 -w test_worker
```

### 启动

```bash
tunasync manager -c ~/tunasync_demo/manager.conf
tunasync worker  -c ~/tunasync_demo/worker.conf
```

镜像数据同步到 `/tmp/tunasync/`。

### 控制

```bash
# 列出所有镜像状态
tunasynctl list --all -p 14242

# 打开交互式运维仪表盘
tunasynctl tui -p 14242

# 启动 / 停止 / 禁用 / 重启（支持精确名称或 glob）
tunasynctl start   elvish      -p 14242
tunasynctl stop    "debian-*"  -p 14242
tunasynctl disable elvish      -p 14242
tunasynctl restart "ubuntu-*"  -p 14242

# 热重载 worker 配置（从磁盘重读，差量应用）
tunasynctl reload test_worker -p 14242

# 清除 disabled 镜像；--stale-only 仅清除 stale 镜像
tunasynctl flush              -p 14242
tunasynctl flush --stale-only -p 14242

# 维护模式
tunasynctl maintenance enable  -p 14242
tunasynctl maintenance status  -p 14242
tunasynctl maintenance disable -p 14242
```

TUI 复用常规的 `ctl.conf`、manager 地址、TLS CA 和 API token 配置。页面每 5 秒自动刷新，网络请求不会阻塞键盘操作。manager 级刷新失败时保留上次成功的完整快照；单个 Worker 暂时不可用时保留该 Worker 的旧镜像行，并在底部提示降级刷新。所有修改操作都需要确认，并精确作用于当前选中的镜像和 Worker。

仪表盘需要交互式终端；脚本和服务请继续使用现有的非交互子命令。

配置 API token 后，TUI 会拒绝远程明文 HTTP，因为周期刷新会反复暴露 bearer token。远程 manager 必须使用 HTTPS；`localhost`、`127.0.0.1`、`::1` 等 loopback 地址仍可使用带 token 的 HTTP。

| 按键 | 操作 |
|---|---|
| `↑` / `↓`、`j` / `k` | 选择镜像 |
| `w` | 循环切换 Worker 筛选 |
| `s` | 循环切换状态筛选 |
| `r` | 立即刷新 |
| `a`、`x`、`R`、`d` | 启动、停止、重启或禁用选中的镜像 |
| `Enter` / `y`、`Esc` / `n` | 确认或取消操作 |
| `q`、`Ctrl-C` | 退出并恢复终端 |

详情面板在显示 upstream 地址前会移除 URL 中的用户名、密码、查询参数和 fragment。

### Shell 自动补全

```bash
# bash（当前用户）
tunasynctl completion bash >> ~/.bashrc && source ~/.bashrc

# zsh
tunasynctl completion zsh >> ~/.zshrc && source ~/.zshrc

# fish
tunasynctl completion fish > ~/.config/fish/completions/tunasynctl.fish
```

### 语言设置

`tunasynctl` 检测系统 locale，当 `LANG` 以 `zh` 开头时输出中文。通过 `TUNASYNCTL_LANG` 强制指定：

```bash
TUNASYNCTL_LANG=zh tunasynctl list --format table
TUNASYNCTL_LANG=en tunasynctl list --format table
```

### 安全

需要加密通信时，在 manager 同时设置 `ssl_cert` 和 `ssl_key`，在 worker 设置 `ca_cert`，并让 `api_base` 使用 `https://`。只提供证书或私钥其中一个会被拒绝。同机部署可使用绑定到 loopback 的 HTTP。

### 以 systemd 服务运行

服务文件在 `initscripts/` 目录：

```bash
# 仅在不存在时创建系统组/账户；使用 nologin 或 false shell。
getent group tunasync >/dev/null || sudo groupadd --system tunasync
id -u tunasync >/dev/null 2>&1 || sudo useradd --system --gid tunasync --shell /usr/sbin/nologin --no-create-home tunasync
sudo cp target/release/{tunasync,tunasynctl,tunasync-netns-broker} /usr/bin/
sudo cp initscripts/tunasync-manager.service /etc/systemd/system/
sudo cp initscripts/tunasync-worker.service  /etc/systemd/system/
sudo install -d -o root -g tunasync -m 0750 /etc/tunasync
# 服务需要读取配置，但 tunasync 账户及组不能拥有写权限。
sudo install -o root -g tunasync -m 0640 manager.conf worker.conf /etc/tunasync/
sudo systemctl daemon-reload
sudo systemctl enable --now tunasync-manager tunasync-worker
sudo systemctl is-active tunasync-manager tunasync-worker
sudo systemctl status tunasync-manager tunasync-worker

# 热重载 worker 配置
sudo systemctl reload tunasync-worker
```

`--with-systemd` 会关闭日志时间戳和 ANSI 颜色，因为 systemd journal 会自行添加时间戳。

随附 worker 单元使用 `ProtectSystem=strict`。systemd 仍会提供可写的 `RuntimeDirectory=`、`StateDirectory=` 和 `LogsDirectory=` 位置；任意配置的其他路径均保持只读。每个实际使用的自定义 `mirror_dir`、`staging_dir`、`log_dir`，以及必须写入的 hook 输出路径，都必须在本地 drop-in 中加 `ReadWritePaths=`：

```ini
# /etc/systemd/system/tunasync-worker.service.d/paths.conf
[Service]
ReadWritePaths=/data/mirrors /data/tunasync/staging /data/tunasync/log
```

需要隔离同步出口时，从发布包安装 broker 服务及 worker drop-in，生成策略，并配置 `[netns_broker]` 和镜像的 `network_namespace`；参见[网络命名空间出口](docs/network-namespaces_zh.md)和 [English guide](docs/network-namespaces.md)。完整指南给出必需顺序：创建账户；安装；配置命名空间/防火墙；安装 root 控制的配置；生成/检查策略；daemon-reload；先启用/启动 broker 再启动 worker；验证单元及命名空间。broker 单元对策略使用 `ConditionPathExists=`，故必须先生成策略才能启动。

### 网络命名空间出口

Linux worker 可让一个镜像的同步命令和探针进入预先存在的命名网络命名空间。这是可选的 root broker 设计，并不负责创建命名空间、管理 VPN 或充当出口防火墙。仅有路由/NAT 不能限制目的地；请通过站点 DNS、路由和默认拒绝防火墙强制仅允许的目的地。最小流程：从发布包安装 `tunasync-netns-broker` 与两个 systemd 文件，配置 `[netns_broker]` 和 `network_namespace`，生成 `/etc/tunasync/netns-policy.json`，然后启用 broker 与 worker 依赖 drop-in。完整安全模型、部署步骤、策略生命周期和 Cloudflare One 边界见 [docs/network-namespaces_zh.md](docs/network-namespaces_zh.md)（[English](docs/network-namespaces.md)）。

### 使用 SysVinit (init.d) 运行

```bash
sudo cp initscripts/tunasync-manager.initd /etc/init.d/tunasync-manager
sudo cp initscripts/tunasync-worker.initd  /etc/init.d/tunasync-worker
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker
sudo update-rc.d tunasync-manager defaults && sudo service tunasync-manager start
sudo update-rc.d tunasync-worker  defaults && sudo service tunasync-worker  start
sudo service tunasync-worker reload   # 热重载
```

### 使用 OpenRC (Alpine、Gentoo) 运行

```bash
sudo cp initscripts/tunasync-manager.openrc /etc/init.d/tunasync-manager
sudo cp initscripts/tunasync-worker.openrc  /etc/init.d/tunasync-worker
sudo chmod +x /etc/init.d/tunasync-manager /etc/init.d/tunasync-worker
sudo rc-update add tunasync-manager default && sudo rc-service tunasync-manager start
sudo rc-update add tunasync-worker  default && sudo rc-service tunasync-worker  start
sudo rc-service tunasync-worker reload   # 热重载
```

## 编译

需要 Rust stable（≥ 1.80），见 `rust-toolchain.toml`。

```bash
cargo build --release
# 产物：target/release/{tunasync, tunasynctl, tunasync-netns-broker}

cargo test --workspace       # 完整测试套件（含线路兼容性验证）
cargo clippy --workspace
cargo fmt --all
```

## 项目结构

```
crates/
├── protocol/    # 线路类型 — 与 Go 的 internal/msg.go JSON 兼容
├── netns/       # Linux 网络命名空间 broker 协议和校验
├── netns-broker/# 有特权、受策略约束的命名空间执行 broker
├── common/      # 日志、HTTP 客户端、配置加载、工具函数
├── manager/     # Manager HTTP 服务器（axum），redb/sqlite/redis 存储
├── worker/      # Worker 运行时：调度器、任务状态机、provider、hooks
├── tunasync/    # manager+worker 合并二进制
├── tunasynctl/  # CLI 控制工具
└── migrate/     # Go→Rust 数据迁移工具
```

### Provider（同步提供者）

| Provider | 说明 |
|----------|------|
| `command` | 任意 shell 命令（自动注入环境变量） |
| `rsync` | 经典 rsync 镜像 |
| `two-stage-rsync` | 阶段一（快速列表）+ 阶段二（完整同步），适合 Debian 等大型仓库 |

三种 provider 均支持 `disk_quota`、`atomic_publish`、`check_upstream`、`upstream_fallback`、`cron`、`timezone`、`blackout`、`priority`。

### 镜像同步状态

| 状态 | 线路值 | 含义 |
|------|--------|------|
| `None` | `"none"` | 已注册，从未运行 |
| `PreSyncing` | `"pre-syncing"` | 同步前钩子执行中 |
| `Syncing` | `"syncing"` | 主同步命令执行中 |
| `Success` | `"success"` | 上次同步成功 |
| `Failed` | `"failed"` | 上次同步失败；`error_msg` 含原因 |
| `Paused` | `"paused"` | 被操作员暂停（`tunasynctl stop`） |
| `Disabled` | `"disabled"` | 已禁用，不再运行直到重新启用 |

按状态过滤：

```bash
tunasynctl list --status failed
tunasynctl list --status syncing,pre-syncing
```

### Hook（钩子）

| 钩子 | 说明 |
|------|------|
| `exec_post` | 同步阶段完成后执行命令 |
| `loglimit` | 轮转/截断日志 |
| `docker` | 在 Docker 容器内包装同步任务 |
| `cgroup` | 通过 cgroup 限制 CPU/内存（Linux） |
| `btrfs_snapshot` | 同步前后创建 Btrfs 快照 |
| `zfs_snapshot` | 同步前后创建 ZFS 快照 |

## CLI 参考

### `tunasync`

```
tunasync manager [OPTIONS]
  -c, --config <CONFIG>    配置文件 [默认: /etc/tunasync/manager.conf]
      --addr <ADDR>        覆盖监听地址
      --port <PORT>        覆盖监听端口（默认 14242）
      --cert / --key       TLS 证书/密钥（启用 HTTPS）
      --db-file / --db-type  覆盖数据库路径/类型
      --check              不打开数据库/监听器，仅校验配置

tunasync worker [OPTIONS]
  -c, --config <CONFIG>    配置文件 [默认: /etc/tunasync/worker.conf]
      --check              不启动 worker，校验配置/include/provider
      --emit-netns-policy <PATH>  生成 broker policy 后退出
```

### `tunasynctl`

```
tunasynctl list       [--all] [-w WORKER] [--status STATUS] [--format json|table]
tunasynctl workers
tunasynctl tui
tunasynctl start      <镜像名|GLOB> [-w WORKER] [-f]
tunasynctl stop       <镜像名|GLOB> [-w WORKER]
tunasynctl disable    <镜像名|GLOB> [-w WORKER]
tunasynctl restart    <镜像名|GLOB> [-w WORKER]
tunasynctl reload     <WORKER>
tunasynctl set-size   <镜像名> <大小> [-w WORKER]
tunasynctl rm-worker  <WORKER>
tunasynctl flush      [--stale-only]
tunasynctl stale      [-w WORKER]
tunasynctl maintenance enable | disable | status
tunasynctl completion bash | zsh | fish | powershell
```

`<镜像名|GLOB>` 支持精确名称或 glob 模式（`*`、`?`、`[…]`）。精确名称不触发查询，无额外开销。

配置文件优先级：`/etc/tunasync/ctl.conf` → `~/.config/tunasync/ctl.conf` → `--config FILE` → 命令行参数。

### `tunasync-migrate`

```
tunasync-migrate <go-manager-url 或 bolt 文件路径> <sqlite 输出路径>

# 离线（Go manager 已停止）
tunasync-migrate /var/lib/tunasync/tunasync.db /var/lib/tunasync/new.db

# 在线（Go manager 正在运行）
tunasync-migrate http://localhost:14242 /var/lib/tunasync/new.db
```

## 线路兼容性

`tunasync-protocol` 完整往返 Go 产生的每种 JSON 结构。conformance 测试见 `crates/protocol/tests/wire_compat.rs`。

注意事项：

- `SyncStatus::PreSyncing` 序列化为 `"pre-syncing"`（含连字符），与 Go 一致
- Go 的 `time.Time{}` 零值（`"0001-01-01T00:00:00Z"`）由 `tunasync_protocol::zero_time()` 保留。**不要**用 `chrono::DateTime::default()` 表示"未设置"时间戳 — 那是 Unix 纪元，是不同的哨兵值
- `MirrorStatus::scheduled` 在线路上为 `next_schedule`（与 Go struct tag 一致）
- `MirrorStatus` 扩展字段（`last_transferred_bytes`、`total_transferred_bytes`、`consecutive_failures`、`stale`）使用 `#[serde(default, skip_serializing_if = "is_default")]`，全部为零/false 时 JSON 与纯 Go 输出完全一致

### 与 Go 版本的已知差异

| 方面 | 差异 | 原因 |
|------|------|------|
| Manager：size 更新 | `&&` 替代 Go 的错误 `\|\|` | Go bug：条件恒为真 |
| Manager：心跳 | 新增 `POST /workers/:id/heartbeat` | 比隐式刷新更健壮 |
| Manager：deleteWorker | 无效 ID 返回 400（Go 返回 500） | 更有用的错误信息 |
| Manager：数据库 | redb、sqlite、redis | 不支持 BoltDB/LevelDB/Badger |
| Manager：GET /jobs/:name | 含 `error_msg` 的镜像详情 | 前端新接口 |
| Manager：维护模式 | `POST/DELETE/GET /maintenance` | Go 无此功能 |
| Worker：调度 | Fixed-delay/fixed-rate + Cron、时区、Blackout | Go 无此功能 |
| Worker：命名空间出口 | 经 broker 的每镜像 Linux 网络命名空间隔离 | Go 无此功能 |
| Worker：manager 上报 | 非阻塞 report actor，有界重放/合并 | Go 无此功能 |
| Worker：磁盘配额 | 同步前空间检查 | Go 无此功能 |
| Worker：优先级 | `PrioritySemaphore` 排序 | Go 无此功能 |
| Worker：原子发布 | `renameat2(RENAME_EXCHANGE)` 交换 | Go 无此功能 |
| Worker：上游探测 | 并发探测，15 秒硬超时 | Go 无此功能 |
| Worker：实时日志 SSE | `GET /jobs/:mirror/log/stream` | Go 无此功能 |
| 配置迁移 | manager/worker 静态 `--check`；仅 Go 支持的数据库必须迁移 | 防止静默语义漂移 |

## API 参考

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/metrics` | Prometheus 指标 |
| GET | `/jobs` | 列出所有镜像（摘要） |
| GET | `/jobs/:name` | 跨所有 worker 的镜像详情（含 `error_msg`） |
| DELETE | `/jobs/disabled` | 清除 disabled 镜像行 |
| GET | `/workers` | 列出已注册 worker（token 脱敏） |
| POST | `/workers` | 注册 worker |
| DELETE | `/workers/:id` | 删除 worker |
| POST | `/workers/:id/heartbeat` | Worker 心跳 |
| GET | `/workers/:id/jobs` | 列出某 worker 的镜像 |
| POST | `/workers/:id/jobs/:job` | 更新镜像状态 |
| POST | `/workers/:id/jobs/:job/size` | 更新镜像大小 |
| POST | `/workers/:id/schedules` | 更新调度信息 |
| POST | `/cmd` | 控制命令（start/stop/disable/reload） |
| POST | `/maintenance` | 启用维护模式 |
| DELETE | `/maintenance` | 关闭维护模式 |
| GET | `/maintenance` | 获取维护模式状态 |

### Prometheus 指标 (`GET /metrics`)

manager 在 `/metrics` 暴露 Prometheus 文本格式的指标。所有按镜像统计的指标都带 `mirror="<名称>",worker="<worker_id>"` 标签。

| 指标名 | 类型 | 说明 |
|---|---|---|
| `tunasync_workers_total` | gauge | 当前已注册的 worker 数量 |
| `tunasync_mirrors_total{status="..."}` | gauge | 按状态分组的镜像数量。`status` 取值：`none`、`pre-syncing`、`syncing`、`success`、`failed`、`paused`、`disabled` |
| `tunasync_mirror_status{mirror,worker}` | gauge | 单个镜像的状态码：`0=none 1=pre-syncing 2=syncing 3=success 4=failed 5=paused 6=disabled` |
| `tunasync_mirror_size_bytes{mirror,worker}` | gauge | 镜像数据大小（字节）。从 rsync `--stats` 输出解析得来；无法识别时为 `-1` |
| `tunasync_mirror_last_success_timestamp_seconds{mirror,worker}` | gauge | 最近一次成功同步（`last_update`）的 Unix 时间戳；从未成功则为 `0` |
| `tunasync_mirror_last_sync_duration_seconds{mirror,worker}` | gauge | 最近一次完成的同步耗时秒数（`last_ended - last_started`），从未运行则为 `0` |
| `tunasync_mirror_last_transferred_bytes{mirror,worker}` | gauge | 最近一次同步传输的字节数；未知时为 `0`。每次新同步会重置 |
| `tunasync_mirror_total_transferred_bytes{mirror,worker}` | counter | 该 `(mirror, worker)` 累计同步传输字节数，单调不减，配合 `rate()` 即可获得带宽 |

#### 常用查询

近 1 小时的传输速率（字节/秒）：
```promql
rate(tunasync_mirror_total_transferred_bytes[1h])
```

镜像同步耗时的 95 分位：
```promql
quantile by (mirror) (0.95, tunasync_mirror_last_sync_duration_seconds)
```

当前处于失败状态的镜像（manager 将 `mirror_status` 从 `3=success` 翻到 `4=failed` 但保留 `last_success_timestamp_seconds`）：
```promql
tunasync_mirror_status == 4
```

48 小时未成功同步的镜像（用时间戳和 Prometheus 的 `time()`）：
```promql
time() - tunasync_mirror_last_success_timestamp_seconds > 48 * 3600
```

#### `total_transferred_bytes` 累加逻辑

worker 从每次 rsync `--stats` 日志解析 `Total transferred file size`（`crates/common/src/util.rs` 中的 `extract_transferred_bytes_from_rsync_log`），作为 `last_transferred_bytes` 上报。manager 端（`crates/manager/src/server.rs` 的 `update_job_of_worker`）只在状态进入 `Success` 的那一次累加新值：

```rust
if incoming.status == SyncStatus::Success && cur.status != SyncStatus::Success {
    incoming.total_transferred_bytes =
        cur.total_transferred_bytes + incoming.last_transferred_bytes;
} else {
    incoming.total_transferred_bytes = cur.total_transferred_bytes;
}
```

也就是说每完成一次同步累加一次。这个数字**不**包含 hook 脚本（如 `exec_post`）上传到 CDN 的流量；执行 `flush` 清除已 disabled 的镜像时整行被删除，重新注册同名镜像时计数器从 0 开始。

非 rsync 类 provider（`command`、`two-stage-rsync`）只有在 `exec_log_file` 输出格式与 rsync stats 兼容时才会汇报 `last_transferred_bytes`，否则两项指标都保持 `0`。

## Worker 同步日志流式接口

worker 暴露了一个 Server-Sent Events（SSE）接口，可以**实时**流式输出某个镜像当前同步的 stdout/stderr，并在客户端连入时**自动回放最近若干行**，避免页面刚打开时一片空白：

```
GET http://<worker-host>:<worker-port>/jobs/<mirror-name>/log/stream
```

响应 `Content-Type: text/event-stream`。客户端连接时，**当前**同步最近 ~10 行会作为普通的 SSE `data:` 事件先回放出来，然后无缝接上实时流。回放缓冲区在每次新同步开始时清空，所以**只会看到本次同步**的内容，看不到上一次的残留。（回放缓冲故意做得很小——只是为了「中途打开页面不空白」，不是历史日志。要看更多历史请直接读 `log_dir` 下轮转的日志文件。）

其它行为：

- **原子性的快照 + 订阅**：握手期间产生的日志行只会到达流一次——不会重复，也不会丢失。
- **15 秒一次的 keep-alive 注释**：用于穿透代理/浏览器的空闲超时。
- **慢消费者保护**：如果订阅者在实时通道上落后超过 1024 行，会收到一个 `event: lag` 通知，然后从最新行继续。
- **超出范围的历史**：早于回放缓冲、或属于上一次同步的内容只在 `log_dir` 下的轮转日志文件里，不在流里。

未知的镜像名返回 `404`。

> **安全提示**：`api_token` 为空时，worker 命令端点（`POST /`）和本 SSE 接口都不带鉴权；配置 `api_token` 后，两者都要求 `Authorization: Bearer <token>`，manager 在转发命令或代理日志时会自动附带该请求头。为保持兼容，省略 `listen_addr` 时仍会绑定 `0.0.0.0`；同机部署应显式设置为 `127.0.0.1`，跨主机部署则应绑定私网接口并用防火墙限制来源。跨主机通信时应使用私有 CA 启用 TLS。**不要**把 worker 端口直接暴露到公网。

### 接口输出示例

向 worker 默认端口发起命令行请求：

```sh
curl -N http://localhost:6000/jobs/debian/log/stream
```

输出（每条 `data:` 行就是 rsync 子进程的一行 stdout/stderr）：

```
: subscribed

data: receiving incremental file list
data: pool/main/a/apt/apt_2.7.14_amd64.deb
data:      2,047,438 100%   23.50MB/s    0:00:00 (xfr#1, ir-chk=1023/4096)
data: pool/main/a/apt/apt_2.7.14_amd64.changes
data:          1,234 100%    1.20KB/s    0:00:00 (xfr#2, ir-chk=1022/4096)

data: sent 32.42K bytes  received 19.85M bytes  4.42M bytes/sec
data: total size is 124.36G  speedup is 6249.18

: keep-alive

: keep-alive
```

说明：
- 第一行 `: subscribed` 是 SSE 注释（以 `:` 开头），用于让客户端立刻确认连接已建立。
- 中间的 `data: ...` 行就是回放缓冲 + 实时输出，回放和实时在线协议层没有区别，前端 `onmessage` 一个回调处理即可。
- 空行（每个事件之间的 `\n\n`）是 SSE 协议的事件分隔符。
- 隔 15 秒一次的 `: keep-alive` 是注释（不会触发 `onmessage`），仅用来保活，前端可以忽略。

如果同步过程很慢，订阅者读得跟不上，会出现：

```
event: lag
data: dropped 47 line(s); subscriber too slow
```

这种事件可以单独监听（见下方前端示例）。

### 浏览器 `EventSource` 用法

未启用 token 鉴权时，可以直接使用浏览器原生 `EventSource`：

```js
const es = new EventSource("/jobs/debian/log/stream");

// 回放和实时两类都通过 onmessage 触发，前端不用分支
es.onmessage = (e) => {
  appendLine(e.data);   // e.data 就是 rsync 输出的一行原文
};

// 跟不上时的提示
es.addEventListener("lag", (e) => {
  console.warn("跟不上了:", e.data);
});

// 连接掉了浏览器会自动重连——不需要业务侧处理
es.onerror = (e) => {
  console.warn("SSE 连接中断，浏览器会自动重连");
};
```

浏览器原生 `EventSource` 无法设置 `Authorization` 请求头。启用 `api_token` 后，应使用支持自定义请求头、基于 `fetch` 的 SSE 客户端，或者由可信的同源反向代理完成鉴权。直接接口位于 worker；一般浏览器部署应优先使用下方的 manager 代理。

### Manager 端代理（推荐用于浏览器前端）

manager 也暴露了同样的路径，会透明地代理到拥有该镜像的 worker：

```
GET http://<manager-host>:<manager-port>/jobs/<mirror-name>/log/stream
```

manager 会先查出镜像归属哪个 worker，打开该 worker 的流（如果配置了 `ca_cert` 会复用 TLS 证书锁定），并将事件原样无缓冲转发出来（同时设置 `X-Accel-Buffering: no` 以兼容前置的 nginx）。这样 Web 前端只需要访问 **manager** 来源即可，worker 端口可以保持仅对 manager 主机开放。启用 `api_token` 后，客户端访问该 manager 路由时同样必须通过鉴权。响应：未知镜像 `404`、归属 worker 不可达 `502`，否则原样转发 worker 的事件流。

## 许可证

GPL-3.0-or-later，与上游相同。
