# 网络命名空间出口

**平台：** 仅 Linux，内核 5.6+。**相关：** [worker 配置](../examples/worker.conf) · [English](network-namespaces.md)

`network_namespace` 让指定镜像的同步命令和上游探针在**预先存在且具名**的 `/run/netns` 命名空间中运行。tunasync 不创建或管理命名空间、路由、DNS、VPN/WARP 客户端、防火墙规则、账户或命名空间生命周期。

## 安全边界

```
tunasync-worker（User=tunasync，宿主网络）
  指定同步命令 + 探针 -- Unix socket/generation -->
tunasync-netns-broker（root、策略受限） --> /run/netns/<name>
```

worker 使用 `NoNewPrivileges=yes`。root broker 通过 `SO_PEERCRED` 认证 peer，验证受信策略，固定命名空间 device/inode 与可执行文件，并关闭失败：绝不回退至宿主网络。它仅接受 root 控制的 ELF、拒绝 shebang 脚本，使用 `openat2`/`execveat`，随后丢弃组、UID/GID、capabilities 并启用 `no_new_privs`。seccomp 阻止 `setsid` 和 `setpgid`。

只有同步命令和探针进入命名空间；manager 流量和 hooks 留在宿主网络。同一镜像的 Docker 与 `network_namespace` 会被拒绝；当前阶段中任一镜像使用命名空间时也会拒绝 `cgroup.enable`。namespaced `upstream`/`upstream_fallback` 不允许 userinfo、query、fragment；策略仅可授权 secret 环境变量的**键**，不会记录其值。

> **警告：路由/NAT 不是隔离。** veth 加 MASQUERADE 只提供连通性，不是出口防火墙。broker 约束进程/命名空间启动；实际目的地限制依赖站点路由、DNS 和防火墙策略。

## 安装与单元顺序

以幂等方式创建账户。如果当前目录是由本版本构建并解压后的发布包，请安装其中的打包路径：

```bash
getent group tunasync >/dev/null || sudo groupadd --system tunasync
id -u tunasync >/dev/null 2>&1 || sudo useradd --system --gid tunasync --shell /usr/sbin/nologin --no-create-home tunasync
sudo install -m 0755 bin/tunasync /usr/bin/tunasync
sudo install -m 0755 bin/tunasynctl /usr/bin/tunasynctl
sudo install -m 0755 bin/tunasync-netns-broker /usr/bin/tunasync-netns-broker
sudo install -m 0644 systemd/tunasync-worker.service /etc/systemd/system/tunasync-worker.service
sudo install -m 0644 systemd/tunasync-netns-broker.service /etc/systemd/system/tunasync-netns-broker.service
sudo install -D -m 0644 systemd/tunasync-worker.service.d/netns.conf /etc/systemd/system/tunasync-worker.service.d/netns.conf
```

如果使用源码 checkout，先执行 `cargo build --release`，再改用仓库中的路径：

```bash
sudo install -m 0755 target/release/tunasync /usr/bin/tunasync
sudo install -m 0755 target/release/tunasynctl /usr/bin/tunasynctl
sudo install -m 0755 target/release/tunasync-netns-broker /usr/bin/tunasync-netns-broker
sudo install -m 0644 initscripts/tunasync-worker.service /etc/systemd/system/tunasync-worker.service
sudo install -m 0644 initscripts/tunasync-netns-broker.service /etc/systemd/system/tunasync-netns-broker.service
sudo install -D -m 0644 initscripts/tunasync-worker-netns.conf /etc/systemd/system/tunasync-worker.service.d/netns.conf
```

`Wants=` 会机会性地请求启动 broker，并使 worker 排在其后；它不是硬依赖。broker 就绪和 namespaced 启动均会关闭失败。broker 单元以 `ConditionPathExists=` 检查策略，因此必须先生成策略再启动。

必需顺序：**创建账户；安装；配置命名空间/防火墙；安装 root 控制的配置；生成/检查策略；daemon-reload；启用/启动 broker 和 worker；验证单元及命名空间。**

`ProtectSystem=strict` 使任意配置路径保持只读，尽管 systemd 会提供可写的 `RuntimeDirectory=`、`StateDirectory=` 和 `LogsDirectory=` 位置。每个实际使用的自定义 `mirror_dir`、`staging_dir`、`log_dir` 以及必须写入的 hook 输出路径都要加入 `ReadWritePaths=`：

```ini
# /etc/systemd/system/tunasync-worker.service.d/paths.conf
[Service]
ReadWritePaths=/data/mirrors /data/tunasync/staging /data/tunasync/log
```

## 命名空间拓扑（临时示例）

前置条件：`iproute2`、`curl` 或其他测试客户端、站点防火墙后端（适用时优先持久化并经审查的 nftables）。下列命令是**临时、非幂等的说明示例**，不是通用防火墙配方。重复执行 `iptables -A` 会重复添加规则。请用发行版/站点工具规划持久化与清理；没有本地生命周期上下文时不要照抄破坏性的删除命令。

```bash
# 用本站实际接口、地址、解析器和允许的端点替换。
sudo ip netns add egress-a
sudo ip link add veth-egress-host type veth peer name veth-egress-ns
sudo ip link set veth-egress-ns netns egress-a
sudo ip addr add 192.0.2.1/24 dev veth-egress-host
sudo ip link set veth-egress-host up
sudo ip netns exec egress-a ip link set lo up
sudo ip netns exec egress-a ip addr add 192.0.2.2/24 dev veth-egress-ns
sudo ip netns exec egress-a ip link set veth-egress-ns up
sudo ip netns exec egress-a ip route add default via 192.0.2.1
sudo mkdir -p /etc/netns/egress-a
# 1.1.1.1 仅为示例：使用站点批准的解析器，不能据此假定为 Gateway DNS。
printf 'nameserver 1.1.1.1\n' | sudo tee /etc/netns/egress-a/resolv.conf >/dev/null
```

`net.ipv4.ip_forward=1` 是主机范围的设置。只应在审查默认拒绝的转发策略后启用；请使用发行版工具持久化和清理。IPv4 NAT **不控制 IPv6**。不使用 IPv6 时要明确为该命名空间/veth 禁用它；否则实现等价的 IPv6 路由和默认拒绝策略。

拓扑完成后，必须实现并审查持久化防火墙策略（适用时优先 nftables），其形状至少包括：来自 veth 的转发默认拒绝；允许 established/related 回包；仅允许显式批准的 DNS 解析器 TCP/UDP 53；仅允许批准的上游目的地/端口（例如 TCP 873/80/443）；除非确有需要，拒绝宿主管理地址、私有/RFC1918、链路本地和 metadata 端点；以及反欺骗。DNS 限制依赖该防火墙及验证。

检查两个协议族，并测试受限的批准端点：

```bash
sudo ip netns exec egress-a ip -4 route
sudo ip netns exec egress-a ip -6 route
sudo ip netns exec egress-a curl --fail --max-time 10 https://approved.example.invalid/healthz
```

一次正向请求不能证明出口身份、WARP/Gateway 使用、DNS 控制或隔离。启用 worker 前，必须对宿主/内部/metadata/未批准的 IPv4 **及 IPv6** 目的地进行负向测试。

## 受信配置与策略生命周期

策略生成是 root 受信的部署步骤。非特权 worker 必须能够读取配置并遍历其目录，但不能修改任何策略输入。配置应由 root 所有，允许专用服务组读取，并确保 `tunasync`、其组或其他用户均不可写。调用 sudo emitter 前，`/etc/tunasync`、`worker.conf`、每个 include 配置及其父目录、每个授权的可执行文件及其所有父路径组件都必须满足此信任边界。例如：

```bash
sudo install -d -o root -g tunasync -m 0750 /etc/tunasync
sudo install -o root -g tunasync -m 0640 worker.conf /etc/tunasync/worker.conf
sudo stat -c '%U:%G %a %n' /etc/tunasync /etc/tunasync/worker.conf
# 对 include、其父目录、可执行文件及可执行文件父路径重复检查。
```

生成后的 broker policy 具有更严格的可见性要求：broker 的最低接受条件是 root 所有且组/其他用户不可写，本指南要求 policy 始终保持 `root:root`、模式 `0600`。配置目录仍由 root 所有且组/其他用户不可写，同时向 `tunasync` 组授予读取/遍历权限。

使用能标识**精确完整** namespaced launch policy 的唯一 generation。内容改变时绝不可重用 generation；替换命名空间对象（新的 device/inode）同样需要新 generation。

```bash
# 准备 root 控制的 worker.conf/映射并选择新 generation 后：
sudo tunasync worker --check -c /etc/tunasync/worker.conf
sudo tunasync worker -c /etc/tunasync/worker.conf --emit-netns-policy /etc/tunasync/netns-policy.json
sudo stat -c '%U:%G %a %n' /etc/tunasync/netns-policy.json
sudo tunasync-netns-broker --check --config /etc/tunasync/netns-policy.json --uid tunasync --gid tunasync
sudo systemctl daemon-reload
sudo systemctl restart tunasync-netns-broker
# 重启 worker；仅在 broker 已就绪且变更兼容时才使用 reload。
sudo systemctl restart tunasync-worker
sudo systemctl is-active tunasync-netns-broker tunasync-worker
sudo systemctl status tunasync-netns-broker tunasync-worker
```

emitter 会原子写入模式 `0600`；只有因它以 root 运行才是 root 所有。必须验证，不能假定。最后进行真实的 namespaced 批准与负向探针。回滚时，必须同时恢复配置**及其匹配的策略/generation**，再校验、检查、重启和重新探测；绝不可混用 generation。

## 排障

- 检查 `/run/netns/egress-a`、IPv4/IPv6 路由、批准的 DNS、防火墙计数器及正负探针。
- 检查策略 generation、socket 访问，并运行 worker 与 broker `--check`。
- 检查所有可执行文件均为绝对路径、root 控制的 ELF，且每个配置/include/父路径均受信。
- 检查每个自定义 `mirror_dir`、`staging_dir`、**`log_dir`** 与可写 hook 输出的 `ReadWritePaths=`。
- 确认内核 5.6+，且容器或 systemd syscall 策略未阻止 `openat2`/`execveat`。

## Cloudflare One WARP/Gateway 边界

官方资料：[手动部署](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/deployment/manual-deployment/)、[Split Tunnels](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/configure/route-traffic/split-tunnels/)、[Local Domain Fallback](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/configure/route-traffic/local-domains/) 和 [egress policies](https://developers.cloudflare.com/cloudflare-one/traffic-policies/egress-policies/)。

Cloudflare 文档要求以**当前用户**而非 root 执行注册：以该用户运行 `warp-cli registration new <team>`，随后验证状态并连接。其手动部署文档指出，在 VM 中单独部署 client 时需要 bridged networking。Gateway egress policies 仅适用于符合条件的 Enterprise 计划。Dedicated egress IP 需要 Cloudflare/账户团队的资格和配置，且具有已文档化的负载均衡/故障切换行为；它不是 tunasync 的每 netns 功能。

Split Tunnels 是路由/可见性机制，不是宿主命名空间防火墙：在 Include 模式下，未被包含的流量会绕过 Gateway network/HTTP policies。宿主 WARP client 可提供共享的宿主/NAT 出口，但 tunasync 不保证每个命名空间的 WARP identity 或 egress。除非 Cloudflare 已验证该架构，不要在网络命名空间中运行多个 `warp-svc`；本指南示例未实现该架构。`cloudflared` Tunnel 不是通用 rsync Internet egress。
