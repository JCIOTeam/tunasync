# Network Namespace Egress

**Platform:** Linux only; kernel 5.6+. **Related:** [worker configuration](../examples/worker.conf) · [中文](network-namespaces_zh.md)

`network_namespace` runs a selected mirror's sync command and upstream probes in a **pre-existing named** `/run/netns` namespace. tunasync does not create or manage namespaces, routes, DNS, VPN/WARP clients, firewall rules, accounts, or namespace lifecycle.

## Security boundary

```
tunasync-worker (User=tunasync, host network)
  selected sync command + probes -- Unix socket/generation -->
tunasync-netns-broker (root, policy constrained) --> /run/netns/<name>
```

The worker has `NoNewPrivileges=yes`. The root broker authenticates its peer with `SO_PEERCRED`, validates a trusted policy, pins the namespace device/inode and executable, and fails closed: no host-network fallback exists. It accepts only root-controlled ELF executables, not shebang scripts; uses `openat2`/`execveat`; then drops groups, UID/GID, capabilities, and applies `no_new_privs`. Seccomp blocks `setsid` and `setpgid`.

Only sync commands and probes enter the namespace. Manager traffic and hooks remain on the host network. Docker plus `network_namespace` is rejected per mirror; the current phase also rejects `cgroup.enable` when any mirror uses a namespace. Namespaced `upstream`/`upstream_fallback` reject userinfo, query, and fragments. Policy may authorize secret environment **keys**, never their values.

> **Warning — routing/NAT is not containment.** A veth plus MASQUERADE only provides connectivity; it is not an egress firewall. The broker constrains process/namespace launch, while actual destination containment depends on site routing, DNS, and firewall policy.

## Install and unit ordering

Create the account idempotently. If you are inside an extracted release archive built from this revision, install its packaged paths:

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

For a source checkout, first run `cargo build --release`, then use the repository paths instead:

```bash
sudo install -m 0755 target/release/tunasync /usr/bin/tunasync
sudo install -m 0755 target/release/tunasynctl /usr/bin/tunasynctl
sudo install -m 0755 target/release/tunasync-netns-broker /usr/bin/tunasync-netns-broker
sudo install -m 0644 initscripts/tunasync-worker.service /etc/systemd/system/tunasync-worker.service
sudo install -m 0644 initscripts/tunasync-netns-broker.service /etc/systemd/system/tunasync-netns-broker.service
sudo install -D -m 0644 initscripts/tunasync-worker-netns.conf /etc/systemd/system/tunasync-worker.service.d/netns.conf
```

`Wants=` opportunistically requests the broker and orders the worker after it; it is not a hard dependency. Broker readiness and namespaced launch fail closed. The broker unit has `ConditionPathExists=` for the policy, so generate policy before starting it.

Required order: **create account; install; configure namespace/firewall; install root-controlled config; generate/check policy; daemon-reload; enable/start broker and worker; verify units and namespace.**

`ProtectSystem=strict` leaves arbitrary configured paths read-only, although systemd provisions writable `RuntimeDirectory=`, `StateDirectory=`, and `LogsDirectory=` locations. Add `ReadWritePaths=` for every effective custom `mirror_dir`, `staging_dir`, and `log_dir`, plus every hook output path that must be writable:

```ini
# /etc/systemd/system/tunasync-worker.service.d/paths.conf
[Service]
ReadWritePaths=/data/mirrors /data/tunasync/staging /data/tunasync/log
```

## Namespace topology (illustrative, transient)

Prerequisites: `iproute2`, `curl` or another test client, and a site firewall backend (prefer persistent reviewed nftables where applicable). The following commands are **transient, non-idempotent illustrations**, not a universal firewall recipe. Re-running `iptables -A` duplicates rules. Plan persistence and teardown with distribution/site tooling; do not copy destructive deletion commands without local lifecycle context.

```bash
# Replace interface, addresses, resolver, and approved endpoint for this site.
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
# 1.1.1.1 is only an example: use a site-approved resolver, not assumed Gateway DNS.
printf 'nameserver 1.1.1.1\n' | sudo tee /etc/netns/egress-a/resolv.conf >/dev/null
```

`net.ipv4.ip_forward=1` is host-wide. Enable it only after reviewing a default-deny forwarding policy; persist and tear it down using distribution tooling. IPv4 NAT provides **no IPv6 control**. Explicitly disable IPv6 for this namespace/veth when unused, or implement equivalent IPv6 routing and default-deny policy.

After topology setup, implement and review a persistent firewall policy with this mandatory shape (nftables preferred where applicable): default-drop forwarding from the veth; permit established/related return traffic; permit only explicit approved DNS resolver TCP/UDP 53; permit approved upstream destinations and ports (for example TCP 873/80/443); deny host management addresses, private/RFC1918 space, link-local, and metadata endpoints unless required; and apply anti-spoofing. DNS confinement requires that firewall policy and validation.

Check both families and test an approved, bounded endpoint:

```bash
sudo ip netns exec egress-a ip -4 route
sudo ip netns exec egress-a ip -6 route
sudo ip netns exec egress-a curl --fail --max-time 10 https://approved.example.invalid/healthz
```

A positive request does not prove egress identity, WARP/Gateway use, DNS control, or containment. Require negative tests for host/internal/metadata/non-approved IPv4 **and IPv6** destinations before enabling the worker.

## Trusted configuration and policy lifecycle

Policy generation is a root-trusted deployment step. The unprivileged worker must be able to read and traverse its configuration, but it must not be able to modify any policy input. Install the configuration as root-owned, group-readable by the dedicated service group, and not writable by `tunasync`, its group, or others. Before invoking the sudo emitter, `/etc/tunasync`, `worker.conf`, every included config and its parent directories, every authorized executable, and every executable parent path component must satisfy that trust boundary. For example:

```bash
sudo install -d -o root -g tunasync -m 0750 /etc/tunasync
sudo install -o root -g tunasync -m 0640 worker.conf /etc/tunasync/worker.conf
sudo stat -c '%U:%G %a %n' /etc/tunasync /etc/tunasync/worker.conf
# Repeat for includes, their parents, executables, and executable parent paths.
```

The generated broker policy has a stricter visibility requirement: broker minimum acceptance is root-owned and not group/other writable, and this guide requires the policy to remain `root:root` mode `0600`. The configuration directory remains root-owned and not group/other writable while granting the `tunasync` group read/traverse access.

Use a unique generation identifying the **exact** complete namespaced launch policy. Never reuse a generation for changed content; namespace object replacement (new device/inode) also requires a new generation.

```bash
# After preparing root-controlled worker.conf/mapping and choosing a new generation:
sudo tunasync worker --check -c /etc/tunasync/worker.conf
sudo tunasync worker -c /etc/tunasync/worker.conf --emit-netns-policy /etc/tunasync/netns-policy.json
sudo stat -c '%U:%G %a %n' /etc/tunasync/netns-policy.json
sudo tunasync-netns-broker --check --config /etc/tunasync/netns-policy.json --uid tunasync --gid tunasync
sudo systemctl daemon-reload
sudo systemctl restart tunasync-netns-broker
# Restart worker, or reload only after broker is ready and the change is compatible.
sudo systemctl restart tunasync-worker
sudo systemctl is-active tunasync-netns-broker tunasync-worker
sudo systemctl status tunasync-netns-broker tunasync-worker
```

The emitter atomically writes mode `0600`; it is root-owned only because it runs as root. Verify instead of assuming. Finally run a real namespaced approved and negative probe. To roll back, restore the configuration **and its matching policy/generation together**, then validate, check, restart, and re-probe; never mix generations.

## Troubleshooting

- Verify `/run/netns/egress-a`, IPv4/IPv6 routes, approved DNS, firewall counters, and both positive and negative probes.
- Verify policy generation and socket access; run worker and broker `--check`.
- Verify every executable is absolute, root-controlled ELF and every config/include/parent path is trusted.
- Check `ReadWritePaths=` for every custom `mirror_dir`, `staging_dir`, **`log_dir`**, and writable hook output.
- Confirm kernel 5.6+ and that `openat2`/`execveat` are not blocked by container or systemd syscall policy.

## Cloudflare One WARP/Gateway boundary

Official background: [manual deployment](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/deployment/manual-deployment/), [Split Tunnels](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/configure/route-traffic/split-tunnels/), [Local Domain Fallback](https://developers.cloudflare.com/cloudflare-one/team-and-resources/devices/cloudflare-one-client/configure/route-traffic/local-domains/), and [egress policies](https://developers.cloudflare.com/cloudflare-one/traffic-policies/egress-policies/).

Cloudflare documents enrollment as the **current user**, not root: run `warp-cli registration new <team>` as that user, then verify status and connect. Its manual deployment documentation says a separately deployed client in a VM requires bridged networking. Gateway egress policies are available only on eligible Enterprise plans. Dedicated egress IPs require Cloudflare/account-team eligibility and configuration and have documented load-balancing/failover behavior; they are not a per-netns tunasync feature.

Split Tunnels is routing/visibility, not a host namespace firewall: in Include mode, traffic not included bypasses Gateway network/HTTP policies. A host WARP client may provide shared host/NAT egress, but tunasync does not guarantee per-namespace WARP identities or egress. Do not run multiple `warp-svc` instances in network namespaces absent Cloudflare validation; this guide's topology does not implement that architecture. `cloudflared` Tunnel is not generic rsync Internet egress.
