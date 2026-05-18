//! `tunasynctl` — CLI control tool for a tunasync manager.
//!
//! Config file priority (matches Go exactly):
//!   1. `/etc/tunasync/ctl.conf`          (system-wide)
//!   2. `$HOME/.config/tunasync/ctl.conf` (user-specific)
//!   3. `--config FILE`                   (explicit override)
//!   4. CLI flags (`--manager`, `--port`, `--ca-cert`)
//!
//! Language detection order (first match wins):
//!   1. `TUNASYNCTL_LANG=zh` (or any value starting with "zh")
//!   2. `LANG`, `LANGUAGE`, `LC_ALL`, `LC_MESSAGES` starting with "zh"
//!   3. Default: English

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::DateTime;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use clap_complete::Shell;
use futures::future::join_all;
use serde::Deserialize;
use tunasync_protocol::{ClientCmd, CmdVerb, MirrorStatus, WebMirrorStatus, WorkerStatus};

// ── i18n ─────────────────────────────────────────────────────────────────────

/// Returns true when the effective locale is Chinese.
///
/// Checks (in order):
///   1. `TUNASYNCTL_LANG` — explicit override (`zh` → Chinese, anything else → English)
///   2. `LANG`, `LANGUAGE`, `LC_ALL`, `LC_MESSAGES` — standard POSIX locale vars
fn is_zh() -> bool {
    if let Ok(v) = std::env::var("TUNASYNCTL_LANG") {
        return v.to_ascii_lowercase().starts_with("zh");
    }
    for var in &["LANG", "LANGUAGE", "LC_ALL", "LC_MESSAGES"] {
        if let Ok(v) = std::env::var(var) {
            if v.to_ascii_lowercase().starts_with("zh") {
                return true;
            }
        }
    }
    false
}

/// Pick between an English and a Chinese string based on the current locale.
macro_rules! t {
    ($en:expr, $zh:expr) => {
        if is_zh() {
            $zh
        } else {
            $en
        }
    };
}

// ── Config file ───────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CtlConfig {
    #[serde(default)]
    manager_addr: String,
    #[serde(default)]
    manager_port: Option<u16>,
    #[serde(default)]
    ca_cert: String,
}

impl CtlConfig {
    fn load(explicit: Option<&PathBuf>) -> Self {
        let mut merged = Self::default();
        let system_path = PathBuf::from("/etc/tunasync/ctl.conf");
        let user_path = std::env::var("HOME")
            .ok()
            .map(|h| PathBuf::from(h).join(".config/tunasync/ctl.conf"))
            .unwrap_or_default();

        for path in [Some(system_path), Some(user_path), explicit.cloned()]
            .into_iter()
            .flatten()
        {
            if path.exists() {
                match tunasync_common::config::load_toml::<CtlConfig>(&path) {
                    Ok(cfg) => {
                        tracing::debug!(path = %path.display(), "loaded ctl config");
                        if !cfg.manager_addr.is_empty() {
                            merged.manager_addr = cfg.manager_addr;
                        }
                        if cfg.manager_port.is_some() {
                            merged.manager_port = cfg.manager_port;
                        }
                        if !cfg.ca_cert.is_empty() {
                            merged.ca_cert = cfg.ca_cert;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(path = %path.display(), error = %e, "failed to load ctl config");
                    }
                }
            }
        }
        merged
    }
}

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "tunasynctl",
    version = tunasync_common::VERSION,
    about = "Control a tunasync manager",
    long_about = None,
)]
struct Cli {
    /// Explicit config file (overrides system and user config files).
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Manager host or IP address.
    #[arg(short, long, env = "TUNASYNC_MANAGER", global = true)]
    manager: Option<String>,

    /// Manager port.
    #[arg(short, long, env = "TUNASYNC_MANAGER_PORT", global = true)]
    port: Option<u16>,

    /// CA cert for TLS verification (enables HTTPS).
    #[arg(long, global = true)]
    ca_cert: Option<PathBuf>,

    /// Verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List all mirror jobs.
    List {
        /// List jobs of a specific worker; omit for all workers.
        #[arg(short, long)]
        worker: Option<String>,
        /// Filter by status (comma-separated: syncing,failed,success,…).
        #[arg(long)]
        status: Option<String>,
        /// Output format: `json` (default) or `table`.
        #[arg(long, default_value = "json")]
        format: String,
        /// Show all workers' jobs.
        #[arg(long)]
        all: bool,
    },
    /// List all registered workers.
    Workers,
    /// Flush all disabled job rows from the manager DB.
    Flush {
        /// Safety guard: only flush if at least one currently-disabled
        /// mirror is also marked stale. Note that the manager endpoint
        /// is all-or-nothing — when this flag is enabled and the guard
        /// passes, ALL disabled mirrors are still removed (not just
        /// the stale ones), because the manager has no per-mirror flush.
        #[arg(long)]
        stale_only: bool,
    },
    /// List all mirrors currently marked as stale by the manager's stale
    /// detector. Read-only — does not flush or modify anything. Useful for
    /// operators checking whether the stale_after threshold has fired.
    Stale,
    /// Remove a worker from the manager.
    RmWorker {
        /// Worker ID.
        worker: String,
    },
    /// Update the size of a mirror (operator override).
    SetSize {
        /// Mirror name.
        mirror: String,
        /// Human-readable size string, e.g. `1.2T`.
        size: String,
        /// Restrict to a specific worker.
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Start a mirror job.
    Start {
        /// Mirror name, or `all` to broadcast.
        mirror: String,
        /// Restrict to a specific worker.
        #[arg(short, long)]
        worker: Option<String>,
        /// Ignore concurrency limit (force-start).
        #[arg(short, long)]
        force: bool,
    },
    /// Stop a running mirror job.
    Stop {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Disable a mirror job.
    Disable {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Restart a mirror job.
    Restart {
        mirror: String,
        #[arg(short, long)]
        worker: Option<String>,
    },
    /// Tell a worker to reload its config from disk.
    Reload {
        /// Worker ID.
        worker: String,
    },
    /// Generate shell completion script.
    ///
    /// Usage:
    ///   tunasynctl completion bash >> ~/.bashrc
    ///   tunasynctl completion zsh  >> ~/.zshrc
    Completion {
        /// Shell type.
        shell: Shell,
    },
    /// Manage global maintenance mode on the manager.
    ///
    /// In maintenance mode the manager rejects all incoming sync-start requests.
    Maintenance {
        #[command(subcommand)]
        action: MaintenanceAction,
    },
}

/// Subcommands for `tunasynctl maintenance`.
#[derive(Debug, Subcommand)]
enum MaintenanceAction {
    /// Enable maintenance mode (POST /maintenance).
    Enable,
    /// Disable maintenance mode (DELETE /maintenance).
    Disable,
    /// Show whether maintenance mode is active (GET /maintenance).
    Status,
}

/// Build the clap Command with help text in the current locale.
///
/// The derive macro generates help strings from `///` doc comments (English).
/// This function patches them with Chinese equivalents when the locale calls for it.
fn build_command() -> clap::Command {
    let mut cmd = Cli::command();

    if !is_zh() {
        return cmd;
    }

    cmd = cmd
        .about("tunasync manager 控制工具")
        .mut_arg("config", |a| {
            a.help("配置文件路径（覆盖系统和用户配置文件）")
        })
        .mut_arg("manager", |a| a.help("Manager 主机地址或 IP"))
        .mut_arg("port", |a| a.help("Manager 端口"))
        .mut_arg("ca-cert", |a| a.help("TLS CA 证书（启用 HTTPS）"))
        .mut_arg("verbose", |a| a.help("详细日志输出"));

    cmd = cmd
        .mut_subcommand("list", |s| {
            s.about("列出镜像任务")
                .mut_arg("worker", |a| a.help("指定 Worker；省略则列出所有"))
                .mut_arg("status", |a| {
                    a.help("按状态过滤（逗号分隔）：syncing、failed、success 等")
                })
                .mut_arg("format", |a| a.help("输出格式：json（默认）或 table"))
                .mut_arg("all", |a| a.help("显示所有 Worker 的任务"))
        })
        .mut_subcommand("workers", |s| s.about("列出所有已注册的 Worker"))
        .mut_subcommand("flush", |s| s.about("从 manager 数据库中清除所有 disabled 任务记录"))
        .mut_subcommand("rm-worker", |s| {
            s.about("从 manager 中移除一个 Worker")
                .mut_arg("worker", |a| a.help("Worker ID"))
        })
        .mut_subcommand("set-size", |s| {
            s.about("手动设置镜像大小（运维覆盖）")
                .mut_arg("mirror", |a| a.help("镜像名称"))
                .mut_arg("size", |a| a.help("大小字符串，如 1.2T"))
                .mut_arg("worker", |a| a.help("限定到指定 Worker"))
        })
        .mut_subcommand("start", |s| {
            s.about("启动镜像同步任务")
                .mut_arg("mirror", |a| a.help("镜像名称，或 `all` 广播给所有"))
                .mut_arg("worker", |a| a.help("限定到指定 Worker"))
                .mut_arg("force", |a| a.help("忽略并发限制强制启动"))
        })
        .mut_subcommand("stop", |s| {
            s.about("停止正在运行的镜像任务")
                .mut_arg("mirror", |a| a.help("镜像名称"))
                .mut_arg("worker", |a| a.help("限定到指定 Worker"))
        })
        .mut_subcommand("disable", |s| {
            s.about("禁用镜像任务（重新启用前不会运行）")
                .mut_arg("mirror", |a| a.help("镜像名称"))
                .mut_arg("worker", |a| a.help("限定到指定 Worker"))
        })
        .mut_subcommand("restart", |s| {
            s.about("重启镜像任务")
                .mut_arg("mirror", |a| a.help("镜像名称"))
                .mut_arg("worker", |a| a.help("限定到指定 Worker"))
        })
        .mut_subcommand("reload", |s| {
            s.about("通知 Worker 从磁盘热重载配置文件")
                .mut_arg("worker", |a| a.help("Worker ID"))
        })
        .mut_subcommand("completion", |s| {
            s.about("生成 Shell 自动补全脚本")
                .after_help(
                    "用法示例：\n  tunasynctl completion bash >> ~/.bashrc\n  tunasynctl completion zsh  >> ~/.zshrc",
                )
                .mut_arg("shell", |a| a.help("Shell 类型"))
        });

    cmd
}

// ── HTTP client ───────────────────────────────────────────────────────────────

struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    fn new(base_url: String, ca_cert: Option<&PathBuf>) -> Result<Self> {
        let http = if let Some(path) = ca_cert {
            tunasync_common::http::HttpClientBuilder::new()
                .ca_cert_pem_from_path(path)?
                .build()?
        } else {
            tunasync_common::http::HttpClientBuilder::new().build()?
        };
        Ok(Self { base_url, http })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.http
            .get(self.url(path))
            .send()
            .await
            .context("GET request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        self.http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .context("POST request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    async fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.http
            .delete(self.url(path))
            .send()
            .await
            .context("DELETE request failed")?
            .error_for_status()
            .context("server returned error")?
            .json::<T>()
            .await
            .context("decode response JSON")
    }

    async fn list_all_jobs(&self) -> Result<Vec<WebMirrorStatus>> {
        self.get("/jobs").await
    }

    async fn list_jobs_of_worker(&self, worker_id: &str) -> Result<Vec<MirrorStatus>> {
        self.get(&format!("/workers/{worker_id}/jobs")).await
    }

    async fn list_workers(&self) -> Result<Vec<WorkerStatus>> {
        self.get("/workers").await
    }

    async fn flush_disabled(&self) -> Result<serde_json::Value> {
        self.delete("/jobs/disabled").await
    }

    async fn remove_worker(&self, worker_id: &str) -> Result<serde_json::Value> {
        self.delete(&format!("/workers/{worker_id}")).await
    }

    async fn set_size(&self, worker_id: &str, mirror: &str, size: &str) -> Result<MirrorStatus> {
        #[derive(serde::Serialize)]
        struct SizeMsg<'a> {
            name: &'a str,
            size: &'a str,
        }
        self.post(
            &format!("/workers/{worker_id}/jobs/{mirror}/size"),
            &SizeMsg { name: mirror, size },
        )
        .await
    }

    async fn send_cmd(&self, cmd: ClientCmd) -> Result<serde_json::Value> {
        self.post("/cmd", &cmd).await
    }

    /// Enable maintenance mode on the manager (POST /maintenance).
    async fn maintenance_enable(&self) -> Result<serde_json::Value> {
        self.post("/maintenance", &serde_json::json!({})).await
    }

    /// Disable maintenance mode on the manager (DELETE /maintenance).
    async fn maintenance_disable(&self) -> Result<serde_json::Value> {
        self.delete("/maintenance").await
    }

    /// Get maintenance mode status from the manager (GET /maintenance).
    async fn maintenance_status(&self) -> Result<serde_json::Value> {
        self.get("/maintenance").await
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn build_base_url(addr: &str, port: u16, has_ca_cert: bool) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        return addr.trim_end_matches('/').to_owned();
    }
    let scheme = if has_ca_cert { "https" } else { "http" };
    format!("{scheme}://{addr}:{port}")
}

async fn resolve_worker(
    client: &Client,
    mirror: &str,
    explicit_worker: Option<&str>,
) -> Result<String> {
    if let Some(w) = explicit_worker {
        return Ok(w.to_owned());
    }
    let workers = client.list_workers().await?;
    for w in &workers {
        let jobs = client.list_jobs_of_worker(&w.id).await.unwrap_or_default();
        if jobs.iter().any(|j| j.name == mirror) {
            return Ok(w.id.clone());
        }
    }
    anyhow::bail!("mirror {mirror:?} not found on any worker; use --worker / --worker 指定")
}

/// Expand a mirror name that may contain glob metacharacters (`*`, `?`, `[`)
/// by fetching all workers' jobs and filtering with the pattern.
///
/// Returns a list of `(mirror_name, worker_id)` pairs.  When the name contains
/// no metacharacters the function skips the fetch and returns the single name
/// with the worker resolved via `resolve_worker`.
async fn expand_glob(
    client: &Client,
    pattern: &str,
    explicit_worker: Option<&str>,
) -> Result<Vec<(String, String)>> {
    let has_glob = pattern.contains(['*', '?', '[']);
    if !has_glob {
        let wid = resolve_worker(client, pattern, explicit_worker).await?;
        return Ok(vec![(pattern.to_owned(), wid)]);
    }

    let pat = glob::Pattern::new(pattern)
        .with_context(|| format!("invalid glob pattern: {pattern:?}"))?;

    // Fetch from all workers (or just the explicit one).
    let workers = client.list_workers().await?;
    let mut matched: Vec<(String, String)> = Vec::new();
    for w in &workers {
        if let Some(ew) = explicit_worker {
            if w.id != ew {
                continue;
            }
        }
        let jobs = client.list_jobs_of_worker(&w.id).await.unwrap_or_default();
        for j in jobs {
            if pat.matches(&j.name) {
                matched.push((j.name, w.id.clone()));
            }
        }
    }

    if matched.is_empty() {
        anyhow::bail!("glob pattern {pattern:?} matched no mirrors");
    }
    Ok(matched)
}

fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(v).context("serialize output")?
    );
    Ok(())
}

/// Format a datetime for display, replacing Go's zero-time
/// (`0001-01-01T00:00:00Z`) with a human-readable "(never)" / "(从未)".
fn fmt_time(dt: &DateTime<chrono::Utc>) -> String {
    if tunasync_protocol::is_zero_time(dt) {
        if is_zh() {
            "(从未)".to_string()
        } else {
            "(never)".to_string()
        }
    } else {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

fn print_table_jobs(jobs: &[WebMirrorStatus]) {
    let (h_name, h_status, h_update, h_size) = if is_zh() {
        ("名称", "状态", "最后更新", "大小")
    } else {
        ("Name", "Status", "Last Update", "Size")
    };
    println!(
        "{:<30} {:<12} {:<20} {}",
        h_name, h_status, h_update, h_size
    );
    println!("{}", "-".repeat(80));
    for j in jobs {
        println!(
            "{:<30} {:<12} {:<20} {}",
            j.name,
            j.status.to_string(),
            fmt_time(&j.last_update),
            j.size,
        );
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install ring crypto provider");

    // Build locale-aware command, parse args.
    let matches = build_command().get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());

    tunasync_common::logger::init(cli.verbose, false);

    // Handle completion before any network activity.
    if let Command::Completion { shell } = &cli.command {
        let mut cmd = build_command();
        let name = cmd.get_name().to_string();
        clap_complete::generate(*shell, &mut cmd, name, &mut std::io::stdout());
        return Ok(());
    }

    // ── Config resolution ─────────────────────────────────────────────────────
    let file_cfg = CtlConfig::load(cli.config.as_ref());

    let manager_addr = cli.manager.clone().unwrap_or_else(|| {
        if !file_cfg.manager_addr.is_empty() {
            file_cfg.manager_addr.clone()
        } else {
            "localhost".to_owned()
        }
    });
    let manager_port = cli.port.or(file_cfg.manager_port).unwrap_or(14242);
    let ca_cert_path: Option<PathBuf> = cli.ca_cert.clone().or_else(|| {
        if !file_cfg.ca_cert.is_empty() {
            Some(PathBuf::from(&file_cfg.ca_cert))
        } else {
            None
        }
    });

    let base_url = build_base_url(&manager_addr, manager_port, ca_cert_path.is_some());
    tracing::info!(%base_url, "connecting to manager");
    let client = Client::new(base_url, ca_cert_path.as_ref())?;

    match cli.command {
        Command::Completion { .. } => unreachable!(),

        Command::List {
            worker,
            status,
            format,
            all: _,
        } => {
            let jobs = if let Some(ref w) = worker {
                client
                    .list_jobs_of_worker(w)
                    .await?
                    .iter()
                    .map(tunasync_protocol::WebMirrorStatus::from_mirror_status)
                    .collect::<Vec<_>>()
            } else {
                client.list_all_jobs().await?
            };

            let jobs = if let Some(status_filter) = status {
                let allowed: Vec<tunasync_protocol::SyncStatus> = status_filter
                    .split(',')
                    .map(|s| s.trim().parse())
                    .collect::<Result<_, _>>()
                    .context("parse --status filter")?;
                jobs.into_iter()
                    .filter(|j| allowed.contains(&j.status))
                    .collect()
            } else {
                jobs
            };

            if format == "table" {
                print_table_jobs(&jobs);
            } else {
                print_json(&jobs)?;
            }
        }

        Command::Workers => {
            let workers = client.list_workers().await?;
            print_json(&workers)?;
        }

        Command::Flush { stale_only } => {
            // The manager's only flush endpoint is `DELETE /jobs/disabled`,
            // which removes *all* disabled rows — there is no per-mirror
            // selective flush. So --stale-only is implemented as a safety
            // guard: it refuses to flush unless at least one of the
            // currently-disabled mirrors is also marked stale. This is the
            // common use case ("clean up mirrors that were disabled, are
            // probably abandoned, and have not been updated in days"); it
            // avoids accidentally clearing recently-disabled mirrors that
            // an operator may still want to re-enable.
            if stale_only {
                let workers = client.list_workers().await?;
                // Find mirrors that are BOTH disabled AND stale.
                let mut stale_disabled: Vec<(String, String)> = Vec::new();
                let mut other_stale: Vec<(String, String)> = Vec::new();
                for w in &workers {
                    let jobs = client.list_jobs_of_worker(&w.id).await.unwrap_or_default();
                    for j in jobs {
                        let is_disabled = j.status.to_string().eq_ignore_ascii_case("disabled");
                        if j.stale && is_disabled {
                            stale_disabled.push((j.name.clone(), w.id.clone()));
                        } else if j.stale {
                            other_stale.push((j.name.clone(), w.id.clone()));
                        }
                    }
                }
                if stale_disabled.is_empty() {
                    println!(
                        "{}",
                        t!(
                            "No stale disabled mirrors found — refusing to flush.",
                            "没有 stale 且 disabled 的镜像 — 拒绝清除。"
                        )
                    );
                    if !other_stale.is_empty() {
                        let names: Vec<&str> =
                            other_stale.iter().map(|(n, _)| n.as_str()).collect();
                        println!(
                            "{}",
                            t!(
                                format!(
                                    "Note: {} stale but non-disabled mirror(s) exist: {:?}",
                                    other_stale.len(),
                                    names
                                ),
                                format!(
                                    "提示：另有 {} 个 stale 但未 disabled 的镜像：{:?}",
                                    other_stale.len(),
                                    names
                                )
                            )
                        );
                    }
                } else {
                    let names: Vec<&str> = stale_disabled.iter().map(|(n, _)| n.as_str()).collect();
                    println!(
                        "{}",
                        t!(
                            format!(
                                "Flushing {} stale-and-disabled mirror(s): {:?}",
                                stale_disabled.len(),
                                names
                            ),
                            format!(
                                "将清除 {} 个 stale 且 disabled 的镜像：{:?}",
                                stale_disabled.len(),
                                names
                            )
                        )
                    );
                    // Note: the manager flushes ALL disabled rows, not just
                    // the stale ones. Be honest with the operator about that.
                    println!(
                        "{}",
                        t!(
                            "Note: manager's flush endpoint is all-or-nothing; \
                             ALL currently-disabled mirrors will be removed.",
                            "提示：manager 的 flush 端点是全清的；所有当前 \
                             disabled 的镜像都将被删除。"
                        )
                    );
                    client.flush_disabled().await?;
                    println!("{}", t!("Flush complete.", "清除完成。"));
                }
            } else {
                client.flush_disabled().await?;
                println!(
                    "{}",
                    t!("Flushed disabled jobs.", "已清除所有 disabled 任务。")
                );
            }
        }

        Command::Stale => {
            let workers = client.list_workers().await?;
            // Group stale mirrors by status (disabled vs other) so the
            // operator can see at a glance which ones are flush-candidates.
            let mut stale_disabled: Vec<(String, String, String)> = Vec::new();
            let mut stale_active: Vec<(String, String, String)> = Vec::new();
            for w in &workers {
                let jobs = client.list_jobs_of_worker(&w.id).await.unwrap_or_default();
                for j in jobs {
                    if !j.stale {
                        continue;
                    }
                    let status_lc = j.status.to_string().to_ascii_lowercase();
                    let last_update = fmt_time(&j.last_update);
                    let entry = (j.name.clone(), w.id.clone(), last_update);
                    if status_lc == "disabled" {
                        stale_disabled.push(entry);
                    } else {
                        stale_active.push(entry);
                    }
                }
            }
            if stale_disabled.is_empty() && stale_active.is_empty() {
                println!("{}", t!("No stale mirrors.", "没有 stale 镜像。"));
            } else {
                let (h_name, h_worker, h_last) = if is_zh() {
                    ("名称", "Worker", "最后更新")
                } else {
                    ("Name", "Worker", "Last update")
                };
                if !stale_active.is_empty() {
                    println!(
                        "{}",
                        t!(
                            format!("Stale and active ({}):", stale_active.len()),
                            format!("Stale 且活跃（共 {} 个）:", stale_active.len())
                        )
                    );
                    println!("{:<30} {:<20} {}", h_name, h_worker, h_last);
                    for (n, w, t) in &stale_active {
                        println!("{n:<30} {w:<20} {t}");
                    }
                }
                if !stale_disabled.is_empty() {
                    if !stale_active.is_empty() {
                        println!();
                    }
                    println!(
                        "{}",
                        t!(
                            format!("Stale and disabled ({}, flushable):", stale_disabled.len()),
                            format!(
                                "Stale 且 disabled（共 {} 个，可清除）:",
                                stale_disabled.len()
                            )
                        )
                    );
                    println!("{:<30} {:<20} {}", h_name, h_worker, h_last);
                    for (n, w, t) in &stale_disabled {
                        println!("{n:<30} {w:<20} {t}");
                    }
                }
            }
        }

        Command::RmWorker { worker } => {
            client.remove_worker(&worker).await?;
            println!(
                "{}",
                t!(
                    format!("Removed worker {worker:?}."),
                    format!("已移除 Worker {worker:?}。")
                )
            );
        }

        Command::SetSize {
            mirror,
            size,
            worker,
        } => {
            let worker_id = resolve_worker(&client, &mirror, worker.as_deref()).await?;
            let updated = client.set_size(&worker_id, &mirror, &size).await?;
            println!(
                "{}",
                t!(
                    format!(
                        "Updated size of mirror {:?} on worker {:?}: {}",
                        updated.name, updated.worker, updated.size
                    ),
                    format!(
                        "已更新镜像 {:?}（Worker {:?}）大小：{}",
                        updated.name, updated.worker, updated.size
                    )
                )
            );
        }

        Command::Start {
            mirror,
            worker,
            force,
        } => {
            let worker_id = resolve_worker(&client, &mirror, worker.as_deref()).await?;
            let mut options = HashMap::new();
            if force {
                options.insert("force".into(), true);
            }
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Start,
                    mirror_id: mirror.clone(),
                    worker_id,
                    args: vec![],
                    options,
                })
                .await?;
            println!(
                "{}",
                t!(
                    format!("Sent start command for mirror {mirror:?}."),
                    format!("已发送启动命令：镜像 {mirror:?}。")
                )
            );
        }

        Command::Stop { mirror, worker } => {
            let targets = expand_glob(&client, &mirror, worker.as_deref()).await?;
            let futs = targets.iter().map(|(name, wid)| {
                let name = name.clone();
                let wid = wid.clone();
                let client = &client;
                async move {
                    client
                        .send_cmd(ClientCmd {
                            cmd: CmdVerb::Stop,
                            mirror_id: name.clone(),
                            worker_id: wid,
                            args: vec![],
                            options: HashMap::new(),
                        })
                        .await?;
                    println!(
                        "{}",
                        t!(
                            format!("Sent stop command for mirror {name:?}."),
                            format!("已发送停止命令：镜像 {name:?}。")
                        )
                    );
                    anyhow::Ok(())
                }
            });
            let results: Vec<_> = join_all(futs).await;
            for r in results {
                r?;
            }
        }

        Command::Disable { mirror, worker } => {
            let targets = expand_glob(&client, &mirror, worker.as_deref()).await?;
            let futs = targets.iter().map(|(name, wid)| {
                let name = name.clone();
                let wid = wid.clone();
                let client = &client;
                async move {
                    client
                        .send_cmd(ClientCmd {
                            cmd: CmdVerb::Disable,
                            mirror_id: name.clone(),
                            worker_id: wid,
                            args: vec![],
                            options: HashMap::new(),
                        })
                        .await?;
                    println!(
                        "{}",
                        t!(
                            format!("Sent disable command for mirror {name:?}."),
                            format!("已发送禁用命令：镜像 {name:?}。")
                        )
                    );
                    anyhow::Ok(())
                }
            });
            let results: Vec<_> = join_all(futs).await;
            for r in results {
                r?;
            }
        }

        Command::Restart { mirror, worker } => {
            let targets = expand_glob(&client, &mirror, worker.as_deref()).await?;
            let futs = targets.iter().map(|(name, wid)| {
                let name = name.clone();
                let wid = wid.clone();
                let client = &client;
                async move {
                    client
                        .send_cmd(ClientCmd {
                            cmd: CmdVerb::Restart,
                            mirror_id: name.clone(),
                            worker_id: wid,
                            args: vec![],
                            options: HashMap::new(),
                        })
                        .await?;
                    println!(
                        "{}",
                        t!(
                            format!("Sent restart command for mirror {name:?}."),
                            format!("已发送重启命令：镜像 {name:?}。")
                        )
                    );
                    anyhow::Ok(())
                }
            });
            let results: Vec<_> = join_all(futs).await;
            for r in results {
                r?;
            }
        }

        Command::Reload { worker } => {
            client
                .send_cmd(ClientCmd {
                    cmd: CmdVerb::Reload,
                    mirror_id: String::new(),
                    worker_id: worker.clone(),
                    args: vec![],
                    options: HashMap::new(),
                })
                .await?;
            println!(
                "{}",
                t!(
                    format!("Sent reload command to worker {worker:?}."),
                    format!("已发送热重载命令：Worker {worker:?}。")
                )
            );
        }

        Command::Maintenance { action } => match action {
            MaintenanceAction::Enable => {
                client.maintenance_enable().await?;
                println!("{}", t!("Maintenance mode enabled.", "已启用维护模式。"));
            }
            MaintenanceAction::Disable => {
                client.maintenance_disable().await?;
                println!("{}", t!("Maintenance mode disabled.", "已禁用维护模式。"));
            }
            MaintenanceAction::Status => {
                let v = client.maintenance_status().await?;
                print_json(&v)?;
            }
        },
    }

    Ok(())
}
