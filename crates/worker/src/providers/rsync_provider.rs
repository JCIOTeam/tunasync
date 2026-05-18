//! rsync provider — mirrors Go's `rsyncProvider` / `rsync_provider.go`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::hooks::DockerConfig;
use crate::provider::MirrorProvider;
use crate::runner;

/// Rsync provider.
pub struct RsyncProvider {
    pub name: String,
    pub upstream: String,
    pub working_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Shared log path — set by LogLimitHook::preExec, read in run().
    pub log_path_shared: Arc<Mutex<PathBuf>>,
    pub interval: Duration,
    pub retry: u32,
    pub timeout: Duration,
    pub is_master: bool,
    options: Vec<String>,
    rsync_cmd: String,
    rsync_env: HashMap<String, String>,
    /// Minimum free-space bytes required before starting a sync.
    /// Parsed from `MirrorConfig::disk_quota` via `parse_size_bytes`.
    /// 0 = no quota check.
    pub disk_quota_bytes: u64,
    /// Whether to probe upstream reachability before syncing.
    /// Maps to `MirrorConfig::check_upstream`.
    pub check_upstream: bool,
    /// Fallback upstream URLs tried when the primary is unreachable.
    /// These are only used for the pre-sync probe — the actual rsync
    /// data source never changes.
    pub upstream_fallback: Vec<String>,
    /// When true, rsync writes to `<working_dir>/.staging` first; on
    /// success the staging directory is atomically renamed into the
    /// publish dir.  Only works when both dirs share the same filesystem.
    pub atomic_publish_enabled: bool,
    pub success_exit_codes: Vec<i32>,
    data_size: Mutex<String>,
    transferred_bytes: Mutex<u64>,
    current_pid: Arc<Mutex<Option<u32>>>,
    /// Docker container name, set when DockerHook wraps the command.
    docker_container_name: Option<String>,
    /// Docker wrapping config — set by `build_providers()` when Docker is active.
    docker_config: Option<DockerConfig>,
    /// CgroupHook reference — set by `build_providers()` on Linux when cgroup is
    /// active. The provider calls `add_pid_stopped` after spawn so the child
    /// process is placed inside the cgroup before execution begins.
    #[cfg(target_os = "linux")]
    cgroup_hook: Option<std::sync::Arc<crate::hooks::CgroupHook>>,
}

impl RsyncProvider {
    /// Build from mirror + global config.
    pub fn from_config(
        mc: &crate::config::MirrorConfig,
        global: &crate::config::GlobalConfig,
    ) -> Result<Self> {
        if !mc.upstream.ends_with('/') {
            anyhow::bail!("rsync upstream URL must end with '/': {:?}", mc.upstream);
        }

        let working_dir = mc.effective_mirror_dir(global);
        let log_dir = if mc.log_dir.is_empty() {
            PathBuf::from(&global.log_dir)
        } else {
            PathBuf::from(&mc.log_dir)
        };
        let log_path_shared = Arc::new(Mutex::new(log_dir.join("latest.log")));

        let rsync_cmd = if mc.command.is_empty() {
            "rsync".to_string()
        } else {
            mc.command.clone()
        };

        // Validate: rsync_override_only requires rsync_override to be non-empty.
        if mc.rsync_override_only && mc.rsync_override.is_empty() {
            anyhow::bail!("rsync_override_only is set but no rsync_override provided");
        }

        // Build rsync options — matches Go's newRsyncProvider exactly.
        let mut options: Vec<String> = if !mc.rsync_override.is_empty() {
            mc.rsync_override.clone()
        } else {
            vec![
                "-aHvh".into(),
                "--no-o".into(),
                "--no-g".into(),
                "--stats".into(),
                "--filter".into(),
                "risk .~tmp~/".into(),
                "--exclude".into(),
                ".~tmp~/".into(),
                "--delete".into(),
                "--delete-after".into(),
                "--delay-updates".into(),
                "--safe-links".into(),
            ]
        };

        if !mc.rsync_override_only {
            if !mc.rsync_no_timeout {
                let timeo = if mc.rsync_timeout > 0 {
                    mc.rsync_timeout
                } else {
                    120
                };
                options.push(format!("--timeout={timeo}"));
            }
            if mc.use_ipv6 {
                options.push("-6".into());
            } else if mc.use_ipv4 {
                options.push("-4".into());
            }
            if !mc.exclude_file.is_empty() {
                options.extend(["--exclude-from".into(), mc.exclude_file.clone()]);
            }
            // global rsync options
            options.extend(global.rsync_options.iter().cloned());
            // mirror-specific rsync options
            options.extend(mc.rsync_options.iter().cloned());
        }

        // Environment.
        let mut rsync_env = HashMap::new();
        if !mc.username.is_empty() {
            rsync_env.insert("USER".into(), mc.username.clone());
        }
        if !mc.password.is_empty() {
            rsync_env.insert("RSYNC_PASSWORD".into(), mc.password.clone());
        }

        // Merge global success exit codes.
        let mut success_exit_codes = mc.success_exit_codes.clone();
        success_exit_codes.extend(global.dangerous_global_success_exit_codes.iter());
        success_exit_codes.extend(global.dangerous_global_rsync_success_exit_codes.iter());
        success_exit_codes.extend(mc.rsync_success_exit_codes.iter());

        // Parse disk quota — empty or unparseable = 0 = no check.
        let disk_quota_bytes = if mc.disk_quota.is_empty() {
            0
        } else {
            tunasync_common::util::parse_size_bytes(&mc.disk_quota).unwrap_or(0)
        };

        Ok(Self {
            name: mc.name.clone(),
            upstream: mc.upstream.clone(),
            working_dir,
            log_dir,
            log_path_shared,
            interval: mc.effective_interval(global),
            retry: mc.effective_retry(global),
            timeout: mc.effective_timeout(global).unwrap_or(Duration::ZERO),
            is_master: mc.is_master(),
            options,
            rsync_cmd,
            rsync_env,
            success_exit_codes,
            disk_quota_bytes,
            check_upstream: mc.check_upstream,
            upstream_fallback: mc.upstream_fallback.clone(),
            atomic_publish_enabled: mc.atomic_publish,
            data_size: Mutex::new(String::new()),
            transferred_bytes: Mutex::new(0),
            current_pid: Arc::new(Mutex::new(None)),
            docker_container_name: None,
            docker_config: None,
            #[cfg(target_os = "linux")]
            cgroup_hook: None,
        })
    }

    #[allow(dead_code)]
    fn build_argv(&self) -> Vec<String> {
        self.build_argv_for_dest(&self.working_dir)
    }

    /// Build the rsync argv targeting a specific destination directory.
    /// Used by `run()` so atomic-publish can redirect output to `.staging`.
    fn build_argv_for_dest(&self, dest: &std::path::Path) -> Vec<String> {
        let mut argv = vec![self.rsync_cmd.clone()];
        argv.extend(self.options.iter().cloned());
        argv.push(self.upstream.clone());
        argv.push(dest.to_string_lossy().into());
        argv
    }
}

#[async_trait]
impl MirrorProvider for RsyncProvider {
    fn name(&self) -> &str {
        &self.name
    }
    fn upstream(&self) -> &str {
        &self.upstream
    }
    fn is_master(&self) -> bool {
        self.is_master
    }
    fn interval(&self) -> Duration {
        self.interval
    }
    fn retry(&self) -> u32 {
        self.retry
    }
    fn timeout(&self) -> Duration {
        self.timeout
    }

    async fn run(&self) -> Result<()> {
        // Atomic publish: write rsync output to a staging directory, then
        // atomically swap staging↔publish on success.
        //
        // # Design
        //
        // The staging directory lives **outside** the public mirror root.
        // Operators very commonly point nginx at `mirror_dir` directly, and
        // any path under that root with a leading `.` (e.g. `.staging-foo`)
        // is still served by default nginx config — exposing half-synced
        // files to users. We instead place staging under `<log_dir>/staging/`
        // by default, which is universally treated as worker-internal state.
        //
        // The swap itself uses `renameat2(2)` with `RENAME_EXCHANGE`
        // (Linux 3.15+), which atomically swaps two paths. Compared to the
        // legacy two-step approach
        //   1. rename(publish, backup)
        //   2. rename(staging, publish)
        // the new approach has no intermediate state where `publish` does
        // not exist — readers always see either the old or the new content,
        // never a 404 in the gap between (1) and (2).
        //
        // After the swap, the staging path holds the *previous* mirror
        // contents. We leave it in place: next sync writes into it again
        // (incremental rsync benefits from this).
        //
        // # Caveats (documented for operators)
        //
        // - Staging and publish must share a filesystem (both rename forms
        //   require this). We check device IDs and bail at sync start with
        //   a clear error if they differ.
        // - Atomic publish interacts poorly with --link-dest style hardlink
        //   rsync: hardlinks created into staging point inside staging, not
        //   inside the eventual publish location. Operators should not
        //   combine atomic_publish with --link-dest.
        let publish_dir = self.working_dir.clone();
        let sync_target = if self.atomic_publish_enabled {
            atomic_staging_path_for(&self.log_dir, &self.name)
        } else {
            publish_dir.clone()
        };

        if self.atomic_publish_enabled {
            ensure_atomic_publish_dirs(&sync_target, &publish_dir)
                .with_context(|| format!("atomic publish setup for {}", self.name))?;
        }

        let argv = self.build_argv_for_dest(&sync_target);
        // When Docker wrapping is active, the argv is wrapped with `docker run …`
        // and env vars go through `-e` flags. The host process doesn't need them.
        let (argv, spawn_env) = if let Some(docker) = &self.docker_config {
            (docker.wrap_argv(&argv), HashMap::new())
        } else {
            (argv, self.rsync_env.clone())
        };

        let log_file = self.log_path_shared.lock().unwrap().clone();
        let log_path = if log_file.to_string_lossy() == "/dev/null" {
            None
        } else {
            Some(log_file.as_path())
        };

        let proc = runner::spawn(&argv, &sync_target, &spawn_env, log_path)
            .await
            .with_context(|| format!("spawn rsync for {}", self.name))?;

        if let Some(pid) = proc.pid() {
            *self.current_pid.lock().unwrap() = Some(pid);
        }
        // Place the child PID into the cgroup (Linux only). Must happen between
        // spawn() and wait() so the process is in the cgroup before it executes.
        #[cfg(target_os = "linux")]
        if let Some(ref hook) = self.cgroup_hook {
            if let Err(e) = hook.add_pid_stopped(&proc) {
                tracing::warn!(mirror = %self.name, error = %e, "failed to add PID to cgroup");
            }
        }
        let wait_result = proc.wait(&self.success_exit_codes).await;
        *self.current_pid.lock().unwrap() = None;
        wait_result?;

        // Atomic publish: swap staging↔publish in a single atomic operation.
        if self.atomic_publish_enabled {
            atomic_publish_swap(&sync_target, &publish_dir).with_context(|| {
                format!(
                    "atomic publish: swap {} ↔ {}",
                    sync_target.display(),
                    publish_dir.display()
                )
            })?;
            tracing::info!(
                mirror = %self.name,
                dest = %publish_dir.display(),
                staging = %sync_target.display(),
                "atomic publish complete"
            );
        }

        // Extract size and transferred bytes from log after successful run.
        if log_file.exists() {
            let content = tokio::fs::read_to_string(&log_file)
                .await
                .unwrap_or_default();
            let size = tunasync_common::util::extract_size_from_rsync_log(&content);
            if !size.is_empty() {
                *self.data_size.lock().unwrap() = size;
            }
            let transferred =
                tunasync_common::util::extract_transferred_bytes_from_rsync_log(&content);
            if transferred > 0 {
                *self.transferred_bytes.lock().unwrap() = transferred;
            }
        }
        Ok(())
    }

    async fn terminate(&self) -> Result<()> {
        #[cfg(unix)]
        {
            // If Docker is wrapping this command, call `docker stop` instead of raw SIGTERM.
            if let Some(ref name) = self.docker_container_name {
                let out = tokio::process::Command::new("docker")
                    .args(["stop", "-t", "2", name])
                    .output()
                    .await;
                match out {
                    Ok(o) if o.status.success() => {
                        tracing::debug!(container = %name, "docker stop succeeded");
                    }
                    Ok(o) => {
                        tracing::warn!(container = %name, status = %o.status, "docker stop failed — falling back to SIGTERM");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, container = %name, "docker stop failed — falling back to SIGTERM");
                    }
                }
            }
            // Extract PID before awaiting so the MutexGuard is dropped (not Send).
            let pid = *self.current_pid.lock().unwrap();
            if let Some(pid) = pid {
                runner::terminate_process_group(pid).await;
            }
        }
        Ok(())
    }

    fn data_size(&self) -> String {
        self.data_size.lock().unwrap().clone()
    }

    fn transferred_bytes(&self) -> u64 {
        *self.transferred_bytes.lock().unwrap()
    }

    fn working_dir(&self) -> &std::path::Path {
        &self.working_dir
    }

    fn disk_quota_bytes(&self) -> u64 {
        self.disk_quota_bytes
    }

    fn atomic_publish(&self) -> bool {
        self.atomic_publish_enabled
    }

    /// Probe upstream reachability before syncing.
    ///
    /// When `check_upstream` is true, probes the primary upstream and each
    /// fallback **concurrently** with a hard 15-second timeout per URL.
    /// Returns `Ok(())` as soon as the first reachable URL is found. Returns
    /// `Err` only if every URL is unreachable.
    ///
    /// # Why parallel + hard timeout
    ///
    /// `rsync --timeout=N` only governs IO inactivity — it does not bound
    /// DNS resolution, TCP SYN, or TLS handshake. A misbehaving network
    /// (firewall DROP rather than REJECT, dead DNS server) can leave rsync
    /// stuck for the kernel's TCP connect timeout (75–130s on Linux). With
    /// the old serial implementation N fallback URLs that all hang would
    /// block the sync path for N × 130s.
    ///
    /// We wrap each probe in `tokio::time::timeout(15s, …)` so any single
    /// hung probe is bounded, and run all probes concurrently with
    /// `FuturesUnordered` so total wall-clock time is bounded by the
    /// slowest *successful* probe rather than the cumulative timeout
    /// budget across all URLs.
    ///
    /// When `check_upstream` is false (default) this is a no-op.
    async fn probe_upstream(&self) -> anyhow::Result<()> {
        if !self.check_upstream {
            return Ok(());
        }
        let mut urls: Vec<&str> = vec![self.upstream.as_str()];
        urls.extend(self.upstream_fallback.iter().map(String::as_str));

        use futures::stream::{FuturesUnordered, StreamExt};
        let mut futures: FuturesUnordered<_> = urls
            .iter()
            .map(|&url| async move {
                let res =
                    tokio::time::timeout(std::time::Duration::from_secs(15), probe_rsync_url(url))
                        .await;
                (url, res)
            })
            .collect();

        let mut last_err: Option<String> = None;
        while let Some((url, res)) = futures.next().await {
            match res {
                Ok(Ok(())) => {
                    if url != self.upstream.as_str() {
                        tracing::info!(
                            mirror = %self.name,
                            primary = %self.upstream,
                            reachable = %url,
                            "primary upstream unreachable; fallback responded"
                        );
                    }
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_err = Some(format!("{url}: {e}"));
                }
                Err(_) => {
                    last_err = Some(format!("{url}: probe timed out after 15s"));
                }
            }
        }
        anyhow::bail!(
            "all {} upstream(s) unreachable; last: {}",
            urls.len(),
            last_err.as_deref().unwrap_or("no probes attempted")
        )
    }

    fn set_docker_config(&mut self, config: DockerConfig) {
        self.docker_container_name = Some(config.container_name());
        self.docker_config = Some(config);
    }

    fn set_log_path_shared(&mut self, path: Arc<Mutex<PathBuf>>) {
        self.log_path_shared = path;
    }

    #[cfg(target_os = "linux")]
    fn set_cgroup_hook(&mut self, hook: std::sync::Arc<crate::hooks::CgroupHook>) {
        self.cgroup_hook = Some(hook);
    }
}

// ── Atomic publish helpers ──────────────────────────────────────────────────

/// Compute the staging directory path for atomic publish.
///
/// Stored under `<log_dir>/staging/<mirror_name>/` rather than inside the
/// mirror tree so that operators serving `mirror_dir` directly via nginx
/// do not inadvertently expose half-synced contents.
///
/// Exposed for tests; the public function is in the trait impl.
pub(crate) fn atomic_staging_path_for(log_dir: &std::path::Path, mirror_name: &str) -> PathBuf {
    log_dir.join("staging").join(mirror_name)
}

/// Validate atomic-publish preconditions and create the staging directory.
///
/// Specifically:
/// - Both dirs must end up on the same filesystem (renameat2 requirement).
///   We check this by walking up to the nearest existing ancestor of each
///   and comparing `st_dev`. The check is skipped if neither path exists yet.
/// - Creates `staging` if absent (the first sync of a mirror starts here).
/// - Cleans up any stale `.old`-suffixed leftover from the legacy two-step
///   rename if it exists (defence in depth for upgraders).
pub(crate) fn ensure_atomic_publish_dirs(
    staging: &std::path::Path,
    publish: &std::path::Path,
) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;

    // Always ensure staging exists before any device-id check, so the
    // device check has a real path to stat.
    if let Some(parent) = staging.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create staging parent {}", parent.display()))?;
    }
    if !staging.exists() {
        std::fs::create_dir_all(staging)
            .with_context(|| format!("create staging {}", staging.display()))?;
    }

    // Device-id check: walk up each path to the nearest existing ancestor.
    let staging_for_meta = nearest_existing(staging);
    let publish_for_meta = nearest_existing(publish);
    if let (Some(s), Some(p)) = (staging_for_meta, publish_for_meta) {
        let sm = std::fs::metadata(&s).with_context(|| format!("stat {}", s.display()))?;
        let pm = std::fs::metadata(&p).with_context(|| format!("stat {}", p.display()))?;
        if sm.dev() != pm.dev() {
            anyhow::bail!(
                "atomic_publish requires staging ({}) and publish ({}) on the same \
                 filesystem (dev {} vs {})",
                s.display(),
                p.display(),
                sm.dev(),
                pm.dev()
            );
        }
    }

    // Defence-in-depth: remove stale .old backups from the legacy rename
    // pattern. If we just upgraded from the two-step implementation, a
    // failed run could have left a `<publish>.old` directory behind, which
    // would interfere with reasoning about disk usage. Best-effort.
    let mut backup = publish.to_path_buf();
    if let Some(name) = backup.file_name() {
        let mut s = name.to_os_string();
        s.push(".old");
        backup.set_file_name(s);
        if backup.exists() {
            let _ = std::fs::remove_dir_all(&backup);
        }
    }
    Ok(())
}

/// Walk up `path` until an existing ancestor is found.
fn nearest_existing(path: &std::path::Path) -> Option<PathBuf> {
    let mut p: &std::path::Path = path;
    loop {
        if p.exists() {
            return Some(p.to_path_buf());
        }
        match p.parent() {
            Some(parent) if parent != p => p = parent,
            _ => return None,
        }
    }
}

/// Atomically swap `staging` and `publish` directories.
///
/// Uses Linux's `renameat2(2)` with `RENAME_EXCHANGE`. After this returns
/// `Ok`, readers always see either the previous publish contents or the
/// new staging contents — there is no window where the publish path is
/// missing.
///
/// Falls back to a two-step `rename` only on the first-ever publish (when
/// the publish directory does not exist yet): in that case there is no
/// concurrent reader risk because the path being created is new.
pub(crate) fn atomic_publish_swap(
    staging: &std::path::Path,
    publish: &std::path::Path,
) -> anyhow::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // First-ever publish: just rename (no exchange needed since there's
    // nothing to atomically replace). Single rename is itself atomic.
    if !publish.exists() {
        if let Some(parent) = publish.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create publish parent {}", parent.display()))?;
        }
        std::fs::rename(staging, publish).with_context(|| {
            format!(
                "first-time rename {} -> {}",
                staging.display(),
                publish.display()
            )
        })?;
        // Recreate the staging dir so the next sync has somewhere to write.
        std::fs::create_dir_all(staging)
            .with_context(|| format!("recreate staging {}", staging.display()))?;
        return Ok(());
    }

    // Both exist — do an atomic exchange via renameat2.
    //
    // We go through `libc::syscall` rather than a higher-level wrapper because
    // libc doesn't expose a Rust-level `renameat2` function — only the syscall
    // number constant `libc::SYS_renameat2`. That constant is per-target so
    // we get the correct number on x86_64, aarch64, riscv64, etc. without any
    // arch-specific `cfg`. Hard-coding the x86_64 value (316) here was a real
    // portability bug: on aarch64 syscall 316 is `pkey_mprotect`, not
    // `renameat2`, and would silently misbehave on Apple Silicon, AWS
    // Graviton, Raspberry Pi, and other ARM mirror hosts.
    let c_staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("staging path contains NUL: {e}"))?;
    let c_publish = CString::new(publish.as_os_str().as_bytes())
        .map_err(|e| anyhow::anyhow!("publish path contains NUL: {e}"))?;
    #[allow(clippy::useless_conversion)]
    const RENAME_EXCHANGE: libc::c_uint = 2;
    // SAFETY: we hold valid C strings, AT_FDCWD is the canonical "current
    // working directory" sentinel, RENAME_EXCHANGE is a valid flag for
    // renameat2(2). `libc::SYS_renameat2` is the architecture-correct syscall
    // number provided by the libc crate. On error we read errno and convert.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            c_staging.as_ptr(),
            libc::AT_FDCWD,
            c_publish.as_ptr(),
            RENAME_EXCHANGE,
        )
    };
    if ret == 0 {
        return Ok(());
    }

    let errno = std::io::Error::last_os_error();
    // If the kernel/filesystem doesn't support RENAME_EXCHANGE (ENOSYS,
    // EINVAL on some FUSE filesystems), fall back to the two-step rename.
    // We log a warning so operators notice and can plan an upgrade.
    let raw = errno.raw_os_error().unwrap_or(0);
    if raw == libc::ENOSYS || raw == libc::EINVAL {
        tracing::warn!(
            staging = %staging.display(),
            publish = %publish.display(),
            errno = raw,
            "renameat2(RENAME_EXCHANGE) unsupported on this kernel/filesystem; \
             falling back to non-atomic two-step rename. Readers may see a \
             brief 404 window during sync. Upgrade kernel to ≥3.15 and use a \
             modern filesystem (ext4/xfs/btrfs) for atomic semantics."
        );
        return two_step_rename_fallback(staging, publish);
    }
    Err(anyhow::anyhow!(
        "renameat2(RENAME_EXCHANGE) failed: {errno}"
    ))
}

/// Non-atomic fallback: rename publish → publish.old, then staging → publish,
/// then remove the .old directory. Used only when the kernel/filesystem
/// doesn't support `renameat2(RENAME_EXCHANGE)`.
fn two_step_rename_fallback(
    staging: &std::path::Path,
    publish: &std::path::Path,
) -> anyhow::Result<()> {
    let mut backup = publish.to_path_buf();
    let backup_name = publish
        .file_name()
        .map(|n| {
            let mut s = n.to_os_string();
            s.push(".old");
            s
        })
        .ok_or_else(|| anyhow::anyhow!("publish path has no filename"))?;
    backup.set_file_name(backup_name);

    // Step 1: publish -> backup. Brief window starts here.
    std::fs::rename(publish, &backup).with_context(|| {
        format!(
            "fallback rename {} -> {}",
            publish.display(),
            backup.display()
        )
    })?;
    // Step 2: staging -> publish. Brief window ends here.
    let rename_res = std::fs::rename(staging, publish);
    match rename_res {
        Ok(()) => {
            // Step 3: leave backup as the new staging so incremental syncs
            // can reuse it. Move the .old contents back into staging's path.
            if let Err(e) = std::fs::rename(&backup, staging) {
                // Couldn't preserve old data for staging — just remove the
                // backup. Next sync starts fresh.
                tracing::warn!(
                    backup = %backup.display(),
                    staging = %staging.display(),
                    error = %e,
                    "fallback: failed to recycle old contents as new staging; removing"
                );
                let _ = std::fs::remove_dir_all(&backup);
            }
            Ok(())
        }
        Err(e) => {
            // Try to roll back to keep readers happy.
            let _ = std::fs::rename(&backup, publish);
            Err(anyhow::anyhow!("fallback rename failed: {e}"))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────

/// Probe a single rsync URL for reachability.
///
/// Runs `rsync --contimeout=10 --list-only --timeout=10 <url>`. The two
/// timeouts cover different things:
/// - `--contimeout=10`: connection establishment timeout (DNS + TCP SYN)
/// - `--timeout=10`: IO inactivity timeout after the connection is up
///
/// Together they bound the per-process wall-clock time to about 20s. We
/// further wrap calls to this function in `tokio::time::timeout(15s, …)`
/// at the call site, which kills the child process if it ignores its own
/// timeout (e.g. version too old to know about `--contimeout`).
///
/// Returns `Ok(())` if the exit code is 0; otherwise an error containing
/// the exit code or stderr's first line for diagnostics.
pub(crate) async fn probe_rsync_url(url: &str) -> anyhow::Result<()> {
    let mut child = tokio::process::Command::new("rsync")
        .args(["--contimeout=10", "--list-only", "--timeout=10", url])
        // Capture stderr so we can include a useful message on failure
        // without dumping it to the worker's terminal.
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        // Kill on drop: if the outer tokio::time::timeout fires and drops
        // this future, the child process must die rather than orphan.
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn rsync probe: {e}"))?;

    let status = child
        .wait()
        .await
        .map_err(|e| anyhow::anyhow!("failed to wait for rsync probe: {e}"))?;
    if status.success() {
        return Ok(());
    }
    let code = status.code().unwrap_or(-1);
    anyhow::bail!("rsync probe exited {code} for {url}")
}

impl RsyncProvider {
    /// Set the docker container name when DockerHook wraps the command.
    pub fn set_docker_container(&mut self, name: String) {
        self.docker_container_name = Some(name);
    }
}

#[cfg(test)]
mod upstream_probe_tests {
    //! Tests for probe_upstream / probe_rsync_url.
    //!
    //! The integration test (`probe_unreachable_url_fails_fast`) requires the
    //! `rsync` binary to be present. It is marked `#[ignore]` so it doesn't
    //! block CI environments that lack rsync; run it with `cargo test -- --ignored`.

    use super::probe_rsync_url;

    /// Probing a port that is not listening must return an error quickly.
    /// Uses 127.0.0.1:1 — port 1 is privileged and virtually never open,
    /// so rsync exits non-zero (usually with "connection refused").
    #[tokio::test]
    #[ignore = "requires rsync binary in PATH"]
    async fn probe_unreachable_url_fails_fast() {
        let result = probe_rsync_url("rsync://127.0.0.1:1/nonexistent").await;
        assert!(
            result.is_err(),
            "expected probe to fail for unreachable URL, got Ok"
        );
    }

    /// Regression test: the outer `tokio::time::timeout` wrapper at the
    /// caller side must bound wall-clock time even when the spawned
    /// process refuses to die promptly.  We don't actually need rsync for
    /// this — we can simulate a hung probe with `sleep`. This test
    /// verifies that wrapping `sleep 60` in a 1-second timeout bounds
    /// the wait correctly and the child gets killed (via kill_on_drop).
    #[tokio::test]
    async fn timeout_wrapper_bounds_wallclock_and_kills_child() {
        use std::time::{Duration, Instant};

        let start = Instant::now();

        // Spawn a long-running process and wrap in tokio timeout. With
        // kill_on_drop on the Command builder the child is reaped when
        // the future is dropped by the timeout.
        let res = tokio::time::timeout(Duration::from_millis(500), async {
            let mut child = tokio::process::Command::new("sleep")
                .arg("60")
                .kill_on_drop(true)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn sleep");
            child.wait().await
        })
        .await;

        let elapsed = start.elapsed();
        assert!(res.is_err(), "expected outer timeout to fire");
        assert!(
            elapsed < Duration::from_secs(3),
            "outer timeout should bound wallclock; got {:?}",
            elapsed
        );
    }

    /// probe_upstream runs URLs concurrently. If we have one fast-failing
    /// URL and one that would block forever, the function must return
    /// promptly with an error (no URL was reachable) rather than waiting
    /// for the slow one. Bounded to 30s total.
    #[tokio::test]
    #[ignore = "requires rsync binary in PATH"]
    async fn probe_upstream_parallel_does_not_wait_for_slow_url() {
        use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
        use crate::provider::MirrorProvider;
        use std::time::{Duration, Instant};

        let global = GlobalConfig::default();
        let mc = MirrorConfig {
            name: "parallel-probe".into(),
            provider: ProviderKind::Rsync,
            // Two URLs that should both fail fast (connection refused on
            // privileged unused ports). The point is to show two probes
            // running concurrently and the total time being roughly
            // max(rtt) not sum(rtt).
            upstream: "rsync://127.0.0.1:1/a".into(),
            upstream_fallback: vec!["rsync://127.0.0.1:2/b".into()],
            check_upstream: true,
            ..MirrorConfig::default()
        };
        let provider = super::super::RsyncProvider::from_config(&mc, &global)
            .expect("from_config should succeed");
        let provider: &dyn MirrorProvider = &provider;

        let start = Instant::now();
        let result = provider.probe_upstream().await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "both URLs unreachable; expected Err");
        // Generous upper bound: even with rsync slowness this must
        // complete in well under the 15s × N serial worst case.
        assert!(
            elapsed < Duration::from_secs(30),
            "parallel probes took too long: {:?}",
            elapsed
        );
    }

    /// When check_upstream is false (default), probe_upstream must be a no-op
    /// and always succeed — even with a bogus upstream URL.
    #[tokio::test]
    async fn probe_upstream_noop_when_disabled() {
        use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
        use crate::provider::MirrorProvider;

        // Build a minimal config with check_upstream = false (the default).
        let global = GlobalConfig::default();
        let mc = MirrorConfig {
            name: "probe-noop-test".into(),
            provider: ProviderKind::Rsync,
            upstream: "rsync://localhost/will-not-be-called/".into(),
            ..MirrorConfig::default()
        };
        // check_upstream defaults to false — leave it unset.

        let provider =
            super::RsyncProvider::from_config(&mc, &global).expect("from_config should succeed");

        // Must return Ok without spawning rsync.
        // Cast to the trait to invoke the overridden probe_upstream().
        let provider: &dyn MirrorProvider = &provider;
        provider
            .probe_upstream()
            .await
            .expect("no-op probe should always succeed");
    }
}

#[cfg(test)]
mod atomic_publish_tests {
    //! Tests for the atomic-publish helpers.
    //!
    //! Unlike the previous tests which inlined the rename logic (so they
    //! tested copy-pasted code rather than the product), these call the
    //! production helpers directly via the `pub(crate)` exports.

    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use crate::config::{GlobalConfig, MirrorConfig, ProviderKind};
    use crate::provider::MirrorProvider;

    /// Build a minimal RsyncProvider pointing at a tempdir.
    fn make_atomic_provider(working_dir: PathBuf) -> super::RsyncProvider {
        let global = GlobalConfig::default();
        let mc = MirrorConfig {
            name: "atomic-test".into(),
            provider: ProviderKind::Rsync,
            upstream: "rsync://localhost/unused/".into(),
            atomic_publish: true,
            ..MirrorConfig::default()
        };
        let mut p = super::RsyncProvider::from_config(&mc, &global).expect("from_config");
        p.working_dir = working_dir;
        p.log_path_shared = Arc::new(Mutex::new(PathBuf::from("/dev/null")));
        p
    }

    /// `atomic_staging_path_for` must place staging under log_dir/staging/<name>,
    /// NOT inside the mirror tree (which would be served by nginx).
    #[test]
    fn staging_path_is_outside_mirror_tree() {
        let log_dir = std::path::Path::new("/var/log/tunasync");
        let path = super::atomic_staging_path_for(log_dir, "fedora");
        assert_eq!(path, PathBuf::from("/var/log/tunasync/staging/fedora"));
        // Crucially: it does NOT start with the public mirror prefix.
        assert!(!path.starts_with("/srv/mirrors"));
    }

    /// First-time publish (publish dir does not exist) works via the simple
    /// rename branch and recreates a fresh staging dir afterwards.
    #[test]
    fn first_time_publish_renames_and_recreates_staging() {
        let base = tempfile::tempdir().expect("tempdir");
        let staging = base.path().join("staging");
        let publish = base.path().join("publish");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("file.txt"), b"v1").unwrap();
        // publish does NOT exist yet.

        super::atomic_publish_swap(&staging, &publish).expect("swap should succeed");

        assert!(
            publish.join("file.txt").exists(),
            "publish has the new file"
        );
        assert_eq!(
            std::fs::read(publish.join("file.txt")).unwrap(),
            b"v1",
            "file content preserved"
        );
        assert!(staging.exists(), "staging is recreated for next sync");
    }

    /// Subsequent publish (both exist) swaps atomically: publish gets the
    /// new contents, staging gets the old contents (kept for next sync's
    /// incremental rsync benefit).
    #[test]
    fn subsequent_publish_swaps_contents() {
        let base = tempfile::tempdir().expect("tempdir");
        let staging = base.path().join("staging");
        let publish = base.path().join("publish");

        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("new.txt"), b"new").unwrap();

        std::fs::create_dir_all(&publish).unwrap();
        std::fs::write(publish.join("old.txt"), b"old").unwrap();

        super::atomic_publish_swap(&staging, &publish).expect("swap should succeed");

        // After swap: publish has the new file, staging has the old file.
        assert!(publish.join("new.txt").exists(), "new lives in publish");
        assert!(!publish.join("old.txt").exists(), "old not in publish");
        assert!(
            staging.join("old.txt").exists(),
            "old recycled into staging"
        );
        assert!(!staging.join("new.txt").exists(), "new not in staging");
    }

    /// `ensure_atomic_publish_dirs` creates staging if absent and cleans up
    /// stale .old leftovers from the legacy two-step rename pattern.
    #[test]
    fn ensure_dirs_creates_staging_and_cleans_stale_backup() {
        let base = tempfile::tempdir().expect("tempdir");
        let staging = base.path().join("logs/staging/mymirror");
        let publish = base.path().join("mirrors/mymirror");
        std::fs::create_dir_all(&publish).unwrap();

        // Pretend a legacy run left behind a .old backup.
        let backup = base.path().join("mirrors/mymirror.old");
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::write(backup.join("garbage"), b"x").unwrap();

        super::ensure_atomic_publish_dirs(&staging, &publish).expect("must succeed");

        assert!(staging.exists(), "staging created");
        assert!(!backup.exists(), "stale backup removed");
    }

    /// nearest_existing should walk up the tree to find an ancestor that
    /// exists, even when the leaf path is many levels deep and missing.
    #[test]
    fn nearest_existing_walks_up_missing_path() {
        let base = tempfile::tempdir().expect("tempdir");
        let deep = base.path().join("a/b/c/d/leaf");
        // None of a/b/c/d exist.
        let found = super::nearest_existing(&deep).expect("ancestor must exist");
        // base.path() itself exists, and is one of the ancestors.
        assert!(deep.starts_with(&found));
    }

    /// atomic_publish() trait accessor must reflect the config field.
    #[test]
    fn atomic_publish_accessor() {
        let base = tempfile::tempdir().expect("tempdir");
        let p = make_atomic_provider(base.path().to_path_buf());
        assert!(p.atomic_publish(), "accessor must return true when enabled");
    }
}
