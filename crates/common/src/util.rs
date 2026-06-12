//! Shared utilities — mirrors Go's `internal/util.go`.

use std::collections::HashMap;

use once_cell::sync::Lazy;

// ---------------------------------------------------------------------------
// Rsync exit-code table (matches Go's rsyncExitValues exactly)
// ---------------------------------------------------------------------------

static RSYNC_EXIT_VALUES: Lazy<HashMap<i32, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(0, "Success");
    m.insert(1, "Syntax or usage error");
    m.insert(2, "Protocol incompatibility");
    m.insert(3, "Errors selecting input/output files, dirs");
    m.insert(4, "Requested action not supported: an attempt was made to manipulate 64-bit files on a platform that cannot support them; or an option was specified that is supported by the client and not by the server.");
    m.insert(5, "Error starting client-server protocol");
    m.insert(6, "Daemon unable to append to log-file");
    m.insert(10, "Error in socket I/O");
    m.insert(11, "Error in file I/O");
    m.insert(12, "Error in rsync protocol data stream");
    m.insert(13, "Errors with program diagnostics");
    m.insert(14, "Error in IPC code");
    m.insert(20, "Received SIGUSR1 or SIGINT");
    m.insert(21, "Some error returned by waitpid()");
    m.insert(22, "Error allocating core memory buffers");
    m.insert(23, "Partial transfer due to error");
    m.insert(24, "Partial transfer due to vanished source files");
    m.insert(25, "The --max-delete limit stopped deletions");
    m.insert(30, "Timeout in data send/receive");
    m.insert(35, "Timeout waiting for daemon connection");
    m
});

/// Translate an rsync exit code to a human-readable message.
///
/// Returns `(exit_code, message)`. If the code is not in the known table,
/// `message` will be empty.
///
/// Mirrors Go's `TranslateRsyncErrorCode`.
pub fn translate_rsync_error_code(exit_code: i32) -> (i32, String) {
    let msg = RSYNC_EXIT_VALUES
        .get(&exit_code)
        .map(|s| format!("rsync error: {s}"))
        .unwrap_or_default();
    (exit_code, msg)
}

/// Extract total-file-size from an rsync log.
///
/// Looks for `Total file size: <N>[KMGTP]? bytes` (rsync `--stats` output).
/// Returns the **last** occurrence — matches Go's `ExtractSizeFromLog` which
/// does `matches[len(matches)-1][1]` (last element of `FindAllSubmatch`).
pub fn extract_size_from_rsync_log(log_content: &str) -> String {
    // Accept the same digit-group forms that `extract_transferred_bytes_from_rsync_log`
    // accepts:
    //   "Total file size: 1234567 bytes"
    //   "Total file size: 1,234,567 bytes"       (some locales / rsync configs)
    //   "Total file size: 1.23G bytes"            (with --human-readable)
    // The previous regex `[0-9.]+[KMGTP]?` rejected the comma form and produced
    // an empty result on those logs (mirror size shown as blank in the UI).
    let re = regex::Regex::new(r"(?m)^Total file size:\s+([0-9][0-9,.]*[KMGTP]?) bytes")
        .expect("static regex");
    re.captures_iter(log_content)
        .last()
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default()
}

/// Extract "Total transferred file size" from rsync `--stats` output.
///
/// This is the number of bytes actually transferred over the network during
/// this sync (as opposed to `Total file size` which is the full mirror size).
/// Returns 0 if not found.
pub fn extract_transferred_bytes_from_rsync_log(log_content: &str) -> u64 {
    static RE: Lazy<regex::Regex> = Lazy::new(|| {
        // rsync outputs lines like:
        //   Total transferred file size: 1,234,567 bytes
        //   Total transferred file size: 1.23M bytes     (-h, powers of 1000)
        //   Total transferred file size: 1.18Mi bytes    (-hh, powers of 1024)
        // Capture the number (commas/decimal point) and an optional
        // K/M/G/T/P suffix with an optional trailing `i`.
        //
        // The previous regex stopped at `[0-9,.]*\s+bytes` and therefore
        // never matched the suffixed -h/-hh forms at all (and `1.23` would
        // not parse as u64 anyway) — mirrors synced with --human-readable
        // silently reported 0 transferred bytes.
        regex::Regex::new(
            r"(?m)^Total transferred file size:\s+([0-9][0-9,.]*)\s*([KMGTPkmgtp]i?)?\s+bytes",
        )
        .expect("static regex")
    });
    RE.captures_iter(log_content)
        .last()
        .map(|c| {
            let num: f64 = c
                .get(1)
                .map(|m| m.as_str().replace(',', ""))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let mult: f64 = match c.get(2).map(|m| m.as_str()) {
                None => 1.0,
                Some(suffix) => {
                    // `-h` uses powers of 1000 (K/M/G…), `-hh` powers of
                    // 1024 with an `i` marker (Ki/Mi/Gi…).
                    let base: f64 = if suffix.ends_with('i') {
                        1024.0
                    } else {
                        1000.0
                    };
                    match suffix.chars().next().map(|ch| ch.to_ascii_uppercase()) {
                        Some('K') => base,
                        Some('M') => base.powi(2),
                        Some('G') => base.powi(3),
                        Some('T') => base.powi(4),
                        Some('P') => base.powi(5),
                        _ => 1.0,
                    }
                }
            };
            (num * mult) as u64
        })
        .unwrap_or(0)
}

/// Extract a size string matching `pattern` from log content.
///
/// Returns the first capture group of the **last** match (or the full last
/// match if no group), mirroring Go's `ExtractSizeFromLog`:
/// `matches[len(matches)-1][1]`.
pub fn extract_size_from_log(log_content: &str, pattern: &regex::Regex) -> String {
    pattern
        .captures_iter(log_content)
        .last()
        .and_then(|c| c.get(1).or_else(|| c.get(0)))
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default()
}

/// Parse a human-readable size string into bytes.
///
/// Supports: "100GB", "2TB", "500MB", "1.5T", "100G", etc.
/// Case insensitive. Returns `None` on parse failure.
pub fn parse_size_bytes(s: &str) -> Option<u64> {
    static RE: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(r"(?i)^([0-9]+(?:\.[0-9]+)?)\s*(k|kb|m|mb|g|gb|t|tb|p|pb)?b?$")
            .expect("static regex")
    });
    let caps = RE.captures(s.trim())?;
    let num: f64 = caps.get(1)?.as_str().parse().ok()?;
    let multiplier: f64 = match caps
        .get(2)
        .map(|m| m.as_str().to_ascii_lowercase())
        .as_deref()
    {
        Some("k") | Some("kb") => 1024.0,
        Some("m") | Some("mb") => 1024.0 * 1024.0,
        Some("g") | Some("gb") => 1024.0 * 1024.0 * 1024.0,
        Some("t") | Some("tb") => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        Some("p") | Some("pb") => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    };
    Some((num * multiplier) as u64)
}

/// Parse a human-readable duration string into seconds.
///
/// Supports: "48h", "2d", "7d", "1d12h", "30m", "3600s".
/// Returns `None` on parse failure.
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    // Match a full duration string from start to end. Without the `^`
    // anchor the regex would accept e.g. "about 48h" (matching just the
    // "48h" suffix) and silently produce a wrong value. We want strict
    // parsing: only well-formed all-digit-plus-unit input is valid.
    static RE: Lazy<regex::Regex> = Lazy::new(|| {
        regex::Regex::new(r"(?i)^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
            .expect("static regex")
    });
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let caps = RE.captures(s)?;
    let d: u64 = caps
        .get(1)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0);
    let h: u64 = caps
        .get(2)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0);
    let m: u64 = caps
        .get(3)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0);
    let sec: u64 = caps
        .get(4)
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(0);
    let total = d * 86400 + h * 3600 + m * 60 + sec;
    if total == 0 {
        None
    } else {
        Some(total)
    }
}

/// Check available disk space at `path`. Returns `(available_bytes, total_bytes)`.
///
/// Uses `statvfs` on Unix. Returns `None` on failure.
#[cfg(unix)]
pub fn disk_space(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return None;
    }
    let avail = stat.f_bavail as u64 * stat.f_frsize as u64;
    let total = stat.f_blocks as u64 * stat.f_frsize as u64;
    Some((avail, total))
}

#[cfg(not(unix))]
pub fn disk_space(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
}

// ---------------------------------------------------------------------------
// API token (bearer) authentication helpers
// ---------------------------------------------------------------------------

/// Constant-time byte-string equality.
///
/// Avoids early-exit timing differences when comparing secrets. Length
/// difference still short-circuits — leaking the *length* of a high-entropy
/// random token is not a practical concern, leaking prefix-match length is.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Validate an `Authorization: Bearer <token>` header value against the
/// expected token. An empty `expected` means authentication is disabled and
/// everything passes (backward compatible default).
pub fn check_bearer(authorization: Option<&str>, expected: &str) -> bool {
    if expected.is_empty() {
        return true;
    }
    let Some(value) = authorization else {
        return false;
    };
    let Some(presented) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(presented.trim().as_bytes(), expected.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsync_exit_code_known() {
        let (code, msg) = translate_rsync_error_code(23);
        assert_eq!(code, 23);
        assert!(msg.contains("Partial transfer"), "got: {msg}");
    }

    #[test]
    fn rsync_exit_code_4_full_message() {
        let (code, msg) = translate_rsync_error_code(4);
        assert_eq!(code, 4);
        assert!(msg.contains("64-bit"), "got: {msg}");
    }

    #[test]
    fn rsync_exit_code_unknown() {
        let (code, msg) = translate_rsync_error_code(99);
        assert_eq!(code, 99);
        assert_eq!(msg, "");
    }

    #[test]
    fn extract_rsync_size() {
        let log = "some stuff\nTotal file size: 1.23G bytes (some suffix)\nmore\n";
        assert_eq!(extract_size_from_rsync_log(log), "1.23G");
    }

    #[test]
    fn extract_rsync_size_takes_last_occurrence() {
        let log = "Total file size: 100K bytes\nTotal file size: 1.23G bytes\n";
        assert_eq!(extract_size_from_rsync_log(log), "1.23G");
    }

    #[test]
    fn extract_rsync_size_empty() {
        assert_eq!(extract_size_from_rsync_log("no size here"), "");
    }

    /// Regression: rsync 3.x with certain --info / locale settings produces
    /// comma-separated digit groups in "Total file size". The original regex
    /// rejected this form and the mirror's size column in the UI went blank
    /// after a successful sync.
    #[test]
    fn extract_rsync_size_handles_commas() {
        let log = "Total file size: 1,234,567,890 bytes\n";
        assert_eq!(extract_size_from_rsync_log(log), "1,234,567,890");
    }

    /// Plain integer (no commas, no suffix) still works.
    #[test]
    fn extract_rsync_size_handles_plain_integer() {
        let log = "Total file size: 42 bytes\n";
        assert_eq!(extract_size_from_rsync_log(log), "42");
    }

    #[test]
    fn extract_transferred_bytes() {
        let log = "Number of files: 100\nTotal transferred file size: 1,234,567 bytes\n";
        assert_eq!(extract_transferred_bytes_from_rsync_log(log), 1234567);
    }

    #[test]
    fn bearer_check() {
        use super::check_bearer;
        // disabled auth passes everything
        assert!(check_bearer(None, ""));
        assert!(check_bearer(Some("garbage"), ""));
        // enabled auth
        assert!(check_bearer(Some("Bearer s3cret"), "s3cret"));
        assert!(check_bearer(Some("Bearer s3cret  "), "s3cret"));
        assert!(!check_bearer(Some("Bearer wrong"), "s3cret"));
        assert!(!check_bearer(Some("s3cret"), "s3cret")); // missing scheme
        assert!(!check_bearer(None, "s3cret"));
    }

    #[test]
    fn extract_transferred_bytes_empty() {
        assert_eq!(extract_transferred_bytes_from_rsync_log("no data"), 0);
    }

    /// Regression: `--human-readable` (-h) suffixed stats used to silently
    /// parse as 0 because the regex never matched the suffixed form.
    #[test]
    fn extract_transferred_bytes_human_readable() {
        // -h: powers of 1000
        let log = "Total transferred file size: 1.23M bytes\n";
        assert_eq!(extract_transferred_bytes_from_rsync_log(log), 1_230_000);
        let log = "Total transferred file size: 2.5G bytes\n";
        assert_eq!(extract_transferred_bytes_from_rsync_log(log), 2_500_000_000);
        // -hh: powers of 1024 with `i` marker
        let log = "Total transferred file size: 1.00Ki bytes\n";
        assert_eq!(extract_transferred_bytes_from_rsync_log(log), 1024);
        let log = "Total transferred file size: 1.50Gi bytes\n";
        assert_eq!(
            extract_transferred_bytes_from_rsync_log(log),
            (1.5f64 * 1024.0 * 1024.0 * 1024.0) as u64
        );
    }

    #[test]
    fn parse_size_variants() {
        assert_eq!(parse_size_bytes("100GB"), Some(100 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_bytes("2TB"), Some(2 * 1024 * 1024 * 1024 * 1024));
        assert_eq!(parse_size_bytes("500MB"), Some(500 * 1024 * 1024));
        assert_eq!(parse_size_bytes("1024"), Some(1024));
        assert_eq!(parse_size_bytes("1G"), Some(1024 * 1024 * 1024));
        assert!(parse_size_bytes("").is_none());
        assert!(parse_size_bytes("abc").is_none());
    }

    #[test]
    fn parse_duration_variants() {
        assert_eq!(parse_duration_secs("48h"), Some(48 * 3600));
        assert_eq!(parse_duration_secs("2d"), Some(2 * 86400));
        assert_eq!(parse_duration_secs("1d12h"), Some(86400 + 12 * 3600));
        assert_eq!(parse_duration_secs("30m"), Some(30 * 60));
        assert_eq!(parse_duration_secs("3600s"), Some(3600));
        assert!(parse_duration_secs("").is_none());
    }

    /// Reject inputs with junk *before* the duration. The original regex
    /// lacked a `^` anchor and silently accepted "about 48h" as 48h, which
    /// makes typos like "stale_after = '~48h'" or "max_age = 'about 48h'"
    /// produce a wrong value rather than a config error.
    #[test]
    fn parse_duration_rejects_unanchored_prefix() {
        assert!(parse_duration_secs("about 48h").is_none());
        assert!(parse_duration_secs("~48h").is_none());
        assert!(parse_duration_secs("foo48h").is_none());
        assert!(parse_duration_secs(" 48h").is_some()); // trim() handles leading WS
        assert!(parse_duration_secs("48h ").is_some()); // trim() handles trailing WS
        assert!(parse_duration_secs("48 h").is_none()); // internal space rejected
        assert!(parse_duration_secs("48h junk").is_none());
    }

    #[test]
    fn disk_space_on_tmp() {
        // Just verify it doesn't panic on a real path.
        let result = disk_space(std::path::Path::new("/tmp"));
        // On CI this might not work, so just test it returns Some on Linux.
        #[cfg(target_os = "linux")]
        assert!(result.is_some(), "expected Some on /tmp");
        let _ = result;
    }
}
