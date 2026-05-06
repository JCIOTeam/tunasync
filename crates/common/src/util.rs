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
/// The last occurrence is correct because rsync can emit multiple stats
/// blocks (e.g. in two-stage or incremental runs); only the final summary
/// reflects the total mirror size.
///
/// Mirrors Go's `ExtractSizeFromRsyncLog` / `ExtractSizeFromLog`.
pub fn extract_size_from_rsync_log(log_content: &str) -> String {
    let re =
        regex::Regex::new(r"(?m)^Total file size: ([0-9.]+[KMGTP]?) bytes").expect("static regex");
    // Collect all matches and return the last capture group of the last match.
    re.captures_iter(log_content)
        .last()
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default()
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
        // rsync may emit multiple stats blocks; we want the last one.
        let log = "Total file size: 100K bytes\nTotal file size: 1.23G bytes\n";
        assert_eq!(extract_size_from_rsync_log(log), "1.23G");
    }

    #[test]
    fn extract_rsync_size_empty() {
        assert_eq!(extract_size_from_rsync_log("no size here"), "");
    }
}
