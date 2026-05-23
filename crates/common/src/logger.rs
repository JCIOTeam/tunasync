//! Tracing-based logger initialisation.
//!
//! Replaces Go tunasync's `logrus`-based logger. The default level is
//! controlled by the `RUST_LOG` env var, falling back to `info` (or `debug`
//! when [`init`] is called with `verbose = true`).
//!
//! When `systemd_mode` is true (i.e. `--with-systemd` flag), the log layer
//! omits timestamps and ANSI colours since systemd journal already prefixes
//! timestamps — matching Go's `--with-systemd` behaviour.
//!
//! When `ansi` is false, ANSI colour codes are suppressed in log output.
//! CLI tools like `tunasynctl` should pass `ansi = false` to avoid terminal
//! rendering issues (e.g. white backgrounds on URL values).

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialise the global tracing subscriber.
///
/// `verbose` raises the default level from `info` to `debug`. `systemd_mode`
/// suppresses timestamps and ANSI colours (journald adds its own timestamps).
/// `ansi` controls ANSI colour output; pass `false` for CLI tools that should
/// not emit colour codes.
///
/// `RUST_LOG` (if set) takes precedence over both.
pub fn init(verbose: bool, systemd_mode: bool, ansi: bool) {
    let default_level = if verbose { "debug" } else { "info" };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    // `without_time()` changes the layer's format type, so the two branches
    // must be built separately rather than conditionally mutating one layer.
    if systemd_mode {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_line_number(false)
                    .with_ansi(false)
                    .without_time(),
            )
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_line_number(false)
                    .with_ansi(ansi),
            )
            .try_init();
    }
}
