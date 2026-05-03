//! Tracing-based logger initialisation.
//!
//! Replaces Go tunasync's `logrus`-based logger. The default level is
//! controlled by the `RUST_LOG` env var, falling back to `info` (or `debug`
//! when [`init`] is called with `verbose = true`).
//!
//! When `systemd_mode` is true (i.e. `--with-systemd` flag), the log layer
//! omits timestamps and ANSI colours since systemd journal already prefixes
//! timestamps — matching Go's `--with-systemd` behaviour.

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialise the global tracing subscriber.
///
/// `verbose` raises the default level from `info` to `debug`. `systemd_mode`
/// suppresses timestamps and ANSI colours (journald adds its own timestamps).
///
/// `RUST_LOG` (if set) takes precedence over both.
pub fn init(verbose: bool, systemd_mode: bool) {
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
                    .with_line_number(false),
            )
            .try_init();
    }
}
