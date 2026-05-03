//! Tracing-based logger initialisation.
//!
//! Replaces Go tunasync's `logrus`-based logger. The default level is
//! controlled by the `RUST_LOG` env var, falling back to `info` (or `debug`
//! when [`init`] is called with `verbose = true`).

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialise the global tracing subscriber.
///
/// Idempotent in the sense that repeated calls after the first will be
/// silently ignored by `tracing-subscriber` — useful in tests where multiple
/// integration cases each try to initialise logging.
///
/// `verbose` raises the default level from `info` to `debug`. Either way,
/// `RUST_LOG` (if set) takes precedence so operators can dial in per-module
/// filters at runtime.
pub fn init(verbose: bool) {
    let default_level = if verbose { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(default_level));

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
