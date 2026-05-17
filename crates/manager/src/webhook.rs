//! Webhook notification sender.
//!
//! Sends POST requests to a configured URL when mirror events occur:
//! - Mirror becomes stale (no successful sync for longer than `stale_after`)
//! - Mirror hits `alert_after_failures` consecutive failures
//! - Mirror recovers from stale / failure streak
//!
//! Payload is a simple `{ "text": "..." }` JSON body compatible with
//! Slack incoming webhooks, Discord webhooks (with `/slack` suffix),
//! Feishu custom bots, and WeChat Work group bots.

use reqwest::Client;
use tracing::{debug, warn};

/// Send a plain-text webhook notification.
///
/// Non-blocking: logs errors but never fails — webhook delivery is
/// best-effort and must not affect the manager's operation.
pub async fn send(client: &Client, url: &str, text: &str) {
    if url.is_empty() {
        return;
    }

    #[derive(serde::Serialize)]
    struct Payload<'a> {
        text: &'a str,
    }

    debug!(url, text, "sending webhook notification");

    match client
        .post(url)
        .json(&Payload { text })
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            debug!(url, "webhook delivered successfully");
        }
        Ok(resp) => {
            warn!(
                url,
                status = %resp.status(),
                "webhook delivery got non-2xx response"
            );
        }
        Err(e) => {
            warn!(url, error = %e, "webhook delivery failed");
        }
    }
}
