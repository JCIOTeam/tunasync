//! Per-mirror live log broadcast for the streaming HTTP API.
//!
//! Each line of stdout/stderr produced by a sync's child process is published
//! into a `tokio::sync::broadcast` channel keyed by mirror name. The worker
//! HTTP server exposes `GET /jobs/<mirror>/log/stream` (SSE) which subscribes
//! to the channel and forwards lines to the client in real time.
//!
//! The broadcaster registry is shared between:
//!
//! 1. The worker (creates it at startup, hands a per-mirror `Sender` to each
//!    provider when its job is wired up).
//! 2. The HTTP server (subscribes to the per-mirror channel on demand).
//!
//! Channel capacity is bounded — if no subscribers are attached, sent lines
//! are simply discarded (a `broadcast::Sender::send` with zero receivers
//! returns `Err(SendError)` which we ignore). Slow subscribers that fall
//! behind by more than the capacity will receive a `RecvError::Lagged` and
//! can decide whether to skip or disconnect; the SSE handler logs the lag
//! as a comment event and continues.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;

/// How many lines may be buffered for a slow subscriber before lines start
/// being dropped. 1024 is comfortably above typical rsync output rates yet
/// small enough that a stalled subscriber doesn't pin unbounded memory.
const CHANNEL_CAPACITY: usize = 1024;

/// Registry of per-mirror live-log broadcast channels.
///
/// Senders are created lazily on first request (whichever side gets there
/// first — provider or HTTP subscriber — wins; the other reuses the same
/// channel). Senders are never removed because mirror names are stable
/// across hot-reloads and the per-channel memory is negligible.
#[derive(Default)]
pub struct LogBroadcaster {
    senders: RwLock<HashMap<String, broadcast::Sender<String>>>,
}

impl LogBroadcaster {
    /// Construct an empty broadcaster wrapped in an `Arc` for sharing.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Return (or create) the broadcast sender for a mirror.
    ///
    /// Called by the worker when wiring up a provider so the runner can
    /// publish each stdout/stderr line into the channel.
    pub fn sender_for(&self, name: &str) -> broadcast::Sender<String> {
        if let Some(s) = self.senders.read().get(name) {
            return s.clone();
        }
        let mut w = self.senders.write();
        w.entry(name.to_owned())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .clone()
    }

    /// Subscribe to live log lines for a mirror.
    ///
    /// The returned receiver only observes lines produced *after* the
    /// subscription. For historical content, clients should read the rotated
    /// log file directly (out of scope for this API).
    pub fn subscribe(&self, name: &str) -> broadcast::Receiver<String> {
        self.sender_for(name).subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    #[tokio::test]
    async fn sender_and_subscriber_share_channel() {
        let b = LogBroadcaster::new();
        let mut rx = b.subscribe("debian");
        let tx = b.sender_for("debian");
        tx.send("hello".into()).unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn lines_before_subscription_are_dropped() {
        let b = LogBroadcaster::new();
        let tx = b.sender_for("ubuntu");
        // No receivers yet — send returns Err but we don't care.
        let _ = tx.send("orphan".into());
        let mut rx = b.subscribe("ubuntu");
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn different_mirrors_have_independent_channels() {
        let b = LogBroadcaster::new();
        let mut rx_a = b.subscribe("a");
        let mut rx_b = b.subscribe("b");
        b.sender_for("a").send("line-a".into()).unwrap();
        assert_eq!(rx_a.recv().await.unwrap(), "line-a");
        assert!(matches!(rx_b.try_recv(), Err(TryRecvError::Empty)));
    }
}
