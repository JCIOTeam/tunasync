//! Per-mirror live log broadcast with bounded replay buffer.
//!
//! Each line of stdout/stderr produced by a sync's child process flows into
//! a per-mirror state cell that owns two things:
//!
//! 1. A `tokio::sync::broadcast::Sender<String>` — the live channel that
//!    SSE subscribers tail.
//! 2. A `VecDeque<String>` of the most recent `BUFFER_CAPACITY` lines —
//!    used to *replay* recent context to a client that connects mid-sync,
//!    so the UI is never empty when somebody opens the page in the middle
//!    of a long-running rsync.
//!
//! The buffer is **cleared on each `runner::spawn` invocation** so that a
//! client only ever sees lines from the current sync — matching the file
//! semantics (`tee_to_log` truncates the log file on open).
//!
//! # Concurrency
//!
//! Pushes and snapshots are serialised by a single `parking_lot::Mutex`
//! per mirror — held only long enough to update the deque and call
//! `Sender::send` / `Sender::subscribe`, both of which are non-blocking.
//! This makes `snapshot_and_subscribe` *atomic*: a line published after
//! the snapshot is guaranteed to land on the subscription stream, never
//! both or neither.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;

/// How many recent lines to keep for replay-on-connect. Small on purpose:
/// the buffer exists so the UI isn't blank when a client connects mid-sync,
/// *not* to serve up historical context. 10 lines is plenty to show that
/// "something is happening"; anything older lives in the rotated log file.
const BUFFER_CAPACITY: usize = 10;

/// Broadcast channel capacity — the maximum lag a slow subscriber can
/// tolerate before receiving a `Lagged(n)` notification and skipping ahead.
const CHANNEL_CAPACITY: usize = 1024;

/// Per-mirror cell. Holds the live broadcast channel and the recent-lines
/// buffer used for replay-on-connect.
struct Cell {
    sender: broadcast::Sender<String>,
    buffer: VecDeque<String>,
}

impl Cell {
    fn new() -> Self {
        let (sender, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            sender,
            buffer: VecDeque::with_capacity(BUFFER_CAPACITY),
        }
    }
}

/// Registry of per-mirror broadcast cells.
#[derive(Default)]
pub struct LogBroadcaster {
    cells: RwLock<HashMap<String, Arc<Mutex<Cell>>>>,
}

impl LogBroadcaster {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Get (or lazily create) the cell for a mirror.
    fn cell(&self, name: &str) -> Arc<Mutex<Cell>> {
        if let Some(c) = self.cells.read().get(name) {
            return Arc::clone(c);
        }
        let mut w = self.cells.write();
        Arc::clone(
            w.entry(name.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(Cell::new()))),
        )
    }

    /// Return a publisher handle the runner uses to push lines.
    ///
    /// Takes `self: &Arc<Self>` so the publisher shares the **same**
    /// underlying broadcaster — writes via the publisher are visible to
    /// `snapshot_and_subscribe` on the same broadcaster instance.
    pub fn publisher_for(self: &Arc<Self>, name: &str) -> LogPublisher {
        // Eagerly create the cell so subscribers can attach before the
        // provider's first sync — they then get an empty replay rather
        // than 404-flavoured surprises.
        let _ = self.cell(name);
        LogPublisher {
            broadcaster: Arc::clone(self),
            name: name.to_owned(),
        }
    }

    /// Atomically snapshot the recent-lines buffer and subscribe to all
    /// future lines. The returned receiver is guaranteed to see every line
    /// published *after* the snapshot was taken — no gap, no duplicate.
    pub fn snapshot_and_subscribe(&self, name: &str) -> (Vec<String>, broadcast::Receiver<String>) {
        let cell = self.cell(name);
        let guard = cell.lock();
        let history: Vec<String> = guard.buffer.iter().cloned().collect();
        let rx = guard.sender.subscribe();
        (history, rx)
    }

    // --- internal use by LogPublisher --------------------------------------

    fn push(&self, name: &str, line: String) {
        let cell = self.cell(name);
        let mut guard = cell.lock();
        if guard.buffer.len() == BUFFER_CAPACITY {
            guard.buffer.pop_front();
        }
        guard.buffer.push_back(line.clone());
        // SendError (no live subscribers) is fine — the buffer still has it.
        let _ = guard.sender.send(line);
    }

    fn reset(&self, name: &str) {
        let cell = self.cell(name);
        let mut guard = cell.lock();
        guard.buffer.clear();
    }
}

/// Publisher handle bound to a single mirror — held by a provider and
/// passed into `runner::spawn`.
///
/// Cheap to clone (just an `Arc` and a `String`).
#[derive(Clone)]
pub struct LogPublisher {
    broadcaster: Arc<LogBroadcaster>,
    name: String,
}

impl LogPublisher {
    /// Clear the replay buffer. Called by `runner::spawn` once per
    /// invocation so a mid-sync subscriber only sees lines from the
    /// current run, not leftovers from earlier (failed/cancelled) ones.
    pub fn reset(&self) {
        self.broadcaster.reset(&self.name);
    }

    /// Append a single output line to the buffer and publish to all
    /// current subscribers. Best-effort: lines emitted when no
    /// subscribers are listening are still added to the buffer for
    /// later replay.
    pub fn push(&self, line: String) {
        self.broadcaster.push(&self.name, line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::TryRecvError;

    fn pub_and_broadcaster(name: &str) -> (Arc<LogBroadcaster>, LogPublisher) {
        let b = LogBroadcaster::new();
        let p = b.publisher_for(name);
        (b, p)
    }

    #[tokio::test]
    async fn live_lines_reach_subscriber() {
        let (b, p) = pub_and_broadcaster("a");
        let (history, mut rx) = b.snapshot_and_subscribe("a");
        assert!(history.is_empty());
        p.push("hello".into());
        assert_eq!(rx.recv().await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn mid_sync_subscriber_sees_replay_then_live() {
        let (b, p) = pub_and_broadcaster("a");
        for i in 0..3 {
            p.push(format!("pre-{i}"));
        }
        let (history, mut rx) = b.snapshot_and_subscribe("a");
        assert_eq!(history, vec!["pre-0", "pre-1", "pre-2"]);
        p.push("post-0".into());
        assert_eq!(rx.recv().await.unwrap(), "post-0");
    }

    #[tokio::test]
    async fn reset_clears_buffer_but_leaves_subscription_alive() {
        let (b, p) = pub_and_broadcaster("a");
        // Push BEFORE any subscriber exists — line goes into the buffer but
        // is dropped from the broadcast channel (no listeners).
        p.push("stale".into());
        let (history, mut rx) = b.snapshot_and_subscribe("a");
        assert_eq!(history, vec!["stale".to_owned()]);
        // Reset wipes the buffer; the live subscription is unaffected.
        p.reset();
        p.push("fresh".into());
        assert_eq!(rx.recv().await.unwrap(), "fresh");
        // A *new* subscriber connecting after reset only sees post-reset
        // content in its replay.
        let (history2, _rx2) = b.snapshot_and_subscribe("a");
        assert_eq!(history2, vec!["fresh".to_owned()]);
    }

    #[tokio::test]
    async fn buffer_evicts_oldest_when_full() {
        let (b, p) = pub_and_broadcaster("a");
        for i in 0..(BUFFER_CAPACITY + 50) {
            p.push(format!("L{i}"));
        }
        let (history, _rx) = b.snapshot_and_subscribe("a");
        assert_eq!(history.len(), BUFFER_CAPACITY);
        assert_eq!(history.first().unwrap(), &format!("L{}", 50));
        assert_eq!(
            history.last().unwrap(),
            &format!("L{}", BUFFER_CAPACITY + 49)
        );
    }

    #[tokio::test]
    async fn different_mirrors_have_independent_state() {
        let b = LogBroadcaster::new();
        let pa = b.publisher_for("a");
        let pb = b.publisher_for("b");
        pa.push("from-a".into());
        let (hist_a, _) = b.snapshot_and_subscribe("a");
        let (hist_b, mut rx_b) = b.snapshot_and_subscribe("b");
        assert_eq!(hist_a, vec!["from-a".to_owned()]);
        assert!(hist_b.is_empty());
        assert!(matches!(rx_b.try_recv(), Err(TryRecvError::Empty)));
        pb.push("from-b".into());
        assert_eq!(rx_b.recv().await.unwrap(), "from-b");
    }
}
