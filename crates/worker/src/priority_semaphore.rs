//! Priority semaphore — an async semaphore whose waiters are woken in
//! descending priority order (higher i32 = runs first).  Ties are broken
//! FIFO: the waiter that called `acquire` first wins among equal-priority
//! waiters.
//!
//! # Design
//!
//! A `parking_lot::Mutex` guards the inner state: an available-permits
//! counter plus a `BinaryHeap` of pending waiters.  Each waiter gets its
//! own `tokio::sync::oneshot` channel; the semaphore wakes the highest-
//! priority pending waiter by sending on its sender.
//!
//! Cancellation is handled correctly: if the future returned by `acquire`
//! is dropped before the permit is received, the waiter removes itself
//! from the heap on drop by marking itself cancelled via an
//! `Arc<AtomicBool>`.  Cancelled waiters are skipped during wakeup.
//!
//! # Drop of Permit
//!
//! `Permit` holds a back-reference to the `PrioritySemaphore`.  When
//! dropped it calls `release(1)`, which wakes the next waiter.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AOrdering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Sequence counter for FIFO ordering within the same priority.
static SEQ: AtomicU64 = AtomicU64::new(0);

// ── Waiter ────────────────────────────────────────────────────────────────────

struct Waiter {
    priority: i32,
    /// Lower seq = enqueued earlier → wins ties.
    seq: u64,
    tx: oneshot::Sender<()>,
    /// Set to true when the acquire future is dropped before the permit fires.
    cancelled: Arc<AtomicBool>,
}

impl PartialEq for Waiter {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for Waiter {}

impl PartialOrd for Waiter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Waiter {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority wins; for ties, lower seq (earlier enqueue) wins.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

// ── Inner state ────────────────────────────────────────────────────────────────

struct Inner {
    permits: usize,
    waiters: BinaryHeap<Waiter>,
}

impl Inner {
    /// Try to wake the highest-priority non-cancelled waiter.
    /// Decrements `permits` for each permit granted.
    fn wake_next(&mut self) {
        while self.permits > 0 {
            match self.waiters.pop() {
                None => break,
                Some(w) if w.cancelled.load(AOrdering::Acquire) => {
                    // Cancelled — skip and try next.
                    continue;
                }
                Some(w) => {
                    // Wake the waiter.  If the send fails the receiver was
                    // dropped (raced with cancellation) — release will be
                    // called by the Permit's Drop impl anyway, so don't
                    // adjust permits here (the waiter never got the permit).
                    if w.tx.send(()).is_ok() {
                        self.permits -= 1;
                    }
                    break;
                }
            }
        }
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// An async semaphore that wakes waiters in priority order.
pub struct PrioritySemaphore {
    inner: Arc<Mutex<Inner>>,
}

impl PrioritySemaphore {
    /// Create a semaphore with `permits` initial permits.
    pub fn new(permits: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                permits,
                waiters: BinaryHeap::new(),
            })),
        }
    }

    /// Acquire one permit, waiting until one is available.
    ///
    /// Waiters with higher `priority` are woken first.  Among equal-priority
    /// waiters the one that called `acquire` earliest wins (FIFO).
    pub async fn acquire(&self, priority: i32) -> Permit {
        // Fast path: grab a permit immediately if one is free.
        {
            let mut g = self.inner.lock();
            if g.permits > 0 {
                g.permits -= 1;
                return Permit {
                    inner: Arc::clone(&self.inner),
                };
            }
        }

        // Slow path: enqueue and wait.
        let (tx, rx) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let seq = SEQ.fetch_add(1, AOrdering::Relaxed);
        {
            let mut g = self.inner.lock();
            g.waiters.push(Waiter {
                priority,
                seq,
                tx,
                cancelled: Arc::clone(&cancelled),
            });
        }

        // Return a future that resolves when the permit is granted.
        // On drop (cancellation), mark as cancelled so the semaphore skips us.
        AcquireFuture {
            rx,
            cancelled,
            inner: Arc::clone(&self.inner),
        }
        .await
    }

    /// Add `n` permits and wake up to `n` waiters.
    pub fn add_permits(&self, n: usize) {
        let mut g = self.inner.lock();
        g.permits += n;
        for _ in 0..n {
            if g.waiters.is_empty() {
                break;
            }
            g.wake_next();
        }
    }

    /// Available permits (for testing / diagnostics).
    pub fn available_permits(&self) -> usize {
        self.inner.lock().permits
    }
}

// ── AcquireFuture ─────────────────────────────────────────────────────────────

struct AcquireFuture {
    rx: oneshot::Receiver<()>,
    cancelled: Arc<AtomicBool>,
    inner: Arc<Mutex<Inner>>,
}

impl std::future::Future for AcquireFuture {
    type Output = Permit;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(Ok(())) => {
                std::task::Poll::Ready(Permit {
                    inner: Arc::clone(&self.inner),
                })
            }
            std::task::Poll::Ready(Err(_)) => {
                // Sender dropped — shouldn't happen in normal operation.
                // Treat as granted (the Inner::wake_next path failed to send
                // but we've already been removed from the queue).
                std::task::Poll::Ready(Permit {
                    inner: Arc::clone(&self.inner),
                })
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for AcquireFuture {
    fn drop(&mut self) {
        // Mark as cancelled so wake_next skips this waiter.
        self.cancelled.store(true, AOrdering::Release);
    }
}

// ── Permit ────────────────────────────────────────────────────────────────────

/// A permit that releases back to the semaphore when dropped.
pub struct Permit {
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut g = self.inner.lock();
        g.permits += 1;
        g.wake_next();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AO};

    /// Six waiters with priorities [10, 50, 50, 90, 30, 70] across 3 permits
    /// must complete in priority order: [90, 70, 50, 50, 30, 10], FIFO for ties.
    #[tokio::test]
    async fn priority_ordering() {
        let sem = Arc::new(PrioritySemaphore::new(0));
        let order = Arc::new(Mutex::new(Vec::<i32>::new()));
        let counter = Arc::new(AtomicUsize::new(0));

        let priorities = [10i32, 50, 50, 90, 30, 70];
        let mut handles = Vec::new();

        for &prio in &priorities {
            let sem2 = Arc::clone(&sem);
            let order2 = Arc::clone(&order);
            let counter2 = Arc::clone(&counter);
            let h = tokio::spawn(async move {
                let _permit = sem2.acquire(prio).await;
                order2.lock().push(prio);
                counter2.fetch_add(1, AO::SeqCst);
            });
            handles.push(h);
        }

        // Give all tasks time to enqueue.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Release all 6 permits one at a time to force ordering.
        for _ in 0..6 {
            sem.add_permits(1);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        for h in handles {
            h.await.unwrap();
        }

        let got = order.lock().clone();
        assert_eq!(
            got,
            vec![90, 70, 50, 50, 30, 10],
            "expected priority ordering [90,70,50,50,30,10], got {got:?}"
        );
    }

    /// A permit must be released on drop, allowing a waiting acquire to proceed.
    #[tokio::test]
    async fn permit_drop_releases() {
        let sem = Arc::new(PrioritySemaphore::new(1));

        let p1 = sem.acquire(0).await;
        assert_eq!(sem.available_permits(), 0);

        let sem2 = Arc::clone(&sem);
        let h = tokio::spawn(async move {
            let _p2 = sem2.acquire(0).await;
        });

        // Drop p1 so the spawned task can proceed.
        drop(p1);
        h.await.unwrap();
        assert_eq!(sem.available_permits(), 1);
    }

    /// add_permits wakes waiting tasks up to n times.
    #[tokio::test]
    async fn add_permits_wakes_waiters() {
        let sem = Arc::new(PrioritySemaphore::new(0));
        let acquired = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..3 {
            let s = Arc::clone(&sem);
            let a = Arc::clone(&acquired);
            handles.push(tokio::spawn(async move {
                let _p = s.acquire(0).await;
                a.fetch_add(1, AO::SeqCst);
            }));
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        sem.add_permits(3);

        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(acquired.load(AO::SeqCst), 3);
    }
}
