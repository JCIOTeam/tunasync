//! Schedule queue for mirror jobs.
//!
//! Mirrors Go's `scheduleQueue` (a heap ordered by next-run time).
//! The worker's main loop pops the earliest job and either runs it immediately
//! (if its time has come) or sleeps until it's due.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use chrono::{DateTime, Utc};

/// One entry in the schedule queue.
#[derive(Debug, Clone)]
pub struct ScheduleEntry {
    /// Mirror name.
    pub name: String,
    /// Intended UTC wall-clock time for this occurrence.
    pub scheduled_at: DateTime<Utc>,
    /// Monotonic replacement generation for stale-entry detection.
    pub generation: u64,
}

impl PartialEq for ScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation && self.name == other.name
    }
}
impl Eq for ScheduleEntry {}

// Flip ordering so BinaryHeap (max-heap) behaves as a min-heap.
impl PartialOrd for ScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScheduleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: smaller scheduled time wins; generation breaks ties.
        other
            .scheduled_at
            .cmp(&self.scheduled_at)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}

/// Min-heap of scheduled mirror jobs ordered by next-run time.
/// Uses a HashMap alongside the heap to deduplicate entries — matching Go's
/// skiplist behaviour where a new AddJob replaces any existing entry for
/// the same job name.
#[derive(Default)]
pub struct ScheduleQueue {
    heap: BinaryHeap<ScheduleEntry>,
    /// Tracks the latest generation for each job name.
    latest: HashMap<String, u64>,
    next_generation: u64,
}

impl ScheduleQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a job's next-run time.
    /// If a job with the same name already exists, it is replaced.
    pub fn push(&mut self, name: String, scheduled_at: DateTime<Utc>) {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("schedule generation exhausted");
        let generation = self.next_generation;
        self.latest.insert(name.clone(), generation);
        self.heap.push(ScheduleEntry {
            name,
            scheduled_at,
            generation,
        });
    }

    /// Peek at the earliest-due entry, removing stale duplicates from the top.
    /// Returns an owned `ScheduleEntry` (clone of the valid heap top) so the
    /// caller doesn't hold a borrow that would conflict with a subsequent `pop()`.
    pub fn peek(&mut self) -> Option<ScheduleEntry> {
        while let Some(top) = self.heap.peek() {
            if self.latest.get(&top.name) == Some(&top.generation) {
                return Some(top.clone());
            }
            // Stale entry — remove from heap and continue.
            self.heap.pop();
        }
        None
    }

    /// Pop the earliest-due entry, skipping stale duplicates.
    pub fn pop(&mut self) -> Option<ScheduleEntry> {
        while let Some(entry) = self.heap.pop() {
            // Skip if this is a stale entry (a newer push replaced it).
            if self.latest.get(&entry.name) == Some(&entry.generation) {
                self.latest.remove(&entry.name);
                return Some(entry);
            }
        }
        None
    }

    /// Remove a job from the schedule. Matches Go's skiplist Remove().
    /// Removes the name from the `latest` map so existing heap entries
    /// become stale and are skipped on `pop()`.
    pub fn remove(&mut self, name: &str) {
        self.latest.remove(name);
    }

    /// Returns true when there are no live entries to schedule.
    ///
    /// Filters out stale entries (heap entries whose name has been
    /// re-scheduled or removed via `latest`). Without this filter the
    /// method would report non-empty for a queue containing only stale
    /// entries that `pop()` would silently skip. Currently only the
    /// test suite calls this; production code uses `peek()` / `pop()`,
    /// both of which already filter correctly.
    pub fn is_empty(&self) -> bool {
        self.heap.iter().all(|e| {
            self.latest
                .get(&e.name)
                .map(|&latest| latest != e.generation)
                .unwrap_or(true)
        })
    }

    /// Number of live (non-stale) entries in the queue.
    ///
    /// Like `is_empty`, this filters stale entries so the result matches
    /// what an exhaustive `pop()` loop would yield. O(n) — not a hot path.
    pub fn len(&self) -> usize {
        self.heap
            .iter()
            .filter(|e| self.latest.get(&e.name) == Some(&e.generation))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, Utc};

    fn utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn retains_intended_utc_time_and_stales_same_time_replacement_by_generation() {
        let mut q = ScheduleQueue::new();
        let due = utc("2026-01-01T00:00:00Z");
        q.push("ubuntu".into(), due);
        q.push("ubuntu".into(), due);

        let entry = q.pop().unwrap();
        assert_eq!(entry.scheduled_at, due);
        assert!(entry.generation > 1);
        assert!(q.pop().is_none());
    }

    fn at(offset_secs: i64) -> DateTime<Utc> {
        utc("2026-01-01T00:00:00Z") + Duration::seconds(offset_secs)
    }

    #[test]
    fn push_and_pop_in_order() {
        let mut q = ScheduleQueue::new();
        q.push("late".into(), at(100));
        q.push("early".into(), at(10));
        q.push("middle".into(), at(50));

        assert_eq!(q.pop().unwrap().name, "early");
        assert_eq!(q.pop().unwrap().name, "middle");
        assert_eq!(q.pop().unwrap().name, "late");
        assert!(q.pop().is_none());
    }

    #[test]
    fn push_replaces_existing_entry() {
        let mut q = ScheduleQueue::new();
        let t_old = at(100);
        let t_new = at(10);
        q.push("ubuntu".into(), t_old);
        // Re-schedule to an earlier time — the old entry must become stale.
        q.push("ubuntu".into(), t_new);

        let entry = q.pop().unwrap();
        assert_eq!(entry.name, "ubuntu");
        assert_eq!(entry.scheduled_at, t_new);

        // Only one logical entry — subsequent pop must return None.
        assert!(q.pop().is_none());
    }

    #[test]
    fn push_replaces_with_later_time() {
        let mut q = ScheduleQueue::new();
        let t_early = at(10);
        let t_late = at(100);
        q.push("ubuntu".into(), t_early);
        // Re-schedule to a later time.
        q.push("ubuntu".into(), t_late);

        let entry = q.pop().unwrap();
        assert_eq!(entry.scheduled_at, t_late);
        assert!(q.pop().is_none());
    }

    #[test]
    fn remove_makes_entry_stale() {
        let mut q = ScheduleQueue::new();
        q.push("ubuntu".into(), at(10));
        q.push("debian".into(), at(20));

        q.remove("ubuntu");

        // Popping should skip the removed entry.
        let entry = q.pop().unwrap();
        assert_eq!(entry.name, "debian");
        assert!(q.pop().is_none());
    }

    #[test]
    fn peek_does_not_consume_entry() {
        let mut q = ScheduleQueue::new();
        q.push("ubuntu".into(), at(10));

        let peeked = q.peek().unwrap();
        assert_eq!(peeked.name, "ubuntu");

        // Entry still present after peek.
        let popped = q.pop().unwrap();
        assert_eq!(popped.name, "ubuntu");
        assert!(q.pop().is_none());
    }

    #[test]
    fn peek_skips_stale_and_returns_valid() {
        let mut q = ScheduleQueue::new();
        q.push("a".into(), at(10));
        q.push("a".into(), at(50)); // makes at(10) stale
        q.push("b".into(), at(20));

        // Peek should return "b" (earliest non-stale), not stale "a@10".
        let top = q.peek().unwrap();
        assert_eq!(top.name, "b");
    }

    #[test]
    fn is_empty_and_len() {
        let mut q = ScheduleQueue::new();
        assert!(q.is_empty());

        q.push("a".into(), at(1));
        q.push("b".into(), at(2));
        assert!(!q.is_empty());
        // Both entries are the latest for their names → both live.
        assert_eq!(q.len(), 2);

        q.pop();
        q.pop();
        // After popping all valid entries the heap should drain stale ones too.
        assert!(q.peek().is_none());
    }

    /// is_empty() and len() must filter stale entries — i.e. they should
    /// agree with what pop() / peek() see, not the raw heap size. Before
    /// this fix, a queue containing only stale entries reported len() > 0
    /// and is_empty() = false, misleading callers.
    #[test]
    fn is_empty_and_len_skip_stale_entries() {
        let mut q = ScheduleQueue::new();
        // Push two entries for "a"; the first becomes stale.
        q.push("a".into(), at(10));
        q.push("a".into(), at(20));
        // Heap holds 2 entries but only 1 is live (at=20).
        assert_eq!(q.len(), 1, "stale entry must not count in len");
        assert!(!q.is_empty());

        // Remove "a" entirely — both heap entries are now stale.
        q.remove("a");
        assert_eq!(q.len(), 0, "all-stale queue must report len=0");
        assert!(q.is_empty(), "all-stale queue must report empty");
        assert!(q.peek().is_none());
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut q = ScheduleQueue::new();
        q.push("ubuntu".into(), at(10));
        q.remove("nobody"); // should not panic
        assert_eq!(q.pop().unwrap().name, "ubuntu");
    }
}
