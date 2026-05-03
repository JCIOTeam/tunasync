//! Schedule queue for mirror jobs.
//!
//! Mirrors Go's `scheduleQueue` (a heap ordered by next-run time).
//! The worker's main loop pops the earliest job and either runs it immediately
//! (if its time has come) or sleeps until it's due.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

/// One entry in the schedule queue.
#[derive(Debug, Clone)]
pub struct ScheduleEntry {
    /// Mirror name.
    pub name: String,
    /// When this job should next run.
    pub next_run: Instant,
}

impl PartialEq for ScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.next_run == other.next_run
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
        // Reverse: smaller `next_run` wins.
        other.next_run.cmp(&self.next_run)
    }
}

/// Min-heap of scheduled mirror jobs ordered by next-run time.
/// Uses a HashMap alongside the heap to deduplicate entries — matching Go's
/// skiplist behaviour where a new AddJob replaces any existing entry for
/// the same job name.
#[derive(Default)]
pub struct ScheduleQueue {
    heap: BinaryHeap<ScheduleEntry>,
    /// Tracks the latest next_run for each job name. Stale heap entries
    /// (whose next_run doesn't match this map) are skipped on pop.
    latest: HashMap<String, Instant>,
}

impl ScheduleQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or update a job's next-run time.
    /// If a job with the same name already exists, it is replaced.
    pub fn push(&mut self, name: String, next_run: Instant) {
        self.latest.insert(name.clone(), next_run);
        self.heap.push(ScheduleEntry { name, next_run });
    }

    /// Peek at the earliest-due entry without removing it.
    pub fn peek(&self) -> Option<&ScheduleEntry> {
        self.heap.peek()
    }

    /// Pop the earliest-due entry, skipping stale duplicates.
    pub fn pop(&mut self) -> Option<ScheduleEntry> {
        while let Some(entry) = self.heap.pop() {
            // Skip if this is a stale entry (a newer push replaced it).
            if self.latest.get(&entry.name) == Some(&entry.next_run) {
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

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }
}
