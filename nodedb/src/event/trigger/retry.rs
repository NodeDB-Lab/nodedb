//! Exponential backoff retry queue for failed async trigger DML.
//!
//! When a trigger body's DML fails (constraint violation, timeout, shard
//! unavailable), the event is enqueued for retry with exponential backoff.
//! After `max_retries` (default 5), the event is sent to the trigger DLQ.
//!
//! The retry queue is in-memory (trigger events are WAL-backed — on restart,
//! the Event Plane replays missed events and re-fires triggers).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tracing::{debug, warn};

/// Default maximum retries before DLQ.
const DEFAULT_MAX_RETRIES: u32 = 5;

/// Base backoff delay (100ms, doubles each retry: 100, 200, 400, 800, 1600ms).
const BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum backoff cap (prevents absurdly long delays).
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// A trigger execution that failed and needs retry.
#[derive(Debug, Clone)]
pub struct RetryEntry {
    /// Tenant context.
    pub tenant_id: u32,
    /// Collection that triggered the event.
    pub collection: String,
    /// Row ID from the originating event.
    pub row_id: String,
    /// Operation: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Trigger name that failed.
    pub trigger_name: String,
    /// The deserialized row data (new_value).
    pub new_fields: Option<std::collections::HashMap<String, nodedb_types::Value>>,
    /// The deserialized row data (old_value).
    pub old_fields: Option<std::collections::HashMap<String, nodedb_types::Value>>,
    /// Number of retries attempted so far.
    pub attempts: u32,
    /// Last error message.
    pub last_error: String,
    /// When the next retry is due.
    pub next_retry_at: Instant,
    /// Original event's LSN.
    pub source_lsn: u64,
    /// Original event's sequence number.
    pub source_sequence: u64,
    /// Cascade depth from the original event.
    pub cascade_depth: u32,
}

/// In-memory retry queue with exponential backoff.
pub struct TriggerRetryQueue {
    queue: VecDeque<RetryEntry>,
    max_retries: u32,
}

impl TriggerRetryQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }

    /// Enqueue a failed trigger for retry.
    pub fn enqueue(&mut self, mut entry: RetryEntry) {
        entry.attempts += 1;
        let backoff = compute_backoff(entry.attempts);
        entry.next_retry_at = Instant::now() + backoff;

        debug!(
            trigger = %entry.trigger_name,
            collection = %entry.collection,
            attempt = entry.attempts,
            backoff_ms = backoff.as_millis(),
            "trigger retry enqueued"
        );

        self.queue.push_back(entry);
    }

    /// Drain all entries whose retry time has arrived.
    /// Returns `(ready_for_retry, exceeded_max_retries)`.
    pub fn drain_due(&mut self) -> (Vec<RetryEntry>, Vec<RetryEntry>) {
        let now = Instant::now();
        let mut ready = Vec::new();
        let mut exhausted = Vec::new();

        // Drain from front (oldest first).
        while let Some(entry) = self.queue.front() {
            if entry.next_retry_at > now {
                break; // All remaining entries are in the future.
            }
            let entry = self.queue.pop_front().unwrap();
            if entry.attempts >= self.max_retries {
                warn!(
                    trigger = %entry.trigger_name,
                    collection = %entry.collection,
                    attempts = entry.attempts,
                    "trigger exhausted max retries → DLQ"
                );
                exhausted.push(entry);
            } else {
                ready.push(entry);
            }
        }

        (ready, exhausted)
    }

    /// Number of entries pending retry.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Time until the next retry is due, or None if queue is empty.
    pub fn next_retry_delay(&self) -> Option<Duration> {
        self.queue.front().map(|e| {
            let now = Instant::now();
            if e.next_retry_at > now {
                e.next_retry_at - now
            } else {
                Duration::ZERO
            }
        })
    }
}

impl Default for TriggerRetryQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute exponential backoff: base * 2^(attempt-1), capped at MAX_BACKOFF.
fn compute_backoff(attempt: u32) -> Duration {
    let multiplier = 1u64 << (attempt.saturating_sub(1).min(20));
    let delay = BASE_BACKOFF.saturating_mul(multiplier as u32);
    delay.min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(trigger: &str) -> RetryEntry {
        RetryEntry {
            tenant_id: 1,
            collection: "orders".into(),
            row_id: "order-1".into(),
            operation: "INSERT".into(),
            trigger_name: trigger.into(),
            new_fields: None,
            old_fields: None,
            attempts: 0,
            last_error: "timeout".into(),
            next_retry_at: Instant::now(),
            source_lsn: 100,
            source_sequence: 1,
            cascade_depth: 0,
        }
    }

    #[test]
    fn backoff_increases_exponentially() {
        assert_eq!(compute_backoff(1), Duration::from_millis(100));
        assert_eq!(compute_backoff(2), Duration::from_millis(200));
        assert_eq!(compute_backoff(3), Duration::from_millis(400));
        assert_eq!(compute_backoff(4), Duration::from_millis(800));
        assert_eq!(compute_backoff(5), Duration::from_millis(1600));
    }

    #[test]
    fn backoff_capped() {
        let big = compute_backoff(30);
        assert!(big <= MAX_BACKOFF);
    }

    #[test]
    fn enqueue_and_drain() {
        let mut q = TriggerRetryQueue::new();
        let mut entry = make_entry("t1");
        entry.next_retry_at = Instant::now() - Duration::from_secs(1); // already due
        q.enqueue(entry);

        // Wait a tiny bit for backoff (100ms for attempt 1).
        std::thread::sleep(Duration::from_millis(110));

        let (ready, exhausted) = q.drain_due();
        assert_eq!(ready.len(), 1);
        assert!(exhausted.is_empty());
        assert_eq!(ready[0].attempts, 1);
    }

    #[test]
    fn exhausted_after_max_retries() {
        let mut q = TriggerRetryQueue::new();
        q.max_retries = 2;

        let mut entry = make_entry("t1");
        entry.attempts = 2; // Already at max.
        entry.next_retry_at = Instant::now() - Duration::from_secs(1);
        q.queue.push_back(entry);

        let (ready, exhausted) = q.drain_due();
        assert!(ready.is_empty());
        assert_eq!(exhausted.len(), 1);
    }
}
