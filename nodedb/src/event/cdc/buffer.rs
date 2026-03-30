//! Bounded per-stream event retention buffer.
//!
//! Each change stream has its own buffer that holds recent events for
//! consumer consumption. Oldest events are evicted when the buffer
//! exceeds its capacity (max_events) or age limit (max_age_secs).

use std::collections::VecDeque;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use super::event::CdcEvent;
use super::stream_def::RetentionConfig;

/// Per-stream bounded event retention buffer.
pub struct StreamBuffer {
    /// Stream name (for logging).
    name: String,
    /// Buffered events (oldest at front, newest at back).
    events: RwLock<VecDeque<CdcEvent>>,
    /// Retention config.
    retention: RetentionConfig,
    /// Total events ever pushed (monotonic counter).
    total_pushed: std::sync::atomic::AtomicU64,
    /// Total events evicted due to overflow.
    total_evicted: std::sync::atomic::AtomicU64,
}

impl StreamBuffer {
    pub fn new(name: String, retention: RetentionConfig) -> Self {
        Self {
            name,
            events: RwLock::new(VecDeque::with_capacity(
                (retention.max_events as usize).min(65_536),
            )),
            retention,
            total_pushed: std::sync::atomic::AtomicU64::new(0),
            total_evicted: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Push a new event into the buffer. Evicts oldest if at capacity.
    pub fn push(&self, event: CdcEvent) {
        let mut events = self.events.write().unwrap_or_else(|p| p.into_inner());

        // Evict by count.
        while events.len() as u64 >= self.retention.max_events {
            events.pop_front();
            self.total_evicted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Evict by age.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff_ms = now_ms.saturating_sub(self.retention.max_age_secs * 1000);
        while events.front().is_some_and(|e| e.event_time < cutoff_ms) {
            events.pop_front();
            self.total_evicted
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        events.push_back(event);
        self.total_pushed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read events from a given LSN forward (for consumer polling).
    /// Returns events with LSN > `from_lsn`, up to `limit`.
    pub fn read_from_lsn(&self, from_lsn: u64, limit: usize) -> Vec<CdcEvent> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| e.lsn > from_lsn)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Current number of buffered events.
    pub fn len(&self) -> usize {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Earliest LSN in the buffer, or None if empty.
    pub fn earliest_lsn(&self) -> Option<u64> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events.front().map(|e| e.lsn)
    }

    /// Latest LSN in the buffer, or None if empty.
    pub fn latest_lsn(&self) -> Option<u64> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events.back().map(|e| e.lsn)
    }

    pub fn total_pushed(&self) -> u64 {
        self.total_pushed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total_evicted(&self) -> u64 {
        self.total_evicted
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(seq: u64, lsn: u64) -> CdcEvent {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        CdcEvent {
            sequence: seq,
            partition: 0,
            collection: "test".into(),
            op: "INSERT".into(),
            row_id: format!("row-{seq}"),
            event_time: now_ms + seq * 1000, // Future timestamps so they don't expire.
            lsn,
            tenant_id: 1,
            new_value: None,
            old_value: None,
        }
    }

    #[test]
    fn push_and_read() {
        let buf = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 100,
                max_age_secs: 3600,
            },
        );

        for i in 1..=5 {
            buf.push(make_event(i, i * 10));
        }

        assert_eq!(buf.len(), 5);
        assert_eq!(buf.earliest_lsn(), Some(10));
        assert_eq!(buf.latest_lsn(), Some(50));

        let events = buf.read_from_lsn(20, 10);
        assert_eq!(events.len(), 3); // LSN 30, 40, 50
        assert_eq!(events[0].lsn, 30);
    }

    #[test]
    fn evicts_at_capacity() {
        let buf = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 3,
                max_age_secs: 3600,
            },
        );

        for i in 1..=5 {
            buf.push(make_event(i, i * 10));
        }

        assert_eq!(buf.len(), 3);
        assert_eq!(buf.earliest_lsn(), Some(30)); // events 1 and 2 evicted
        assert_eq!(buf.total_evicted(), 2);
    }

    #[test]
    fn read_from_lsn_with_limit() {
        let buf = StreamBuffer::new("test".into(), RetentionConfig::default());

        for i in 1..=10 {
            buf.push(make_event(i, i * 10));
        }

        let events = buf.read_from_lsn(0, 3);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].lsn, 10);
        assert_eq!(events[2].lsn, 30);
    }
}
