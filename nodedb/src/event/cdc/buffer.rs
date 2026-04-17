//! Bounded per-stream event retention buffer.
//!
//! Each change stream has its own buffer that holds recent events for
//! consumer consumption. Oldest events are evicted when the buffer
//! exceeds its capacity (max_events) or age limit (max_age_secs).
//!
//! Events are stored as `Arc<CdcEvent>` so fan-out across matching streams
//! and repeated consumer polls (webhook, Kafka, SHOW, commit) share one
//! allocation per event — deep-cloning a `CdcEvent` (with its
//! `serde_json::Value` payload and field diffs) per subscriber or poll is
//! the hot-path cost the Arc layer exists to eliminate.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use super::event::CdcEvent;
use super::stream_def::RetentionConfig;

/// Per-stream bounded event retention buffer.
pub struct StreamBuffer {
    /// Stream name (for logging).
    name: String,
    /// Buffered events (oldest at front, newest at back).
    events: RwLock<VecDeque<Arc<CdcEvent>>>,
    /// Per-partition high-water-mark LSN observed since buffer creation.
    /// Survives retention eviction — the source of truth for
    /// `COMMIT OFFSETS` auto-commit, which would otherwise miss
    /// partitions whose events have aged out of the ring.
    partition_tails: RwLock<HashMap<u16, u64>>,
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
            partition_tails: RwLock::new(HashMap::new()),
            retention,
            total_pushed: std::sync::atomic::AtomicU64::new(0),
            total_evicted: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Push a new event into the buffer. Evicts oldest if at capacity.
    ///
    /// Accepts anything that can be turned into an `Arc<CdcEvent>`. The
    /// router hands the SAME `Arc` to every matching stream's buffer so
    /// fan-out across N subscribers is N refcount bumps, not N deep clones.
    pub fn push(&self, event: impl Into<Arc<CdcEvent>>) {
        let event = event.into();
        let mut events = self.events.write().unwrap_or_else(|p| {
            tracing::warn!(stream = %self.name, "StreamBuffer RwLock poisoned, recovering");
            p.into_inner()
        });

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

        // Update per-partition tail tracker *before* appending: this runs
        // regardless of whether the ring has space, so partitions whose
        // events get evicted still keep an advancing tail.
        {
            let mut tails = self
                .partition_tails
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let entry = tails.entry(event.partition).or_insert(0);
            if event.lsn > *entry {
                *entry = event.lsn;
            }
        }

        events.push_back(event);
        self.total_pushed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Latest observed LSN per partition, across the entire lifetime of
    /// the buffer — NOT bounded by retention. This is the correct source
    /// of truth for `COMMIT OFFSETS` auto-commit; scanning the retention
    /// ring loses partitions whose events have been evicted.
    pub fn partition_tails(&self) -> HashMap<u16, u64> {
        self.partition_tails
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Read events from a given LSN forward (for consumer polling).
    /// Returns shared `Arc<CdcEvent>` handles so repeated polls by webhook
    /// and Kafka producers share the underlying allocation.
    pub fn read_from_lsn(&self, from_lsn: u64, limit: usize) -> Vec<Arc<CdcEvent>> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| e.lsn > from_lsn)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Read events from a specific partition, starting after `from_lsn`.
    /// Partition = vShard ID. Scans the buffer and filters by partition.
    pub fn read_partition_from_lsn(
        &self,
        partition_id: u16,
        from_lsn: u64,
        limit: usize,
    ) -> Vec<Arc<CdcEvent>> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events
            .iter()
            .filter(|e| e.partition == partition_id && e.lsn > from_lsn)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Compact the buffer: deduplicate by key field, keeping only the latest
    /// event per key value. DELETE events are retained as tombstones until
    /// they exceed `tombstone_grace_secs` age, then removed.
    pub fn compact(&self, key_field: &str, tombstone_grace_secs: u64) -> u32 {
        let mut events = self.events.write().unwrap_or_else(|p| {
            tracing::warn!(stream = %self.name, "StreamBuffer RwLock poisoned during compact, recovering");
            p.into_inner()
        });
        let before = events.len();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let tombstone_cutoff_ms = now_ms.saturating_sub(tombstone_grace_secs * 1000);

        let mut latest: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (idx, event) in events.iter().enumerate() {
            let key_value = extract_key_value(event, key_field);
            latest.insert(key_value, idx);
        }

        let mut keep = vec![false; events.len()];
        for (idx, event) in events.iter().enumerate() {
            let key_value = extract_key_value(event, key_field);
            let is_latest = latest.get(&key_value) == Some(&idx);
            let is_tombstone = event.op == "DELETE";
            if is_latest && !(is_tombstone && event.event_time < tombstone_cutoff_ms) {
                keep[idx] = true;
            }
        }

        let mut new_events = VecDeque::with_capacity(events.len());
        for (idx, event) in events.drain(..).enumerate() {
            if keep[idx] {
                new_events.push_back(event);
            }
        }
        *events = new_events;

        let removed = (before - events.len()) as u32;
        if removed > 0 {
            self.total_evicted
                .fetch_add(removed as u64, std::sync::atomic::Ordering::Relaxed);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.events.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn earliest_lsn(&self) -> Option<u64> {
        let events = self.events.read().unwrap_or_else(|p| p.into_inner());
        events.front().map(|e| e.lsn)
    }

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

fn extract_key_value(event: &CdcEvent, key_field: &str) -> String {
    let value = event.new_value.as_ref().or(event.old_value.as_ref());

    if let Some(obj) = value.and_then(|v| v.as_object())
        && let Some(val) = obj.get(key_field)
    {
        return match val {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
    }

    tracing::warn!(
        collection = %event.collection,
        row_id = %event.row_id,
        key_field,
        "compaction key field not found in event, falling back to row_id"
    );
    event.row_id.clone()
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
            event_time: now_ms + seq * 1000,
            lsn,
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
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
        assert_eq!(events.len(), 3);
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
        assert_eq!(buf.earliest_lsn(), Some(30));
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

    fn make_event_on_partition(seq: u64, partition: u16, lsn: u64) -> CdcEvent {
        let mut e = make_event(seq, lsn);
        e.partition = partition;
        e
    }

    /// The `COMMIT OFFSETS` pgwire handler derives per-partition tail LSNs
    /// by scanning `read_from_lsn(0, usize::MAX)`. After buffer eviction
    /// removes a partition's events, that scan misses the partition
    /// entirely — no offset is committed for it, even though downstream
    /// consumers have already processed those events. The buffer (or a
    /// sibling tail tracker) must retain the latest LSN observed per
    /// partition across eviction so auto-commit can advance every
    /// partition, not just those whose events are still in the ring.
    #[test]
    fn partition_tails_survive_eviction() {
        let buf = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 2,
                max_age_secs: 3600,
            },
        );

        // Partition 0: two early events. These will be evicted.
        buf.push(make_event_on_partition(1, 0, 10));
        buf.push(make_event_on_partition(2, 0, 20));
        // Partition 1: two later events. Eviction kicks in on push 3,
        // pop_front drops partition 0's entries first.
        buf.push(make_event_on_partition(3, 1, 30));
        buf.push(make_event_on_partition(4, 1, 40));
        assert!(
            buf.total_evicted() >= 1,
            "expected eviction to have removed partition 0 events"
        );

        // `partition_tails()` is the source of truth for `COMMIT OFFSETS`.
        // It must retain each partition's high-water-mark LSN even when
        // the underlying event has been evicted from the retention ring.
        let tails = buf.partition_tails();
        assert_eq!(tails.get(&0).copied(), Some(20));
        assert_eq!(tails.get(&1).copied(), Some(40));
    }

    #[test]
    fn push_arc_shares_identity_with_read() {
        let buf = StreamBuffer::new("test".into(), RetentionConfig::default());
        let shared = Arc::new(make_event(1, 10));
        buf.push(Arc::clone(&shared));
        let read = buf.read_from_lsn(0, 10).into_iter().next().unwrap();
        assert!(Arc::ptr_eq(&shared, &read));
    }
}
