// SPDX-License-Identifier: BUSL-1.1

//! Bounded per-stream event retention buffer.
//!
//! Events are shared `Arc<CdcEvent>` values so fan-out and repeated polls do
//! not deep-clone JSON payloads. Consumer cursors are [`CdcOffset`] values,
//! not bare LSNs: sibling events at one LSN remain independently consumable.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::event::CdcEvent;
use super::offset::CdcOffset;
use super::stream_def::RetentionConfig;

/// Per-stream bounded event retention buffer.
pub struct StreamBuffer {
    name: String,
    /// Oldest at front, newest at back. Events are appended in source order.
    events: RwLock<VecDeque<Arc<CdcEvent>>>,
    /// Per-partition high-water-mark position, retained after eviction.
    partition_tails: RwLock<HashMap<u32, CdcOffset>>,
    retention: RetentionConfig,
    total_pushed: std::sync::atomic::AtomicU64,
    total_evicted: std::sync::atomic::AtomicU64,
    /// Set when the retained events changed since the CDC ledger last
    /// persisted them: an event was inserted, evicted or compacted away.
    changed: std::sync::atomic::AtomicBool,
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
            changed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Push an event, preserving its supplied composite position.
    ///
    /// Exact `(partition, position)` duplicates are ignored. This makes
    /// durable-topic startup hydration race-safe: a just-committed publication
    /// and its concurrent hydration replay cannot appear twice.
    pub fn push(&self, event: impl Into<Arc<CdcEvent>>) -> u64 {
        let event = event.into();
        let mut events = self.events.write().unwrap_or_else(|poisoned| {
            tracing::warn!(stream = %self.name, "StreamBuffer RwLock poisoned, recovering");
            poisoned.into_inner()
        });
        match self.push_locked(&mut events, event) {
            Some(evicted) => {
                self.total_pushed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                evicted
            }
            None => 0,
        }
    }

    /// Insert an event in composite-position order, returning `None` if that
    /// exact partition position already exists.
    fn push_locked(
        &self,
        events: &mut VecDeque<Arc<CdcEvent>>,
        event: Arc<CdcEvent>,
    ) -> Option<u64> {
        let position = event.position();
        if events
            .iter()
            .any(|current| current.partition == event.partition && current.position() == position)
        {
            return None;
        }

        let mut tails = self
            .partition_tails
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let tail = tails.entry(event.partition).or_insert(CdcOffset::ZERO);
        if position > *tail {
            *tail = position;
        }
        drop(tails);

        let insertion_index = events
            .iter()
            .position(|current| current.position() > position)
            .unwrap_or(events.len());
        events.insert(insertion_index, event);
        self.changed
            .store(true, std::sync::atomic::Ordering::Release);

        // Evict after ordered insertion. Hydrating an older committed event
        // must not displace a newer one merely because it arrived later.
        let mut evicted_this_push = 0;
        while events.len() as u64 > self.retention.max_events {
            events.pop_front();
            evicted_this_push += 1;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff_ms = now_ms.saturating_sub(self.retention.max_age_secs * 1000);
        while events
            .front()
            .is_some_and(|current| current.event_time < cutoff_ms)
        {
            events.pop_front();
            evicted_this_push += 1;
        }
        if evicted_this_push > 0 {
            self.total_evicted
                .fetch_add(evicted_this_push, std::sync::atomic::Ordering::Relaxed);
        }
        Some(evicted_this_push)
    }

    /// Latest observed composite position per partition, including evicted
    /// events. This is the source of truth for `COMMIT OFFSETS`.
    pub fn partition_tails(&self) -> HashMap<u32, CdcOffset> {
        self.partition_tails
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Read events strictly after one composite position.
    pub fn read_from(&self, from: CdcOffset, limit: usize) -> Vec<Arc<CdcEvent>> {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events
            .iter()
            .filter(|event| event.position() > from)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Read a partition strictly after its committed composite position.
    pub fn read_partition_from(
        &self,
        partition_id: u32,
        from: CdcOffset,
        limit: usize,
    ) -> Vec<Arc<CdcEvent>> {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events
            .iter()
            .filter(|event| event.partition == partition_id && event.position() > from)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Read across partitions, applying each partition's committed position
    /// independently. A global minimum cursor would redeliver already-acked
    /// events from other partitions and can starve a sibling behind `LIMIT`.
    pub fn read_after_partition_offsets(
        &self,
        offsets: &HashMap<u32, CdcOffset>,
        limit: usize,
    ) -> Vec<Arc<CdcEvent>> {
        let events = self
            .events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events
            .iter()
            .filter(|event| {
                event.position()
                    > offsets
                        .get(&event.partition)
                        .copied()
                        .unwrap_or(CdcOffset::ZERO)
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Compact by key field, retaining the latest event for every key.
    pub fn compact(&self, key_field: &str, tombstone_grace_secs: u64) -> u32 {
        let mut events = self.events.write().unwrap_or_else(|poisoned| {
            tracing::warn!(stream = %self.name, "StreamBuffer RwLock poisoned during compact, recovering");
            poisoned.into_inner()
        });
        let before = events.len();
        let cutoff_ms = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64)
            .saturating_sub(tombstone_grace_secs * 1000);
        let mut latest: HashMap<String, (usize, CdcOffset)> = HashMap::new();
        for (index, event) in events.iter().enumerate() {
            let position = event.position();
            latest
                .entry(extract_key_value(event, key_field))
                .and_modify(|current| {
                    if position > current.1 {
                        *current = (index, position);
                    }
                })
                .or_insert((index, position));
        }
        let mut kept = VecDeque::with_capacity(events.len());
        for (index, event) in events.drain(..).enumerate() {
            let key = extract_key_value(&event, key_field);
            let is_latest = latest
                .get(&key)
                .is_some_and(|(latest_index, _)| *latest_index == index);
            if is_latest && !(event.op == "DELETE" && event.event_time < cutoff_ms) {
                kept.push_back(event);
            }
        }
        *events = kept;
        let removed = (before - events.len()) as u32;
        if removed > 0 {
            self.changed
                .store(true, std::sync::atomic::Ordering::Release);
            self.total_evicted
                .fetch_add(removed as u64, std::sync::atomic::Ordering::Relaxed);
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn earliest_offset(&self) -> Option<CdcOffset> {
        self.events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|event| event.position())
            .min()
    }

    pub fn latest_offset(&self) -> Option<CdcOffset> {
        self.events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|event| event.position())
            .max()
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

    /// Clear the changed flag, and return whether it was set. The CDC ledger
    /// calls this before it reads [`Self::snapshot`], so a change made after
    /// the read sets the flag again.
    pub fn take_changed(&self) -> bool {
        self.changed
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Mark the retained events changed again, after a flush that took the
    /// flag did not persist them.
    pub fn mark_changed(&self) {
        self.changed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Every retained event, oldest first.
    pub fn snapshot(&self) -> Vec<Arc<CdcEvent>> {
        self.events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

fn extract_key_value(event: &CdcEvent, key_field: &str) -> String {
    if let Some(object) = event
        .new_value
        .as_ref()
        .or(event.old_value.as_ref())
        .and_then(serde_json::Value::as_object)
        && let Some(value) = object.get(key_field)
    {
        return match value {
            serde_json::Value::String(value) => value.clone(),
            value => value.to_string(),
        };
    }
    tracing::warn!(collection = %event.collection, row_id = %event.row_id, key_field, "compaction key field not found in event, falling back to row_id");
    event.row_id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: u64, lsn: u64) -> CdcEvent {
        CdcEvent {
            sequence,
            partition: 0,
            collection: "test".into(),
            op: "INSERT".into(),
            row_id: format!("row-{sequence}"),
            event_time: u64::MAX,
            lsn,
            database_id: crate::types::DatabaseId::new(7),
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: crate::event::EventSource::User,
        }
    }

    #[test]
    fn transaction_siblings_at_one_lsn_remain_independently_readable() {
        let buffer = StreamBuffer::new("test".into(), RetentionConfig::default());
        buffer.push(event(1, 10));
        buffer.push(event(2, 10));
        assert_eq!(buffer.read_from(CdcOffset::new(10, 1), 1)[0].sequence, 2);
        assert!(buffer.read_from(CdcOffset::new(10, 2), 1).is_empty());
    }

    #[test]
    fn partition_tails_keep_the_exact_position_after_eviction() {
        let buffer = StreamBuffer::new(
            "test".into(),
            RetentionConfig {
                max_events: 1,
                max_age_secs: u64::MAX / 1000,
            },
        );
        buffer.push(event(1, 10));
        buffer.push(event(2, 10));
        assert_eq!(buffer.partition_tails()[&0], CdcOffset::new(10, 2));
    }

    #[test]
    fn positions_are_ordered_and_exact_positions_are_deduplicated() {
        let buffer = StreamBuffer::new("topic:test".into(), RetentionConfig::default());
        buffer.push(event(3, 30));
        buffer.push(event(1, 10));
        buffer.push(event(2, 20));
        buffer.push(event(2, 20));

        let events = buffer.read_from(CdcOffset::ZERO, 16);
        assert_eq!(events.len(), 3);
        assert_eq!(
            events
                .iter()
                .map(|event| event.position())
                .collect::<Vec<_>>(),
            vec![
                CdcOffset::new(10, 1),
                CdcOffset::new(20, 2),
                CdcOffset::new(30, 3)
            ]
        );
        assert_eq!(buffer.total_pushed(), 3);
    }

    #[test]
    fn partition_reads_share_event_arcs() {
        let buffer = StreamBuffer::new("test".into(), RetentionConfig::default());
        let shared = Arc::new(event(1, 10));
        buffer.push(Arc::clone(&shared));
        let read = buffer
            .read_partition_from(0, CdcOffset::ZERO, 1)
            .pop()
            .unwrap();
        assert!(Arc::ptr_eq(&shared, &read));
    }
}
