//! CDC event router: matches WriteEvents to registered change streams.
//!
//! For each WriteEvent, finds all matching change streams (by tenant,
//! collection, and operation filter), formats the event as a CdcEvent,
//! and pushes it into the stream's retention buffer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::trace;

use super::buffer::StreamBuffer;
use super::event::CdcEvent;
use super::registry::StreamRegistry;
use super::stream_def::LateDataPolicy;
use crate::event::types::WriteEvent;
use crate::event::watermark_tracker::WatermarkTracker;

/// Manages per-stream buffers and routes events to matching streams.
pub struct CdcRouter {
    /// Stream registry (shared with DDL handlers).
    registry: Arc<StreamRegistry>,
    /// Per-stream retention buffers, keyed by `(tenant_id, stream_name)`.
    buffers: std::sync::RwLock<HashMap<(u32, String), Arc<StreamBuffer>>>,
}

impl CdcRouter {
    pub fn new(registry: Arc<StreamRegistry>) -> Self {
        Self {
            registry,
            buffers: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Route a WriteEvent to all matching change streams.
    ///
    /// Called from the Event Plane consumer for every event (after trigger dispatch).
    /// `watermark_tracker` is used to enforce late-data policies.
    pub fn route_event(&self, event: &WriteEvent, watermark_tracker: &WatermarkTracker) {
        let matching = self
            .registry
            .find_matching(event.tenant_id.as_u32(), &event.collection);

        if matching.is_empty() {
            return;
        }

        let op_str = event.op.to_string();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Deserialize row data once (shared across all matching streams).
        let new_value = event
            .new_value
            .as_ref()
            .and_then(|v| deserialize_to_json(v));
        let old_value = event
            .old_value
            .as_ref()
            .and_then(|v| deserialize_to_json(v));

        for def in &matching {
            if !def.op_filter.matches(&op_str) {
                continue;
            }

            // Late-data policy enforcement.
            let partition_wm = watermark_tracker.partition_watermark(event.vshard_id.as_u16());
            let is_late = event.lsn.as_u64() <= partition_wm && partition_wm > 0;

            if is_late {
                match def.late_data {
                    LateDataPolicy::Drop => {
                        trace!(
                            stream = %def.name,
                            lsn = event.lsn.as_u64(),
                            watermark = partition_wm,
                            "late event dropped by LATE_DATA = DROP policy"
                        );
                        continue;
                    }
                    LateDataPolicy::Allow => {
                        // Process normally — no special handling.
                    }
                    LateDataPolicy::Recompute => {
                        // Process the late event normally (it will update MV aggregates
                        // via the consumer's MV processing). Then emit a RECOMPUTE
                        // correction event so downstream consumers know a previously-
                        // emitted aggregate was updated.
                    }
                }
            }

            let cdc_event = CdcEvent {
                sequence: event.sequence,
                partition: event.vshard_id.as_u16(),
                collection: event.collection.to_string(),
                op: op_str.clone(),
                row_id: event.row_id.as_str().to_string(),
                event_time: now_ms,
                lsn: event.lsn.as_u64(),
                tenant_id: event.tenant_id.as_u32(),
                new_value: new_value.clone(),
                old_value: old_value.clone(),
                schema_version: 0,
            };

            let buffer = self.get_or_create_buffer(def.tenant_id, &def.name, &def.retention);
            buffer.push(cdc_event);

            // Recompute: emit a correction event after the original event.
            if is_late && def.late_data == LateDataPolicy::Recompute {
                let correction = CdcEvent {
                    sequence: event.sequence,
                    partition: event.vshard_id.as_u16(),
                    collection: event.collection.to_string(),
                    op: "RECOMPUTE".to_string(),
                    row_id: event.row_id.as_str().to_string(),
                    event_time: now_ms,
                    lsn: event.lsn.as_u64(),
                    tenant_id: event.tenant_id.as_u32(),
                    new_value: new_value.clone(),
                    old_value: None,
                    schema_version: 0,
                };
                buffer.push(correction);
                trace!(
                    stream = %def.name,
                    lsn = event.lsn.as_u64(),
                    "RECOMPUTE correction emitted for late event"
                );
            }

            trace!(
                stream = %def.name,
                collection = %event.collection,
                op = %op_str,
                lsn = event.lsn.as_u64(),
                "CDC event routed"
            );
        }
    }

    /// Get or create a buffer for a stream.
    fn get_or_create_buffer(
        &self,
        tenant_id: u32,
        stream_name: &str,
        retention: &super::stream_def::RetentionConfig,
    ) -> Arc<StreamBuffer> {
        let key = (tenant_id, stream_name.to_string());

        // Fast path: read lock.
        {
            let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
            if let Some(buf) = buffers.get(&key) {
                return Arc::clone(buf);
            }
        }

        // Slow path: write lock + create.
        let mut buffers = self.buffers.write().unwrap_or_else(|p| p.into_inner());
        buffers
            .entry(key)
            .or_insert_with(|| {
                Arc::new(StreamBuffer::new(
                    stream_name.to_string(),
                    retention.clone(),
                ))
            })
            .clone()
    }

    /// Ensure a buffer exists for a given key (stream or topic). Creates if missing.
    pub fn ensure_buffer(
        &self,
        tenant_id: u32,
        name: &str,
        retention: &super::stream_def::RetentionConfig,
    ) -> Arc<StreamBuffer> {
        self.get_or_create_buffer(tenant_id, name, retention)
    }

    /// Get a buffer for a stream (if it exists). Used by consumers to poll events.
    pub fn get_buffer(&self, tenant_id: u32, stream_name: &str) -> Option<Arc<StreamBuffer>> {
        let key = (tenant_id, stream_name.to_string());
        let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
        buffers.get(&key).cloned()
    }

    /// Remove a buffer when a stream is dropped.
    pub fn remove_buffer(&self, tenant_id: u32, stream_name: &str) {
        let key = (tenant_id, stream_name.to_string());
        let mut buffers = self.buffers.write().unwrap_or_else(|p| p.into_inner());
        buffers.remove(&key);
    }

    /// Snapshot of all buffer stats (for SHOW CHANGE STREAMS).
    pub fn buffer_stats(&self) -> Vec<BufferStats> {
        let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
        buffers
            .iter()
            .map(|((tid, name), buf)| BufferStats {
                tenant_id: *tid,
                stream_name: name.clone(),
                buffered_events: buf.len(),
                total_pushed: buf.total_pushed(),
                total_evicted: buf.total_evicted(),
                earliest_lsn: buf.earliest_lsn(),
                latest_lsn: buf.latest_lsn(),
            })
            .collect()
    }
}

/// Buffer statistics for observability.
pub struct BufferStats {
    pub tenant_id: u32,
    pub stream_name: String,
    pub buffered_events: usize,
    pub total_pushed: u64,
    pub total_evicted: u64,
    pub earliest_lsn: Option<u64>,
    pub latest_lsn: Option<u64>,
}

/// Deserialize bytes (MessagePack or JSON) to serde_json::Value.
fn deserialize_to_json(bytes: &[u8]) -> Option<serde_json::Value> {
    rmp_serde::from_slice::<serde_json::Value>(bytes)
        .ok()
        .or_else(|| serde_json::from_slice::<serde_json::Value>(bytes).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::stream_def::*;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::event::watermark_tracker::WatermarkTracker;
    use crate::types::{Lsn, TenantId, VShardId};

    fn test_tracker() -> WatermarkTracker {
        WatermarkTracker::new()
    }

    fn make_write_event(collection: &str, seq: u64) -> WriteEvent {
        WriteEvent {
            sequence: seq,
            collection: Arc::from(collection),
            op: WriteOp::Insert,
            row_id: RowId::new(format!("row-{seq}")),
            lsn: Lsn::new(seq * 10),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(
                serde_json::to_vec(&serde_json::json!({"id": seq}))
                    .unwrap()
                    .as_slice(),
            )),
            old_value: None,
        }
    }

    fn sample_def(name: &str, collection: &str) -> ChangeStreamDef {
        ChangeStreamDef {
            tenant_id: 1,
            name: name.into(),
            collection: collection.into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig {
                max_events: 1000,
                max_age_secs: 3600,
            },
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn routes_to_matching_stream() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);

        let buf = router.get_buffer(1, "orders_stream").unwrap();
        assert_eq!(buf.len(), 1);
        let events = buf.read_from_lsn(0, 10);
        assert_eq!(events[0].collection, "orders");
    }

    #[test]
    fn skips_unmatched_collection() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("users", 1), &wt);

        assert!(router.get_buffer(1, "orders_stream").is_none());
    }

    #[test]
    fn wildcard_catches_all() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("all_changes", "*"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);
        router.route_event(&make_write_event("users", 2), &wt);

        let buf = router.get_buffer(1, "all_changes").unwrap();
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn op_filter_skips_non_matching() {
        let registry = Arc::new(StreamRegistry::new());
        let mut def = sample_def("inserts_only", "orders");
        def.op_filter = OpFilter {
            insert: true,
            update: false,
            delete: false,
        };
        registry.register(def);
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        // Insert matches.
        router.route_event(&make_write_event("orders", 1), &wt);

        // DELETE does not match.
        let delete_event = WriteEvent {
            op: WriteOp::Delete,
            ..make_write_event("orders", 2)
        };
        router.route_event(&delete_event, &wt);

        let buf = router.get_buffer(1, "inserts_only").unwrap();
        assert_eq!(buf.len(), 1); // Only the insert.
    }

    #[test]
    fn remove_buffer_on_drop() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("s1", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);
        assert!(router.get_buffer(1, "s1").is_some());

        router.remove_buffer(1, "s1");
        assert!(router.get_buffer(1, "s1").is_none());
    }
}
