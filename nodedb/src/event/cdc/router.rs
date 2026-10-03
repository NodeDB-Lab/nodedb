// SPDX-License-Identifier: BUSL-1.1

//! CDC event router: matches WriteEvents to registered change streams.
//!
//! For each WriteEvent, finds all matching change streams (by tenant,
//! collection, and operation filter), formats the event as a CdcEvent,
//! and pushes it into the stream's retention buffer.
//!
//! Every replica of a data group applies each committed entry and routes its
//! events. The router positions an event by the entry's Raft log index, not
//! by the local WAL LSN, so every replica buffers the event at the same
//! position and a consumer cursor is valid on every node.

use std::collections::HashMap;
use std::sync::Arc;

use sonic_rs;
use tracing::trace;

use super::buffer::StreamBuffer;
use super::event::CdcEvent;
use super::lag_warner::{CdcLagWarner, DEFAULT_THRESHOLD};
use super::offset::CdcOffset;
use super::position::{
    AvailabilityFloors, ChangePositionLedger, IndexSource, PartitionTail, PositionSequencer,
    WritePosition, calvin_partition,
};
use super::registry::StreamRegistry;
use super::stream_def::{ChangeStreamDef, LateDataPolicy};
use crate::control::metrics::system::SystemMetrics;
use crate::event::types::WriteEvent;
use crate::event::watermark_tracker::WatermarkTracker;
use crate::types::DatabaseId;

const NANOS_PER_MS: u64 = 1_000_000;

/// A buffer's identity: `(database_id, tenant_id, stream_name)`.
pub type BufferKey = (DatabaseId, u64, String);

/// Manages per-stream buffers and routes events to matching streams.
pub struct CdcRouter {
    /// Stream registry (shared with DDL handlers).
    registry: Arc<StreamRegistry>,
    /// Per-stream retention buffers, keyed by `(database_id, tenant_id, stream_name)`.
    buffers: std::sync::RwLock<HashMap<BufferKey, Arc<StreamBuffer>>>,
    /// Buffers removed since the CDC ledger last persisted: their persisted
    /// events are deleted by the next flush.
    removed: std::sync::Mutex<Vec<BufferKey>>,
    /// Per-stream drop rate tracker — emits `warn!` when threshold is crossed.
    lag_warner: CdcLagWarner,
    /// System metrics for per-stream Prometheus counters. `None` in unit tests
    /// that construct a router without a full metrics registry.
    metrics: Option<Arc<SystemMetrics>>,
    /// Replicated log position of each locally applied WAL record.
    positions: ChangePositionLedger,
    /// Per-partition position allocation.
    sequencer: PositionSequencer,
    /// Per-partition first position this node serves.
    availability: AvailabilityFloors,
}

impl CdcRouter {
    pub fn new(registry: Arc<StreamRegistry>) -> Self {
        Self {
            registry,
            buffers: std::sync::RwLock::new(HashMap::new()),
            removed: std::sync::Mutex::new(Vec::new()),
            lag_warner: CdcLagWarner::new(DEFAULT_THRESHOLD),
            metrics: None,
            positions: ChangePositionLedger::default(),
            sequencer: PositionSequencer::new(),
            availability: AvailabilityFloors::new(),
        }
    }

    /// Attach system metrics so the router can update
    /// `nodedb_cdc_events_dropped_total{tenant, stream}` on each eviction.
    pub fn with_metrics(mut self, metrics: Arc<SystemMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// The replicated log positions of locally applied WAL records. The write
    /// funnel records into it, and boot recovers it from the WAL.
    pub fn positions(&self) -> &ChangePositionLedger {
        &self.positions
    }

    /// The first position this node serves per partition. A snapshot install
    /// raises it, and the CDC ledger persists it.
    pub fn availability(&self) -> &AvailabilityFloors {
        &self.availability
    }

    /// Route a WriteEvent to all matching change streams.
    ///
    /// Called from the Event Plane consumer for every event (after trigger dispatch).
    /// `watermark_tracker` is used to enforce late-data policies.
    pub fn route_event(&self, event: &WriteEvent, watermark_tracker: &WatermarkTracker) {
        // Heartbeats advance Event Plane watermarks only; a synthetic heartbeat
        // has no database owner and must never enter a database-scoped stream.
        if !event.op.is_data_event() {
            return;
        }
        let op_str = event.op.to_string();
        let matching: Vec<Arc<ChangeStreamDef>> = self
            .registry
            .find_matching(
                event.database_id,
                event.tenant_id.as_u64(),
                &event.collection,
            )
            .into_iter()
            .filter(|def| def.op_filter.matches(&op_str))
            .collect();
        if matching.is_empty() {
            return;
        }

        let record_lsn = event
            .record
            .map_or(event.lsn.as_u64(), |record| record.lsn.as_u64());
        // The write's commit HLC dates the event, so every replica gives it
        // the same time. A live write carries it from its request. An event
        // rebuilt from a WAL record takes the record's.
        let Some(commit_hlc) = event
            .commit_hlc
            .or_else(|| self.positions.commit_hlc(record_lsn))
        else {
            tracing::error!(
                collection = %event.collection,
                record_lsn,
                op = %event.op,
                "CDC: a write event carries no commit HLC, and no WAL record dates it; \
                 its write path must stamp one. The event is not routed"
            );
            return;
        };
        let event_time = commit_hlc / NANOS_PER_MS;
        let (partition, source) = self.index_source(event.vshard_id.as_u32(), record_lsn);
        let position = self.sequencer.next(partition, source, record_lsn, || {
            self.buffered_tail(partition)
        });

        // Deserialize row data once (shared across all matching streams).
        let new_value = event
            .new_value
            .as_ref()
            .and_then(|v| deserialize_to_json(v));
        let old_value = event
            .old_value
            .as_ref()
            .and_then(|v| deserialize_to_json(v));

        // Compute the UPDATE field diffs once per WriteEvent (shared across fan-out).
        let field_diffs = if op_str == "UPDATE" {
            match (&old_value, &new_value) {
                (Some(old), Some(new)) => {
                    let diffs = crate::event::field_diff::compute_field_diffs(old, new);
                    if diffs.is_empty() { None } else { Some(diffs) }
                }
                _ => None,
            }
        } else {
            None
        };

        // Build the CdcEvent once and share the same Arc across every matching
        // stream. Fan-out to N streams becomes N refcount bumps, not N deep
        // copies of the payload + diffs.
        let cdc_event = Arc::new(CdcEvent {
            sequence: position.sequence,
            partition,
            collection: event.collection.to_string(),
            op: op_str.clone(),
            row_id: event.row_id.as_str().to_string(),
            event_time,
            lsn: record_lsn,
            index: position.index,
            epoch: position.epoch,
            tenant_id: event.tenant_id.as_u64(),
            database_id: event.database_id,
            new_value,
            old_value,
            schema_version: 0,
            field_diffs,
            system_time_ms: event.system_time_ms,
            valid_time_ms: event.valid_time_ms,
            source: event.source,
        });

        // RECOMPUTE correction (only built if any stream actually needs it).
        let mut recompute: Option<Arc<CdcEvent>> = None;

        for def in &matching {
            // Events of one write share its LSN, so only a strictly lower LSN
            // is late.
            let partition_wm = watermark_tracker.partition_watermark(partition);
            let is_late = event.lsn.as_u64() < partition_wm;

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
                    LateDataPolicy::Allow => {}
                    LateDataPolicy::Recompute => {}
                }
            }

            let buffer = self.get_or_create_buffer(
                def.database_id,
                def.tenant_id,
                &def.name,
                &def.retention,
            );
            self.push_counted(def, &buffer, Arc::clone(&cdc_event));

            if is_late && def.late_data == LateDataPolicy::Recompute {
                let correction = recompute
                    .get_or_insert_with(|| {
                        Arc::new(CdcEvent {
                            sequence: position.correction().sequence,
                            op: "RECOMPUTE".to_string(),
                            old_value: None,
                            field_diffs: None,
                            ..(*cdc_event).clone()
                        })
                    })
                    .clone();
                self.push_counted(def, &buffer, correction);
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
                offset = %position,
                "CDC event routed"
            );
        }
    }

    /// The partition and position source of a write of `vshard` at local
    /// record `record_lsn`: the replicated position the record applies, when
    /// the ledger knows it. A Calvin transaction goes to the vShard's Calvin
    /// partition. The trigger action lane positions its events by it too.
    pub(crate) fn index_source(&self, vshard: u32, record_lsn: u64) -> (u32, IndexSource) {
        match self.positions.get(record_lsn) {
            Some(WritePosition::Raft(position)) => (
                vshard,
                IndexSource::Replicated {
                    epoch: position.epoch,
                    index: position.log_index,
                    base: 0,
                },
            ),
            Some(WritePosition::Calvin(position)) => (
                calvin_partition(vshard),
                IndexSource::Replicated {
                    epoch: 0,
                    index: position.sequencer_epoch,
                    base: u64::from(position.position),
                },
            ),
            None if self.positions.is_replicated() => (vshard, IndexSource::Unmapped),
            None => (vshard, IndexSource::Local(record_lsn)),
        }
    }

    /// The highest-positioned event any change-stream buffer holds for
    /// `partition`.
    fn buffered_tail(&self, partition: u32) -> Option<PartitionTail> {
        self.stream_buffers()
            .iter()
            .filter_map(|(_, buffer)| buffer.partition_tail_event(partition))
            .max_by_key(|event| event.position())
            .map(|event| PartitionTail {
                position: event.position(),
                record_lsn: event.lsn,
            })
    }

    /// Push one event, and count and report what the push evicted.
    fn push_counted(&self, def: &ChangeStreamDef, buffer: &StreamBuffer, event: Arc<CdcEvent>) {
        let evictions = buffer.push(event);
        if evictions == 0 {
            return;
        }
        let oldest_index = buffer.earliest_offset().map_or(0, |offset| offset.index);
        self.lag_warner
            .record_drops(def.tenant_id, &def.name, evictions, oldest_index);
        if let Some(m) = &self.metrics {
            m.record_cdc_stream_drop(def.tenant_id, &def.name, evictions);
        }
    }

    /// Get or create a buffer for a stream.
    fn get_or_create_buffer(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
        retention: &super::stream_def::RetentionConfig,
    ) -> Arc<StreamBuffer> {
        let key = (database_id, tenant_id, stream_name.to_string());

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
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
        retention: &super::stream_def::RetentionConfig,
    ) -> Arc<StreamBuffer> {
        self.get_or_create_buffer(database_id, tenant_id, name, retention)
    }

    /// Get a buffer for a stream (if it exists). Used by consumers to poll events.
    pub fn get_buffer(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
    ) -> Option<Arc<StreamBuffer>> {
        let key = (database_id, tenant_id, stream_name.to_string());
        let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
        buffers.get(&key).cloned()
    }

    /// Remove a buffer when a stream is dropped.
    pub fn remove_buffer(&self, database_id: DatabaseId, tenant_id: u64, stream_name: &str) {
        let key = (database_id, tenant_id, stream_name.to_string());
        let mut buffers = self.buffers.write().unwrap_or_else(|p| p.into_inner());
        if buffers.remove(&key).is_some() {
            self.removed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(key);
        }
        self.lag_warner.remove_stream(tenant_id, stream_name);
    }

    /// The change streams this router routes to.
    pub fn registry(&self) -> &StreamRegistry {
        &self.registry
    }

    /// The buffer of every registered change stream. Topic buffers are left
    /// out: a durable topic hydrates its own buffer.
    pub fn stream_buffers(&self) -> Vec<(BufferKey, Arc<StreamBuffer>)> {
        let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
        buffers
            .iter()
            .filter(|((database_id, tenant_id, name), _)| {
                self.registry.get(*database_id, *tenant_id, name).is_some()
            })
            .map(|(key, buffer)| (key.clone(), Arc::clone(buffer)))
            .collect()
    }

    /// Take the keys of the buffers removed since the last call.
    pub fn take_removed(&self) -> Vec<BufferKey> {
        std::mem::take(&mut *self.removed.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// Return removed buffer keys a flush took but did not persist.
    pub fn requeue_removed(&self, keys: Vec<BufferKey>) {
        self.removed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(keys);
    }

    /// Snapshot of all buffer stats (for SHOW CHANGE STREAMS).
    pub fn buffer_stats(&self) -> Vec<BufferStats> {
        let buffers = self.buffers.read().unwrap_or_else(|p| p.into_inner());
        buffers
            .iter()
            .map(|((database_id, tid, name), buf)| BufferStats {
                database_id: *database_id,
                tenant_id: *tid,
                stream_name: name.clone(),
                buffered_events: buf.len(),
                total_pushed: buf.total_pushed(),
                total_evicted: buf.total_evicted(),
                earliest_offset: buf.earliest_offset(),
                latest_offset: buf.latest_offset(),
            })
            .collect()
    }
}

/// Buffer statistics for observability.
pub struct BufferStats {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    pub stream_name: String,
    pub buffered_events: usize,
    pub total_pushed: u64,
    pub total_evicted: u64,
    pub earliest_offset: Option<CdcOffset>,
    pub latest_offset: Option<CdcOffset>,
}

/// Deserialize bytes (MessagePack or JSON) to serde_json::Value.
fn deserialize_to_json(bytes: &[u8]) -> Option<serde_json::Value> {
    nodedb_types::json_from_msgpack(bytes)
        .ok()
        .or_else(|| sonic_rs::from_slice::<serde_json::Value>(bytes).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::position::ReplicatedPosition;
    use crate::event::cdc::stream_def::*;
    use crate::event::types::{EventSource, RecordPosition, RowId, WriteOp};
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
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key(format!(
                "row-{seq}"
            ))),
            lsn: Lsn::new(seq * 10),
            record: None,
            database_id: DatabaseId::new(7),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(
                serde_json::to_vec(&serde_json::json!({"id": seq}))
                    .unwrap()
                    .as_slice(),
            )),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: Some(crate::event::test_utils::test_commit_hlc()),
            image_fault: None,
        }
    }

    fn sample_def(name: &str, collection: &str) -> ChangeStreamDef {
        ChangeStreamDef {
            database_id: DatabaseId::new(7),
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
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
            subscriber_roles: Vec::new(),
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn routes_to_matching_stream() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);

        let buf = router
            .get_buffer(DatabaseId::new(7), 1, "orders_stream")
            .unwrap();
        assert_eq!(buf.len(), 1);
        let events = buf.read_from(CdcOffset::ZERO, 10);
        assert_eq!(events[0].collection, "orders");
    }

    #[test]
    fn an_event_dates_by_the_commit_hlc_its_write_carried() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);
        let event = make_write_event("orders", 1);
        let carried = event
            .commit_hlc
            .expect("the test event carries a commit HLC");
        // The watermark LSN the event names belongs to another record, which
        // committed at another instant.
        router
            .positions()
            .record_commit_hlc(event.lsn.as_u64(), carried - 5_000_000_000);

        router.route_event(&event, &test_tracker());

        let buf = router
            .get_buffer(DatabaseId::new(7), 1, "orders_stream")
            .unwrap();
        let events = buf.read_from(CdcOffset::ZERO, 10);
        assert_eq!(events[0].event_time, carried / NANOS_PER_MS);
    }

    #[test]
    fn an_event_nothing_dates_is_not_routed() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);
        let undated = WriteEvent {
            commit_hlc: None,
            ..make_write_event("orders", 1)
        };

        router.route_event(&undated, &test_tracker());

        assert!(
            router
                .get_buffer(DatabaseId::new(7), 1, "orders_stream")
                .is_none_or(|buf| buf.is_empty())
        );
    }

    #[test]
    fn skips_unmatched_collection() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("users", 1), &wt);

        assert!(
            router
                .get_buffer(DatabaseId::new(7), 1, "orders_stream")
                .is_none()
        );
    }

    #[test]
    fn wildcard_catches_all() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("all_changes", "*"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);
        router.route_event(&make_write_event("users", 2), &wt);

        let buf = router
            .get_buffer(DatabaseId::new(7), 1, "all_changes")
            .unwrap();
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

        let buf = router
            .get_buffer(DatabaseId::new(7), 1, "inserts_only")
            .unwrap();
        assert_eq!(buf.len(), 1); // Only the insert.
    }

    #[test]
    fn heartbeat_never_enters_database_scoped_stream() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("orders_stream", "_heartbeat"));
        let router = CdcRouter::new(registry);
        let heartbeat = WriteEvent {
            op: WriteOp::Heartbeat,
            ..make_write_event("_heartbeat", 1)
        };

        router.route_event(&heartbeat, &test_tracker());
        assert!(
            router
                .get_buffer(DatabaseId::new(7), 1, "orders_stream")
                .is_none()
        );
    }

    #[test]
    fn remove_buffer_on_drop() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("s1", "orders"));
        let router = CdcRouter::new(registry);

        let wt = test_tracker();
        router.route_event(&make_write_event("orders", 1), &wt);
        assert!(router.get_buffer(DatabaseId::new(7), 1, "s1").is_some());

        router.remove_buffer(DatabaseId::new(7), 1, "s1");
        assert!(router.get_buffer(DatabaseId::new(7), 1, "s1").is_none());
    }

    fn positions(router: &CdcRouter, stream: &str) -> Vec<CdcOffset> {
        router
            .get_buffer(DatabaseId::new(7), 1, stream)
            .map(|buffer| {
                buffer
                    .read_from(CdcOffset::ZERO, 16)
                    .iter()
                    .map(|event| event.position())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// One write whose record sits at `local_lsn` on this node.
    fn write_at(local_lsn: u64, row: u64) -> WriteEvent {
        WriteEvent {
            lsn: Lsn::new(local_lsn),
            record: Some(RecordPosition::first(Lsn::new(local_lsn))),
            ..make_write_event("orders", row)
        }
    }

    #[test]
    fn replicas_position_one_entry_alike_whatever_their_local_lsns() {
        let replica = || {
            let registry = Arc::new(StreamRegistry::new());
            registry.register(sample_def("s", "orders"));
            CdcRouter::new(registry)
        };
        let leader = replica();
        let follower = replica();
        let wt = test_tracker();
        // Raft entry 55 lands at a different local WAL LSN on each replica.
        for (router, local_lsn) in [(&leader, 100), (&follower, 7_000)] {
            router.positions().record(
                local_lsn,
                ReplicatedPosition {
                    epoch: 0,
                    group_id: 2,
                    log_index: 55,
                },
            );
            router.route_event(&write_at(local_lsn, 1), &wt);
            router.route_event(&write_at(local_lsn, 2), &wt);
        }
        assert_eq!(
            positions(&leader, "s"),
            vec![
                CdcOffset::data_event(0, 55, 1),
                CdcOffset::data_event(0, 55, 2)
            ]
        );
        assert_eq!(positions(&leader, "s"), positions(&follower, "s"));
    }

    #[test]
    fn replicas_number_a_calvin_transaction_alike_in_its_own_partition() {
        use crate::event::cdc::position::{CalvinPosition, calvin_partition};
        let replica = || {
            let registry = Arc::new(StreamRegistry::new());
            registry.register(sample_def("s", "orders"));
            CdcRouter::new(registry)
        };
        let leader = replica();
        let follower = replica();
        let wt = test_tracker();
        let txn = CalvinPosition {
            sequencer_epoch: 31,
            position: 2,
        };
        for (router, local_lsn) in [(&leader, 300), (&follower, 8_000)] {
            router.positions().record_calvin(local_lsn, txn);
            router.route_event(&write_at(local_lsn, 1), &wt);
            router.route_event(&write_at(local_lsn, 2), &wt);
        }
        let seen = |router: &CdcRouter| {
            router
                .get_buffer(DatabaseId::new(7), 1, "s")
                .expect("buffer")
                .read_from(CdcOffset::ZERO, 16)
                .iter()
                .map(|event| (event.partition, event.position()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            seen(&leader),
            vec![
                (calvin_partition(0), CdcOffset::data_event_in(0, 31, 2, 1)),
                (calvin_partition(0), CdcOffset::data_event_in(0, 31, 2, 2)),
            ]
        );
        assert_eq!(seen(&leader), seen(&follower));
    }

    #[test]
    fn a_node_without_raft_positions_by_local_wal_lsn() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("s", "orders"));
        let router = CdcRouter::new(registry);
        router.route_event(&write_at(40, 1), &test_tracker());
        assert_eq!(
            positions(&router, "s"),
            vec![CdcOffset::data_event(0, 40, 1)]
        );
    }

    #[test]
    fn an_unmapped_event_on_a_replica_joins_the_current_entry() {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("s", "orders"));
        let router = CdcRouter::new(registry);
        let wt = test_tracker();
        router.positions().record(
            100,
            ReplicatedPosition {
                epoch: 0,
                group_id: 2,
                log_index: 9,
            },
        );
        router.route_event(&write_at(100, 1), &wt);
        // A local LSN the ledger never recorded must not open a new index.
        router.route_event(&write_at(90_000, 2), &wt);
        assert_eq!(
            positions(&router, "s"),
            vec![
                CdcOffset::data_event(0, 9, 1),
                CdcOffset::data_event(0, 9, 2)
            ]
        );
    }

    #[test]
    fn a_recompute_correction_is_kept_beside_its_event() {
        let registry = Arc::new(StreamRegistry::new());
        let mut def = sample_def("late", "orders");
        def.late_data = LateDataPolicy::Recompute;
        registry.register(def);
        let router = CdcRouter::new(registry);
        let wt = test_tracker();
        wt.advance(0, 1_000, 0);
        router.route_event(&make_write_event("orders", 1), &wt);
        let events = router
            .get_buffer(DatabaseId::new(7), 1, "late")
            .expect("late buffer")
            .read_from(CdcOffset::ZERO, 16);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].op, "INSERT");
        assert_eq!(events[1].op, "RECOMPUTE");
        assert_eq!(events[1].position(), events[0].position().correction());
    }

    #[test]
    fn an_event_at_the_watermark_is_not_late() {
        let registry = Arc::new(StreamRegistry::new());
        let mut def = sample_def("strict", "orders");
        def.late_data = LateDataPolicy::Drop;
        registry.register(def);
        let router = CdcRouter::new(registry);
        let wt = test_tracker();
        // The sibling event of the write the watermark already covers, and
        // the next write, are both on time. Only an older LSN is late.
        wt.advance(0, 10, 0);
        router.route_event(&make_write_event("orders", 1), &wt);
        router.route_event(&make_write_event("orders", 2), &wt);
        wt.advance(0, 20, 0);
        router.route_event(&make_write_event("orders", 1), &wt);
        let events = router
            .get_buffer(DatabaseId::new(7), 1, "strict")
            .expect("strict buffer")
            .read_from(CdcOffset::ZERO, 16);
        assert_eq!(events.len(), 2);
    }

    /// A router whose buffer, restored from the CDC ledger, holds the first
    /// event of entry 12, which local record 500 applied.
    fn restarted_router() -> CdcRouter {
        let registry = Arc::new(StreamRegistry::new());
        registry.register(sample_def("s", "orders"));
        let router = CdcRouter::new(registry);
        router
            .ensure_buffer(
                DatabaseId::new(7),
                1,
                "s",
                &RetentionConfig {
                    max_events: 100,
                    max_age_secs: 3_600,
                },
            )
            .push(CdcEvent {
                sequence: CdcOffset::data_event(0, 12, 1).sequence,
                index: 12,
                epoch: 0,
                lsn: 500,
                ..(*router_event_template()).clone()
            });
        router
    }

    fn entry_12_at(router: &CdcRouter, record_lsn: u64) {
        router.positions().record(
            record_lsn,
            ReplicatedPosition {
                epoch: 0,
                group_id: 2,
                log_index: 12,
            },
        );
    }

    #[test]
    fn a_catch_up_continues_the_record_it_stopped_in() {
        let router = restarted_router();
        entry_12_at(&router, 500);
        router.route_event(&write_at(500, 2), &test_tracker());
        assert_eq!(
            positions(&router, "s"),
            vec![
                CdcOffset::data_event(0, 12, 1),
                CdcOffset::data_event(0, 12, 2)
            ]
        );
    }

    #[test]
    fn a_reapplied_entry_duplicates_nothing() {
        let router = restarted_router();
        // Raft delivers entry 12 again after the restart, and this node
        // writes it as record 900.
        entry_12_at(&router, 900);
        router.route_event(&write_at(900, 1), &test_tracker());
        assert_eq!(
            positions(&router, "s"),
            vec![CdcOffset::data_event(0, 12, 1)]
        );
    }

    fn router_event_template() -> Arc<CdcEvent> {
        Arc::new(CdcEvent {
            sequence: 0,
            partition: 0,
            collection: "orders".into(),
            op: "INSERT".into(),
            row_id: "row-restored".into(),
            event_time: u64::MAX,
            lsn: 0,
            index: 0,
            epoch: 0,
            database_id: DatabaseId::new(7),
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: EventSource::User,
        })
    }
}
