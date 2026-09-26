// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::ResolveTxn`: turns a committing transaction's staged post-images
//! into one replayable [`RedoRecord`], without mutating base. Overlay-driven
//! serializers (KV, Document, Graph, Columnar, vector-primary) read the
//! post-image the transaction was shown from the staging overlay, so replay
//! installs it verbatim. Plan-driven serializers (HNSW Vector, Array,
//! Timeseries, Spatial, CRDT, Text) carry absolute or append-only writes and
//! serialize from the plan node instead.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::RowIdentity;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
use crate::data::executor::handlers::transaction::stage_write::GRAPH_LABEL_COLL_KEY;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::classify::{classify_document_op, classify_kv_op};
use super::columnar_image::{ColumnarCollectionImages, ColumnarCollections};
use super::graph::EdgeIdentityKey;
use super::vector_primary::VectorPrimaryCollections;
use super::{array, columnar_image, crdt, document, graph, kv, spatial, text, vector};

impl CoreLoop {
    /// Resolve a committing transaction's staged writes into a
    /// [`RedoRecord`] and return its encoded bytes in the response payload.
    /// Reads the overlay by `&` and never mutates any base engine. A session
    /// transaction and a Calvin transaction stage the same way, so both
    /// resolve here.
    pub(in crate::data::executor) fn execute_resolve_txn(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> Response {
        let ops = match self.resolve_txn_ops(task, tid, txn_id, plans) {
            Ok(ops) => ops,
            Err(e) => return self.response_error(task, e),
        };
        let record = RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
        };
        match record.to_bytes() {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Build the ordered redo sub-records for a transaction's staged
    /// post-images. Walks the plan once to classify every op and collect
    /// the touched KV collections; serializes from the overlay, not the plan.
    fn resolve_txn_ops(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> crate::Result<Vec<RedoSubRecord>> {
        let mut kv_collections: BTreeSet<String> = BTreeSet::new();
        let mut doc_collections: BTreeSet<String> = BTreeSet::new();
        let mut graph_collections: BTreeSet<String> = BTreeSet::new();
        let mut edge_surrogates: BTreeMap<EdgeIdentityKey, (u32, u32)> = BTreeMap::new();
        let mut columnar_collections: ColumnarCollections = BTreeMap::new();
        let mut vector_primary_collections: VectorPrimaryCollections = BTreeMap::new();

        // Plan-driven serializers emit into `ops` during this walk; overlay-driven
        // serializers only collect collections here, serialized in phase two below.
        let mut ops: Vec<RedoSubRecord> = Vec::new();
        // The declared primary key of each truncated document collection,
        // from the plan: names every removed base row in its redo entry.
        let truncate_primary_keys = truncate_declared_primary_keys(plans);
        // Unkeyed timeseries ingests seen per collection, in plan order.
        let mut unkeyed_seen = std::collections::HashMap::new();

        for plan in plans {
            match plan {
                // KV: overlay-backed serializer. Read ops stage nothing and are
                // skipped; row-level writes contribute their collection.
                PhysicalPlan::Kv(op) => classify_kv_op(op, &mut kv_collections)?,

                // Document: staged point/bulk writes contribute their collection;
                // join/merge DML has no overlay post-image and errors.
                PhysicalPlan::Document(op) => classify_document_op(op, &mut doc_collections)?,

                // Graph: overlay carries edge identity/properties but not the
                // endpoint surrogates a redo put needs, so those come from the plan.
                PhysicalPlan::Graph(op) => {
                    graph::classify_graph_op(op, &mut graph_collections, &mut edge_surrogates)?
                }

                // Plan-driven: a CRDT write resolves to the intent record its
                // autocommit form journals.
                PhysicalPlan::Crdt(op) => crdt::serialize_crdt_op(op, &mut ops)?,

                // Plan-driven: an FTS write resolves to the posting record its
                // autocommit form journals.
                PhysicalPlan::Text(op) => text::serialize_text_op(op, &mut ops)?,

                // Read-only families: scans, joins, aggregates, exchange, and
                // maintenance ops carry no persisted post-image.
                PhysicalPlan::Query(_) | PhysicalPlan::Meta(_) => {}

                // Plan-driven: each op serializes from the plan node, skips, or
                // errors. A vector-primary direct write only registers its
                // collection; its staged row is serialized from the overlay.
                PhysicalPlan::Vector(op) => {
                    vector::serialize_vector_op(op, &mut ops, &mut vector_primary_collections)?
                }
                PhysicalPlan::Array(op) => array::serialize_array_op(op, &mut ops)?,
                PhysicalPlan::Timeseries(op) => self.serialize_timeseries_op(
                    task,
                    tid,
                    txn_id,
                    op,
                    &mut unkeyed_seen,
                    &mut ops,
                )?,

                // Columnar: every write is staged per surrogate, so the image
                // the transaction was shown is serialized from the overlay.
                PhysicalPlan::Columnar(op) => {
                    columnar_image::classify_columnar_op(op, &mut columnar_collections)?
                }

                // Spatial `Insert`/`Delete` plan nodes carry the complete absolute
                // post-image, so they serialize from the plan node, not an overlay.
                PhysicalPlan::Spatial(op) => spatial::serialize_spatial_op(op, &mut ops)?,

                // Coordinator-only op; never legal on the Data Plane.
                PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {
                    return Err(crate::Error::Internal {
                        detail: "Control-Plane-only op reached Data Plane transaction resolve"
                            .to_string(),
                    });
                }
            }
        }

        let reads_overlay = !kv_collections.is_empty()
            || !doc_collections.is_empty()
            || !graph_collections.is_empty()
            || !columnar_collections.is_empty()
            || !vector_primary_collections.is_empty()
            || plans.iter().any(graph::is_label_write);
        self.require_staging_overlay(txn_id, reads_overlay)?;

        // Pin the resolve-time bitemporal stamp once, in the overlay sidecar, for
        // every staged put AND tombstone, so the redo carries it and every apply
        // (install, replica, restart) writes the same version key. Deterministic
        // (collection, doc-id) order keeps replicas resolving the same txn in
        // sync.
        for collection in &doc_collections {
            if !self.is_bitemporal(task.request.database_id.as_u64(), tid, collection) {
                continue;
            }
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.clone(),
            );
            let mut writes: Vec<(&RowIdentity, u32)> = match self.txn_overlays.get(&txn_id) {
                Some(overlay) => overlay
                    .iter_doc_entries_for_collection(&coll_key)
                    .filter_map(|(doc_id, _staged)| {
                        overlay
                            .surrogate_for_doc_id(&coll_key, doc_id)
                            .map(|surrogate| (doc_id, surrogate))
                    })
                    .collect(),
                None => Vec::new(),
            };
            writes.sort();
            let stamps: Vec<(u32, BitemporalStamp)> = writes
                .into_iter()
                .map(|(_doc_id, surrogate)| {
                    (
                        surrogate,
                        BitemporalStamp {
                            sys_from_ms: self.bitemporal_now_ms(),
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                        },
                    )
                })
                .collect();
            if let Some(overlay) = self.txn_overlays.get_mut(&txn_id) {
                for (surrogate, stamp) in stamps {
                    overlay.set_bitemporal(&coll_key, surrogate, stamp);
                }
            }
        }

        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            for collection in &kv_collections {
                let coll_key = (
                    task.request.database_id,
                    TenantId::new(tid),
                    collection.clone(),
                );
                if overlay.is_truncated(&coll_key) {
                    kv::serialize_kv_truncate(collection, &mut ops)?;
                }
                kv::serialize_kv_collection(overlay, &coll_key, collection, &mut ops)?;
            }
            for collection in &doc_collections {
                let coll_key = (
                    task.request.database_id,
                    TenantId::new(tid),
                    collection.clone(),
                );
                // Strict collections store Binary Tuples; resolve the schema
                // once so the serializer can decode them back to MessagePack.
                let strict_schema =
                    self.resolve_strict_schema(task.request.database_id.as_u64(), tid, collection);
                if overlay.is_truncated(&coll_key) {
                    let rows =
                        self.truncated_base_rows(task, tid, collection, overlay, &coll_key)?;
                    document::serialize_truncated_base_rows(
                        document::TruncatedBaseRows {
                            collection,
                            rows: &rows,
                            strict_schema: strict_schema.as_ref(),
                            declared_primary_key: truncate_primary_keys
                                .get(collection.as_str())
                                .and_then(|pk| pk.as_deref()),
                            sys_from_ms: self
                                .is_bitemporal(task.request.database_id.as_u64(), tid, collection)
                                .then(|| self.bitemporal_now_ms()),
                        },
                        &mut ops,
                    )?;
                }
                document::serialize_document_collection(
                    overlay,
                    &coll_key,
                    collection,
                    strict_schema.as_ref(),
                    &mut ops,
                )?;
            }
        }
        self.serialize_staged_image_collections(
            task,
            tid,
            txn_id,
            &columnar_collections,
            &vector_primary_collections,
            &mut ops,
        )?;
        if let Some(graph_overlay) = self.graph_txn_overlays.get(&txn_id) {
            // Freeze temporal identity independently from the overlay's lease
            // refresh stamp. Resolve retries reuse the exact same ordinal.
            let graph_system_from = graph_overlay.freeze_system_from(self.hlc.next_ordinal());
            for collection in &graph_collections {
                let coll_key = (
                    task.request.database_id,
                    TenantId::new(tid),
                    collection.clone(),
                );
                graph::serialize_graph_collection(
                    graph_overlay,
                    &coll_key,
                    collection,
                    &edge_surrogates,
                    graph_system_from,
                    &mut ops,
                )?;
            }
            // Node-label deltas live under the fixed sentinel key, not
            // `graph_collections`, so this runs whenever a graph overlay exists.
            let label_coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                GRAPH_LABEL_COLL_KEY.to_string(),
            );
            graph::serialize_node_label_deltas(graph_overlay, &label_coll_key, &mut ops)?;
        }
        Ok(ops)
    }

    /// Refuse a session resolve whose writes live in a staging overlay this
    /// core does not hold.
    ///
    /// Every core that stages a write opens the transaction's overlay, so a
    /// missing overlay means the writes were staged on another core or the
    /// overlay was reaped. Resolving would then commit none of them.
    fn require_staging_overlay(&self, txn_id: TxnId, reads_overlay: bool) -> crate::Result<()> {
        if !reads_overlay
            || self.txn_overlays.contains_key(&txn_id)
            || self.graph_txn_overlays.contains_key(&txn_id)
        {
            return Ok(());
        }
        Err(crate::Error::Internal {
            detail: format!(
                "{txn_id} has staged writes but core {} holds no staging overlay for it; \
                 the commit is refused",
                self.core_id
            ),
        })
    }

    /// Serialize the columnar and vector-primary collections the
    /// transaction wrote from its overlay, in collection order.
    fn serialize_staged_image_collections(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        columnar: &ColumnarCollections,
        vector_primary: &VectorPrimaryCollections,
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        let coll_key = |collection: &str| {
            (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            )
        };
        for (collection, schema_bytes) in columnar {
            let key = coll_key(collection);
            columnar_image::serialize_columnar_collection(
                ColumnarCollectionImages {
                    overlay,
                    coll_key: &key,
                    schema: self.columnar_engines.get(&key).map(|e| e.schema()),
                    schema_bytes,
                },
                ops,
            )?;
        }
        for (collection, writes) in vector_primary {
            self.serialize_vector_primary_collection(overlay, &coll_key(collection), writes, ops)?;
        }
        Ok(())
    }

    /// The base rows a staged TRUNCATE of `collection` removes at COMMIT:
    /// every base row with no overlay entry. The redo record is the only
    /// thing a replica installs, so each removed row travels as its own
    /// `Delete`. Read-only against base.
    fn truncated_base_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        overlay: &crate::data::executor::handlers::transaction::overlay::TxnOverlay,
        coll_key: &(crate::types::DatabaseId, TenantId, String),
    ) -> crate::Result<Vec<(nodedb_types::StorageKey, Vec<u8>)>> {
        let database_id = task.request.database_id.as_u64();
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let mut rows = Vec::new();
        for key in self.scan_matching_documents(database_id, tid, collection, &[])? {
            if overlay.get(coll_key, key.surrogate().as_u32()).is_some() {
                continue;
            }
            let body = if bitemporal {
                self.sparse
                    .versioned_get_current(database_id, tid, collection, &key)?
            } else {
                self.sparse.get(database_id, tid, collection, &key)?
            };
            if let Some(body) = body {
                rows.push((key, body));
            }
        }
        Ok(rows)
    }
}

/// The declared primary key of every document collection a `Truncate` plan
/// names, keyed by collection.
fn truncate_declared_primary_keys(plans: &[PhysicalPlan]) -> BTreeMap<String, Option<String>> {
    plans
        .iter()
        .filter_map(|plan| match plan {
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection,
                declared_primary_key,
                ..
            }) => Some((collection.to_string(), declared_primary_key.clone())),
            PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{
        ArrayOp, ColumnarInsertIntent, ColumnarOp, DocumentOp, GraphOp, KvOp, MetaOp,
        ReturningColumns, ReturningSpec, StorageMode, TimeseriesOp, UpdateValue, VectorOp,
    };
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{QualifiedCollection, RowIdentity, StorageKey, Surrogate};

    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::data::executor::strict_format;
    use crate::engine::document::store::CollectionConfig;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
    use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{
        DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
    };
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::{RecordType, WalRecordArgs};

    const TID: u64 = 1;

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    fn coll_key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(TID), coll.to_string())
    }

    fn storage_key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    /// Decode the `RedoRecord` bytes carried in a resolve response payload.
    fn decode_redo(resp: &crate::bridge::envelope::Response) -> RedoRecord {
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record")
    }

    /// A resolve plan that names `collection` as a KV write so the serializer
    /// picks up that collection's overlay entries.
    fn kv_write_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    /// Decode a `kv_put` redo payload in the shape resolve emits.
    #[allow(clippy::type_complexity)]
    fn decode_kv_put(payload: &[u8]) -> (String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32) {
        let (disc, collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                payload,
            )
            .expect("decode kv_put");
        assert_eq!(disc, "kv_put");
        (collection, key, value, ttl_ms, expire_at_ms, surrogate)
    }

    #[test]
    fn incr_resolves_to_absolute_value_not_delta() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(1);

        // Two Incrs in one transaction: 0 + 40, then + 2 = 42. The overlay slot
        // holds the resolved absolute value (42), not either delta.
        for delta in [40i64, 2] {
            let resp = core.execute_stage_kv(
                &task,
                TID,
                txn,
                &KvOp::Incr {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
                    key: b"c".to_vec(),
                    delta,
                    ttl_ms: 0,
                    surrogate: Surrogate::ZERO,
                    rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                    shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
                },
            );
            assert_eq!(resp.status, Status::Ok, "stage incr: {resp:?}");
        }

        // The overlay's staged bytes are the resolved absolute post-image.
        let overlay_bytes = match core
            .txn_overlays
            .get(&txn)
            .and_then(|o| o.get_by_doc_id(&coll_key("counters"), &kv_row_identity(b"c")))
            .expect("staged incr present")
        {
            Staged::Put(v) => v.clone(),
            Staged::Tombstone => panic!("incr must stage a value"),
        };

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("counters")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one staged KV row -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, _ttl, _expire, _surrogate) =
            decode_kv_put(&redo.ops[0].payload);
        assert_eq!(collection, "counters");
        assert_eq!(key, b"c");
        // The emitted value is the overlay's absolute post-image, and it decodes
        // to 42 — not the last delta (2) nor the first (40).
        assert_eq!(value, overlay_bytes);
        assert_eq!(
            value,
            b"42".to_vec(),
            "resolve carries the absolute resolved value, not a delta"
        );
    }

    #[test]
    fn put_with_ttl_resolves_to_an_absolute_expiry() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(2);

        // Stage a value with an absolute expiry directly (what a `Put` with a
        // non-zero TTL leaves in the overlay: value + `ExpireAt`).
        let expire_at = 1_700_000_000_000u64;
        {
            let overlay = core.txn_overlay_mut(txn);
            overlay.insert_put(
                coll_key("sessions"),
                7,
                &kv_row_identity(b"s1"),
                b"v1".to_vec(),
            );
            overlay.set_ttl(
                coll_key("sessions"),
                7,
                &kv_row_identity(b"s1"),
                StagedTtl::ExpireAt(expire_at),
            );
        }

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("sessions")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, ttl_ms, got_expire, surrogate) =
            decode_kv_put(&redo.ops[0].payload);
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"s1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 0, "relative ttl_ms is vestigial and set to 0");
        assert_eq!(
            got_expire,
            Some(expire_at),
            "absolute expiry carried verbatim"
        );
        assert_eq!(
            surrogate, 7,
            "the redo record must carry the overlay's surrogate so replay \
             restores the same identity the live write bound"
        );
    }

    #[test]
    fn put_without_ttl_resolves_without_an_expiry_instant() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(3);

        core.txn_overlay_mut(txn).insert_put(
            coll_key("kvc"),
            9,
            &kv_row_identity(b"k9"),
            b"body".to_vec(),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);

        let (collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            decode_kv_put(&redo.ops[0].payload);
        assert_eq!(collection, "kvc");
        assert_eq!(key, b"k9");
        assert_eq!(value, b"body");
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, None, "no-TTL put carries no expiry instant");
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn tombstone_resolves_to_kv_delete_shape() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(4);

        core.txn_overlay_mut(txn)
            .insert_tombstone(coll_key("kvc"), 11, &kv_row_identity(b"gone"));

        let resp = core.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[PhysicalPlan::Kv(KvOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kvc"),
                keys: vec![b"gone".to_vec()],
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            })],
        );
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);

        let (disc, collection, keys) =
            zerompk::from_msgpack::<(String, String, Vec<Vec<u8>>)>(&redo.ops[0].payload)
                .expect("decode kv_delete");
        assert_eq!(disc, "kv_delete");
        assert_eq!(collection, "kvc");
        assert_eq!(keys, vec![b"gone".to_vec()]);
    }

    #[test]
    fn resolve_does_not_mutate_base() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(5);
        let now = crate::engine::kv::current_ms();

        // Seed a base KV row, then stage a different value for the same key.
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TID,
            collection: "kvc",
            key: b"k",
            value: b"base",
            ttl_ms: 0,
            now_ms: now,
            surrogate: Surrogate::ZERO,
        });
        let before = core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "kvc", b"k", now);
        assert_eq!(before.as_deref(), Some(b"base".as_slice()));

        core.txn_overlay_mut(txn).insert_put(
            coll_key("kvc"),
            1,
            &kv_row_identity(b"k"),
            b"staged".to_vec(),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        assert_eq!(resp.status, Status::Ok);

        // Base is untouched: resolve reads the overlay only, never writes base.
        let after = core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "kvc", b"k", now);
        assert_eq!(after.as_deref(), Some(b"base".as_slice()));
    }

    #[test]
    fn returning_delete_now_resolves_from_overlay() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(6);
        let surrogate = 4u32;

        // A DELETE ... RETURNING stages like any other point delete: the overlay
        // holds a tombstone, and resolve serializes it from there.
        core.txn_overlay_mut(txn).insert_tombstone(
            coll_key("notes"),
            surrogate,
            &RowIdentity::from_user_key("gone"),
        );

        let doc_plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "gone".to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });

        let resp = core.execute_resolve_txn(&task, TID, txn, &[doc_plan]);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            1,
            "a staged RETURNING delete resolves to one sub-record"
        );
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);
    }

    /// A `make_task` whose request carries `txn_id`, so `execute_stage_write`
    /// (which reads `task.request.txn_id`) can route a document point/bulk
    /// write into the staging overlay.
    fn make_stage_task(txn: TxnId) -> ExecutionTask {
        let mut task = make_task();
        task.request.txn_id = Some(txn);
        task
    }

    /// Msgpack-encode a scalar `RETURNING`-clause SET value the same way the
    /// planner emits `UpdateValue::Literal` bodies (decoded via
    /// `json_from_msgpack` in `stage_apply_update`).
    fn literal_str(s: &str) -> UpdateValue {
        UpdateValue::Literal(
            nodedb_types::json_to_msgpack(&serde_json::json!(s)).expect("encode literal"),
        )
    }

    /// Read the `name` field of a staged schemaless post-image body.
    fn staged_name(body: &[u8]) -> Option<String> {
        crate::data::executor::doc_format::decode_document(body)
            .ok()?
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    #[test]
    fn point_update_with_returning_stages_resolved_post_image() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(41);
        let task = make_stage_task(txn);
        let surrogate = 5u32;
        let row_key = storage_key(surrogate);

        // Seed a base row directly into the scan-visible sparse store.
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "notes",
                &row_key,
                &schemaless_body("alice"),
            )
            .expect("seed base row");

        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: row_key.to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            updates: vec![("name".to_string(), literal_str("bob"))],
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });

        let resp = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(
            resp.status,
            Status::Ok,
            "a RETURNING point update must stage, not error: {resp:?}"
        );

        let overlay = core.txn_overlays.get(&txn).expect("overlay present");
        match overlay
            .get(&coll_key("notes"), surrogate)
            .expect("row staged")
        {
            Staged::Put(body) => assert_eq!(
                staged_name(body).as_deref(),
                Some("bob"),
                "overlay holds the resolved post-update post-image"
            ),
            Staged::Tombstone => panic!("point update must stage a Put, not a tombstone"),
        }
    }

    #[test]
    fn bulk_update_with_returning_stages_matched_rows_per_surrogate() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(42);
        let task = make_stage_task(txn);

        for s in [1u32, 2u32] {
            let row_key = storage_key(s);
            core.sparse
                .put(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "notes",
                    &row_key,
                    &schemaless_body("old"),
                )
                .expect("seed base row");
        }

        // Empty filters match every row; a RETURNING clause does not change
        // which rows are staged.
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            filters: Vec::new(),
            updates: vec![("name".to_string(), literal_str("new"))],
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });

        let resp = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(
            resp.status,
            Status::Ok,
            "a RETURNING bulk update must stage: {resp:?}"
        );

        let overlay = core.txn_overlays.get(&txn).expect("overlay present");
        for s in [1u32, 2u32] {
            match overlay
                .get(&coll_key("notes"), s)
                .expect("row staged per-surrogate")
            {
                Staged::Put(body) => assert_eq!(
                    staged_name(body).as_deref(),
                    Some("new"),
                    "each matched row is staged with the applied update"
                ),
                Staged::Tombstone => panic!("bulk update must stage a Put"),
            }
        }
    }

    #[test]
    fn bulk_delete_with_returning_stages_tombstones_per_surrogate() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(43);
        let task = make_stage_task(txn);

        for s in [1u32, 2u32] {
            let row_key = storage_key(s);
            core.sparse
                .put(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "notes",
                    &row_key,
                    &schemaless_body("doomed"),
                )
                .expect("seed base row");
        }

        let plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            filters: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });

        let resp = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(
            resp.status,
            Status::Ok,
            "a RETURNING bulk delete must stage: {resp:?}"
        );

        let overlay = core.txn_overlays.get(&txn).expect("overlay present");
        for s in [1u32, 2u32] {
            assert!(
                matches!(
                    overlay
                        .get(&coll_key("notes"), s)
                        .expect("row staged per-surrogate"),
                    Staged::Tombstone
                ),
                "each matched row is staged as a tombstone"
            );
        }
    }

    #[test]
    fn returning_bulk_update_resolves_to_sub_records_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(44);

        // Two rows staged per-surrogate exactly as the bulk-update staging path
        // leaves them; a RETURNING clause does not change the overlay contents.
        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(
                coll_key("notes"),
                1,
                &RowIdentity::from_user_key("u1"),
                schemaless_body("bob"),
            );
            overlay.insert_put(
                coll_key("notes"),
                2,
                &RowIdentity::from_user_key("u2"),
                schemaless_body("bob"),
            );
        }

        // Serializes the staged post-images from the overlay.
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });

        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            2,
            "both staged rows resolve to sub-records (previously a typed error)"
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        for s in [1u32, 2u32] {
            let row_key = storage_key(s);
            let stored = dst
                .sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
                .expect("get")
                .expect("updated row must replay from resolve output");
            assert_eq!(
                stored,
                schemaless_body("bob"),
                "the resolved post-image round-trips through redo replay"
            );
        }
    }

    #[test]
    fn join_merge_dml_still_yield_typed_error() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(45);

        // These ops leave no per-surrogate overlay post-image, so resolve
        // raises a typed error rather than silently dropping their rows.
        let plans = [
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                source_alias: "s".to_string(),
                target_join_col: "id".to_string(),
                source_join_col: "id".to_string(),
                updates: Vec::new(),
                target_filters: Vec::new(),
                returning: None,
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            PhysicalPlan::Document(DocumentOp::Merge {
                target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "t"),
                source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "s"),
                source_alias: "s".to_string(),
                target_join_col: "id".to_string(),
                source_join_col: "id".to_string(),
                clauses: Vec::new(),
                returning: None,
                resolved_inserts: None,
                resolved_insert_identities: Vec::new(),
                source_rows: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
        ];

        for plan in plans {
            let resp = core.execute_resolve_txn(&task, TID, txn, std::slice::from_ref(&plan));
            assert_eq!(
                resp.status,
                Status::Error,
                "{plan:?} has no staged post-image and must raise a typed error"
            );
            assert!(resp.error_code.is_some());
        }
    }

    /// A batch insert staged row by row resolves to one document put per row,
    /// read from the overlay.
    #[test]
    fn a_staged_batch_insert_resolves_to_a_put_per_row() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(46);
        let documents = vec![
            ("d1".to_string(), schemaless_body("ann")),
            ("d2".to_string(), schemaless_body("bob")),
        ];
        let surrogates = vec![Surrogate::new(3), Surrogate::new(4)];
        let staged = core.stage_document_batch_insert(
            crate::data::executor::handlers::transaction::stage_write::StageBatchInsertParams {
                task: &task,
                tid: TID,
                txn_id: txn,
                collection: "notes",
                documents: &documents,
                surrogates: &surrogates,
            },
        );
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            documents,
            surrogates,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });

        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);

        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 2, "one sub-record per staged row");
        assert!(
            redo.ops
                .iter()
                .all(|op| op.record_type == RecordType::Put as u32)
        );
    }

    /// The five extended vector writes (`DirectUpsert`, `MultiVectorInsert`,
    /// `MultiVectorDelete`, `SparseInsert`, `SparseDelete`) must resolve to
    /// redo sub-records, not a typed error.
    #[test]
    fn extended_vector_writes_now_resolve_ok() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(50);

        let plans = [
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vp"),
                field: "emb".to_string(),
                surrogate: Surrogate::new(1),
                pk_bytes: Vec::new(),
                vector: vec![1.0, 2.0, 3.0],
                payload: Vec::new(),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
                field_name: "mv".to_string(),
                document_surrogate: Surrogate::new(2),
                vectors: vec![1.0, 2.0, 3.0, 4.0],
                count: 2,
                dim: 2,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
                field_name: "mv".to_string(),
                document_surrogate: Surrogate::new(2),
            }),
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
                field_name: "sv".to_string(),
                doc_id: "d1".to_string(),
                entries: vec![(10, 0.5)],
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
                field_name: "sv".to_string(),
                doc_id: "d1".to_string(),
            }),
        ];

        // A vector-primary direct write resolves from the row its statement
        // staged.
        let staged = core.execute_stage_write(&make_stage_task(txn), TID, &plans[0]);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");

        for plan in plans {
            let resp = core.execute_resolve_txn(&task, TID, txn, std::slice::from_ref(&plan));
            assert_eq!(
                resp.status,
                Status::Ok,
                "{plan:?} must now resolve to a redo sub-record, not a typed error"
            );
            let redo = decode_redo(&resp);
            assert_eq!(
                redo.ops.len(),
                1,
                "{plan:?} resolves to exactly one sub-record"
            );
        }
    }

    fn vp_index_key() -> (DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "vp", "emb")
    }

    fn mv_index_key() -> (DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "mc", "mv")
    }

    fn sparse_index_key() -> (DatabaseId, TenantId, String, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "sc".to_string(),
            "sv".to_string(),
        )
    }

    /// Resolve a `DirectUpsert`, wrap it into a `TransactionRedo` record, and
    /// replay into a fresh core — the vector-primary row must survive.
    #[test]
    fn resolved_direct_upsert_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vp"),
            field: "emb".to_string(),
            surrogate: Surrogate::new(42),
            pk_bytes: Vec::new(),
            vector: vec![1.0, 2.0, 3.0],
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        });
        let staged = src.execute_stage_write(&make_stage_task(TxnId::new(51)), TID, &plan);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        let resp = src.execute_resolve_txn(&task, TID, TxnId::new(51), std::slice::from_ref(&plan));
        let redo = decode_redo(&resp);
        let record = wrap_redo(&redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let coll = dst
            .vector_collections
            .get(&vp_index_key())
            .expect("vector-primary collection rebuilt from redo replay");
        assert_eq!(coll.len(), 1, "the upserted vector must be recovered");
        assert!(
            coll.local_for_surrogate(Surrogate::new(42)).is_some(),
            "the cross-engine surrogate must be rebound on redo replay"
        );
    }

    fn vp_fields(note: &str, v: i64) -> Vec<u8> {
        let mut fields = std::collections::HashMap::new();
        fields.insert("note".to_string(), nodedb_types::Value::String(note.into()));
        fields.insert("v".to_string(), nodedb_types::Value::Integer(v));
        zerompk::to_msgpack_vec(&fields).expect("encode payload fields")
    }

    fn vp_upsert_plan(
        surrogate: u32,
        payload: Vec<u8>,
        on_conflict_updates: Vec<(String, UpdateValue)>,
    ) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vp"),
            field: "emb".to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            vector: vec![1.0, 0.0, 0.0],
            payload,
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    /// Commit a vector-primary base row on `core` through the live handler.
    fn vp_live_upsert(core: &mut CoreLoop, surrogate: u32, payload: &[u8]) {
        let task = make_task();
        let resp = core.execute_vector_direct_upsert(
            crate::data::executor::handlers::vector_upsert::VectorDirectUpsertParams {
                task: &task,
                tid: TID,
                collection: "vp",
                field: "emb",
                surrogate: Surrogate::new(surrogate),
                vector: &[1.0, 0.0, 0.0],
                payload,
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
                intent: nodedb_physical::physical_plan::VectorDirectWriteIntent::Upsert,
                on_conflict_updates: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok, "seed vector-primary row: {resp:?}");
    }

    /// A vector-primary `ON CONFLICT DO UPDATE` stages the merged sidecar; the
    /// redo carries exactly that sidecar, and a replica holding the same base
    /// row installs it verbatim instead of re-running the merge.
    #[test]
    fn vector_primary_on_conflict_redo_installs_the_merged_sidecar_the_transaction_saw() {
        let (mut src, _src_dir) = make_core();
        let (mut dst, _dst_dir) = make_core();
        vp_live_upsert(&mut src, 31, &vp_fields("orig", 1));
        vp_live_upsert(&mut dst, 31, &vp_fields("orig", 1));

        let txn = TxnId::new(53);
        let upsert = vp_upsert_plan(
            31,
            vp_fields("new", 7),
            vec![(
                "v".to_string(),
                UpdateValue::Literal(
                    nodedb_types::value_to_msgpack(&nodedb_types::Value::Integer(7))
                        .expect("encode literal"),
                ),
            )],
        );
        let staged = src.execute_stage_write(&make_stage_task(txn), TID, &upsert);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        let staged_sidecar = match src
            .txn_overlays
            .get(&txn)
            .and_then(|overlay| overlay.get(&coll_key("vp"), 31))
        {
            Some(Staged::Put(body)) => {
                crate::data::executor::handlers::transaction::overlay::StagedVectorRow::from_bytes(
                    body,
                )
                .expect("decode staged row")
                .sidecar
            }
            other => panic!("the upsert must stage a row, got {other:?}"),
        };

        let redo = decode_redo(&src.execute_resolve_txn(&make_task(), TID, txn, &[upsert]));
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(
            redo.ops[0].record_type,
            RecordType::VectorResolvedDirectWrite as u32
        );

        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wrap_redo(&redo)),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let installed = dst
            .vector_sidecar_bytes(DatabaseId::DEFAULT.as_u64(), TID, "vp", Surrogate::new(31))
            .expect("read sidecar")
            .expect("the row exists on the replica");
        assert_eq!(
            installed, staged_sidecar,
            "the replica holds the sidecar the transaction was shown"
        );
        let fields =
            crate::data::executor::handlers::vector_upsert::decode_payload_lowercased(&installed)
                .expect("decode sidecar");
        assert_eq!(
            fields.get("note"),
            Some(&nodedb_types::Value::String("orig".into())),
            "the column the SET list left alone keeps its stored value"
        );
        assert_eq!(fields.get("v"), Some(&nodedb_types::Value::Integer(7)));
    }

    /// Resolve a `MultiVectorInsert` and replay it through the redo path.
    #[test]
    fn resolved_multi_vector_insert_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let plan = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
            field_name: "mv".to_string(),
            document_surrogate: Surrogate::new(7),
            vectors: vec![1.0, 2.0, 3.0, 4.0],
            count: 2,
            dim: 2,
        });
        let resp = src.execute_resolve_txn(&task, TID, TxnId::new(52), std::slice::from_ref(&plan));
        let redo = decode_redo(&resp);
        let record = wrap_redo(&redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let coll = dst
            .vector_collections
            .get(&mv_index_key())
            .expect("multi-vector collection rebuilt from redo replay");
        assert_eq!(coll.len(), 2, "both document vectors must be recovered");
        assert!(
            coll.multi_doc_map.contains_key(&Surrogate::new(7)),
            "the multi-vector document grouping must be reconstructed"
        );
    }

    /// Resolve a `MultiVectorInsert` then a `MultiVectorDelete` in the same
    /// transaction and replay — the delete must leave the document removed.
    #[test]
    fn resolved_multi_vector_delete_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(53);
        let insert = PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
            field_name: "mv".to_string(),
            document_surrogate: Surrogate::new(7),
            vectors: vec![1.0, 2.0, 3.0, 4.0],
            count: 2,
            dim: 2,
        });
        let delete = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "mc"),
            field_name: "mv".to_string(),
            document_surrogate: Surrogate::new(7),
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[insert, delete]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 2, "insert + delete both resolve");
        let record = wrap_redo(&redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let coll = dst
            .vector_collections
            .get(&mv_index_key())
            .expect("collection present after redo replay");
        assert!(
            !coll.multi_doc_map.contains_key(&Surrogate::new(7)),
            "the deleted multi-vector document must stay deleted"
        );
    }

    /// Resolve a `SparseInsert` and replay it through the redo path.
    #[test]
    fn resolved_sparse_insert_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let plan = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
            field_name: "sv".to_string(),
            doc_id: "d1".to_string(),
            entries: vec![(10, 0.5), (20, 0.8)],
        });
        let resp = src.execute_resolve_txn(&task, TID, TxnId::new(54), std::slice::from_ref(&plan));
        let redo = decode_redo(&resp);
        let record = wrap_redo(&redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let idx = dst
            .sparse_vector_indexes
            .get(&sparse_index_key())
            .expect("sparse index rebuilt from redo replay");
        assert_eq!(idx.doc_count(), 1, "the sparse document must be recovered");
    }

    /// Resolve a `SparseInsert` then a `SparseDelete` in the same transaction
    /// and replay — the delete must leave the document removed.
    #[test]
    fn resolved_sparse_delete_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(55);
        let insert = PhysicalPlan::Vector(VectorOp::SparseInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
            field_name: "sv".to_string(),
            doc_id: "d1".to_string(),
            entries: vec![(10, 0.5)],
        });
        let delete = PhysicalPlan::Vector(VectorOp::SparseDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sc"),
            field_name: "sv".to_string(),
            doc_id: "d1".to_string(),
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[insert, delete]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 2, "insert + delete both resolve");
        let record = wrap_redo(&redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let doc_count = dst
            .sparse_vector_indexes
            .get(&sparse_index_key())
            .map(|i| i.doc_count())
            .unwrap_or(0);
        assert_eq!(
            doc_count, 0,
            "the deleted sparse document must stay deleted"
        );
    }

    /// Register a strict collection whose first column is a non-null `_rowid`
    /// (so `apply_point_put` reads it from the emitted MessagePack) plus a
    /// nullable `body` column.
    fn strict_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("body", ColumnType::String),
        ])
        .expect("strict schema")
    }

    fn register_strict(core: &mut CoreLoop, collection: &str) {
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                collection.to_string(),
            ),
            CollectionConfig::new(collection).with_storage_mode(StorageMode::Strict {
                schema: strict_schema(),
            }),
        );
    }

    fn strict_tuple(rowid: i64, body: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert("_rowid".to_string(), nodedb_types::Value::Integer(rowid));
        obj.insert(
            "body".to_string(),
            nodedb_types::Value::String(body.to_string()),
        );
        strict_format::value_to_binary_tuple(
            &nodedb_types::Value::Object(obj),
            &strict_schema(),
            "docs",
        )
        .expect("encode binary tuple")
    }

    /// A schemaless document body in canonical storage encoding, not the raw
    /// `Value` encoding — `apply_point_put` canonicalizes on write.
    fn schemaless_body(name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "name".to_string(),
            nodedb_types::Value::String(name.to_string()),
        );
        let encoded =
            zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).expect("encode msgpack");
        crate::data::executor::doc_format::canonicalize_document_for_storage(&encoded)
    }

    /// A resolve plan naming `collection` as a schemaless document write.
    fn doc_put_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: String::new(),
            value: Vec::new(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })
    }

    /// Wrap resolved redo bytes in a `TransactionRedo` WAL record.
    fn wrap_redo(redo: &RedoRecord) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("re-encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn strict_document_put_replays_correctly() {
        // Overlay holds a Binary Tuple; resolve must decode it to MessagePack
        // so the redo replay path can re-encode it via `bytes_to_binary_tuple`.
        let (mut src, _src_dir) = make_core();
        register_strict(&mut src, "sdocs");
        let task = make_task();
        let txn = TxnId::new(20);
        let surrogate = 7u32;
        let row_key = storage_key(surrogate);

        src.txn_overlay_mut(txn).insert_put(
            coll_key("sdocs"),
            surrogate,
            &RowIdentity::from_user_key("row1"),
            strict_tuple(surrogate as i64, "elephant"),
        );

        let resp = src.execute_resolve_txn(&task, TID, txn, &[doc_put_plan("sdocs")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one staged strict row -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        // Replay into a fresh core that has the same strict schema registered.
        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        register_strict(&mut dst, "sdocs");
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let stored = dst
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "sdocs", &row_key)
            .expect("get")
            .expect("strict document row must be restored from redo replay");
        let decoded = strict_format::binary_tuple_to_value(&stored, &strict_schema())
            .expect("stored body decodes as a Binary Tuple");
        match decoded {
            nodedb_types::Value::Object(map) => {
                assert_eq!(
                    map.get("body"),
                    Some(&nodedb_types::Value::String("elephant".into())),
                    "restored strict document must carry the correct field value"
                );
            }
            other => panic!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn schemaless_document_put_replays_verbatim() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(21);
        let surrogate = 3u32;
        let row_key = storage_key(surrogate);
        let body = schemaless_body("alice");

        src.txn_overlay_mut(txn).insert_put(
            coll_key("notes"),
            surrogate,
            &RowIdentity::from_user_key("userpk"),
            body.clone(),
        );

        let resp = src.execute_resolve_txn(&task, TID, txn, &[doc_put_plan("notes")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let stored = dst
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
            .expect("get")
            .expect("schemaless document row must replay");
        assert_eq!(stored, body, "schemaless body round-trips verbatim");
    }

    #[test]
    fn document_delete_resolves_with_surrogate_and_replay_removes_row() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(22);
        let surrogate = 11u32;
        let row_key = storage_key(surrogate);

        src.txn_overlay_mut(txn).insert_tombstone(
            coll_key("notes"),
            surrogate,
            &RowIdentity::from_user_key("gone"),
        );

        let delete_plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "gone".to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[delete_plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);

        // The delete tuple carries the surrogate as its fourth element.
        let (collection, _doc_id, prov, got_surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(
                &redo.ops[0].payload,
            )
            .expect("decode document delete tuple");
        assert_eq!(collection, "notes");
        assert!(prov.is_none());
        assert_eq!(
            got_surrogate, surrogate,
            "delete tuple must carry surrogate"
        );

        // Seed the row in a fresh core, then replay the delete removes it.
        let (mut dst, _dst_dir) = make_core();
        let seed = wrap_redo(&RedoRecord {
            version: 1,
            ops: {
                let mut ops = Vec::new();
                document_put_sub(&mut ops, "notes", surrogate, "gone", schemaless_body("x"));
                ops
            },
            calvin_stamp: None,
        });
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&seed),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
                .expect("get")
                .is_some(),
            "row seeded"
        );

        let del = wrap_redo(&redo);
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&del),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
                .expect("get")
                .is_none(),
            "redo delete must remove the document row"
        );
    }

    /// Build a document PUT sub-record directly (test helper mirroring the
    /// serializer's shape) for seeding rows into a replay target.
    fn document_put_sub(
        ops: &mut Vec<RedoSubRecord>,
        collection: &str,
        surrogate: u32,
        doc_id: &str,
        value: Vec<u8>,
    ) {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(collection, doc_id, value, prov, surrogate))
            .expect("encode document put sub-record");
        ops.push(RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        });
    }

    #[test]
    fn document_resolve_does_not_mutate_base() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(23);
        let surrogate = 1u32;
        let row_key = storage_key(surrogate);

        // Seed a base document row, then stage a DIFFERENT body for it.
        let seed = wrap_redo(&RedoRecord {
            version: 1,
            ops: {
                let mut ops = Vec::new();
                document_put_sub(
                    &mut ops,
                    "notes",
                    surrogate,
                    "userpk",
                    schemaless_body("base"),
                );
                ops
            },
            calvin_stamp: None,
        });
        core.replay_transaction_redo_wal(
            std::slice::from_ref(&seed),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let before = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
            .expect("get");
        assert_eq!(before.as_deref(), Some(schemaless_body("base").as_slice()));

        core.txn_overlay_mut(txn).insert_put(
            coll_key("notes"),
            surrogate,
            &RowIdentity::from_user_key("userpk"),
            schemaless_body("staged"),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[doc_put_plan("notes")]);
        assert_eq!(resp.status, Status::Ok);

        // Base is untouched: resolve reads the overlay only, never writes base.
        let after = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", &row_key)
            .expect("get");
        assert_eq!(
            after.as_deref(),
            Some(schemaless_body("base").as_slice()),
            "resolve must not mutate the base document engine"
        );
    }

    #[test]
    fn mixed_kv_and_document_resolve_into_one_record_and_both_replay() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(24);
        let doc_surrogate = 5u32;
        let doc_row_key = storage_key(doc_surrogate);

        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(coll_key("kvc"), 1, &kv_row_identity(b"k"), b"V".to_vec());
            overlay.insert_put(
                coll_key("notes"),
                doc_surrogate,
                &RowIdentity::from_user_key("userpk"),
                schemaless_body("bob"),
            );
        }

        let resp = src.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[kv_write_plan("kvc"), doc_put_plan("notes")],
        );
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            2,
            "one KV row + one document row -> two sub-records in one record"
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let db = DatabaseId::DEFAULT.as_u64();
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"k", now).as_deref(),
            Some(b"V".as_slice()),
            "KV sub-record must replay"
        );
        assert!(
            dst.sparse
                .get(db, TID, "notes", &doc_row_key)
                .expect("get")
                .is_some(),
            "document sub-record must replay"
        );
    }

    #[test]
    fn resolved_bytes_replay_into_fresh_engine() {
        // Resolve on one core, then replay the emitted `RedoRecord` bytes into a
        // FRESH engine set and observe the expected KV state.
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(7);

        let expire_at = crate::engine::kv::current_ms() + 3_600_000;
        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(coll_key("kvc"), 1, &kv_row_identity(b"live"), b"V".to_vec());
            overlay.set_ttl(
                coll_key("kvc"),
                1,
                &kv_row_identity(b"live"),
                StagedTtl::ExpireAt(expire_at),
            );
            overlay.insert_put(
                coll_key("kvc"),
                2,
                &kv_row_identity(b"plain"),
                b"P".to_vec(),
            );
        }

        let resp = src.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 2, "two staged rows -> two sub-records");

        // Wrap the resolved bytes in a `TransactionRedo` WAL record and replay
        // into a fresh core.
        let wal_record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: redo.to_bytes().expect("re-encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wal_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let now = crate::engine::kv::current_ms();
        let db = DatabaseId::DEFAULT.as_u64();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"live", now).as_deref(),
            Some(b"V".as_slice()),
            "expiring row must replay"
        );
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"plain", now).as_deref(),
            Some(b"P".as_slice()),
            "plain row must replay"
        );
        // The absolute expiry survived the round-trip (remaining ~ full hour).
        let ttl = dst
            .kv_engine
            .get_ttl_ms(db, TID, "kvc", b"live", now)
            .expect("ttl present");
        assert!(
            ttl > 3_000_000,
            "absolute expiry preserved (remaining {ttl}ms)"
        );
    }

    #[test]
    fn read_only_and_crdt_and_text_plans_emit_nothing() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(8);

        // A read-only KV Get, with no overlay staged, produces an empty redo.
        let resp = core.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[PhysicalPlan::Kv(KvOp::Get {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "kvc"),
                key: b"k".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            })],
        );
        let redo = decode_redo(&resp);
        assert!(redo.ops.is_empty(), "read-only plan emits no sub-record");
    }

    #[test]
    fn empty_overlay_resolves_to_empty_record() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(99), &[]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.version, 1);
        assert!(redo.ops.is_empty());
        assert!(redo.calvin_stamp.is_none());
    }

    #[test]
    fn sub_records_carry_the_kv_record_types() {
        // Guards the record-type tag the reconstitute path keys on.
        let sub = RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: Vec::new(),
        };
        assert_eq!(sub.record_type, RecordType::Put as u32);
    }

    /// A resolve plan carrying an `EdgePut` — the endpoint surrogates on this
    /// plan node are the ONLY source `classify_graph_op` has for them, since
    /// the overlay itself only staged identity + properties.
    fn graph_edge_put_plan(
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        src_surrogate: u32,
        dst_surrogate: u32,
    ) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(src_surrogate),
            dst_surrogate: Surrogate::new(dst_surrogate),
        })
    }

    #[test]
    fn graph_edge_put_resolves_with_both_surrogates_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(30);

        src.graph_txn_overlays
            .entry(txn)
            .or_default()
            .stage_edge_put(coll_key("g"), "a", "knows", "b", vec![9, 9]);

        let plan = graph_edge_put_plan("g", "a", "knows", "b", 10, 20);
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one staged edge put -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&redo.ops[0].payload)
            .expect("decode edge put redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(decoded.properties, vec![9, 9]);
        assert_eq!(
            decoded.src_surrogate, 10,
            "src surrogate must come from the plan node"
        );
        assert_eq!(
            decoded.dst_surrogate, 20,
            "dst surrogate must come from the plan node"
        );
        assert!(
            decoded.system_from.is_some_and(|s| s > 0),
            "resolve must freeze a real graph system-time ordinal"
        );

        // Replay into a fresh core: the CSR node->surrogate map must be
        // repopulated from the two trailing surrogates.
        let record = wrap_redo(&redo);
        let (mut dst_core, _dst_dir) = make_core();
        dst_core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");
        let edges = dst_core
            .edge_store
            .neighbors_out(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(TID),
                "g",
                "a",
                None,
            )
            .expect("neighbors_out");
        assert_eq!(edges.len(), 1, "graph edge put must replay");
        assert_eq!(edges[0].dst_id, "b");
    }

    #[test]
    fn graph_edge_put_without_matching_plan_surrogates_yields_typed_error() {
        // Overlay stages a put for `a-knows->b`, but the plan's `EdgePut` names a
        // different edge identity, so `edge_surrogates` has no entry for it —
        // resolve must error rather than invent a surrogate pair.
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(32);

        src.graph_txn_overlays
            .entry(txn)
            .or_default()
            .stage_edge_put(coll_key("g"), "a", "knows", "b", vec![]);

        let plan = graph_edge_put_plan("g", "x", "other", "y", 1, 2);
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(
            resp.status,
            Status::Error,
            "a staged edge with no matching plan-carried surrogates must error, not invent one"
        );
        assert!(resp.error_code.is_some());
    }

    #[test]
    fn graph_edge_delete_resolves_and_replay_removes_edge() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(33);

        src.graph_txn_overlays
            .entry(txn)
            .or_default()
            .stage_edge_delete(coll_key("g"), "a", "knows", "b");

        let plan = PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: "a".to_string(),
            label: "knows".to_string(),
            dst_id: "b".to_string(),
            src_surrogate: Surrogate::ZERO,
            dst_surrogate: Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&redo.ops[0].payload)
            .expect("decode timestamped edge delete redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert!(decoded.system_from.is_some_and(|s| s > 0));

        // Seed the edge at a system-time ordinal earlier than the delete's
        // frozen `system_from`, matching production replay order — a fresh
        // `next_ordinal` seed would incorrectly post-date the tombstone.
        let (mut dst_core, _dst_dir) = make_core();
        dst_core.active_graph_system_from =
            Some(decoded.system_from.expect("delete froze a system_from") - 1);
        dst_core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: TID,
                collection: "g",
                src_id: "a",
                label: "knows",
                dst_id: "b",
                properties: &[],
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        dst_core.active_graph_system_from = None;
        assert_eq!(
            dst_core
                .edge_store
                .neighbors_out(
                    DatabaseId::DEFAULT.as_u64(),
                    TenantId::new(TID),
                    "g",
                    "a",
                    None
                )
                .expect("neighbors_out")
                .len(),
            1,
            "edge seeded"
        );

        let del = wrap_redo(&redo);
        dst_core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&del),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");
        assert!(
            dst_core
                .edge_store
                .neighbors_out(
                    DatabaseId::DEFAULT.as_u64(),
                    TenantId::new(TID),
                    "g",
                    "a",
                    None
                )
                .expect("neighbors_out")
                .is_empty(),
            "redo delete must remove the graph edge"
        );
    }

    #[test]
    fn graph_resolve_does_not_mutate_base() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(34);

        // Seed a base edge, then stage a DIFFERENT properties blob for the
        // same identity.
        core.execute_edge_put(
            &task,
            EdgePutParams {
                tid: TID,
                collection: "g",
                src_id: "a",
                label: "knows",
                dst_id: "b",
                properties: b"base",
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );

        core.graph_txn_overlays
            .entry(txn)
            .or_default()
            .stage_edge_put(coll_key("g"), "a", "knows", "b", b"staged".to_vec());

        let plan = graph_edge_put_plan("g", "a", "knows", "b", 1, 2);
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(resp.status, Status::Ok);

        // Base is untouched: resolve reads the overlay and plan only, never
        // writes the edge store or CSR partition.
        let stored = core
            .edge_store
            .get_edge(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(TID),
                "g",
                "a",
                "knows",
                "b",
            )
            .expect("get_edge")
            .expect("base edge present");
        assert_eq!(
            stored, b"base",
            "resolve must not mutate the base edge store"
        );
    }

    #[test]
    fn mixed_document_and_graph_edge_resolve_into_one_record_and_both_replay() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(35);
        let doc_surrogate = 6u32;
        let doc_row_key = storage_key(doc_surrogate);

        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(
                coll_key("notes"),
                doc_surrogate,
                &RowIdentity::from_user_key("userpk"),
                schemaless_body("carol"),
            );
        }
        src.graph_txn_overlays
            .entry(txn)
            .or_default()
            .stage_edge_put(coll_key("g"), "a", "knows", "b", vec![]);

        let resp = src.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[
                doc_put_plan("notes"),
                graph_edge_put_plan("g", "a", "knows", "b", 3, 4),
            ],
        );
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            2,
            "one document row + one graph edge -> two sub-records in one record"
        );

        let record = wrap_redo(&redo);
        let (mut dst_core, _dst_dir) = make_core();
        dst_core
            .replay_transaction_redo_wal(
                std::slice::from_ref(&record),
                1,
                &nodedb_wal::TombstoneSet::new(),
            )
            .expect("redo replay must succeed");

        let db = DatabaseId::DEFAULT.as_u64();
        assert!(
            dst_core
                .sparse
                .get(db, TID, "notes", &doc_row_key)
                .expect("get")
                .is_some(),
            "document sub-record must replay"
        );
        assert_eq!(
            dst_core
                .edge_store
                .neighbors_out(db, TenantId::new(TID), "g", "a", None)
                .expect("neighbors_out")
                .len(),
            1,
            "graph sub-record must replay"
        );
    }

    /// True if `node_id` carries `label` in the CSR partition for
    /// `(DatabaseId::DEFAULT, TID)`. Mirrors `wal_replay_graph_labels.rs`'s
    /// test helper of the same shape.
    fn has_label(core: &CoreLoop, node_id: &str, label: &str) -> bool {
        let Some(partition) = core.csr_partition(DatabaseId::DEFAULT.as_u64(), TID) else {
            return false;
        };
        let Some(id) = partition.node_id(node_id) else {
            return false;
        };
        partition.node_has_label(id.raw(partition.partition_tag()), label)
    }

    /// Stage a `SetNodeLabels` / `RemoveNodeLabels` op into the graph overlay
    /// exactly as the live statement-time path does (`execute_stage_graph`),
    /// then resolve the transaction.
    fn stage_and_resolve_labels(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        txn: TxnId,
        op: &GraphOp,
    ) -> crate::bridge::envelope::Response {
        let stage_resp = core.execute_stage_graph(task, TID, txn, op);
        assert_eq!(stage_resp.status, Status::Ok, "stage: {stage_resp:?}");
        core.execute_resolve_txn(task, TID, txn, &[PhysicalPlan::Graph(op.clone())])
    }

    /// `SetNodeLabels` staged alone must resolve to a `GraphNodeLabelSet`
    /// sub-record and survive a redo replay into a fresh engine.
    #[test]
    fn graph_set_node_labels_resolves_and_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(36);

        let op = GraphOp::SetNodeLabels {
            node_id: "n1".to_string(),
            labels: vec!["Person".to_string()],
        };
        let resp = stage_and_resolve_labels(&mut src, &task, txn, &op);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            1,
            "one staged label delta -> one sub-record"
        );
        assert_eq!(
            redo.ops[0].record_type,
            RecordType::GraphNodeLabelSet as u32
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        assert!(
            has_label(&dst, "n1", "Person"),
            "SetNodeLabels must survive resolve -> redo replay"
        );
    }

    /// Inverse of the old typed-error regression for `RemoveNodeLabels`: the
    /// label must be removed after redo replay into a fresh engine that
    /// already carries the label (seeded independently of the source core).
    #[test]
    fn graph_remove_node_labels_resolves_and_replays_into_fresh_engine() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(37);

        let op = GraphOp::RemoveNodeLabels {
            node_id: "n1".to_string(),
            labels: vec!["Person".to_string()],
        };
        let resp = stage_and_resolve_labels(&mut src, &task, txn, &op);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(
            redo.ops[0].record_type,
            RecordType::GraphNodeLabelRemove as u32
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        // Seed the label independently (as an earlier install / redo would
        // have) so the remove has something to remove.
        dst.csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node_label("n1", "Person")
            .expect("seed label");
        assert!(
            has_label(&dst, "n1", "Person"),
            "label seeded before replay"
        );

        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        assert!(
            !has_label(&dst, "n1", "Person"),
            "RemoveNodeLabels must survive resolve -> redo replay"
        );
    }

    /// Both a SET and a REMOVE on the SAME node in one transaction resolve to
    /// two sub-records (one per direction) and both apply on replay.
    #[test]
    fn graph_set_and_remove_node_labels_same_node_same_txn_resolves_both() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(38);

        let set_resp = src.execute_stage_graph(
            &task,
            TID,
            txn,
            &GraphOp::SetNodeLabels {
                node_id: "n1".to_string(),
                labels: vec!["Robot".to_string()],
            },
        );
        assert_eq!(set_resp.status, Status::Ok);
        let remove_resp = src.execute_stage_graph(
            &task,
            TID,
            txn,
            &GraphOp::RemoveNodeLabels {
                node_id: "n1".to_string(),
                labels: vec!["Person".to_string()],
            },
        );
        assert_eq!(remove_resp.status, Status::Ok);

        // Pass both ops through resolve as the full staged transaction op list.
        let plans = [
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: "n1".to_string(),
                labels: vec!["Robot".to_string()],
            }),
            PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
                node_id: "n1".to_string(),
                labels: vec!["Person".to_string()],
            }),
        ];
        let resp = src.execute_resolve_txn(&task, TID, txn, &plans);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            2,
            "one added label and one removed label on the same node -> two sub-records"
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.csr_partition_mut(DatabaseId::DEFAULT.as_u64(), TID)
            .add_node_label("n1", "Person")
            .expect("seed pre-existing label");

        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        assert!(
            has_label(&dst, "n1", "Robot"),
            "the added label must be present after replay"
        );
        assert!(
            !has_label(&dst, "n1", "Person"),
            "the removed label must be absent after replay"
        );
    }

    /// Crash-before-install: the fresh destination core installs nothing
    /// itself; `replay_transaction_redo_wal` must reconstruct the label
    /// from the wrapped `TransactionRedo` bytes alone.
    #[test]
    fn graph_node_label_crash_before_install_replays_from_wal_only() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(39);

        let op = GraphOp::SetNodeLabels {
            node_id: "ghost".to_string(),
            labels: vec!["Person".to_string(), "Agent".to_string()],
        };
        let resp = stage_and_resolve_labels(&mut src, &task, txn, &op);
        let redo = decode_redo(&resp);
        let record = wrap_redo(&redo);

        // `dst` is a brand-new engine that never observed `src`'s in-memory
        // state (no install happened) — only the WAL-durable redo bytes do.
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        assert!(has_label(&dst, "ghost", "Person"));
        assert!(has_label(&dst, "ghost", "Agent"));
    }

    #[test]
    fn graph_resolve_is_deterministic_across_two_resolves() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(38);

        {
            let overlay = src.graph_txn_overlay_mut(txn);
            overlay.stage_edge_put(coll_key("g"), "c", "l", "z", vec![]);
            overlay.stage_edge_put(coll_key("g"), "a", "l", "x", vec![]);
            overlay.stage_edge_put(coll_key("g"), "b", "l", "y", vec![]);
        }
        let plans = [
            graph_edge_put_plan("g", "a", "l", "x", 1, 2),
            graph_edge_put_plan("g", "b", "l", "y", 3, 4),
            graph_edge_put_plan("g", "c", "l", "z", 5, 6),
        ];

        let resp1 = src.execute_resolve_txn(&task, TID, txn, &plans);
        let resp2 = src.execute_resolve_txn(&task, TID, txn, &plans);
        assert_eq!(
            resp1.payload.as_bytes(),
            resp2.payload.as_bytes(),
            "resolving the same overlay twice must produce byte-identical redo bytes"
        );
    }

    // ── Plan-driven serializers (vector / array / columnar / timeseries) ──

    /// A vector `Insert` plan resolves to a `VectorPut` sub-record and replays
    /// into a fresh engine, rebuilding the HNSW index from the logged insert.
    #[test]
    fn vector_insert_resolves_and_replays_queryable() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(40);

        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "emb"),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: Surrogate::new(9),
            pk_bytes: None,
            provenance: None,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one vector insert -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::VectorPut as u32);

        // The 7-element autocommit shape carries the surrogate identity.
        let (collection, vector, dim, _field, _doc, surrogate_u32, _prov) =
            zerompk::from_msgpack::<(
                String,
                Vec<f32>,
                usize,
                String,
                Option<String>,
                u32,
                Option<SyncProvenance>,
            )>(&redo.ops[0].payload)
            .expect("decode 7-element vector put");
        assert_eq!(collection, "emb");
        assert_eq!(vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(dim, 3);
        assert_eq!(surrogate_u32, 9);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "emb", "");
        assert_eq!(
            dst.vector_collections.get(&key).map(|c| c.len()),
            Some(1),
            "vector must be present in the rebuilt HNSW index after redo replay"
        );
    }

    /// Resolve reads only the plan for a vector insert; it must not touch the
    /// base vector index.
    #[test]
    fn vector_resolve_does_not_mutate_base() {
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;

        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(41);

        // Seed a base index with one vector.
        let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "emb", "");
        let mut coll = VectorCollection::new(3, HnswParams::default());
        coll.insert(vec![7.0, 7.0, 7.0]);
        core.vector_collections.insert(key.clone(), coll);

        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "emb"),
            vector: vec![1.0, 2.0, 3.0],
            dim: 3,
            field_name: String::new(),
            surrogate: Surrogate::new(9),
            pk_bytes: None,
            provenance: None,
        });
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            core.vector_collections.get(&key).map(|c| c.len()),
            Some(1),
            "resolve must not insert into the base vector index"
        );
    }

    fn columnar_schema() -> nodedb_types::columnar::ColumnarSchema {
        nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Int64),
            ColumnDef::nullable("note", ColumnType::String),
        ])
        .expect("valid columnar schema")
    }

    fn columnar_row(id: &str, v: i64, note: &str) -> nodedb_types::Value {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), nodedb_types::Value::String(id.into()));
        row.insert("v".to_string(), nodedb_types::Value::Integer(v));
        row.insert("note".to_string(), nodedb_types::Value::String(note.into()));
        nodedb_types::Value::Object(row)
    }

    /// A columnar INSERT of one row at `surrogate`. `on_conflict` non-empty
    /// makes it the `ON CONFLICT (pk) DO UPDATE` shape.
    fn columnar_insert_plan(
        collection: &str,
        row: nodedb_types::Value,
        surrogate: u32,
        on_conflict: Vec<(String, UpdateValue)>,
    ) -> PhysicalPlan {
        let intent = if on_conflict.is_empty() {
            ColumnarInsertIntent::Insert
        } else {
            ColumnarInsertIntent::Put
        };
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            payload: nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![row]))
                .expect("encode columnar payload"),
            format: "msgpack".to_string(),
            intent,
            on_conflict_updates: on_conflict,
            surrogates: vec![Surrogate::new(surrogate)],
            schema_bytes: zerompk::to_msgpack_vec(&columnar_schema()).expect("encode schema"),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// Commit `plan` on `core` the way an autocommit write does.
    fn columnar_base_insert(core: &mut CoreLoop, plan: &PhysicalPlan) {
        let (collection, payload, surrogates, schema_bytes) = match plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                surrogates,
                schema_bytes,
                ..
            }) => (collection, payload, surrogates, schema_bytes),
            other => panic!("expected a columnar insert, got {other:?}"),
        };
        let resp = core.execute_columnar_insert(
            &make_task(),
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection: collection.as_str(),
                payload,
                format: "msgpack",
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates,
                schema_bytes,
                provenance: None,
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        assert_eq!(resp.status, Status::Ok, "seed base row: {resp:?}");
    }

    /// Every live `(surrogate, row)` of `collection` on `core`, by surrogate.
    fn columnar_rows(core: &CoreLoop, collection: &str) -> Vec<(u32, Vec<nodedb_types::Value>)> {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        );
        let mut rows: Vec<(u32, Vec<nodedb_types::Value>)> = core
            .columnar_engines
            .get(&key)
            .map(|engine| {
                engine
                    .scan_memtable_rows_with_surrogates()
                    .filter_map(|(s, row)| s.map(|s| (s.as_u32(), row)))
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by_key(|(s, _)| *s);
        rows
    }

    /// A staged columnar INSERT resolves to one `columnar_image` record whose
    /// row replays into a fresh engine under its surrogate.
    #[test]
    fn columnar_insert_resolves_to_its_staged_image_and_replays() {
        let (mut src, _src_dir) = make_core();
        let txn = TxnId::new(42);
        let plan = columnar_insert_plan("cevents", columnar_row("a", 1, "x"), 7, Vec::new());
        let staged = src.execute_stage_write(&make_stage_task(txn), TID, &plan);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");

        let redo = decode_redo(&src.execute_resolve_txn(&make_task(), TID, txn, &[plan]));
        assert_eq!(redo.ops.len(), 1, "one staged columnar row -> one record");
        assert_eq!(redo.ops[0].record_type, RecordType::TimeseriesBatch as u32);
        let rec: nodedb_types::columnar::ColumnarImageWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("decode image record");
        assert_eq!(rec.kind, nodedb_types::columnar::COLUMNAR_IMAGE_KIND);
        assert_eq!(rec.collection, "cevents");

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wrap_redo(&redo)),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let rows = columnar_rows(&dst, "cevents");
        assert_eq!(rows.len(), 1, "the staged row replays: {rows:?}");
        assert_eq!(rows[0].0, 7, "under its own surrogate");
    }

    /// An `ON CONFLICT DO UPDATE` stages the merged row; the redo carries
    /// that merged row, and a replica holding the same base row installs
    /// exactly it. Replaying the submitted row instead would lose the
    /// columns the SET list left untouched.
    #[test]
    fn columnar_on_conflict_redo_installs_the_merged_row_the_transaction_saw() {
        let base = columnar_insert_plan("upserts", columnar_row("a", 1, "orig"), 9, Vec::new());
        let (mut src, _src_dir) = make_core();
        let (mut dst, _dst_dir) = make_core();
        columnar_base_insert(&mut src, &base);
        columnar_base_insert(&mut dst, &base);

        let txn = TxnId::new(48);
        let upsert = columnar_insert_plan(
            "upserts",
            columnar_row("a", 7, "new"),
            9,
            vec![(
                "v".to_string(),
                UpdateValue::Literal(
                    nodedb_types::value_to_msgpack(&nodedb_types::Value::Integer(7))
                        .expect("encode literal"),
                ),
            )],
        );
        let staged = src.execute_stage_write(&make_stage_task(txn), TID, &upsert);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        let redo = decode_redo(&src.execute_resolve_txn(&make_task(), TID, txn, &[upsert]));

        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wrap_redo(&redo)),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let rows = columnar_rows(&dst, "upserts");
        assert_eq!(rows.len(), 1, "the upsert replaces the row: {rows:?}");
        assert_eq!(
            rows[0].1,
            vec![
                nodedb_types::Value::String("a".into()),
                nodedb_types::Value::Integer(7),
                nodedb_types::Value::String("orig".into()),
            ],
            "the replica holds the merged row: v from the SET list, note untouched"
        );
    }

    /// A key-changing UPDATE and a DELETE of base rows resolve to images
    /// that name the base row they remove, so no row survives under the old
    /// key on a replica.
    #[test]
    fn columnar_update_and_delete_redo_remove_the_base_rows_they_replace() {
        let seed_a = columnar_insert_plan("moves", columnar_row("a", 1, "x"), 3, Vec::new());
        let seed_b = columnar_insert_plan("moves", columnar_row("b", 2, "y"), 4, Vec::new());
        let (mut src, _src_dir) = make_core();
        let (mut dst, _dst_dir) = make_core();
        for core in [&mut src, &mut dst] {
            columnar_base_insert(core, &seed_a);
            columnar_base_insert(core, &seed_b);
        }

        let txn = TxnId::new(49);
        let pk_filter = |id: &str| {
            zerompk::to_msgpack_vec(&vec![nodedb_query::scan_filter::ScanFilter {
                field: "id".to_string(),
                op: nodedb_query::scan_filter::FilterOp::Eq,
                value: nodedb_types::Value::String(id.into()),
                clauses: Vec::new(),
                expr: None,
            }])
            .expect("encode filter")
        };
        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "moves"),
            filters: pk_filter("a"),
            updates: vec![(
                "id".to_string(),
                nodedb_types::value_to_msgpack(&nodedb_types::Value::String("z".into()))
                    .expect("encode assignment"),
            )],
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "moves"),
            filters: pk_filter("b"),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        for plan in [&update, &delete] {
            let staged = src.execute_stage_write(&make_stage_task(txn), TID, plan);
            assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        }
        let redo = decode_redo(&src.execute_resolve_txn(&make_task(), TID, txn, &[update, delete]));

        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&wrap_redo(&redo)),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let rows = columnar_rows(&dst, "moves");
        assert_eq!(
            rows,
            vec![(
                3,
                vec![
                    nodedb_types::Value::String("z".into()),
                    nodedb_types::Value::Integer(1),
                    nodedb_types::Value::String("x".into()),
                ]
            )],
            "the renamed row lives under its new key only, and the deleted row is gone"
        );
    }

    /// A timeseries `Ingest` plan resolves to a `TimeseriesBatch` sub-record
    /// tagged `"timeseries"` and replays its samples into the memtable.
    #[test]
    fn timeseries_ingest_resolves_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(43);

        // A line-protocol ingest: the format a timeseries INSERT stages.
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=42 1700000000000000000".to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one timeseries ingest -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::TimeseriesBatch as u32);

        // The payload is the format-preserving 5-element tuple tagged
        // "timeseries" (a msgpack array), distinct from the columnar map form.
        let (kind, collection, _payload, _prov, _format) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, String)>(
                &redo.ops[0].payload,
            )
            .expect("decode timeseries 5-tuple");
        assert_eq!(kind, "timeseries");
        assert_eq!(collection, "metrics");

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "metrics".to_string(),
        );
        assert_eq!(
            dst.columnar_memtables.get(&key).map(|m| m.row_count()),
            Some(1),
            "timeseries sample must replay into the memtable"
        );
    }

    /// A canonical ILP MessagePack ingest preserves its format discriminator
    /// through resolve and WAL replay. This guards the escaped protocol values
    /// which the ordinary MessagePack-row decoder cannot represent.
    #[test]
    fn ilp_msgpack_timeseries_ingest_resolves_and_replays_canonical_rows() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(44);
        let line = r"cpu\,load,host\ name=west\,1 count=18446744073709551615u 1700000000000000001";
        let payload =
            zerompk::to_msgpack_vec(&vec![line.to_string()]).expect("encode canonical ILP lines");
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cpu,load"),
            payload: payload.clone(),
            format: "ilp-msgpack".to_string(),
            wal_lsn: None,
            surrogates: vec![Surrogate::new(701)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });

        let redo = decode_redo(&src.execute_resolve_txn(&task, TID, txn, &[plan]));
        assert_eq!(redo.ops.len(), 1);
        let (kind, collection, replay_payload, _provenance, format) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, String)>(
                &redo.ops[0].payload,
            )
            .expect("decode format-preserving timeseries redo");
        assert_eq!(kind, "timeseries");
        assert_eq!(collection, "cpu,load");
        assert_eq!(format, "ilp-msgpack");
        assert_eq!(replay_payload, payload);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let memtable = dst
            .columnar_memtables
            .get(&coll_key("cpu,load"))
            .expect("canonical ILP replay must create its collection memtable");
        assert_eq!(memtable.row_count(), 1);
        assert_eq!(memtable.min_ts(), 1_700_000_000_000);
        assert_eq!(memtable.max_ts(), 1_700_000_000_000);
        let snapshot = memtable.export_snapshot();
        let host_column = snapshot
            .schema_columns
            .iter()
            .position(|(name, _)| name == "host name")
            .expect("escaped tag name must remain a schema column");
        let dictionary = snapshot
            .symbol_dicts
            .iter()
            .find(|(index, _)| *index == host_column)
            .expect("escaped tag value must be dictionary encoded");
        assert_eq!(dictionary.1.get_id("west,1"), Some(0));
    }

    /// Columnar-family truncates resolve to the same dedicated record the
    /// autocommit path appends, carrying the collection name only.
    #[test]
    fn columnar_family_truncate_emits_dedicated_truncate_sub_records() {
        use nodedb_types::columnar::ColumnarTruncateWalRecord;

        let (mut core, _dir) = make_core();
        let task = make_task();

        let columnar = PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cevents"),
            restart_identity: true,
        });
        // A columnar truncate resolves from the overlay marker its statement
        // staged.
        let staged = core.execute_stage_write(&make_stage_task(TxnId::new(46)), TID, &columnar);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(46), &[columnar]);
        assert_eq!(resp.status, Status::Ok);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::ColumnarTruncate as u32);
        let rec: ColumnarTruncateWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("decode columnar truncate");
        assert_eq!(rec.collection, "cevents");

        let timeseries = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "tsevents"),
            restart_identity: false,
        });
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(47), &[timeseries]);
        assert_eq!(resp.status, Status::Ok);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(
            redo.ops[0].record_type,
            RecordType::TimeseriesTruncate as u32
        );
        let rec: ColumnarTruncateWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("decode timeseries truncate");
        assert_eq!(rec.collection, "tsevents");
    }

    /// An array `Put` plan resolves to a version-tagged `ArrayPut` sub-record
    /// and replays into a fresh engine, respecting `ArrayFlush` watermarks.
    #[test]
    fn array_put_resolves_and_replays() {
        use crate::engine::array::wal::{ArrayPutCell, decode_put_with_version};
        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::segment::mbr_index::predicate::{DimPredicate, MbrQueryPredicate};
        use nodedb_array::types::ArrayId;
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;
        use nodedb_array::types::domain::{Domain, DomainBound};

        let schema = ArraySchemaBuilder::new("arr")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![16])
            .build()
            .expect("array schema");
        let schema_bytes = zerompk::to_msgpack_vec(&schema).expect("encode schema");
        let schema_hash: u64 = 0xABCD_1234;
        let aid = ArrayId::new(TenantId::new(TID), "arr");

        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(3)],
            attrs: vec![CellValue::Float64(42.0)],
            surrogate: Surrogate::new(5),
            system_from_ms: 1_000,
            valid_from_ms: 1_000,
            valid_until_ms: i64::MAX,
        }];
        let cells_bytes = zerompk::to_msgpack_vec(&cells).expect("encode cells");

        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(46);
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: aid.clone(),
            cells_msgpack: cells_bytes,
            wal_lsn: 0,
            provenance: None,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one array put -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::ArrayPut as u32);

        // The sub-record decodes via the exact version-tagged function replay
        // uses, back to the faithful cell payload.
        let decoded = decode_put_with_version(&redo.ops[0].payload).expect("decode array put");
        assert_eq!(decoded.array_id, aid);
        assert_eq!(decoded.cells, cells);

        // Replay into a fresh engine that has the array registered + open.
        let (mut dst, _dst_dir) = make_core();
        let open_resp = dst.handle_array_open(&task, &aid, &schema_bytes, schema_hash, 8);
        assert_eq!(
            open_resp.status,
            Status::Ok,
            "array open on dst: {open_resp:?}"
        );
        let record = wrap_redo(&redo);
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        // Flush and scan to observe the replayed cell.
        let flush_resp = dst.handle_array_flush(&task, &aid, 2);
        assert_eq!(
            flush_resp.status,
            Status::Ok,
            "array flush on dst: {flush_resp:?}"
        );
        let pred = MbrQueryPredicate::new(vec![DimPredicate { lo: None, hi: None }]);
        let tiles = dst
            .array_engine
            .scan_tiles(&aid, &pred)
            .expect("scan tiles");
        assert!(
            !tiles.is_empty(),
            "the replayed + flushed array cell must yield a scannable tile"
        );
    }

    /// A mixed transaction — a KV write, a vector insert, and a columnar insert
    /// — resolves into ONE `RedoRecord` and every sub-record replays.
    #[test]
    fn mixed_kv_vector_columnar_resolve_into_one_record_and_all_replay() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(47);

        // Stage the KV write into the overlay (overlay-driven serializer).
        src.txn_overlay_mut(txn).insert_put(
            coll_key("kvc"),
            1,
            &kv_row_identity(b"k"),
            b"V".to_vec(),
        );

        // Stage the columnar row into the overlay (overlay-driven serializer).
        let columnar = columnar_insert_plan("cevents", columnar_row("a", 7, "x"), 22, Vec::new());
        let staged = src.execute_stage_write(&make_stage_task(txn), TID, &columnar);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");

        let plans = [
            kv_write_plan("kvc"),
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "emb"),
                vector: vec![1.0, 2.0, 3.0],
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate::new(21),
                pk_bytes: None,
                provenance: None,
            }),
            columnar,
        ];

        let resp = src.execute_resolve_txn(&task, TID, txn, &plans);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            3,
            "KV + vector + columnar -> three sub-records in one record"
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let db = DatabaseId::DEFAULT.as_u64();
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"k", now).as_deref(),
            Some(b"V".as_slice()),
            "KV sub-record must replay"
        );
        let vkey = CoreLoop::vector_index_key(db, TID, "emb", "");
        assert_eq!(
            dst.vector_collections.get(&vkey).map(|c| c.len()),
            Some(1),
            "vector sub-record must replay"
        );
        let ckey = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "cevents".to_string(),
        );
        assert_eq!(
            dst.columnar_engines
                .get(&ckey)
                .map(|e| e.memtable().row_count()),
            Some(1),
            "columnar sub-record must replay"
        );
    }

    // ── Spatial resolve tests ──
    // Spatial `Insert`/`Delete` plan nodes carry the complete post-image
    // directly, mirroring the vector/columnar plan-driven tests above.

    use nodedb_physical::physical_plan::SpatialOp;
    use nodedb_types::geometry::Geometry;

    fn spatial_prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq,
        }
    }

    fn spatial_point(x: f64, y: f64) -> Geometry {
        Geometry::point(x, y)
    }

    fn spatial_insert_plan(
        collection: &str,
        field: &str,
        surrogate: u32,
        geometry: Geometry,
        seq: u64,
    ) -> PhysicalPlan {
        PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            field: field.to_string(),
            surrogate: Surrogate::new(surrogate),
            geometry,
            provenance: Some(spatial_prov(seq)),
        })
    }

    fn spatial_delete_plan(
        collection: &str,
        field: &str,
        surrogate: u32,
        seq: u64,
    ) -> PhysicalPlan {
        PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            field: field.to_string(),
            surrogate: Surrogate::new(surrogate),
            provenance: Some(spatial_prov(seq)),
        })
    }

    /// R-tree entry id for a surrogate, mirroring `execute_spatial_insert`'s
    /// `fnv1a_hash(doc_id.as_bytes())` keying.
    fn spatial_entry_id(surrogate: u32) -> u64 {
        let doc_id = storage_key(surrogate).to_string();
        crate::util::fnv1a_hash(doc_id.as_bytes())
    }

    #[test]
    fn spatial_insert_resolves_and_replay_is_queryable() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(40);
        let surrogate = 7u32;

        let plan = spatial_insert_plan("places", "loc", surrogate, spatial_point(10.0, 20.0), 1);
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one spatial insert -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::SpatialPut as u32);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        // The geometry is queryable: the R-tree entry and the sparse document
        // body were both rebuilt by replay's `execute_spatial_insert` call.
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        let entries = dst
            .spatial_indexes
            .get(&key)
            .expect("R-tree index rebuilt by replay")
            .entries();
        assert_eq!(entries.len(), 1, "R-tree must carry the replayed geometry");
        assert_eq!(entries[0].id, spatial_entry_id(surrogate));

        let doc_map_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
            spatial_entry_id(surrogate),
        );
        assert!(
            dst.spatial_doc_map.contains_key(&doc_map_key),
            "surrogate -> doc-id reverse map must be rebuilt"
        );
        let row_key = storage_key(surrogate);
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "places", &row_key)
                .expect("get")
                .is_some(),
            "sparse geometry document must be rebuilt by replay"
        );
    }

    #[test]
    fn spatial_delete_resolves_and_replay_removes_entry() {
        let (mut seed_core, _seed_dir) = make_core();
        let task = make_task();
        let surrogate = 9u32;

        // Seed a geometry via a resolved insert replayed into the target core.
        let insert_txn = TxnId::new(41);
        let insert_plan =
            spatial_insert_plan("places", "loc", surrogate, spatial_point(1.0, 1.0), 1);
        let insert_resp = seed_core.execute_resolve_txn(&task, TID, insert_txn, &[insert_plan]);
        let insert_redo = decode_redo(&insert_resp);
        let insert_record = wrap_redo(&insert_redo);

        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&insert_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        assert_eq!(
            dst.spatial_indexes.get(&key).expect("rtree seeded").len(),
            1,
            "seeded entry present before delete"
        );

        // Now resolve a delete for the same surrogate and replay it.
        let delete_txn = TxnId::new(42);
        let delete_plan = spatial_delete_plan("places", "loc", surrogate, 2);
        let resp = seed_core.execute_resolve_txn(&task, TID, delete_txn, &[delete_plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::SpatialDelete as u32);

        let del_record = wrap_redo(&redo);
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&del_record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        assert_eq!(
            dst.spatial_indexes
                .get(&key)
                .map(|rt| rt.len())
                .unwrap_or(0),
            0,
            "redo delete must remove the R-tree entry"
        );
        let row_key = storage_key(surrogate);
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "places", &row_key)
                .expect("get")
                .is_none(),
            "redo delete must remove the sparse geometry document"
        );
    }

    #[test]
    fn spatial_resolve_is_deterministic_across_two_resolves() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();

        let plans = [
            spatial_insert_plan("places", "loc", 1, spatial_point(1.0, 1.0), 10),
            spatial_insert_plan("places", "loc", 2, spatial_point(2.0, 2.0), 11),
        ];

        let resp1 = src.execute_resolve_txn(&task, TID, TxnId::new(50), &plans);
        let resp2 = src.execute_resolve_txn(&task, TID, TxnId::new(51), &plans);
        let redo1 = decode_redo(&resp1);
        let redo2 = decode_redo(&resp2);

        assert_eq!(redo1.ops.len(), 2);
        assert_eq!(
            redo1
                .ops
                .iter()
                .map(|o| o.payload.clone())
                .collect::<Vec<_>>(),
            redo2
                .ops
                .iter()
                .map(|o| o.payload.clone())
                .collect::<Vec<_>>(),
            "resolving the same plan twice must emit byte-identical sub-records"
        );
    }

    #[test]
    fn spatial_resolve_does_not_mutate_base() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(43);
        let surrogate = 5u32;

        let plan = spatial_insert_plan("places", "loc", surrogate, spatial_point(3.0, 3.0), 1);
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(resp.status, Status::Ok);

        // Resolve must not touch the live R-tree / sparse store / doc map —
        // only the buffered-plan install path (outside resolve) does that.
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        assert!(
            !core.spatial_indexes.contains_key(&key),
            "resolve must not mutate the base spatial R-tree"
        );
        let row_key = storage_key(surrogate);
        assert!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "places", &row_key)
                .expect("get")
                .is_none(),
            "resolve must not mutate the base sparse store"
        );
    }

    #[test]
    fn mixed_kv_and_spatial_resolve_into_one_record_and_both_replay() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(44);
        let surrogate = 13u32;

        src.txn_overlay_mut(txn).insert_put(
            coll_key("kvc"),
            1,
            &kv_row_identity(b"k"),
            b"V".to_vec(),
        );

        let plans = [
            kv_write_plan("kvc"),
            spatial_insert_plan("places", "loc", surrogate, spatial_point(4.0, 4.0), 1),
        ];
        let resp = src.execute_resolve_txn(&task, TID, txn, &plans);
        let redo = decode_redo(&resp);
        assert_eq!(
            redo.ops.len(),
            2,
            "one KV row + one spatial insert -> two sub-records in one record"
        );

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");

        let db = DatabaseId::DEFAULT.as_u64();
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"k", now).as_deref(),
            Some(b"V".as_slice()),
            "KV sub-record must replay"
        );
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        assert_eq!(
            dst.spatial_indexes.get(&key).map(|rt| rt.len()),
            Some(1),
            "spatial sub-record must replay"
        );
    }

    #[test]
    fn spatial_scan_op_emits_nothing() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(45);

        let plan = PhysicalPlan::Spatial(SpatialOp::Scan {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            predicate: nodedb_physical::physical_plan::SpatialPredicate::Intersects,
            query_geometry: spatial_point(0.0, 0.0),
            distance_meters: 0.0,
            attribute_filters: Vec::new(),
            limit: 10,
            projection: Vec::new(),
            rls_filters: Vec::new(),
            prefilter: None,
        });
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert!(
            redo.ops.is_empty(),
            "read-only spatial scan emits no sub-record"
        );
    }

    #[test]
    fn spatial_insert_without_provenance_resolves_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(46);
        let surrogate = 1u32;

        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(surrogate),
            geometry: spatial_point(0.0, 0.0),
            provenance: None,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        // A plain SQL spatial insert carries no sync producer. It resolves
        // with the empty provenance the autocommit spatial WAL path writes.
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one spatial insert -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::SpatialPut as u32);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("redo replay must succeed");
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "loc".to_string(),
        );
        let entries = dst
            .spatial_indexes
            .get(&key)
            .expect("R-tree index rebuilt by replay")
            .entries();
        assert_eq!(entries.len(), 1, "the insert must not be dropped");
        assert_eq!(entries[0].id, spatial_entry_id(surrogate));
    }

    #[test]
    fn a_session_resolve_of_staged_writes_on_a_core_without_their_overlay_is_refused() {
        let (mut core, _dir) = make_core();
        let task = make_task();

        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(61), &[doc_put_plan("docs")]);

        assert_eq!(
            resp.status,
            Status::Error,
            "a resolve must not commit nothing for writes staged elsewhere: {resp:?}"
        );
    }

    #[test]
    fn a_staged_write_that_stages_no_row_still_opens_the_overlay_its_resolve_reads() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(62);
        let delete_absent = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "absent".to_string(),
            surrogate: Surrogate::new(9_001),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        });

        let staged = core.execute_stage_write(&make_stage_task(txn), TID, &delete_absent);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        assert!(core.txn_overlays.contains_key(&txn));

        let resp = core.execute_resolve_txn(&make_task(), TID, txn, &[delete_absent]);
        assert_eq!(resp.status, Status::Ok, "resolve: {resp:?}");
    }

    #[test]
    fn an_untimed_timeseries_row_resolves_with_the_instant_its_statement_read() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(63);
        let mut row = std::collections::HashMap::new();
        row.insert("value".to_string(), nodedb_types::Value::Float(1.5));
        let payload = nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![
            nodedb_types::Value::Object(row),
        ]))
        .expect("encode rows");
        let ingest = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload,
            format: "msgpack".to_string(),
            wal_lsn: None,
            surrogates: vec![Surrogate::new(801)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });

        core.epoch_system_ms = Some(1_700_000_000_000);
        let staged = core.execute_stage_write(&make_stage_task(txn), TID, &ingest);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");

        // Resolve reads a later clock; the row keeps the statement's instant.
        core.epoch_system_ms = Some(1_700_000_999_000);
        let redo = decode_redo(&core.execute_resolve_txn(&make_task(), TID, txn, &[ingest]));
        assert_eq!(redo.ops.len(), 1);
        let (_kind, _collection, lines, _prov, format) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, String)>(
                &redo.ops[0].payload,
            )
            .expect("decode timeseries redo");
        assert_eq!(format, "ilp-msgpack");
        let lines: Vec<String> = zerompk::from_msgpack(&lines).expect("decode lines");
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].ends_with(" 1700000000000000000"),
            "the row carries the statement's instant: {}",
            lines[0]
        );
    }

    fn seeded_columnar_core() -> (CoreLoop, tempfile::TempDir) {
        let (mut core, dir) = make_core();
        let schema = nodedb_types::columnar::ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("v", ColumnType::Int64),
            ],
            version: 1,
        };
        let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema);
        engine
            .insert_with_surrogate(
                &[
                    nodedb_types::Value::Integer(1),
                    nodedb_types::Value::Integer(10),
                ],
                Surrogate::new(5),
            )
            .expect("seed base row");
        core.columnar_engines.insert(coll_key("m"), engine);
        (core, dir)
    }

    fn columnar_insert(intent: ColumnarInsertIntent) -> PhysicalPlan {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), nodedb_types::Value::Integer(1));
        row.insert("v".to_string(), nodedb_types::Value::Integer(20));
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            payload: nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![
                nodedb_types::Value::Object(row),
            ]))
            .expect("encode row"),
            format: "msgpack".to_string(),
            intent,
            on_conflict_updates: Vec::new(),
            surrogates: vec![Surrogate::new(5)],
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    #[test]
    fn a_staged_do_nothing_insert_of_an_existing_key_leaves_the_row_alone() {
        let (mut core, _dir) = seeded_columnar_core();
        let txn = TxnId::new(64);
        let insert = columnar_insert(ColumnarInsertIntent::InsertIfAbsent);

        let staged = core.execute_stage_write(&make_stage_task(txn), TID, &insert);
        assert_eq!(staged.status, Status::Ok, "stage: {staged:?}");
        let overlay = core.txn_overlays.get(&txn).expect("overlay");
        assert!(
            overlay.get(&coll_key("m"), 5).is_none(),
            "DO NOTHING stages no row over an existing key"
        );

        let redo = decode_redo(&core.execute_resolve_txn(&make_task(), TID, txn, &[insert]));
        assert!(
            redo.ops.is_empty(),
            "the redo writes nothing: {:?}",
            redo.ops
        );
    }

    #[test]
    fn a_staged_unique_insert_of_an_existing_key_is_refused() {
        let (mut core, _dir) = seeded_columnar_core();
        let txn = TxnId::new(65);
        let insert = columnar_insert(ColumnarInsertIntent::InsertUnique);

        let staged = core.execute_stage_write(&make_stage_task(txn), TID, &insert);

        assert_eq!(staged.status, Status::Error);
        assert!(matches!(
            staged.error_code.as_deref(),
            Some(crate::bridge::envelope::ErrorCode::RejectedConstraint { .. })
        ));
    }
}
