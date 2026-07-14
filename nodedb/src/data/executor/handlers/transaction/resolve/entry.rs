// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::ResolveTxn` handler: turn a committing transaction's staged
//! post-images into ONE replayable [`RedoRecord`], WITHOUT mutating base.
//!
//! Resolve reads the per-transaction staging overlay (`CoreLoop::txn_overlays`)
//! by shared reference and emits, for every staged KV post-image, the
//! engine-native WAL sub-record shape that engine's autocommit path already
//! produces. The Control Plane appends the returned bytes as a single
//! `RecordType::TransactionRedo` record; a later install phase replays them. No
//! base engine is touched here.
//!
//! Two serializer families live in this module. The overlay-driven family (KV,
//! Document, Graph) reads resolved per-surrogate post-images from the staging
//! overlay. The plan-driven family (Vector, Array, Columnar, Timeseries,
//! Spatial) is not staged into any overlay for redo purposes — vector
//! post-images are inexpressible, array / columnar / timeseries batches ride
//! the buffered-plan path re-running the engine-native batch payload, and
//! spatial `Insert` / `Delete` plan nodes already carry their complete
//! absolute post-image (the overlay `stage_spatial` writes into exists only
//! for same-transaction read-your-own-writes, not for redo — see
//! `resolve/spatial.rs` module docs) — so all four serialize directly from
//! the plan node. Every writing op either serializes to its engine-native
//! sub-record or raises a typed error; none is silently omitted, since
//! dropping an op class would lose those rows on install.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, KvOp, PhysicalPlan};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{BitemporalStamp, Staged};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use crate::wal::{RedoRecord, RedoSubRecord};

use super::graph::EdgeIdentityKey;
use super::{array, columnar, document, graph, kv, spatial, vector};

impl CoreLoop {
    /// Resolve a committing transaction's staged writes into a [`RedoRecord`]
    /// and return its encoded bytes in the response payload. Reads the overlay
    /// by `&` and never mutates any base engine.
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
    /// post-images.
    ///
    /// The plan set is walked once to (a) classify every op exhaustively and
    /// (b) collect the distinct KV collections whose overlay entries must be
    /// serialized. Serialization is overlay-driven: the resolved absolute
    /// post-image (value, tombstone, absolute expiry) lives in the overlay, not
    /// the plan.
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

        // Plan-driven serializers (vector / array / columnar / timeseries) emit
        // directly from the plan node into `ops` during this walk, in plan
        // order. Overlay-driven serializers (KV / document / graph) only
        // collect their touched collections here and are serialized from the
        // overlay in the second phase below. Both append to the same `ops`.
        let mut ops: Vec<RedoSubRecord> = Vec::new();

        for plan in plans {
            match plan {
                // KV: overlay-backed serializer. Read ops stage nothing and are
                // skipped; row-level writes contribute their collection.
                PhysicalPlan::Kv(op) => classify_kv_op(op, &mut kv_collections)?,

                // Document: overlay-backed serializer. Staged point/bulk writes
                // contribute their collection; read-only ops stage nothing;
                // RETURNING variants and join/merge DML have no overlay
                // post-image and raise a typed error.
                PhysicalPlan::Document(op) => classify_document_op(op, &mut doc_collections)?,

                // Graph: overlay-backed serializer for edge puts/deletes. The
                // overlay carries edge identity + properties but not the
                // endpoint surrogates a redo put must emit, so those are
                // collected here from the plan nodes themselves (resolved at
                // plan-construction time) into `edge_surrogates`. Node-label
                // deltas have no redo shape and raise a typed error.
                PhysicalPlan::Graph(op) => {
                    classify_graph_op(op, &mut graph_collections, &mut edge_surrogates)?
                }

                // CRDT deltas ride their own `CrdtDelta` WAL record, never redo
                // sub-records (see `replay_transaction_redo_wal`).
                PhysicalPlan::Crdt(_) => {}

                // FTS postings are re-derived from the owning document at
                // install time, so a text op contributes no redo sub-record.
                PhysicalPlan::Text(_) => {}

                // Read-only families: scans, joins, aggregates, exchange, and
                // maintenance ops carry no persisted post-image.
                PhysicalPlan::Query(_) | PhysicalPlan::Meta(_) => {}

                // Plan-driven serializers. These engines are not staged into a
                // transaction overlay (vector post-images are inexpressible;
                // array / columnar / timeseries batches ride the buffered-plan
                // path), and their redo replay re-runs the engine-native batch
                // payload rather than a per-row shape. So they serialize
                // directly from the plan node in plan order. Each op either
                // emits its engine-native sub-record, skips (reads /
                // maintenance), or raises a typed error (writes with no redo
                // shape — e.g. `Columnar::{Update, Delete}` predicate DML).
                PhysicalPlan::Vector(op) => vector::serialize_vector_op(op, &mut ops)?,
                PhysicalPlan::Array(op) => array::serialize_array_op(op, &mut ops)?,
                PhysicalPlan::Columnar(op) => columnar::serialize_columnar_op(op, &mut ops)?,
                PhysicalPlan::Timeseries(op) => columnar::serialize_timeseries_op(op, &mut ops)?,

                // Spatial `Insert` / `Delete` plan nodes already carry the
                // complete absolute post-image (collection, field, surrogate,
                // geometry, provenance), so — like vector — they serialize
                // directly from the plan node rather than an overlay walk.
                // `Scan` is a read and emits nothing; either write with no
                // sync provenance raises a typed error (see
                // `resolve/spatial.rs` module docs).
                PhysicalPlan::Spatial(op) => spatial::serialize_spatial_op(op, &mut ops)?,

                // Coordinator-only op; never legal on the Data Plane.
                PhysicalPlan::ClusterArray(_) => {
                    return Err(crate::Error::Internal {
                        detail: "cluster-array op reached Data Plane transaction resolve"
                            .to_string(),
                    });
                }
            }
        }

        // Assign the resolve-time bitemporal stamp for every staged `Put` in a
        // `bitemporal=true` document collection, ONCE, and store it in the
        // overlay sidecar. `serialize_document_collection` emits it in the redo
        // 8-tuple and the commit-time base install reads it back out of the same
        // sidecar, so redo and install agree on the version key (a divergent
        // stamp would write a second version on a normal restart). Stamps are
        // assigned in deterministic (collection, doc-id) order so replicas
        // resolving the same transaction produce identical version keys. The
        // monotonic `bitemporal_now_ms` lives on the core, so this is the one
        // place the stamp can be pinned before both the redo and the install.
        for collection in &doc_collections {
            if !self.is_bitemporal(tid, collection) {
                continue;
            }
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.clone(),
            );
            let mut puts: Vec<(String, u32)> = match self.txn_overlays.get(&txn_id) {
                Some(overlay) => overlay
                    .iter_doc_entries_for_collection(&coll_key)
                    .filter_map(|(doc_id, staged)| match staged {
                        Staged::Put(_) => overlay
                            .surrogate_for_doc_id(&coll_key, doc_id)
                            .map(|surrogate| (doc_id.to_string(), surrogate)),
                        Staged::Tombstone => None,
                    })
                    .collect(),
                None => Vec::new(),
            };
            puts.sort();
            let stamps: Vec<(u32, BitemporalStamp)> = puts
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
                let strict_schema = self.resolve_strict_schema(tid, collection);
                document::serialize_document_collection(
                    overlay,
                    &coll_key,
                    collection,
                    strict_schema.as_ref(),
                    &mut ops,
                )?;
            }
        }
        if let Some(graph_overlay) = self.graph_txn_overlays.get(&txn_id) {
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
                    &mut ops,
                )?;
            }
        }
        Ok(ops)
    }
}

/// Classify a KV op for transaction resolve: collect the collection of a
/// row-level write into `collections`, skip read-only ops, and reject the ops
/// that have no row-level redo representation.
fn classify_kv_op(op: &KvOp, collections: &mut BTreeSet<String>) -> crate::Result<()> {
    match op {
        // Row-level writes: the resolved post-image (value or tombstone) is in
        // the overlay, keyed by collection.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Transfer { collection, .. } => {
            collections.insert(collection.clone());
            Ok(())
        }
        // `TransferItem` moves a row across collections: the source holds a
        // staged tombstone and the destination a staged value.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            collections.insert(source_collection.clone());
            collections.insert(dest_collection.clone());
            Ok(())
        }

        // Read-only: nothing staged, nothing to persist.
        KvOp::Get { .. }
        | KvOp::BatchGet { .. }
        | KvOp::Scan { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => Ok(()),

        // TTL-only writes: `Expire` / `Persist` stage a TTL delta with NO value
        // post-image (`stage_kv_ttl.rs`). The KV redo shapes carry a TTL only as
        // the sixth element of a value put, so a standalone TTL change on a base
        // row has no redo representation. Rejecting is deliberate — silently
        // skipping would drop the change from the install path.
        KvOp::Expire { .. } | KvOp::Persist { .. } => Err(crate::Error::PlanError {
            detail: "kv EXPIRE/PERSIST is not supported in transaction resolve".to_string(),
        }),

        // Index / DDL / truncate: never stageable into the overlay, so no
        // row-level redo shape carries them.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::Truncate { .. } => Err(crate::Error::PlanError {
            detail: "kv index/DDL/truncate op is not supported in transaction resolve".to_string(),
        }),
    }
}

/// Classify a Document op for transaction resolve: collect the collection of a
/// staged point/bulk write into `collections`, skip read-only ops, and reject
/// the writes that leave no overlay post-image.
fn classify_document_op(op: &DocumentOp, collections: &mut BTreeSet<String>) -> crate::Result<()> {
    match op {
        // Staged writes (`is_point_write`): the resolved post-image (value or
        // tombstone) is in the overlay, keyed by the user primary key. A
        // `RETURNING` clause does not affect staging — the `stage_*` handlers
        // record the matched rows' post-images identically whether or not one
        // is present — so these serialize from the overlay like any other
        // point/bulk write. The RETURNING projection itself is a
        // response-shape concern the Control Plane already discards inside a
        // transaction; it leaves no separate post-image to carry here.
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. } => {
            collections.insert(collection.clone());
            Ok(())
        }
        // `INSERT ... SELECT` stages the copied rows into the target collection.
        DocumentOp::InsertSelect {
            target_collection, ..
        } => {
            collections.insert(target_collection.clone());
            Ok(())
        }

        // Read-only families: scans, lookups, point-gets, and estimates carry
        // no persisted post-image.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. } => Ok(()),

        // Join/merge/batch DML is never staged: `UpdateFromJoin` and `Merge`
        // resolve a multi-row cross-collection effect that has no
        // per-surrogate absolute post-image, and `BatchInsert` rides the
        // buffered-plan path rather than the overlay. The overlay holds no
        // post-image for them, so rejecting keeps their rows out of a silently
        // lossy redo record. This is permanent — not "not yet supported".
        DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::BatchInsert { .. } => Err(crate::Error::PlanError {
            detail: "document join/merge/batch DML has no staged post-image and is not \
                     supported in transaction resolve"
                .to_string(),
        }),

        // Index / DDL / truncate: never stageable into the overlay, so no
        // row-level redo shape carries them.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. } => Err(crate::Error::PlanError {
            detail: "document index/DDL/truncate op is not supported in transaction resolve"
                .to_string(),
        }),
    }
}

/// Classify a Graph op for transaction resolve: collect the collection of a
/// staged edge write into `collections` (so the serializer walks its
/// overlay), collect the endpoint surrogates of every staged edge PUT into
/// `edge_surrogates` (the overlay itself carries only identity + properties,
/// not surrogates — see `resolve/graph.rs` module docs), skip read-only
/// traversal/algorithm ops, and reject node-label ops that have no redo
/// sub-record shape.
fn classify_graph_op(
    op: &GraphOp,
    collections: &mut BTreeSet<String>,
    edge_surrogates: &mut BTreeMap<EdgeIdentityKey, (u32, u32)>,
) -> crate::Result<()> {
    match op {
        // Edge put: the overlay holds the resolved post-image (identity +
        // properties); the endpoint surrogates are resolved once at
        // construction time and only live here on the plan node.
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        } => {
            collections.insert(collection.clone());
            edge_surrogates.insert(
                (
                    collection.clone(),
                    src_id.clone(),
                    label.clone(),
                    dst_id.clone(),
                ),
                (src_surrogate.as_u32(), dst_surrogate.as_u32()),
            );
            Ok(())
        }
        GraphOp::EdgePutBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.clone());
                edge_surrogates.insert(
                    (
                        edge.collection.clone(),
                        edge.src_id.clone(),
                        edge.label.clone(),
                        edge.dst_id.clone(),
                    ),
                    (edge.src_surrogate.as_u32(), edge.dst_surrogate.as_u32()),
                );
            }
            Ok(())
        }

        // Edge delete: the redo delete tuple carries no surrogate, so only
        // the collection is needed to walk the overlay's tombstone set.
        GraphOp::EdgeDelete { collection, .. } => {
            collections.insert(collection.clone());
            Ok(())
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.clone());
            }
            Ok(())
        }

        // Read-only families: traversal, pattern matching, algorithms, and
        // stats carry no persisted post-image.
        GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => Ok(()),

        // Node-label mutations stage a delta (added/removed sets), not an
        // absolute post-image. `RecordType::GraphNodeLabelSet` /
        // `GraphNodeLabelRemove` exist for the autocommit path
        // (`wal_replay_graph_labels.rs`), but no `RedoSubRecord` shape or
        // decoder exists yet for a delta-shaped node-label mutation staged
        // inside a transaction (see `resolve/graph.rs`'s module doc).
        // Silently omitting the change from the redo record would lose it on
        // install, so this is a typed error.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => {
            Err(crate::Error::PlanError {
                detail: "graph node-label ops have no redo sub-record shape and are not \
                         supported in transaction resolve"
                    .to_string(),
            })
        }
    }
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
    use nodedb_types::Surrogate;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::sync::wire::SyncProvenance;

    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::data::executor::strict_format;
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::transaction::overlay::{Staged, StagedTtl};
    use crate::data::executor::handlers::transaction::stage_write::hex_key;
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

    /// Decode the `RedoRecord` bytes carried in a resolve response payload.
    fn decode_redo(resp: &crate::bridge::envelope::Response) -> RedoRecord {
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record")
    }

    /// A resolve plan that names `collection` as a KV write so the serializer
    /// picks up that collection's overlay entries.
    fn kv_write_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.to_string(),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
        })
    }

    /// Decode a six-element `kv_put` redo payload.
    fn decode_kv_put6(payload: &[u8]) -> (String, Vec<u8>, Vec<u8>, u64, u64) {
        let (disc, collection, key, value, ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, u64)>(payload)
                .expect("decode 6-element kv_put");
        assert_eq!(disc, "kv_put");
        (collection, key, value, ttl_ms, expire_at_ms)
    }

    /// Decode a five-element `kv_put` redo payload.
    fn decode_kv_put5(payload: &[u8]) -> (String, Vec<u8>, Vec<u8>, u64) {
        let (disc, collection, key, value, ttl_ms) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64)>(payload)
                .expect("decode 5-element kv_put");
        assert_eq!(disc, "kv_put");
        (collection, key, value, ttl_ms)
    }

    #[test]
    fn incr_resolves_to_absolute_value_not_delta() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(1);

        // Two Incrs in one transaction: 0 + 40, then + 2 = 42. The overlay slot
        // holds the resolved ABSOLUTE value (42), not either delta.
        for delta in [40i64, 2] {
            let resp = core.execute_stage_kv(
                &task,
                TID,
                txn,
                &KvOp::Incr {
                    collection: "counters".to_string(),
                    key: b"c".to_vec(),
                    delta,
                    ttl_ms: 0,
                    surrogate: Surrogate::ZERO,
                },
            );
            assert_eq!(resp.status, Status::Ok, "stage incr: {resp:?}");
        }

        // The overlay's staged bytes are the resolved absolute post-image.
        let overlay_bytes = match core
            .txn_overlays
            .get(&txn)
            .and_then(|o| o.get_by_doc_id(&coll_key("counters"), &hex_key(b"c")))
            .expect("staged incr present")
        {
            Staged::Put(v) => v.clone(),
            Staged::Tombstone => panic!("incr must stage a value"),
        };

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("counters")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one staged KV row -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, _ttl) = decode_kv_put5(&redo.ops[0].payload);
        assert_eq!(collection, "counters");
        assert_eq!(key, b"c");
        // The emitted value is the overlay's absolute post-image, and it decodes
        // to 42 — not the last delta (2) nor the first (40).
        assert_eq!(value, overlay_bytes);
        assert_eq!(
            zerompk::from_msgpack::<i64>(&value).expect("i64"),
            42,
            "resolve carries the absolute resolved value, not a delta"
        );
    }

    #[test]
    fn put_with_ttl_resolves_to_six_element_absolute_expiry() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(2);

        // Stage a value with an absolute expiry directly (what a `Put` with a
        // non-zero TTL leaves in the overlay: value + `ExpireAt`).
        let expire_at = 1_700_000_000_000u64;
        {
            let overlay = core.txn_overlay_mut(txn);
            overlay.insert_put(coll_key("sessions"), 7, &hex_key(b"s1"), b"v1".to_vec());
            overlay.set_ttl(
                coll_key("sessions"),
                7,
                &hex_key(b"s1"),
                StagedTtl::ExpireAt(expire_at),
            );
        }

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("sessions")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Put as u32);

        let (collection, key, value, ttl_ms, got_expire) = decode_kv_put6(&redo.ops[0].payload);
        assert_eq!(collection, "sessions");
        assert_eq!(key, b"s1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 0, "relative ttl_ms is vestigial and set to 0");
        assert_eq!(got_expire, expire_at, "absolute expiry carried verbatim");
    }

    #[test]
    fn put_without_ttl_resolves_to_five_element_form() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(3);

        core.txn_overlay_mut(txn)
            .insert_put(coll_key("kvc"), 9, &hex_key(b"k9"), b"body".to_vec());

        let resp = core.execute_resolve_txn(&task, TID, txn, &[kv_write_plan("kvc")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);

        // The six-element decode must reject the payload (strict array length),
        // proving the five-element form was emitted.
        assert!(
            zerompk::from_msgpack::<(String, String, Vec<u8>, Vec<u8>, u64, u64)>(
                &redo.ops[0].payload
            )
            .is_err(),
            "no-TTL put must emit the five-element form"
        );
        let (collection, key, value, ttl_ms) = decode_kv_put5(&redo.ops[0].payload);
        assert_eq!(collection, "kvc");
        assert_eq!(key, b"k9");
        assert_eq!(value, b"body");
        assert_eq!(ttl_ms, 0);
    }

    #[test]
    fn tombstone_resolves_to_kv_delete_shape() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(4);

        core.txn_overlay_mut(txn)
            .insert_tombstone(coll_key("kvc"), 11, &hex_key(b"gone"));

        let resp = core.execute_resolve_txn(
            &task,
            TID,
            txn,
            &[PhysicalPlan::Kv(KvOp::Delete {
                collection: "kvc".to_string(),
                keys: vec![b"gone".to_vec()],
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

        // Seed a base KV row, then stage a DIFFERENT value for the same key.
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
            &hex_key(b"k"),
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

        // A DELETE ... RETURNING is now staged like any other point delete: the
        // overlay holds a tombstone, so resolve serializes it from the overlay
        // instead of raising the old typed error. (The RETURNING projection is a
        // response-shape concern the Control Plane already discards inside a
        // transaction; it leaves no separate post-image to preserve.)
        core.txn_overlay_mut(txn)
            .insert_tombstone(coll_key("notes"), surrogate, "gone");

        let doc_plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: "notes".to_string(),
            document_id: "gone".to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
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
        crate::data::executor::doc_format::decode_document(body)?
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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));

        // Seed a base row directly into the scan-visible sparse store.
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "notes",
                row_key.as_str(),
                &schemaless_body("alice"),
            )
            .expect("seed base row");

        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "notes".to_string(),
            document_id: row_key.as_str().to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            updates: vec![("name".to_string(), literal_str("bob"))],
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
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
            let row_key = surrogate_to_doc_id(Surrogate::new(s));
            core.sparse
                .put(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "notes",
                    row_key.as_str(),
                    &schemaless_body("old"),
                )
                .expect("seed base row");
        }

        // Empty filters match every row; a RETURNING clause does not change
        // which rows are staged.
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "notes".to_string(),
            filters: Vec::new(),
            updates: vec![("name".to_string(), literal_str("new"))],
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
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
            let row_key = surrogate_to_doc_id(Surrogate::new(s));
            core.sparse
                .put(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "notes",
                    row_key.as_str(),
                    &schemaless_body("doomed"),
                )
                .expect("seed base row");
        }

        let plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: "notes".to_string(),
            filters: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
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
            overlay.insert_put(coll_key("notes"), 1, "u1", schemaless_body("bob"));
            overlay.insert_put(coll_key("notes"), 2, "u2", schemaless_body("bob"));
        }

        // Previously this plan raised a typed error in `classify_document_op`;
        // now it serializes the staged post-images from the overlay.
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "notes".to_string(),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: Some(ReturningSpec {
                columns: ReturningColumns::Star,
            }),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
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
        );

        for s in [1u32, 2u32] {
            let row_key = surrogate_to_doc_id(Surrogate::new(s));
            let stored = dst
                .sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
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
    fn join_merge_batch_dml_still_yield_typed_error() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(45);

        // These ops leave no per-surrogate overlay post-image (join/merge are
        // multi-row cross-collection effects; batch insert rides the buffered
        // plan), so resolve must still raise a typed error rather than silently
        // drop their rows.
        let plans = [
            PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection: "t".to_string(),
                source_collection: "s".to_string(),
                source_alias: "s".to_string(),
                target_join_col: "id".to_string(),
                source_join_col: "id".to_string(),
                updates: Vec::new(),
                target_filters: Vec::new(),
                returning: None,
                resolve_only: false,
                source_rows: None,
            }),
            PhysicalPlan::Document(DocumentOp::Merge {
                target_collection: "t".to_string(),
                source_collection: "s".to_string(),
                source_alias: "s".to_string(),
                target_join_col: "id".to_string(),
                source_join_col: "id".to_string(),
                clauses: Vec::new(),
                returning: None,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: None,
            }),
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: "notes".to_string(),
                documents: vec![("d1".to_string(), Vec::new())],
                surrogates: vec![Surrogate::ZERO],
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
            (TenantId::new(TID), collection.to_string()),
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
        strict_format::value_to_binary_tuple(&nodedb_types::Value::Object(obj), &strict_schema())
            .expect("encode binary tuple")
    }

    /// A schemaless document body in the form staging and the document store
    /// hold it: the canonical storage encoding, not the raw `Value` encoding.
    /// `apply_point_put` canonicalizes on write, so a body in any other shape
    /// would not survive a byte-for-byte round-trip through replay.
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
            collection: collection.to_string(),
            document_id: String::new(),
            value: Vec::new(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
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
        // The strict-mode regression: the overlay holds a Binary Tuple. Resolve
        // must decode it to MessagePack so the document redo replay path (which
        // re-encodes via `bytes_to_binary_tuple`) restores the row. Emitting the
        // Binary Tuple verbatim would make replay's decode fail and drop the row
        // — this test would then FAIL, which is the whole point of the unit.
        let (mut src, _src_dir) = make_core();
        register_strict(&mut src, "sdocs");
        let task = make_task();
        let txn = TxnId::new(20);
        let surrogate = 7u32;
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));

        src.txn_overlay_mut(txn).insert_put(
            coll_key("sdocs"),
            surrogate,
            "row1",
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
        );

        let stored = dst
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "sdocs", row_key.as_str())
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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        let body = schemaless_body("alice");

        src.txn_overlay_mut(txn)
            .insert_put(coll_key("notes"), surrogate, "userpk", body.clone());

        let resp = src.execute_resolve_txn(&task, TID, txn, &[doc_put_plan("notes")]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let stored = dst
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));

        src.txn_overlay_mut(txn)
            .insert_tombstone(coll_key("notes"), surrogate, "gone");

        let delete_plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: "notes".to_string(),
            document_id: "gone".to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: None,
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
        );
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
                .expect("get")
                .is_some(),
            "row seeded"
        );

        let del = wrap_redo(&redo);
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&del),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(
            dst.sparse
                .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));

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
        );
        let before = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
            .expect("get");
        assert_eq!(before.as_deref(), Some(schemaless_body("base").as_slice()));

        core.txn_overlay_mut(txn).insert_put(
            coll_key("notes"),
            surrogate,
            "userpk",
            schemaless_body("staged"),
        );

        let resp = core.execute_resolve_txn(&task, TID, txn, &[doc_put_plan("notes")]);
        assert_eq!(resp.status, Status::Ok);

        // Base is untouched: resolve reads the overlay only, never writes base.
        let after = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), TID, "notes", row_key.as_str())
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
        let doc_row_key = surrogate_to_doc_id(Surrogate::new(doc_surrogate));

        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(coll_key("kvc"), 1, &hex_key(b"k"), b"V".to_vec());
            overlay.insert_put(
                coll_key("notes"),
                doc_surrogate,
                "userpk",
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
        );

        let db = DatabaseId::DEFAULT.as_u64();
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            dst.kv_engine.get(db, TID, "kvc", b"k", now).as_deref(),
            Some(b"V".as_slice()),
            "KV sub-record must replay"
        );
        assert!(
            dst.sparse
                .get(db, TID, "notes", doc_row_key.as_str())
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
            overlay.insert_put(coll_key("kvc"), 1, &hex_key(b"live"), b"V".to_vec());
            overlay.set_ttl(
                coll_key("kvc"),
                1,
                &hex_key(b"live"),
                StagedTtl::ExpireAt(expire_at),
            );
            overlay.insert_put(coll_key("kvc"), 2, &hex_key(b"plain"), b"P".to_vec());
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
        );

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
                collection: "kvc".to_string(),
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
            collection: collection.to_string(),
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

        let (collection, src_id, label, dst_id, properties, src_sur, dst_sur) =
            zerompk::from_msgpack::<(String, String, String, String, Vec<u8>, u32, u32)>(
                &redo.ops[0].payload,
            )
            .expect("decode edge put tuple");
        assert_eq!(collection, "g");
        assert_eq!(src_id, "a");
        assert_eq!(label, "knows");
        assert_eq!(dst_id, "b");
        assert_eq!(properties, vec![9, 9]);
        assert_eq!(src_sur, 10, "src surrogate must come from the plan node");
        assert_eq!(dst_sur, 20, "dst surrogate must come from the plan node");

        // Replay into a fresh core: the CSR node->surrogate map must be
        // repopulated from the two trailing surrogates.
        let record = wrap_redo(&redo);
        let (mut dst_core, _dst_dir) = make_core();
        dst_core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
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
        // The overlay stages a put for `a-knows->b`, but the only `EdgePut`
        // plan in this resolve names a DIFFERENT edge identity in the same
        // collection. `graph_collections` still names "g" (so the staged
        // `a-knows->b` post-image IS walked), but `edge_surrogates` has no
        // entry for it: resolve must raise a typed error rather than invent a
        // surrogate pair for an edge no plan node accounts for.
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
            collection: "g".to_string(),
            src_id: "a".to_string(),
            label: "knows".to_string(),
            dst_id: "b".to_string(),
            src_surrogate: Surrogate::ZERO,
            dst_surrogate: Surrogate::ZERO,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::Delete as u32);

        let (collection, src_id, label, dst_id) =
            zerompk::from_msgpack::<(String, String, String, String)>(&redo.ops[0].payload)
                .expect("decode edge delete tuple");
        assert_eq!(collection, "g");
        assert_eq!(src_id, "a");
        assert_eq!(label, "knows");
        assert_eq!(dst_id, "b");

        // Seed the edge in a fresh core, then replay the delete removes it.
        let (mut dst_core, _dst_dir) = make_core();
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
        dst_core.replay_transaction_redo_wal(
            std::slice::from_ref(&del),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
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
        let doc_row_key = surrogate_to_doc_id(Surrogate::new(doc_surrogate));

        {
            let overlay = src.txn_overlay_mut(txn);
            overlay.insert_put(
                coll_key("notes"),
                doc_surrogate,
                "userpk",
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
        dst_core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let db = DatabaseId::DEFAULT.as_u64();
        assert!(
            dst_core
                .sparse
                .get(db, TID, "notes", doc_row_key.as_str())
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

    #[test]
    fn graph_set_node_labels_has_no_redo_shape_and_yields_typed_error() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(36);

        // `SetNodeLabels` stages a delta, not an absolute post-image, and no
        // redo sub-record shape exists for it: resolve must raise a typed
        // error rather than silently drop the label change.
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "n1".to_string(),
            labels: vec!["Person".to_string()],
        });
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(
            resp.status,
            Status::Error,
            "node-label ops have no redo shape and must raise a typed error"
        );
        assert!(resp.error_code.is_some());
    }

    #[test]
    fn graph_remove_node_labels_has_no_redo_shape_and_yields_typed_error() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(37);

        let plan = PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
            node_id: "n1".to_string(),
            labels: vec!["Person".to_string()],
        });
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(
            resp.status,
            Status::Error,
            "node-label ops have no redo shape and must raise a typed error"
        );
        assert!(resp.error_code.is_some());
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
    /// into a FRESH engine such that the vector is present in the rebuilt HNSW
    /// index (post-images are inexpressible; the redo logs the insert and
    /// replay rebuilds).
    #[test]
    fn vector_insert_resolves_and_replays_queryable() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(40);

        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: "emb".to_string(),
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
        );

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
            collection: "emb".to_string(),
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

    /// A columnar `Insert` plan resolves to a `TimeseriesBatch` sub-record whose
    /// payload is a map-shaped `ColumnarWalRecord` (`kind: "columnar"`) and
    /// replays into the columnar engine's memtable.
    #[test]
    fn columnar_insert_resolves_to_columnar_batch_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(42);

        let mut row = std::collections::HashMap::new();
        row.insert("a".to_string(), nodedb_types::Value::Integer(1));
        row.insert("b".to_string(), nodedb_types::Value::Integer(2));
        let payload = nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![
            nodedb_types::Value::Object(row),
        ]))
        .expect("encode columnar payload");

        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "cevents".to_string(),
            payload,
            format: "msgpack".to_string(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one columnar insert -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::TimeseriesBatch as u32);

        // The payload decodes as a `ColumnarWalRecord` with kind "columnar".
        let rec = zerompk::from_msgpack::<nodedb_types::columnar::ColumnarWalRecord>(
            &redo.ops[0].payload,
        )
        .expect("decode columnar wal record");
        assert_eq!(rec.kind, "columnar");
        assert_eq!(rec.collection, "cevents");

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "cevents".to_string(),
        );
        assert_eq!(
            dst.columnar_engines
                .get(&key)
                .map(|e| e.memtable().row_count()),
            Some(1),
            "columnar row must replay into the memtable"
        );
    }

    /// A timeseries `Ingest` plan resolves to a `TimeseriesBatch` sub-record
    /// tagged `"timeseries"` and replays its samples into the memtable.
    #[test]
    fn timeseries_ingest_resolves_and_replays() {
        let (mut src, _src_dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(43);

        // A `TimeseriesWalBatch` is what `replay_timeseries_payload` decodes to
        // ingest samples directly into the memtable.
        let batch = nodedb_types::timeseries::TimeseriesWalBatch {
            collection: "metrics".to_string(),
            samples: vec![(11u64, 1_700_000_000_000i64, 42.0f64)],
            provenance: None,
        };
        let payload = zerompk::to_msgpack_vec(&batch).expect("encode ts batch");

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".to_string(),
            payload,
            format: "samples".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
        });
        let resp = src.execute_resolve_txn(&task, TID, txn, &[plan]);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1, "one timeseries ingest -> one sub-record");
        assert_eq!(redo.ops[0].record_type, RecordType::TimeseriesBatch as u32);

        // The payload is the 4-element tuple tagged "timeseries" (a msgpack
        // array), distinct from the columnar map form.
        let (kind, collection, _payload, _prov) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>)>(
                &redo.ops[0].payload,
            )
            .expect("decode timeseries 4-tuple");
        assert_eq!(kind, "timeseries");
        assert_eq!(collection, "metrics");

        let record = wrap_redo(&redo);
        let (mut dst, _dst_dir) = make_core();
        dst.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

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

    /// `Columnar::Update` / `Columnar::Delete` are predicate DML: resolve emits
    /// the SAME `columnar_dml` (`TimeseriesBatch`) sub-record the autocommit path
    /// appends, carrying the predicate (and, for update, the assignments), so an
    /// in-tx columnar UPDATE/DELETE is restart-durable exactly like its
    /// autocommit twin.
    #[test]
    fn columnar_update_and_delete_emit_columnar_dml_sub_record() {
        use nodedb_types::columnar::ColumnarDmlWalRecord;

        let (mut core, _dir) = make_core();
        let task = make_task();

        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: "cevents".to_string(),
            filters: Vec::new(),
            updates: vec![("a".to_string(), vec![1, 2, 3])],
        });
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(44), &[update]);
        assert_eq!(resp.status, Status::Ok);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        assert_eq!(redo.ops[0].record_type, RecordType::TimeseriesBatch as u32);
        let rec: ColumnarDmlWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("decode columnar_dml");
        assert_eq!(rec.kind, "columnar_dml");
        assert_eq!(rec.collection, "cevents");
        assert!(rec.is_update, "UPDATE must carry is_update = true");
        assert_eq!(rec.updates, vec![("a".to_string(), vec![1, 2, 3])]);

        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: "cevents".to_string(),
            filters: Vec::new(),
        });
        let resp = core.execute_resolve_txn(&task, TID, TxnId::new(45), &[delete]);
        assert_eq!(resp.status, Status::Ok);
        let redo = decode_redo(&resp);
        assert_eq!(redo.ops.len(), 1);
        let rec: ColumnarDmlWalRecord =
            zerompk::from_msgpack(&redo.ops[0].payload).expect("decode columnar_dml");
        assert_eq!(rec.kind, "columnar_dml");
        assert!(!rec.is_update, "DELETE must carry is_update = false");
        assert!(rec.updates.is_empty(), "DELETE carries no assignments");
    }

    /// An array `Put` plan resolves to a version-tagged `ArrayPut` sub-record
    /// (decodable by the exact function replay uses) and replays into a fresh
    /// engine, respecting the `ArrayFlush` watermark discipline. A flushed,
    /// non-empty put yields at least one scannable tile.
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
        );

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
        src.txn_overlay_mut(txn)
            .insert_put(coll_key("kvc"), 1, &hex_key(b"k"), b"V".to_vec());

        let mut row = std::collections::HashMap::new();
        row.insert("a".to_string(), nodedb_types::Value::Integer(7));
        let col_payload = nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![
            nodedb_types::Value::Object(row),
        ]))
        .expect("encode columnar payload");

        let plans = [
            kv_write_plan("kvc"),
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: "emb".to_string(),
                vector: vec![1.0, 2.0, 3.0],
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate::new(21),
                pk_bytes: None,
                provenance: None,
            }),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: "cevents".to_string(),
                payload: col_payload,
                format: "msgpack".to_string(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
            }),
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
        );

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

    // ── Spatial resolve tests ────────────────────────────────────────────────
    //
    // Spatial `Insert` / `Delete` plan nodes carry the complete post-image
    // directly (see `resolve/spatial.rs`), so these mirror the vector/columnar
    // plan-driven tests above rather than the KV/document overlay tests.

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
            collection: collection.to_string(),
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
            collection: collection.to_string(),
            field: field.to_string(),
            surrogate: Surrogate::new(surrogate),
            provenance: Some(spatial_prov(seq)),
        })
    }

    /// R-tree entry id for a surrogate, mirroring `execute_spatial_insert`'s
    /// `fnv1a_hash(doc_id.as_bytes())` keying.
    fn spatial_entry_id(surrogate: u32) -> u64 {
        let doc_id = surrogate_to_doc_id(Surrogate::new(surrogate));
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
        );

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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        assert!(
            dst.sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "places",
                    row_key.as_str()
                )
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
        );
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
        );

        assert_eq!(
            dst.spatial_indexes
                .get(&key)
                .map(|rt| rt.len())
                .unwrap_or(0),
            0,
            "redo delete must remove the R-tree entry"
        );
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        assert!(
            dst.sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "places",
                    row_key.as_str()
                )
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
        let row_key = surrogate_to_doc_id(Surrogate::new(surrogate));
        assert!(
            core.sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "places",
                    row_key.as_str()
                )
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

        src.txn_overlay_mut(txn)
            .insert_put(coll_key("kvc"), 1, &hex_key(b"k"), b"V".to_vec());

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
        );

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
            collection: "places".to_string(),
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
    fn spatial_insert_without_provenance_yields_typed_error() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn = TxnId::new(46);

        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: "places".to_string(),
            field: "loc".to_string(),
            surrogate: Surrogate::new(1),
            geometry: spatial_point(0.0, 0.0),
            provenance: None,
        });
        let resp = core.execute_resolve_txn(&task, TID, txn, &[plan]);
        assert_eq!(
            resp.status,
            Status::Error,
            "a spatial insert with no provenance must raise a typed error, not silently drop"
        );
        assert!(resp.error_code.is_some());
    }
}
