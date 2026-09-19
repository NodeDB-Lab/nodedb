// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral predicates for the in-transaction write-staging gate.
//! Shared by every protocol's dispatch loop; no pgwire types imported here.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, CrdtWriteVerb, DocumentOp, GraphOp, KvOp, SpatialOp, TimeseriesOp,
    VectorOp,
};

/// Allow-list of plans the in-transaction path stages at statement time: Document
/// point writes, predicate `BulkUpdate`/`BulkDelete`, and `Upsert`. `InsertSelect`
/// is not here — it resolves into `PointInsert` ops that flow through on their own.
pub fn is_point_write(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Document(
            DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::PointDelete { .. }
                | DocumentOp::PointUpdate { .. }
                | DocumentOp::BulkUpdate { .. }
                | DocumentOp::BulkDelete { .. }
                | DocumentOp::Upsert { .. }
        )
    )
}

/// The command a stageable write resolves to, decided from the plan alone.
/// `ConflictUpsert` is the one shape whose tag the response decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedWriteShape {
    Insert,
    Update,
    Delete,
    Upsert,
    ConflictUpsert,
    RawPayload,
    /// `TRUNCATE`: an overlay marker with no row count.
    Truncate,
}

/// Classify a plan into the [`StagedWriteShape`] the in-transaction staging gate
/// stages it as, or `None` if the plan is not stageable. `None` covers: Document
/// scans/reads, KV reads/predicate ops/autocommit-only resolve ops, Columnar/
/// Timeseries/Spatial/Graph/Array reads and maintenance ops, Vector search, every
/// `CrdtOp` other than the row writes `DocUpsert`/`DocDelete`, and every
/// non-write-engine `PhysicalPlan` variant (`Text`, `Query`, `Meta`,
/// `ClusterArray`, `ClusterEvent`). The single-node `ArrayOp::{Put, Delete}` stages
/// here; the `ClusterArrayOp::{Put, Delete}` routing wrapper is not listed because
/// `route_in_tx_write` fans it out per vShard first (`session::array_fanout_stage`),
/// then stages each per-vShard `ArrayOp`. `Incr`/`Cas`/`GetSet`/`BatchPut` also
/// stage TTL into the overlay so a same-txn `GetTtl` sees it.
pub fn stageable_write_shape(plan: &PhysicalPlan) -> Option<StagedWriteShape> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. } | DocumentOp::PointInsert { .. }) => {
            Some(StagedWriteShape::Insert)
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. } | DocumentOp::BulkUpdate { .. }) => {
            Some(StagedWriteShape::Update)
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { .. } | DocumentOp::BulkDelete { .. }) => {
            Some(StagedWriteShape::Delete)
        }
        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => Some(StagedWriteShape::Upsert),
        PhysicalPlan::Document(DocumentOp::Truncate { .. }) => Some(StagedWriteShape::Truncate),
        PhysicalPlan::Document(_) => None,

        PhysicalPlan::Kv(op) => kv_write_shape(op),

        PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => Some(StagedWriteShape::Insert),
        // Same Update/Delete shapes as the Document bulk predicate-DML arms above.
        PhysicalPlan::Columnar(ColumnarOp::Update { .. } | ColumnarOp::ResolvedUpdate { .. }) => {
            Some(StagedWriteShape::Update)
        }
        // `ResolvedDelete` is the resolved-row-set form of the same statement, same shape.
        PhysicalPlan::Columnar(ColumnarOp::Delete { .. } | ColumnarOp::ResolvedDelete { .. }) => {
            Some(StagedWriteShape::Delete)
        }
        PhysicalPlan::Columnar(_) => None,

        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => Some(StagedWriteShape::Insert),
        PhysicalPlan::Timeseries(_) => None,

        PhysicalPlan::Spatial(SpatialOp::Insert { .. }) => Some(StagedWriteShape::Insert),
        PhysicalPlan::Spatial(SpatialOp::Delete { .. }) => Some(StagedWriteShape::Delete),
        PhysicalPlan::Spatial(_) => None,

        // Matches the autocommit `execute_edge_put` path: an edge either exists or it doesn't.
        PhysicalPlan::Graph(GraphOp::EdgePut { .. } | GraphOp::EdgePutBatch { .. }) => {
            Some(StagedWriteShape::Insert)
        }
        PhysicalPlan::Graph(GraphOp::EdgeDelete { .. } | GraphOp::EdgeDeleteBatch { .. }) => {
            Some(StagedWriteShape::Delete)
        }
        // Mutates an existing node's label bitset in place, not a row Insert/Delete.
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. }) => {
            Some(StagedWriteShape::Update)
        }
        PhysicalPlan::Graph(_) => None,

        PhysicalPlan::Array(ArrayOp::Put { .. }) => Some(StagedWriteShape::Insert),
        PhysicalPlan::Array(ArrayOp::Delete { .. }) => Some(StagedWriteShape::Delete),
        PhysicalPlan::Array(_) => None,

        // Vector-primary direct writes: the same shapes `describe_vector` renders
        // for the autocommit statement.
        PhysicalPlan::Vector(
            VectorOp::DirectInsert { .. } | VectorOp::DirectInsertIfAbsent { .. },
        ) => Some(StagedWriteShape::Insert),
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            on_conflict_updates,
            ..
        }) if !on_conflict_updates.is_empty() => Some(StagedWriteShape::ConflictUpsert),
        PhysicalPlan::Vector(VectorOp::DirectUpsert { .. }) => Some(StagedWriteShape::Upsert),
        PhysicalPlan::Vector(VectorOp::DirectDelete { .. }) => Some(StagedWriteShape::Delete),
        PhysicalPlan::Vector(VectorOp::DirectTruncate { .. }) => Some(StagedWriteShape::Truncate),
        PhysicalPlan::Vector(VectorOp::DirectUpdate { .. }) => Some(StagedWriteShape::Update),
        PhysicalPlan::Vector(_) => None,

        // A CRDT row write stages under the tag of the SQL verb that produced
        // it: the plan fixes the verb, so the shape is decided here.
        PhysicalPlan::Crdt(CrdtOp::DocUpsert { verb, .. }) => Some(match verb {
            CrdtWriteVerb::Insert => StagedWriteShape::Insert,
            CrdtWriteVerb::Upsert => StagedWriteShape::Upsert,
            CrdtWriteVerb::Update => StagedWriteShape::Update,
        }),
        PhysicalPlan::Crdt(CrdtOp::DocDelete { .. }) => Some(StagedWriteShape::Delete),
        // `Apply`/`ApplyAuthenticated` are refused inside a transaction by
        // `route_in_tx_write`. The `List*` edits, snapshot import, constraint
        // and policy DDL, history ops, and reads buffer for COMMIT or run as
        // reads.
        PhysicalPlan::Crdt(
            CrdtOp::Read { .. }
            | CrdtOp::Apply { .. }
            | CrdtOp::ApplyAuthenticated { .. }
            | CrdtOp::ImportSnapshot { .. }
            | CrdtOp::SetConstraints { .. }
            | CrdtOp::DropConstraints { .. }
            | CrdtOp::ReadConstraints { .. }
            | CrdtOp::SetPolicy { .. }
            | CrdtOp::GetPolicy { .. }
            | CrdtOp::ReadAtVersion { .. }
            | CrdtOp::GetVersionVector { .. }
            | CrdtOp::ExportDelta { .. }
            | CrdtOp::RestoreToVersion { .. }
            | CrdtOp::CompactAtVersion { .. }
            | CrdtOp::ListInsert { .. }
            | CrdtOp::ListDelete { .. }
            | CrdtOp::ListMove { .. }
            | CrdtOp::PreviewApply { .. },
        ) => None,

        PhysicalPlan::Text(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Classify a `KvOp` into the [`StagedWriteShape`] it stages as, or `None` if it
/// is a read, a predicate op, or autocommit-only. Exhaustive over every `KvOp`
/// variant.
fn kv_write_shape(op: &KvOp) -> Option<StagedWriteShape> {
    match op {
        // The SQL `UPSERT` statement, tagged like its `DocumentOp::Upsert`
        // sibling and like the autocommit `describe_plan` arm.
        KvOp::Put { .. } => Some(StagedWriteShape::Upsert),
        KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } | KvOp::BatchPut { .. } => {
            Some(StagedWriteShape::Insert)
        }
        KvOp::InsertOnConflictUpdate { .. } => Some(StagedWriteShape::ConflictUpsert),
        KvOp::Delete { .. } => Some(StagedWriteShape::Delete),
        // These return a computed value, not a row count — forward the payload verbatim.
        KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. } => Some(StagedWriteShape::RawPayload),
        // Mutates TTL metadata in place, not Insert/Delete of the row itself.
        KvOp::Expire { .. } | KvOp::Persist { .. } => Some(StagedWriteShape::Update),
        KvOp::Truncate { .. } => Some(StagedWriteShape::Truncate),
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::BatchGet { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::MaterializeScan { .. }
        // Autocommit-only: transaction resolve rejects both.
        | KvOp::ResolveWrite(_)
        | KvOp::ResolvedWrite { .. }
        // Autocommit-only: a predicate resolves its row set at apply time.
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. } => None,
    }
}

/// Allow-list of plans staged via `MetaOp::StageWrite`: [`is_point_write`] plus
/// stageable KV/Columnar/Timeseries/Spatial/Graph/Array writes and the
/// vector-primary direct writes. See [`stageable_write_shape`] for the per-plan
/// classification this is derived from.
pub fn is_stageable_write(plan: &PhysicalPlan) -> bool {
    stageable_write_shape(plan).is_some()
}

/// Extract affected row count from a JSON or MessagePack payload. Looks for
/// `"affected"`, `"truncated"`, `"inserted"`, `"accepted"`, or `"deleted"` — every
/// name a write emits must appear here. `None` is never a licence to default.
pub fn extract_affected_count(payload: &[u8]) -> Option<u64> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("affected")
        .or_else(|| v.get("truncated"))
        .or_else(|| v.get("inserted"))
        .or_else(|| v.get("accepted"))
        .or_else(|| v.get("deleted"))
        .and_then(|n| n.as_u64())
}

/// The affected-row count a DML response must carry, or a typed error. A
/// count-bearing plan whose response has no count is a broken handler
/// invariant — surfacing it loudly beats defaulting to `1`.
pub fn require_affected_count(payload: &[u8]) -> crate::Result<u64> {
    extract_affected_count(payload).ok_or_else(|| crate::Error::Internal {
        detail: "write response carried no affected-row count; the handler for this plan must \
                 report one (see CoreLoop::response_affected)"
            .to_owned(),
    })
}

/// Extract the `"op"` field a staged `KvOp::InsertOnConflictUpdate` or
/// conflict-patching `VectorOp::DirectUpsert` response carries (`"insert"`
/// or `"update"`). `None` for any other payload shape.
pub fn extract_kv_conflict_op(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let v: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    v.get("op").and_then(|n| n.as_str()).map(str::to_string)
}

/// Neutral classification of the command a staged write resolved to, used to
/// render a protocol-specific "command complete" tag. `KvUpsert` carries whether
/// it resolved to an update or insert — the one outcome the plan shape can't decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedTagKind {
    Insert,
    Update,
    Delete,
    /// An `ON CONFLICT DO UPDATE` write whose verb the stage handler decided
    /// from the row's existence: KV `InsertOnConflictUpdate` and the
    /// vector-primary `DirectUpsert` with a conflict patch. Mirrors the
    /// autocommit `DmlResultByOp` shape.
    KvUpsert {
        updated: bool,
    },
    /// The SQL `UPSERT` statement (`DocumentOp::Upsert`, `KvOp::Put`): the
    /// literal `UPSERT n` tag whatever the insert-vs-update outcome.
    Upsert,
    /// In-transaction `MERGE`, staged as concrete point ops; `affected` is the
    /// total across arms. pgwire renders `MERGE <n>`.
    Merge,
    /// In-transaction `UPDATE ... FROM <source>`, staged as `PointPut` ops;
    /// pgwire renders `UPDATE <n>`.
    UpdateFromJoin,
    /// The staged handler computed a value, not a row count (`Incr`/`IncrFloat`/
    /// `Cas`/`GetSet`) — caller forwards the payload verbatim.
    RawPayload,
    /// Bare `TRUNCATE`, no row count, matching the autocommit tag.
    Truncate,
}

impl StagedWriteShape {
    /// The tag a staged write of this shape renders, given the stage handler's
    /// response payload.
    pub fn tag_kind(self, payload: &[u8]) -> StagedTagKind {
        match self {
            StagedWriteShape::Insert => StagedTagKind::Insert,
            StagedWriteShape::Update => StagedTagKind::Update,
            StagedWriteShape::Delete => StagedTagKind::Delete,
            StagedWriteShape::Upsert => StagedTagKind::Upsert,
            // KV `InsertOnConflictUpdate` and the vector-primary `DirectUpsert` with a
            // conflict patch both decide insert-vs-update from the stage handler's response.
            StagedWriteShape::ConflictUpsert => StagedTagKind::KvUpsert {
                updated: extract_kv_conflict_op(payload).as_deref() == Some("update"),
            },
            StagedWriteShape::RawPayload => StagedTagKind::RawPayload,
            StagedWriteShape::Truncate => StagedTagKind::Truncate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{UpdateValue, VectorWriteTargets};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    #[test]
    fn extract_affected_count_reads_msgpack_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 3 })).unwrap();
        assert_eq!(extract_affected_count(&payload), Some(3));
    }

    #[test]
    fn extract_kv_conflict_op_reads_op_field() {
        let payload =
            nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1, "op": "update"}))
                .unwrap();
        assert_eq!(extract_kv_conflict_op(&payload).as_deref(), Some("update"));
    }

    #[test]
    fn extract_kv_conflict_op_none_when_absent() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1})).unwrap();
        assert_eq!(extract_kv_conflict_op(&payload), None);
    }

    fn kv_plan(op: KvOp) -> PhysicalPlan {
        PhysicalPlan::Kv(op)
    }

    #[test]
    fn returning_document_writes_are_stageable_and_tagged_by_command() {
        use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
        let ret = || {
            Some(ReturningSpec {
                columns: ReturningColumns::Star,
            })
        };

        // RETURNING doesn't force the buffer path: these stage and render an affected-count tag.
        let point_update = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: ret(),
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(is_point_write(&point_update));
        assert!(is_stageable_write(&point_update));
        assert_eq!(
            stageable_write_shape(&point_update)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Update
        );

        let point_delete = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            returning: ret(),
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_stageable_write(&point_delete));
        assert_eq!(
            stageable_write_shape(&point_delete)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Delete
        );

        let bulk_update = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: ret(),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(is_stageable_write(&bulk_update));
        assert_eq!(
            stageable_write_shape(&bulk_update)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Update
        );

        let bulk_delete = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            filters: Vec::new(),
            returning: ret(),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(is_stageable_write(&bulk_delete));
        assert_eq!(
            stageable_write_shape(&bulk_delete)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Delete
        );
    }

    #[test]
    fn is_stageable_write_accepts_the_kv_atomics_and_batch_put() {
        assert!(is_stageable_write(&kv_plan(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::IncrFloat {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            delta: 1.0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::Cas {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            expected: vec![],
            new_value: b"v".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::GetSet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            new_value: b"v".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::BatchPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
            ttl_ms: 0,
            surrogates: vec![nodedb_types::Surrogate::ZERO],
            returning: None,
            rls_filters: Vec::new(),
        })));
    }

    #[test]
    fn kv_write_shape_atomics_forward_raw_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "value": 5 })).unwrap();
        for op in [
            KvOp::Incr {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                delta: 1,
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
            KvOp::IncrFloat {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                delta: 1.0,
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
            KvOp::Cas {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                expected: vec![],
                new_value: b"v".to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
            KvOp::GetSet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                key: b"k".to_vec(),
                new_value: b"v".to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            },
        ] {
            assert_eq!(
                kv_write_shape(&op).expect("stageable").tag_kind(&payload),
                StagedTagKind::RawPayload,
                "{op:?} must classify as RawPayload"
            );
        }
    }

    #[test]
    fn staged_kv_put_is_the_upsert_tag() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 1 })).unwrap();
        let op = KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(
            kv_write_shape(&op).expect("stageable").tag_kind(&payload),
            StagedTagKind::Upsert
        );
    }

    #[test]
    fn kv_write_shape_batch_put_is_insert() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 2 })).unwrap();
        let op = KvOp::BatchPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
            ttl_ms: 0,
            surrogates: vec![nodedb_types::Surrogate::ZERO],
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(
            kv_write_shape(&op).expect("stageable").tag_kind(&payload),
            StagedTagKind::Insert
        );
    }

    #[test]
    fn array_put_and_delete_are_stageable_and_tagged() {
        use nodedb_array::types::ArrayId;
        use nodedb_types::TenantId;
        let put = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "a"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        assert!(is_stageable_write(&put));
        assert!(!is_point_write(&put));
        assert_eq!(
            stageable_write_shape(&put)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Insert
        );

        let delete = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "a"),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        assert!(is_stageable_write(&delete));
        assert_eq!(
            stageable_write_shape(&delete)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Delete
        );

        let slice = PhysicalPlan::Array(ArrayOp::Project {
            array_id: ArrayId::new(TenantId::new(1), "a"),
            attr_indices: Vec::new(),
        });
        assert!(!is_stageable_write(&slice));
    }

    fn direct_upsert(on_conflict_updates: Vec<(String, UpdateValue)>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: b"r1".to_vec(),
            vector: vec![1.0, 0.0],
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })
    }

    #[test]
    fn vector_primary_direct_writes_are_stageable_and_tagged() {
        let insert = PhysicalPlan::Vector(VectorOp::DirectInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: b"r1".to_vec(),
            vector: vec![1.0, 0.0],
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(is_stageable_write(&insert));
        assert!(!is_point_write(&insert));
        assert_eq!(
            stageable_write_shape(&insert)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Insert
        );

        let if_absent = PhysicalPlan::Vector(VectorOp::DirectInsertIfAbsent {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: b"r1".to_vec(),
            vector: vec![1.0, 0.0],
            payload: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(is_stageable_write(&if_absent));
        assert_eq!(
            stageable_write_shape(&if_absent)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Insert
        );

        let upsert = direct_upsert(Vec::new());
        assert!(is_stageable_write(&upsert));
        assert_eq!(
            stageable_write_shape(&upsert)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Upsert
        );

        let patched = direct_upsert(vec![("owner".into(), UpdateValue::Literal(vec![0xc0]))]);
        let updated =
            nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1, "op": "update"}))
                .unwrap();
        assert_eq!(
            stageable_write_shape(&patched)
                .expect("stageable")
                .tag_kind(&updated),
            StagedTagKind::KvUpsert { updated: true }
        );
        let inserted =
            nodedb_types::json_to_msgpack(&serde_json::json!({"affected": 1, "op": "insert"}))
                .unwrap();
        assert_eq!(
            stageable_write_shape(&patched)
                .expect("stageable")
                .tag_kind(&inserted),
            StagedTagKind::KvUpsert { updated: false }
        );

        let delete = PhysicalPlan::Vector(VectorOp::DirectDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            targets: VectorWriteTargets::Surrogates(Vec::new()),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_stageable_write(&delete));
        assert_eq!(
            stageable_write_shape(&delete)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Delete
        );

        let update = PhysicalPlan::Vector(VectorOp::DirectUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            targets: VectorWriteTargets::Predicate(Vec::new()),
            new_vector: None,
            payload_patch: Vec::new(),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(is_stageable_write(&update));
        assert_eq!(
            stageable_write_shape(&update)
                .expect("stageable")
                .tag_kind(&[]),
            StagedTagKind::Update
        );

        let search = PhysicalPlan::Vector(VectorOp::MultiSearch {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            query_vector: Vec::new(),
            top_k: 1,
            ef_search: 0,
            filter_bitmap: None,
            rls_filters: Vec::new(),
        });
        assert!(!is_stageable_write(&search));
    }

    #[test]
    fn is_stageable_write_accepts_expire_and_persist() {
        assert!(is_stageable_write(&kv_plan(KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
        assert!(is_stageable_write(&kv_plan(KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })));
    }

    #[test]
    fn kv_write_shape_expire_and_persist_are_update() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({})).unwrap();
        let expire = KvOp::Expire {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        };
        let persist = KvOp::Persist {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        };
        assert_eq!(
            kv_write_shape(&expire)
                .expect("stageable")
                .tag_kind(&payload),
            StagedTagKind::Update
        );
        assert_eq!(
            kv_write_shape(&persist)
                .expect("stageable")
                .tag_kind(&payload),
            StagedTagKind::Update
        );
    }

    #[test]
    fn stageable_write_shape_is_none_for_a_read_plan() {
        let get = PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert!(!is_stageable_write(&get));
        assert_eq!(stageable_write_shape(&get), None);
    }

    #[test]
    fn document_truncate_stages_as_truncate_with_a_bare_tag() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(is_stageable_write(&plan));
        assert_eq!(
            stageable_write_shape(&plan),
            Some(StagedWriteShape::Truncate)
        );
        assert_eq!(
            StagedWriteShape::Truncate.tag_kind(&[]),
            StagedTagKind::Truncate
        );
    }

    #[test]
    fn kv_truncate_stages_as_truncate_with_a_bare_tag() {
        let plan = kv_plan(KvOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            restart_identity: false,
        });
        assert!(is_stageable_write(&plan));
        assert_eq!(
            stageable_write_shape(&plan),
            Some(StagedWriteShape::Truncate)
        );
        assert_eq!(
            StagedWriteShape::Truncate.tag_kind(&[]),
            StagedTagKind::Truncate
        );
    }

    #[test]
    fn vector_truncate_stages_as_truncate_with_a_bare_tag() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectTruncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "v"),
            field: "vec".into(),
            restart_identity: false,
        });
        assert!(is_stageable_write(&plan));
        assert_eq!(
            stageable_write_shape(&plan),
            Some(StagedWriteShape::Truncate)
        );
    }

    #[test]
    fn vector_primary_direct_upsert_with_patch_is_conflict_upsert() {
        let patched = direct_upsert(vec![("owner".into(), UpdateValue::Literal(vec![0xc0]))]);
        assert_eq!(
            stageable_write_shape(&patched),
            Some(StagedWriteShape::ConflictUpsert)
        );
    }

    fn crdt_doc_upsert(verb: CrdtWriteVerb) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "a".into(),
            fields_json: r#"{"title":"t"}"#.into(),
            surrogate: nodedb_types::Surrogate(7),
            partial: verb == CrdtWriteVerb::Update,
            verb,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    #[test]
    fn crdt_doc_upsert_stages_under_its_verb() {
        for (verb, shape) in [
            (CrdtWriteVerb::Insert, StagedWriteShape::Insert),
            (CrdtWriteVerb::Upsert, StagedWriteShape::Upsert),
            (CrdtWriteVerb::Update, StagedWriteShape::Update),
        ] {
            let plan = crdt_doc_upsert(verb);
            assert!(is_stageable_write(&plan), "{verb:?} must stage");
            assert_eq!(stageable_write_shape(&plan), Some(shape), "{verb:?}");
        }
    }

    #[test]
    fn crdt_doc_delete_stages_as_delete() {
        let plan = PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "a".into(),
            surrogate: nodedb_types::Surrogate(7),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(is_stageable_write(&plan));
        assert_eq!(stageable_write_shape(&plan), Some(StagedWriteShape::Delete));
    }

    #[test]
    fn crdt_read_is_not_stageable() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Read {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "a".into(),
        });
        assert!(!is_stageable_write(&plan));
        assert_eq!(stageable_write_shape(&plan), None);
    }
}
