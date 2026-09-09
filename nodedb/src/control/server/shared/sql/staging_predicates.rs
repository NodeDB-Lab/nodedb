// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral predicates for the in-transaction write-staging gate.
//! Shared by every protocol's dispatch loop; no pgwire types imported here.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, KvOp, SpatialOp, TimeseriesOp,
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

/// Allow-list of plans staged via `MetaOp::StageWrite`: [`is_point_write`] plus
/// stageable KV/Columnar/Timeseries/Spatial/Graph writes. `Incr`/`Cas`/`GetSet`/
/// `BatchPut` also stage TTL into the overlay so a same-txn `GetTtl` sees it.
pub fn is_stageable_write(plan: &PhysicalPlan) -> bool {
    is_point_write(plan)
        || matches!(
            plan,
            PhysicalPlan::Kv(
                KvOp::Put { .. }
                    | KvOp::Insert { .. }
                    | KvOp::InsertIfAbsent { .. }
                    | KvOp::InsertOnConflictUpdate { .. }
                    | KvOp::Delete { .. }
                    | KvOp::BatchPut { .. }
                    | KvOp::Incr { .. }
                    | KvOp::IncrFloat { .. }
                    | KvOp::Cas { .. }
                    | KvOp::GetSet { .. }
                    | KvOp::FieldSet { .. }
                    | KvOp::Transfer { .. }
                    | KvOp::TransferItem { .. }
                    | KvOp::Expire { .. }
                    | KvOp::Persist { .. }
            )
        )
        || matches!(
            plan,
            PhysicalPlan::Columnar(
                ColumnarOp::Insert { .. }
                    | ColumnarOp::Update { .. }
                    | ColumnarOp::Delete { .. }
                    | ColumnarOp::ResolvedUpdate { .. }
                    | ColumnarOp::ResolvedDelete { .. }
            )
        )
        || matches!(plan, PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }))
        || matches!(
            plan,
            PhysicalPlan::Spatial(SpatialOp::Insert { .. } | SpatialOp::Delete { .. })
        )
        || matches!(
            plan,
            PhysicalPlan::Graph(
                GraphOp::EdgePut { .. }
                    | GraphOp::EdgeDelete { .. }
                    | GraphOp::EdgePutBatch { .. }
                    | GraphOp::EdgeDeleteBatch { .. }
                    | GraphOp::SetNodeLabels { .. }
                    | GraphOp::RemoveNodeLabels { .. }
            )
        )
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

/// Extract the `"op"` field a staged `KvOp::InsertOnConflictUpdate` response
/// carries (`"insert"` or `"update"`). `None` for any other payload shape.
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
    KvUpsert {
        updated: bool,
    },
    DocUpsert,
    /// In-transaction `MERGE`, staged as concrete point ops; `affected` is the
    /// total across arms. pgwire renders `MERGE <n>`.
    Merge,
    /// In-transaction `UPDATE ... FROM <source>`, staged as `PointPut` ops;
    /// pgwire renders `UPDATE <n>`.
    UpdateFromJoin,
    /// The staged handler computed a value, not a row count (`Incr`/`IncrFloat`/
    /// `Cas`/`GetSet`) — caller forwards the payload verbatim.
    RawPayload,
}

/// Decide the [`StagedTagKind`] for a staged write, given the plan and the stage
/// handler's raw response payload. Caller invariant: `plan` passed [`is_stageable_write`].
pub fn staged_tag_kind(plan: &PhysicalPlan, payload: &[u8]) -> StagedTagKind {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut { .. } | DocumentOp::PointInsert { .. }) => {
            StagedTagKind::Insert
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { .. } | DocumentOp::BulkUpdate { .. }) => {
            StagedTagKind::Update
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { .. } | DocumentOp::BulkDelete { .. }) => {
            StagedTagKind::Delete
        }
        PhysicalPlan::Document(DocumentOp::Upsert { .. }) => StagedTagKind::DocUpsert,
        PhysicalPlan::Kv(op) => staged_kv_tag_kind(op, payload),
        PhysicalPlan::Columnar(ColumnarOp::Insert { .. }) => StagedTagKind::Insert,
        // Same Update/Delete tags as the Document bulk predicate-DML arms above.
        PhysicalPlan::Columnar(ColumnarOp::Update { .. }) => StagedTagKind::Update,
        PhysicalPlan::Columnar(ColumnarOp::Delete { .. }) => StagedTagKind::Delete,
        // Resolved-row-set form of the same statement, same tags.
        PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate { .. }) => StagedTagKind::Update,
        PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete { .. }) => StagedTagKind::Delete,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. }) => StagedTagKind::Insert,
        PhysicalPlan::Spatial(SpatialOp::Insert { .. }) => StagedTagKind::Insert,
        PhysicalPlan::Spatial(SpatialOp::Delete { .. }) => StagedTagKind::Delete,
        // Matches the autocommit `execute_edge_put` path: an edge either exists or it doesn't.
        PhysicalPlan::Graph(GraphOp::EdgePut { .. } | GraphOp::EdgePutBatch { .. }) => {
            StagedTagKind::Insert
        }
        PhysicalPlan::Graph(GraphOp::EdgeDelete { .. } | GraphOp::EdgeDeleteBatch { .. }) => {
            StagedTagKind::Delete
        }
        // Mutates an existing node's label bitset in place, not a row Insert/Delete.
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. }) => {
            StagedTagKind::Update
        }
        other => unreachable!(
            "staged_tag_kind called on a non-stageable-write plan; \
             is_stageable_write invariant broken: {other:?}"
        ),
    }
}

/// Decide the [`StagedTagKind`] for a staged `KvOp` write. Caller invariant: `op`
/// must be a stageable KV write — the enclosing plan already passed [`is_stageable_write`].
fn staged_kv_tag_kind(op: &KvOp, payload: &[u8]) -> StagedTagKind {
    match op {
        KvOp::Put { .. } | KvOp::Insert { .. } | KvOp::InsertIfAbsent { .. } => {
            StagedTagKind::Insert
        }
        KvOp::InsertOnConflictUpdate { .. } => StagedTagKind::KvUpsert {
            updated: extract_kv_conflict_op(payload).as_deref() == Some("update"),
        },
        KvOp::Delete { .. } => StagedTagKind::Delete,
        KvOp::BatchPut { .. } => StagedTagKind::Insert,
        // These return a computed value, not a row count — forward the payload verbatim.
        KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. } => StagedTagKind::RawPayload,
        // Mutates TTL metadata in place, not Insert/Delete of the row itself.
        KvOp::Expire { .. } | KvOp::Persist { .. } => StagedTagKind::Update,
        KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::BatchGet { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::GetTtl { .. }
        | KvOp::Truncate { .. }
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
        | KvOp::PredicateDelete { .. } => unreachable!(
            "staged_kv_tag_kind called on a non-stageable KvOp; \
             is_stageable_write invariant broken: {op:?}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(staged_tag_kind(&point_update, &[]), StagedTagKind::Update);

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
        assert_eq!(staged_tag_kind(&point_delete, &[]), StagedTagKind::Delete);

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
        assert_eq!(staged_tag_kind(&bulk_update, &[]), StagedTagKind::Update);

        let bulk_delete = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            filters: Vec::new(),
            returning: ret(),
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(is_stageable_write(&bulk_delete));
        assert_eq!(staged_tag_kind(&bulk_delete, &[]), StagedTagKind::Delete);
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
    fn staged_kv_tag_kind_atomics_forward_raw_payload() {
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
                staged_kv_tag_kind(&op, &payload),
                StagedTagKind::RawPayload,
                "{op:?} must classify as RawPayload"
            );
        }
    }

    #[test]
    fn staged_kv_tag_kind_batch_put_is_insert() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 2 })).unwrap();
        let op = KvOp::BatchPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
            entries: vec![(b"k".to_vec(), b"v".to_vec())],
            ttl_ms: 0,
            surrogates: vec![nodedb_types::Surrogate::ZERO],
            returning: None,
            rls_filters: Vec::new(),
        };
        assert_eq!(staged_kv_tag_kind(&op, &payload), StagedTagKind::Insert);
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
    fn staged_kv_tag_kind_expire_and_persist_are_update() {
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
        assert_eq!(staged_kv_tag_kind(&expire, &payload), StagedTagKind::Update);
        assert_eq!(
            staged_kv_tag_kind(&persist, &payload),
            StagedTagKind::Update
        );
    }
}
