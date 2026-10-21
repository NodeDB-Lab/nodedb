// SPDX-License-Identifier: BUSL-1.1

//! Write-metadata extraction for dispatched writes: which rows a physical plan
//! changes, named the way a CDC subscriber addresses them.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::change_stream::ChangeOperation;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, DocumentResolvedMutation, MetaOp, TimeseriesOp,
    VectorOp, VectorResolvedMutation, VectorWriteTargets,
};
use nodedb_types::{RowIdentity, StorageKey};

use super::cluster_array::cluster_array_change_meta;
use super::redo::redo_change_meta;

/// One row change a plan yields: `(collection, row identity, op)`.
pub(super) type WriteChangeMeta = (String, RowIdentity, ChangeOperation);

/// The identity a batch or predicate write reports: every row in the
/// collection, not one addressable row. A subscriber sees `"*"`.
pub(super) fn every_row() -> RowIdentity {
    RowIdentity::from_user_key("*")
}

/// One event per point-targeted vector-primary surrogate; a predicate write
/// names every row, like `BulkUpdate` / `BulkDelete`.
fn vector_target_events(
    collection: &nodedb_types::QualifiedCollection,
    targets: &VectorWriteTargets,
    operation: ChangeOperation,
) -> Vec<WriteChangeMeta> {
    match targets {
        VectorWriteTargets::Surrogates(surrogates) => surrogates
            .iter()
            .map(|s| {
                (
                    collection.to_string(),
                    StorageKey::for_surrogate(*s).to_identity(),
                    operation,
                )
            })
            .collect(),
        VectorWriteTargets::Predicate(_) => {
            vec![(collection.to_string(), every_row(), operation)]
        }
    }
}

/// One event per resolved vector-primary mutation, naming every row touched —
/// never collapsed to "*". An upsert's pre-image is what the resolve found
/// stored: absent is an insert, present is an update.
fn vector_resolved_change_meta(
    collection: &str,
    mutations: &[VectorResolvedMutation],
) -> Vec<WriteChangeMeta> {
    mutations
        .iter()
        .map(|mutation| {
            let operation = match mutation {
                VectorResolvedMutation::Delete { .. } => ChangeOperation::Delete,
                VectorResolvedMutation::Update { .. } => ChangeOperation::Update,
                VectorResolvedMutation::Upsert { old_payload, .. } => match old_payload {
                    Some(_) => ChangeOperation::Update,
                    None => ChangeOperation::Insert,
                },
            };
            (
                collection.to_owned(),
                StorageKey::for_surrogate(mutation.surrogate()).to_identity(),
                operation,
            )
        })
        .collect()
}

/// Extract write metadata from a physical plan for change event publishing.
///
/// One `(collection, identity, op)` tuple per row change; empty for reads/DDL.
/// The identity is the one a CDC subscriber addresses the row by: the
/// user-facing primary key, the KV key bytes, or the decimal surrogate.
/// Exhaustive over [`PhysicalPlan`] — no catch-all — so a new variant is a compile error.
pub(super) fn extract_write_metadata(
    plan: &PhysicalPlan,
    _tenant_id: TenantId,
) -> Vec<WriteChangeMeta> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Delete,
        )],
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Update,
        )],
        // `PointInsert` is plain SQL INSERT; distinct from `PointPut` (unconditional
        // overwrite, used by non-SQL write paths).
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Insert,
        )],
        // One event per row of the page, reported like a point `Upsert`.
        PhysicalPlan::Document(DocumentOp::BatchUpsert {
            collection,
            documents,
            ..
        }) => documents
            .iter()
            .map(|(document_id, _)| {
                (
                    collection.to_string(),
                    RowIdentity::from_user_key(document_id.clone()),
                    ChangeOperation::Insert,
                )
            })
            .collect(),
        PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection, ..
        }) => vec![(
            target_collection.to_string(),
            every_row(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Update)]
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection, ..
        }) => vec![(
            target_collection.to_string(),
            every_row(),
            ChangeOperation::Update,
        )],
        // MERGE mixes INSERT/UPDATE/DELETE per arm; not individually addressable, so
        // reported as one `Update` like `BulkUpdate`.
        PhysicalPlan::Document(DocumentOp::Merge {
            target_collection, ..
        }) => vec![(
            target_collection.to_string(),
            every_row(),
            ChangeOperation::Update,
        )],
        // Reports one event per mutation, naming every row touched — never collapses to "*".
        // Precondition is what the resolve found stored: absent = insert, present = update.
        PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) => mutations
            .iter()
            .map(|mutation| {
                let operation = match mutation {
                    DocumentResolvedMutation::Delete { .. } => ChangeOperation::Delete,
                    DocumentResolvedMutation::Put { precondition, .. } => match precondition {
                        Some(_) => ChangeOperation::Update,
                        None => ChangeOperation::Insert,
                    },
                };
                (
                    mutation.collection().to_string(),
                    RowIdentity::from_user_key(mutation.document_id().to_string()),
                    operation,
                )
            })
            .collect(),
        // Remaining DocumentOp variants are reads or catalog/schema DDL — no row changed.
        PhysicalPlan::Document(_) => Vec::new(),

        // Batch write and truncate: document_id="*" names every row. Per-row
        // events will flood the bus — subscribe via collection_filter.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Timeseries(TimeseriesOp::Truncate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        // Scan and the resolve pass are reads — no row changed.
        // A refresh cut and partition maintenance change no row either.
        PhysicalPlan::Timeseries(
            TimeseriesOp::Scan { .. }
            | TimeseriesOp::ResolveIngest(_)
            | TimeseriesOp::RefreshAggregate { .. }
            | TimeseriesOp::Partitions { .. }
            | TimeseriesOp::ReadAggregate { .. },
        ) => Vec::new(),

        PhysicalPlan::Kv(op) => super::kv::kv_change_meta(op),

        // `spatial` rows are stored via the same `ColumnarOp` path as `columnar`.
        PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Insert)]
        }
        // The resolved-row-set forms are the same statements, same CDC event.
        PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Update)]
        }
        PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Truncate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        // Scan / MaterializeScan are reads — no row changed.
        PhysicalPlan::Columnar(_) => Vec::new(),

        // Array cells are data-bearing rows, not an index — need CDC.
        // `array_id.name` is the user-visible collection name.
        PhysicalPlan::Array(ArrayOp::Put { array_id, .. }) => {
            vec![(array_id.name.clone(), every_row(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Array(ArrayOp::Delete { array_id, .. }) => {
            vec![(array_id.name.clone(), every_row(), ChangeOperation::Delete)]
        }
        // Remaining ArrayOp variants are reads or maintenance — no user-data row changed.
        PhysicalPlan::Array(_) => Vec::new(),

        // Implicit edges mirror a document INSERT that already published the event.
        // `GRAPH INSERT EDGE` and `SetNodeLabels`/`RemoveNodeLabels` are known CDC gaps.
        PhysicalPlan::Graph(_) => Vec::new(),

        // Vector is normally a Document secondary index — publishing here will duplicate.
        // The direct write family is the exception: the sole writes for a vector-primary
        // collection. The row's identity is its PK-bound surrogate, rendered as the decimal
        // surrogate, which is what a sidecar read reports as the row id.
        PhysicalPlan::Vector(
            VectorOp::DirectUpsert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectInsert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectInsertIfAbsent {
                collection,
                surrogate,
                ..
            },
        ) => vec![(
            collection.to_string(),
            StorageKey::for_surrogate(*surrogate).to_identity(),
            ChangeOperation::Insert,
        )],
        // One event per row of the page, reported like its point write.
        PhysicalPlan::Vector(VectorOp::DirectBatchWrite {
            collection, rows, ..
        }) => rows
            .iter()
            .map(|row| {
                (
                    collection.to_string(),
                    StorageKey::for_surrogate(row.surrogate).to_identity(),
                    ChangeOperation::Insert,
                )
            })
            .collect(),
        PhysicalPlan::Vector(VectorOp::DirectDelete {
            collection,
            targets,
            ..
        }) => vector_target_events(collection, targets, ChangeOperation::Delete),
        PhysicalPlan::Vector(VectorOp::DirectTruncate { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Delete)]
        }
        PhysicalPlan::Vector(VectorOp::DirectUpdate {
            collection,
            targets,
            ..
        }) => vector_target_events(collection, targets, ChangeOperation::Update),
        PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
            collection,
            mutations,
            ..
        }) => vector_resolved_change_meta(&collection.to_string(), mutations),
        // The resolve pass reads; the rows it decides are published by the
        // resolved write that applies them.
        PhysicalPlan::Vector(_) => Vec::new(),

        // Spatial R-tree writes are index maintenance for a row already published
        // via `PhysicalPlan::Columnar` above.
        PhysicalPlan::Spatial(_) => Vec::new(),

        // FTS writes are BM25 index maintenance for a row that already published its event.
        PhysicalPlan::Text(_) => Vec::new(),

        // CRDT is data-bearing (Loro-backed content), so mutating ops need CDC.
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Delete,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection,
            document_id,
            ..
        })
        | PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Update,
        )],
        // Collection-wide snapshot import: no single document identity.
        PhysicalPlan::Crdt(CrdtOp::ImportSnapshot { collection, .. }) => {
            vec![(collection.to_string(), every_row(), ChangeOperation::Update)]
        }
        // Full replace = Insert, partial update = Update.
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection,
            document_id,
            partial,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            if *partial {
                ChangeOperation::Update
            } else {
                ChangeOperation::Insert
            },
        )],
        PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.to_string(),
            RowIdentity::from_user_key(document_id.clone()),
            ChangeOperation::Delete,
        )],
        // Remaining CrdtOp variants are reads, history maintenance, or config/DDL.
        PhysicalPlan::Crdt(_) => Vec::new(),

        // Query: joins, aggregates, coordinator Exchange nodes are read-only.
        PhysicalPlan::Query(_) => Vec::new(),

        // Publishes the same events its constituent writes will emit under autocommit.
        PhysicalPlan::Meta(MetaOp::TransactionBatch { plans, .. }) => plans
            .iter()
            .flat_map(|plan| extract_write_metadata(plan, _tenant_id))
            .collect(),
        // A committed transaction's redo publishes the rows it installs.
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { redo, .. }) => redo_change_meta(redo),
        PhysicalPlan::Meta(_) => Vec::new(),

        // A coordinator op publishes nothing itself: each shard's committed
        // array write publishes as its replicas apply it.
        PhysicalPlan::ClusterArray(op) => cluster_array_change_meta(op),
        PhysicalPlan::ClusterEvent(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::ClusterArrayOp;
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, GraphOp, KvOp};
    use nodedb_types::{
        DatabaseId, QualifiedCollection, Surrogate, VectorQuantization, VectorStorageDtype,
    };

    // Guards `Columnar`/`Array`/`Vector(DirectUpsert)` against a blanket `_ => None`.

    #[test]
    fn transaction_batch_emits_each_subplan_change_event() {
        let plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                    document_id: "u1".into(),
                    value: Vec::new(),
                    surrogate: Surrogate::new(1),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
                    document_id: "u2".into(),
                    surrogate: Some(Surrogate::new(2)),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                    resolved_sum_targets: Vec::new(),
                }),
            ],
            txn_id: None,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![
                (
                    "users".into(),
                    RowIdentity::from_user_key("u1"),
                    ChangeOperation::Insert
                ),
                (
                    "users".into(),
                    RowIdentity::from_user_key("u2"),
                    ChangeOperation::Delete
                ),
            ]
        );
    }

    #[test]
    fn columnar_insert_emits_change_event() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("metrics".to_string(), every_row(), ChangeOperation::Insert)]
        );
    }

    #[test]
    fn columnar_delete_emits_change_event() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            filters: Vec::new(),
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("metrics".to_string(), every_row(), ChangeOperation::Delete)]
        );
    }

    #[test]
    fn array_put_emits_change_event() {
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("genome".to_string(), every_row(), ChangeOperation::Insert)]
        );
    }

    #[test]
    fn array_delete_emits_change_event() {
        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 0,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("genome".to_string(), every_row(), ChangeOperation::Delete)]
        );
    }

    #[test]
    fn cluster_array_put_emits_change_event() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            cells: Vec::new(),
            wal_lsn: 7,
            prefix_bits: 8,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("genome".to_string(), every_row(), ChangeOperation::Insert)]
        );
    }

    #[test]
    fn cluster_array_delete_emits_change_event() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            coords: Vec::new(),
            wal_lsn: 7,
            prefix_bits: 8,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![("genome".to_string(), every_row(), ChangeOperation::Delete)]
        );
    }

    #[test]
    fn cluster_array_slice_and_agg_emit_no_change_event() {
        let slice = PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            slice_msgpack: Vec::new(),
            attr_projection: Vec::new(),
            limit: 0,
            slice_tile_keys: Vec::new(),
            prefix_bits: 8,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        assert!(extract_write_metadata(&slice, TenantId::new(1)).is_empty());

        let agg = PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            attr_idx: 0,
            reducer_msgpack: Vec::new(),
            group_by_dim: -1,
            slice_tile_keys: Vec::new(),
            prefix_bits: 8,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            region_msgpack: Vec::new(),
            physical: false,
        });
        assert!(extract_write_metadata(&agg, TenantId::new(1)).is_empty());
    }

    // Implicit edges mirror into a separate `GraphOp::EdgePut`; the underlying
    // `DocumentOp` already published the event — emitting here will double-publish.
    #[test]
    fn graph_edge_put_emits_no_change_event() {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: "alice".into(),
            label: "FOLLOWS".into(),
            dst_id: "bob".into(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            valid_from_ms: None,
            valid_until_ms: None,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert!(meta.is_empty());
    }

    #[test]
    fn vector_direct_upsert_emits_change_event() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
            field: "emb".into(),
            surrogate: Surrogate::new(42),
            pk_bytes: Vec::new(),
            vector: vec![0.0, 1.0],
            payload: Vec::new(),
            quantization: VectorQuantization::default(),
            storage_dtype: VectorStorageDtype::default(),
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        // The row id is the client identity of the row, the decimal
        // surrogate, so a consumer can address the row the event describes.
        assert_eq!(
            meta,
            vec![(
                "embeddings".to_string(),
                RowIdentity::from_user_key("42"),
                ChangeOperation::Insert
            )]
        );
    }

    // Non-`DirectUpsert` Vector ops stay silent — the Document write already published it.
    #[test]
    fn vector_secondary_index_insert_emits_no_change_event() {
        let plan = PhysicalPlan::Vector(VectorOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            vector_id: 7,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert!(meta.is_empty());
    }

    #[test]
    fn document_point_insert_emits_change_event() {
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            document_id: "u1".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "users".to_string(),
                RowIdentity::from_user_key("u1"),
                ChangeOperation::Insert
            )]
        );
    }

    #[test]
    fn kv_transfer_item_emits_two_change_events_across_collections() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "inventory_a"),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "inventory_b"),
            item_key: b"sword".to_vec(),
            dest_key: b"sword".to_vec(),
            surrogate: Surrogate::new(9),
            source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![
                (
                    "inventory_a".to_string(),
                    RowIdentity::from_user_key("sword"),
                    ChangeOperation::Delete
                ),
                (
                    "inventory_b".to_string(),
                    RowIdentity::from_user_key("sword"),
                    ChangeOperation::Insert
                ),
            ]
        );
    }
}
