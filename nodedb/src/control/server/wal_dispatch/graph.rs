// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Graph(GraphOp)`, plus batched graph
//! edge writes (`EdgePutBatch`/`EdgeDeleteBatch`) from `CREATE GRAPH INDEX`.
//!
//! Each batch edge appends as its own single-edge `Put`/`Delete` record. Batch
//! `properties` is always empty, matching what `execute_edge_put_batch` applies.
//!
//! An edge record is an [`EdgePutRedo`] or [`EdgeDeleteRedo`], the same payload
//! a transaction's redo carries. It carries both endpoint surrogates. A write
//! whose endpoint surrogate is `Surrogate::ZERO` is refused before any append.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;
use crate::wal::{EdgeDeleteRedo, EdgePutRedo};

/// Append the WAL record for a single `GraphOp`, returning the allocated LSN
/// for edge/node-label writes or `None` for traversal/algorithm/read variants.
/// Exhaustive match so a future write variant can't silently become non-durable.
pub(super) fn wal_append_graph_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &GraphOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } => {
            let entry = encode_edge_put(EdgePutRedo {
                collection: collection.to_string(),
                src_id: src_id.clone(),
                label: label.clone(),
                dst_id: dst_id.clone(),
                properties: properties.clone(),
                src_surrogate: src_surrogate.as_u32(),
                dst_surrogate: dst_surrogate.as_u32(),
                system_from: None,
                applied: None,
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Compiled write predicate is a planning-time artifact, deliberately not in the WAL entry.
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check: _,
        } => {
            let entry = encode_edge_delete(EdgeDeleteRedo {
                collection: collection.to_string(),
                src_id: src_id.clone(),
                label: label.clone(),
                dst_id: dst_id.clone(),
                src_surrogate: src_surrogate.as_u32(),
                dst_surrogate: dst_surrogate.as_u32(),
                system_from: None,
                applied: None,
            })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        GraphOp::SetNodeLabels { node_id, labels } => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_set(tenant_id, vshard_id, database_id, &entry)?)
        }
        GraphOp::RemoveNodeLabels { node_id, labels } => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_remove(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Batched edge writes (`CREATE GRAPH INDEX` build/rollback). See module
        // doc for encoding and the last-LSN-as-watermark contract.
        GraphOp::EdgePutBatch { edges } => {
            wal_append_graph_edge_put_batch(wal, tenant_id, vshard_id, database_id, edges)?
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            wal_append_graph_edge_delete_batch(wal, tenant_id, vshard_id, database_id, edges)?
        }
        // Reads / query ops / read-only resolve pass — no engine mutation here.
        GraphOp::ResolveEdgeDelete(_)
        | GraphOp::Hop { .. }
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
        | GraphOp::Stats { .. }
        | GraphOp::NodePresenceRead { .. } => None,
        // A node delete's guard writes nothing. A TRUNCATE's edge share runs
        // only inside a Calvin transaction, whose resolved redo journals its
        // tombstones.
        GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. } => None,
    };
    Ok(appended)
}

/// Append one `Put` WAL record per edge in a batched edge insert. Returns the
/// last record's LSN as a "durable through here" watermark. Empty batch → `Ok(None)`.
pub(crate) fn wal_append_graph_edge_put_batch(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    edges: &[BatchEdge],
) -> crate::Result<Option<Lsn>> {
    let entries = edges
        .iter()
        .map(|edge| {
            encode_edge_put(EdgePutRedo {
                collection: edge.collection.to_string(),
                src_id: edge.src_id.clone(),
                label: edge.label.clone(),
                dst_id: edge.dst_id.clone(),
                properties: Vec::new(),
                src_surrogate: edge.src_surrogate.as_u32(),
                dst_surrogate: edge.dst_surrogate.as_u32(),
                system_from: None,
                applied: None,
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let mut last_lsn = None;
    for entry in entries {
        last_lsn = Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?);
    }
    Ok(last_lsn)
}

/// Append one `Delete` WAL record per edge in a batched edge delete (`CREATE
/// GRAPH INDEX` rollback). Same last-LSN-as-watermark contract as [`wal_append_graph_edge_put_batch`].
pub(crate) fn wal_append_graph_edge_delete_batch(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    edges: &[BatchEdge],
) -> crate::Result<Option<Lsn>> {
    let entries = edges
        .iter()
        .map(|edge| {
            encode_edge_delete(EdgeDeleteRedo {
                collection: edge.collection.to_string(),
                src_id: edge.src_id.clone(),
                label: edge.label.clone(),
                dst_id: edge.dst_id.clone(),
                src_surrogate: edge.src_surrogate.as_u32(),
                dst_surrogate: edge.dst_surrogate.as_u32(),
                system_from: None,
                applied: None,
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let mut last_lsn = None;
    for entry in entries {
        last_lsn = Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?);
    }
    Ok(last_lsn)
}

/// The refusal for an edge record whose endpoint surrogate is unbound.
fn unbound_edge(collection: &str, src_id: &str, label: &str, dst_id: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "edge '{src_id}'-'{label}'->'{dst_id}' in '{collection}' carries an unbound \
             endpoint surrogate; every edge write carries both endpoint identities"
        ),
    }
}

/// Encode an edge put record. Refuses an unbound endpoint surrogate.
pub(super) fn encode_edge_put(record: EdgePutRedo) -> crate::Result<Vec<u8>> {
    if record.endpoints().is_none() {
        return Err(unbound_edge(
            &record.collection,
            &record.src_id,
            &record.label,
            &record.dst_id,
        ));
    }
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal edge put: {e}"),
    })
}

/// Encode an edge delete record. Refuses an unbound endpoint surrogate.
pub(super) fn encode_edge_delete(record: EdgeDeleteRedo) -> crate::Result<Vec<u8>> {
    if record.endpoints().is_none() {
        return Err(unbound_edge(
            &record.collection,
            &record.src_id,
            &record.label,
            &record.dst_id,
        ));
    }
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal edge delete: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
    use nodedb_types::Surrogate;

    fn edge(collection: &str, src: &str, label: &str, dst: &str) -> BatchEdge {
        BatchEdge {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        }
    }

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    #[test]
    fn put_batch_appends_one_record_per_edge_and_returns_last_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let edges = vec![
            edge("knows", "a", "KNOWS", "b"),
            edge("knows", "b", "KNOWS", "c"),
            edge("knows", "c", "KNOWS", "d"),
        ];

        let lsn = wal_append_graph_edge_put_batch(
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &edges,
        )
        .expect("append put batch")
        .expect("non-empty batch must produce Some(lsn)");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        let puts: Vec<_> = records
            .iter()
            .filter(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::Put)
            })
            .collect();
        assert_eq!(puts.len(), 3, "one Put record per edge");

        let put = zerompk::from_msgpack::<EdgePutRedo>(&puts[0].payload)
            .expect("decode edge put payload");
        assert_eq!(put.collection, "knows");
        assert_eq!(put.src_id, "a");
        assert_eq!(put.label, "KNOWS");
        assert_eq!(put.dst_id, "b");
        assert!(put.properties.is_empty(), "batch edges carry no properties");
        assert_eq!(
            put.endpoints(),
            Some((Surrogate::new(1), Surrogate::new(2))),
            "each batch edge record carries both endpoint surrogates"
        );

        assert_eq!(
            lsn.as_u64(),
            puts.last().expect("at least one record").header.lsn,
            "returned LSN is the last appended record's LSN"
        );
    }

    #[test]
    fn delete_batch_appends_one_record_per_edge_and_returns_last_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let edges = vec![
            edge("knows", "a", "KNOWS", "b"),
            edge("knows", "b", "KNOWS", "c"),
        ];

        let lsn = wal_append_graph_edge_delete_batch(
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &edges,
        )
        .expect("append delete batch")
        .expect("non-empty batch must produce Some(lsn)");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        let deletes: Vec<_> = records
            .iter()
            .filter(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::Delete)
            })
            .collect();
        assert_eq!(deletes.len(), 2, "one Delete record per edge");

        let delete = zerompk::from_msgpack::<EdgeDeleteRedo>(&deletes[0].payload)
            .expect("decode edge delete payload");
        assert_eq!(delete.collection, "knows");
        assert_eq!(delete.src_id, "a");
        assert_eq!(delete.label, "KNOWS");
        assert_eq!(delete.dst_id, "b");
        assert_eq!(
            delete.endpoints(),
            Some((Surrogate::new(1), Surrogate::new(2))),
            "each batch edge delete record carries both endpoint surrogates"
        );

        assert_eq!(
            lsn.as_u64(),
            deletes.last().expect("at least one record").header.lsn
        );
    }

    #[test]
    fn empty_put_batch_returns_none_explicitly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());

        let lsn = wal_append_graph_edge_put_batch(
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &[],
        )
        .expect("append empty put batch");

        assert_eq!(lsn, None, "empty batch has no durable record");
    }

    #[test]
    fn empty_delete_batch_returns_none_explicitly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());

        let lsn = wal_append_graph_edge_delete_batch(
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User),
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &[],
        )
        .expect("append empty delete batch");

        assert_eq!(lsn, None, "empty batch has no durable record");
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn edge_put_appends_put_record() {
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "knows"),
            src_id: "a".to_string(),
            label: "KNOWS".to_string(),
            dst_id: "b".to_string(),
            properties: vec![],
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "EdgePut must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::Put
        ));
    }

    /// The autocommit edge records carry both endpoint surrogates, and an edge
    /// write with an unbound endpoint is refused before any append.
    #[test]
    fn edge_records_carry_endpoint_identity_and_refuse_unbound() {
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let append = |plan: &PhysicalPlan| {
            super::super::wal_append_if_write(
                &wal,
                TenantId::new(7),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
        };
        let collection = nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "knows");
        let put = |src: Surrogate| {
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: collection.clone(),
                src_id: "a".to_string(),
                label: "KNOWS".to_string(),
                dst_id: "b".to_string(),
                properties: vec![],
                src_surrogate: src,
                dst_surrogate: Surrogate::new(6),
            })
        };
        let delete = |dst: Surrogate| {
            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                collection: collection.clone(),
                src_id: "a".to_string(),
                label: "KNOWS".to_string(),
                dst_id: "b".to_string(),
                src_surrogate: Surrogate::new(5),
                dst_surrogate: dst,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            })
        };
        assert!(
            append(&put(Surrogate::ZERO)).is_err(),
            "unbound put refused"
        );
        assert!(
            append(&delete(Surrogate::ZERO)).is_err(),
            "unbound delete refused"
        );
        append(&put(Surrogate::new(5))).expect("bound put");
        append(&delete(Surrogate::new(6))).expect("bound delete");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        assert_eq!(records.len(), 2, "only the bound writes append");
        let put = zerompk::from_msgpack::<EdgePutRedo>(&records[0].payload).expect("put");
        let delete = zerompk::from_msgpack::<EdgeDeleteRedo>(&records[1].payload).expect("delete");
        let bound = Some((Surrogate::new(5), Surrogate::new(6)));
        assert_eq!(put.endpoints(), bound);
        assert_eq!(delete.endpoints(), bound);
    }

    #[test]
    fn set_node_labels_appends_label_set_record() {
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::SetNodeLabels {
            node_id: "a".to_string(),
            labels: vec!["Person".to_string()],
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SetNodeLabels must produce a durable LSN"
        );
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::GraphNodeLabelSet
        ));
    }

    #[test]
    fn read_op_appends_nothing() {
        use nodedb_graph::Direction;
        use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
            node_id: "a".to_string(),
            edge_labels: Vec::new(),
            direction: Direction::Out,
            rls_filters: vec![],
            collection: None,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(7),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "read op must produce no durable LSN");
    }
}
