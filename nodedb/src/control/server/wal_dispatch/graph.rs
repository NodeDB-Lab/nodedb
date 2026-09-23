// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Graph(GraphOp)`, plus batched graph
//! edge writes (`EdgePutBatch`/`EdgeDeleteBatch`) from `CREATE GRAPH INDEX`.
//!
//! Each batch edge appends as its own single-edge `Put`/`Delete` record. Batch
//! `properties` is always empty, matching what `execute_edge_put_batch` applies.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

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
            src_surrogate: _,
            dst_surrogate: _,
        } => {
            let entry = zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id, properties))
                .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal edge put: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Compiled write predicate is a planning-time artifact, deliberately not in the WAL entry.
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate: _,
            dst_surrogate: _,
            rls_write_check: _,
        } => {
            let entry =
                zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal edge delete: {e}"),
                    }
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
        | GraphOp::Stats { .. } => None,
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
    let mut last_lsn = None;
    for edge in edges {
        let properties: Vec<u8> = Vec::new();
        let entry = zerompk::to_msgpack_vec(&(
            &edge.collection,
            &edge.src_id,
            &edge.label,
            &edge.dst_id,
            &properties,
        ))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal edge put batch: {e}"),
        })?;
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
    let mut last_lsn = None;
    for edge in edges {
        let entry =
            zerompk::to_msgpack_vec(&(&edge.collection, &edge.src_id, &edge.label, &edge.dst_id))
                .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal edge delete batch: {e}"),
            })?;
        last_lsn = Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?);
    }
    Ok(last_lsn)
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
            wal.appender(NO_APPLY_KEY),
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

        let (collection, src_id, label, dst_id, properties) =
            zerompk::from_msgpack::<(String, String, String, String, Vec<u8>)>(&puts[0].payload)
                .expect("decode edge put payload");
        assert_eq!(collection, "knows");
        assert_eq!(src_id, "a");
        assert_eq!(label, "KNOWS");
        assert_eq!(dst_id, "b");
        assert!(properties.is_empty(), "batch edges carry no properties");

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
            wal.appender(NO_APPLY_KEY),
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

        let (collection, src_id, label, dst_id) =
            zerompk::from_msgpack::<(String, String, String, String)>(&deletes[0].payload)
                .expect("decode edge delete payload");
        assert_eq!(collection, "knows");
        assert_eq!(src_id, "a");
        assert_eq!(label, "KNOWS");
        assert_eq!(dst_id, "b");

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
            wal.appender(NO_APPLY_KEY),
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
            wal.appender(NO_APPLY_KEY),
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
            edge_label: None,
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
