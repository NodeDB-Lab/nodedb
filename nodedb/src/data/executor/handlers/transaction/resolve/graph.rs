// SPDX-License-Identifier: BUSL-1.1

//! Graph serializer for transaction resolve. Turns staged edge post-images
//! into the same WAL sub-record shapes the autocommit path produces. Each
//! edge put and delete carries its two endpoint surrogates, which the overlay
//! does not hold (`entry.rs` collects them from the plan nodes). Overlay entries
//! are `HashMap`/`HashSet`-keyed, so they're sorted before emitting so
//! replicas produce identical ops.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, NodeLabelDelta,
};
use crate::wal::{NodeLabelRedo, RedoSubRecord};

use super::edge_ordinal::EdgeOrdinals;

/// Edge identity key: `(collection, src_id, label, dst_id)`. Scoped by
/// collection because `entry.rs` collects surrogates for every touched
/// collection into one map before calling this serializer per collection.
pub(super) type EdgeIdentityKey = (String, String, String, String);

/// Append the redo sub-records for every graph edge post-image staged in
/// `overlay` for `coll_key` to `ops`, in deterministic edge-identity order.
/// `edge_surrogates` maps identity to the `(src, dst)` surrogate pair
/// `entry.rs` collected from the plan nodes — the overlay carries none. A
/// staged edge with no bound pair is refused. `ordinals` names each
/// version's system-time ordinal.
pub(super) fn serialize_graph_collection(
    overlay: &GraphTxnOverlay,
    coll_key: &GraphCollKey,
    collection: &str,
    edge_surrogates: &BTreeMap<EdgeIdentityKey, (u32, u32)>,
    ordinals: &EdgeOrdinals<'_>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut puts: BTreeMap<(String, String, String), Vec<u8>> = BTreeMap::new();
    for (src, label, dst, properties) in overlay.staged_edges_for_collection(coll_key) {
        puts.insert(
            (src.to_string(), label.to_string(), dst.to_string()),
            properties.to_vec(),
        );
    }

    let mut deletes: BTreeSet<(String, String, String)> = BTreeSet::new();
    for (src, label, dst) in overlay.staged_tombstones_for_collection(coll_key) {
        deletes.insert((src.to_string(), label.to_string(), dst.to_string()));
    }

    for ((src, label, dst), properties) in puts {
        let valid = overlay.staged_edge_valid_time(coll_key, &src, &label, &dst);
        let identity_key = (
            collection.to_string(),
            src.clone(),
            label.clone(),
            dst.clone(),
        );
        let (src_surrogate, dst_surrogate) =
            bound_endpoints(edge_surrogates, &identity_key, "put")?;
        let stamp = ordinals.for_edge(collection, &src, &label, &dst)?;
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: collection.to_string(),
            src_id: src,
            label,
            dst_id: dst,
            properties,
            src_surrogate,
            dst_surrogate,
            system_from: Some(stamp.system_from),
            applied: (stamp.applied != stamp.system_from).then_some(stamp.applied),
            valid_from_ms: valid.valid_from_ms,
            valid_until_ms: valid.valid_until_ms,
        })
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("graph resolve edge put: {e}"),
        })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        });
    }

    for (src, label, dst) in deletes {
        let identity_key = (
            collection.to_string(),
            src.clone(),
            label.clone(),
            dst.clone(),
        );
        let (src_surrogate, dst_surrogate) =
            bound_endpoints(edge_surrogates, &identity_key, "delete")?;
        let stamp = ordinals.for_edge(collection, &src, &label, &dst)?;
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgeDeleteRedo {
            collection: collection.to_string(),
            src_id: src,
            label,
            dst_id: dst,
            src_surrogate,
            dst_surrogate,
            system_from: Some(stamp.system_from),
            applied: (stamp.applied != stamp.system_from).then_some(stamp.applied),
        })
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("graph resolve edge delete: {e}"),
        })?;
        ops.push(RedoSubRecord {
            record_type: RecordType::Delete as u32,
            payload,
        });
    }
    Ok(())
}

/// The bound `(src, dst)` surrogate pair of a staged edge. Refuses an edge the
/// plan nodes named no pair for, and a pair holding `Surrogate::ZERO`.
fn bound_endpoints(
    edge_surrogates: &BTreeMap<EdgeIdentityKey, (u32, u32)>,
    identity_key: &EdgeIdentityKey,
    verb: &str,
) -> crate::Result<(u32, u32)> {
    let unbound = || {
        let (collection, src, label, dst) = identity_key;
        crate::Error::Internal {
            detail: format!(
                "graph resolve: staged edge {verb} '{collection}'/'{src}'-'{label}'->'{dst}' \
                 has no bound endpoint surrogates"
            ),
        }
    };
    let (src, dst) = edge_surrogates
        .get(identity_key)
        .copied()
        .ok_or_else(unbound)?;
    let zero = nodedb_types::Surrogate::ZERO.as_u32();
    if src == zero || dst == zero {
        return Err(unbound());
    }
    Ok((src, dst))
}

/// Append the redo sub-records for every staged node-label delta in
/// `overlay` under `label_coll_key`, in deterministic node-id order.
/// `added`/`removed` are disjoint by construction. Each `HashSet` is sorted
/// into a `Vec` first, so replicas produce byte-identical payloads. Every
/// sub-record of a node carries `ordinal_of(node)` as its `system_from`, so
/// every replica writes the node's label version at the same ordinal.
pub(super) fn serialize_node_label_deltas(
    overlay: &GraphTxnOverlay,
    label_coll_key: &GraphCollKey,
    ordinal_of: impl Fn(&str) -> crate::Result<i64>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut deltas: BTreeMap<&str, &NodeLabelDelta> = BTreeMap::new();
    for (node_id, delta) in overlay.staged_node_label_deltas_for_collection(label_coll_key) {
        deltas.insert(node_id, delta);
    }

    for (node_id, delta) in deltas {
        let system_from = Some(ordinal_of(node_id)?);
        let parts = [
            (RecordType::GraphNodeLabelSet, &delta.added),
            (RecordType::GraphNodeLabelRemove, &delta.removed),
        ];
        for (record_type, labels) in parts {
            if labels.is_empty() {
                continue;
            }
            let payload = NodeLabelRedo {
                node_id: node_id.to_string(),
                labels: sorted_labels(labels),
                system_from,
                exact: false,
            }
            .encode()?;
            ops.push(RedoSubRecord {
                record_type: record_type as u32,
                payload,
            });
        }
    }
    Ok(())
}

/// The ordinal a transaction at `txn` stamps on `node`'s label version: `txn`,
/// raised one above the node's newest stored version when that one is at or
/// above it (a restored version from a clock that ran ahead). The node's
/// version then sorts after every earlier write of its labels.
pub(super) fn label_ordinal(
    store: &crate::engine::graph::edge_store::EdgeStore,
    database_id: crate::types::DatabaseId,
    tenant_id: crate::types::TenantId,
    node: &str,
    txn: i64,
) -> crate::Result<i64> {
    Ok(
        match store.newest_node_label_ordinal(database_id, tenant_id, node)? {
            Some(newest) if newest >= txn => newest.saturating_add(1),
            _ => txn,
        },
    )
}

/// Sort a staged label `HashSet` into a deterministic `Vec` before encoding.
fn sorted_labels(labels: &std::collections::HashSet<String>) -> Vec<String> {
    let mut sorted: Vec<String> = labels.iter().cloned().collect();
    sorted.sort();
    sorted
}

/// Classify a Graph op for transaction resolve: collect a staged edge
/// write's collection, collect edge-put endpoint surrogates into
/// `edge_surrogates`, skip read-only ops, and skip node-label ops — their
/// deltas live under a fixed sentinel key, serialized unconditionally by
/// `resolve_txn_ops`.
pub(super) fn classify_graph_op(
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
            collections.insert(collection.to_string());
            edge_surrogates.insert(
                (
                    collection.to_string(),
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
                collections.insert(edge.collection.to_string());
                edge_surrogates.insert(
                    (
                        edge.collection.to_string(),
                        edge.src_id.clone(),
                        edge.label.clone(),
                        edge.dst_id.clone(),
                    ),
                    (edge.src_surrogate.as_u32(), edge.dst_surrogate.as_u32()),
                );
            }
            Ok(())
        }

        // Edge delete: the redo delete carries both endpoint surrogates like
        // a put, so they are collected from the plan node the same way.
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        } => {
            collections.insert(collection.to_string());
            edge_surrogates.insert(
                (
                    collection.to_string(),
                    src_id.clone(),
                    label.clone(),
                    dst_id.clone(),
                ),
                (src_surrogate.as_u32(), dst_surrogate.as_u32()),
            );
            Ok(())
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.to_string());
                edge_surrogates.insert(
                    (
                        edge.collection.to_string(),
                        edge.src_id.clone(),
                        edge.label.clone(),
                        edge.dst_id.clone(),
                    ),
                    (edge.src_surrogate.as_u32(), edge.dst_surrogate.as_u32()),
                );
            }
            Ok(())
        }

        // Read-only families: traversal, pattern matching, algorithms, and
        // stats carry no persisted post-image. Nor does the resolve pass.
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
        | GraphOp::Stats { .. }
        | GraphOp::NodePresenceRead { .. } => Ok(()),

        // Node-label deltas live under the fixed sentinel key, not a
        // per-collection post-image — nothing to collect here.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => Ok(()),

        // A guard writes nothing. A Calvin transaction checks it when it
        // stages, and a session COMMIT checks it before its resolve
        // (`execute_resolve_session_txn`).
        GraphOp::NodeEdgeGuard { .. } | GraphOp::NodePresenceGuard { .. } => Ok(()),

        // Stages nothing: `serialize_truncated_edges` writes the share's cut.
        GraphOp::TruncateEdges { .. } => Ok(()),
    }
}

impl CoreLoop {
    /// Append one cut sub-record per TRUNCATE share among `plans`, at the
    /// Calvin transaction's ordinal.
    ///
    /// The cut reads no stored edge: every read hides the collection's
    /// versions applied below it. Its effect is the same whatever this core
    /// applied before or after it, so a core that holds several vShards, or
    /// both homes of an edge, reaches the state every other replica does.
    /// Every share of one TRUNCATE records the same cut.
    pub(super) fn serialize_truncated_edges(
        &self,
        plans: &[PhysicalPlan],
        ops: &mut Vec<RedoSubRecord>,
    ) -> crate::Result<()> {
        for plan in plans {
            let PhysicalPlan::Graph(GraphOp::TruncateEdges { collection, .. }) = plan else {
                continue;
            };
            let cut =
                self.apply_scope
                    .calvin_txn_ordinal
                    .ok_or_else(|| crate::Error::Internal {
                        detail: "a TRUNCATE edge share resolved outside a Calvin transaction"
                            .into(),
                    })?;
            let payload = zerompk::to_msgpack_vec(&crate::wal::EdgeCutRedo {
                collection: collection.as_str().to_string(),
                cut,
            })
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("graph resolve truncate cut: {e}"),
            })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::GraphEdgeCut as u32,
                payload,
            });
        }
        Ok(())
    }
}

/// Whether `plan` writes node labels, which stage into the graph overlay
/// under its fixed label key.
pub(super) fn is_label_write(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn coll_key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(DB), TenantId::new(TID), coll.to_string())
    }

    #[test]
    fn edge_put_emits_timestamped_tuple_with_surrogates() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", vec![1, 2, 3]);

        let mut surrogates = BTreeMap::new();
        surrogates.insert(
            (
                "g".to_string(),
                "a".to_string(),
                "knows".to_string(),
                "b".to_string(),
            ),
            (10u32, 20u32),
        );

        let mut ops = Vec::new();
        serialize_graph_collection(
            &overlay,
            &coll_key("g"),
            "g",
            &surrogates,
            &EdgeOrdinals::fixed(123),
            &mut ops,
        )
        .expect("serialize edge put");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Put as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&ops[0].payload)
            .expect("decode edge put redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(decoded.properties, vec![1, 2, 3]);
        assert_eq!(decoded.src_surrogate, 10);
        assert_eq!(decoded.dst_surrogate, 20);
        assert_eq!(decoded.system_from, Some(123));
    }

    #[test]
    fn edge_put_without_bound_surrogates_is_typed_error() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "a", "knows", "b", vec![]);

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        let result = serialize_graph_collection(
            &overlay,
            &coll_key("g"),
            "g",
            &surrogates,
            &EdgeOrdinals::fixed(123),
            &mut ops,
        );
        assert!(
            result.is_err(),
            "a staged put with no matching plan-carried surrogates must error, not invent one"
        );
    }

    #[test]
    fn edge_delete_emits_timestamped_redo() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");

        let mut surrogates = BTreeMap::new();
        surrogates.insert(
            (
                "g".to_string(),
                "a".to_string(),
                "knows".to_string(),
                "b".to_string(),
            ),
            (10u32, 20u32),
        );
        let mut ops = Vec::new();
        serialize_graph_collection(
            &overlay,
            &coll_key("g"),
            "g",
            &surrogates,
            &EdgeOrdinals::fixed(123),
            &mut ops,
        )
        .expect("serialize edge delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Delete as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&ops[0].payload)
            .expect("decode edge delete redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(decoded.src_surrogate, 10);
        assert_eq!(decoded.dst_surrogate, 20);
        assert_eq!(
            decoded.system_from,
            Some(123),
            "delete must carry the frozen system-time ordinal for deterministic replay"
        );
    }

    /// A staged delete whose plan named no pair, or an unbound one, is refused.
    #[test]
    fn edge_delete_without_bound_surrogates_is_typed_error() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");
        let key = (
            "g".to_string(),
            "a".to_string(),
            "knows".to_string(),
            "b".to_string(),
        );
        for surrogates in [
            BTreeMap::new(),
            BTreeMap::from([(key.clone(), (10u32, 0u32))]),
        ] {
            let mut ops = Vec::new();
            let result = serialize_graph_collection(
                &overlay,
                &coll_key("g"),
                "g",
                &surrogates,
                &EdgeOrdinals::fixed(123),
                &mut ops,
            );
            assert!(result.is_err(), "an unbound staged delete must be refused");
        }
    }

    #[test]
    fn entries_emit_in_deterministic_edge_identity_order() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(coll_key("g"), "c", "l", "z", vec![]);
        overlay.stage_edge_put(coll_key("g"), "a", "l", "x", vec![]);
        overlay.stage_edge_put(coll_key("g"), "b", "l", "y", vec![]);

        let mut surrogates = BTreeMap::new();
        for (s, d) in [("a", "x"), ("b", "y"), ("c", "z")] {
            surrogates.insert(
                (
                    "g".to_string(),
                    s.to_string(),
                    "l".to_string(),
                    d.to_string(),
                ),
                (1u32, 2u32),
            );
        }

        let mut ops = Vec::new();
        serialize_graph_collection(
            &overlay,
            &coll_key("g"),
            "g",
            &surrogates,
            &EdgeOrdinals::fixed(123),
            &mut ops,
        )
        .expect("serialize");
        let srcs: Vec<String> = ops
            .iter()
            .map(|op| {
                zerompk::from_msgpack::<crate::wal::EdgePutRedo>(&op.payload)
                    .expect("decode")
                    .src_id
            })
            .collect();
        assert_eq!(srcs, vec!["a", "b", "c"], "src-id ascending order");
    }

    #[test]
    fn node_label_set_emits_graph_node_label_set_subrecord() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), |_| Ok(123), &mut ops)
            .expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelSet as u32);

        let decoded = NodeLabelRedo::decode(&ops[0].payload).expect("decode");
        assert_eq!(decoded.node_id, "n1");
        assert_eq!(decoded.labels, vec!["Person".to_string()]);
        assert_eq!(
            decoded.system_from,
            Some(123),
            "the sub-record carries the transaction's ordinal"
        );
    }

    #[test]
    fn node_label_remove_emits_graph_node_label_remove_subrecord() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), |_| Ok(123), &mut ops)
            .expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelRemove as u32);

        let decoded = NodeLabelRedo::decode(&ops[0].payload).expect("decode");
        assert_eq!(decoded.node_id, "n1");
        assert_eq!(decoded.labels, vec!["Person".to_string()]);
        assert_eq!(decoded.system_from, Some(123));
    }

    #[test]
    fn node_label_added_and_removed_on_same_node_emit_both_subrecords() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(coll_key("g"), "n1", &["Robot".to_string()]);
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), |_| Ok(123), &mut ops)
            .expect("serialize");
        assert_eq!(
            ops.len(),
            2,
            "a node with both an added and a removed label emits both sub-records"
        );

        let types: Vec<u32> = ops.iter().map(|op| op.record_type).collect();
        assert!(types.contains(&(RecordType::GraphNodeLabelSet as u32)));
        assert!(types.contains(&(RecordType::GraphNodeLabelRemove as u32)));
    }

    #[test]
    fn node_label_deltas_emit_deterministic_bytes_regardless_of_insertion_order() {
        // Two overlays, same final delta, different HashSet insertion order —
        // the encoded redo sub-record bytes must be byte-identical.
        let mut overlay_a = GraphTxnOverlay::new();
        overlay_a.stage_node_labels_set(
            coll_key("g"),
            "n1",
            &["Zeta".to_string(), "Alpha".to_string(), "Mu".to_string()],
        );

        let mut overlay_b = GraphTxnOverlay::new();
        overlay_b.stage_node_labels_set(
            coll_key("g"),
            "n1",
            &["Mu".to_string(), "Zeta".to_string(), "Alpha".to_string()],
        );

        let mut ops_a = Vec::new();
        serialize_node_label_deltas(&overlay_a, &coll_key("g"), |_| Ok(123), &mut ops_a)
            .expect("serialize a");
        let mut ops_b = Vec::new();
        serialize_node_label_deltas(&overlay_b, &coll_key("g"), |_| Ok(123), &mut ops_b)
            .expect("serialize b");

        assert_eq!(ops_a.len(), 1);
        assert_eq!(
            ops_a[0].payload, ops_b[0].payload,
            "sorted labels must produce byte-identical payloads regardless of \
             HashSet insertion order"
        );
    }

    #[test]
    fn no_staged_labels_emits_nothing() {
        let overlay = GraphTxnOverlay::new();
        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), |_| Ok(123), &mut ops)
            .expect("serialize");
        assert!(ops.is_empty());
    }
}
