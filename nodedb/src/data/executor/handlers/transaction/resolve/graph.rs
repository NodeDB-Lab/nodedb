// SPDX-License-Identifier: BUSL-1.1

//! Graph serializer for transaction resolve. Turns staged edge post-images
//! into the same WAL sub-record shapes the autocommit path produces, plus
//! the two endpoint surrogates a redo put needs but the overlay doesn't
//! carry (`entry.rs` collects those from the plan nodes). Overlay entries
//! are `HashMap`/`HashSet`-keyed, so they're sorted before emitting so
//! replicas produce identical ops.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::encode_graph_node_label_payload;
use crate::data::executor::handlers::transaction::overlay::{
    GraphCollKey, GraphTxnOverlay, NodeLabelDelta,
};
use crate::wal::RedoSubRecord;

/// Edge identity key: `(collection, src_id, label, dst_id)`. Scoped by
/// collection because `entry.rs` collects surrogates for every touched
/// collection into one map before calling this serializer per collection.
pub(super) type EdgeIdentityKey = (String, String, String, String);

/// Append the redo sub-records for every graph edge post-image staged in
/// `overlay` for `coll_key` to `ops`, in deterministic edge-identity order.
/// `edge_surrogates` maps identity to the `(src, dst)` surrogate pair
/// `entry.rs` collected from the plan nodes — the overlay carries none.
pub(super) fn serialize_graph_collection(
    overlay: &GraphTxnOverlay,
    coll_key: &GraphCollKey,
    collection: &str,
    edge_surrogates: &BTreeMap<EdgeIdentityKey, (u32, u32)>,
    system_from: i64,
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
        let identity_key = (
            collection.to_string(),
            src.clone(),
            label.clone(),
            dst.clone(),
        );
        let (src_surrogate, dst_surrogate) = edge_surrogates
            .get(&identity_key)
            .copied()
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "graph resolve: staged edge put '{collection}'/'{src}'-'{label}'->'{dst}' \
                         has no bound endpoint surrogates"
                ),
            })?;
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: collection.to_string(),
            src_id: src.clone(),
            label: label.clone(),
            dst_id: dst.clone(),
            properties: properties.clone(),
            src_surrogate,
            dst_surrogate,
            system_from: Some(system_from),
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
        let payload = zerompk::to_msgpack_vec(&crate::wal::EdgeDeleteRedo {
            collection: collection.to_string(),
            src_id: src.clone(),
            label: label.clone(),
            dst_id: dst.clone(),
            system_from: Some(system_from),
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

/// Append the redo sub-records for every staged node-label delta in
/// `overlay` under `label_coll_key`, in deterministic node-id order.
/// `added`/`removed` are disjoint by construction, mapping directly onto
/// the autocommit `(node_id, labels)` payload shape. Each `HashSet` is
/// sorted into a `Vec` first, so replicas produce byte-identical payloads.
pub(super) fn serialize_node_label_deltas(
    overlay: &GraphTxnOverlay,
    label_coll_key: &GraphCollKey,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let mut deltas: BTreeMap<&str, &NodeLabelDelta> = BTreeMap::new();
    for (node_id, delta) in overlay.staged_node_label_deltas_for_collection(label_coll_key) {
        deltas.insert(node_id, delta);
    }

    for (node_id, delta) in deltas {
        if !delta.added.is_empty() {
            let payload = encode_graph_node_label_payload(node_id, &sorted_labels(&delta.added))?;
            ops.push(RedoSubRecord {
                record_type: RecordType::GraphNodeLabelSet as u32,
                payload,
            });
        }
        if !delta.removed.is_empty() {
            let payload = encode_graph_node_label_payload(node_id, &sorted_labels(&delta.removed))?;
            ops.push(RedoSubRecord {
                record_type: RecordType::GraphNodeLabelRemove as u32,
                payload,
            });
        }
    }
    Ok(())
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

        // Edge delete: the redo delete tuple carries no surrogate, so only
        // the collection is needed to walk the overlay's tombstone set.
        GraphOp::EdgeDelete { collection, .. } => {
            collections.insert(collection.to_string());
            Ok(())
        }
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                collections.insert(edge.collection.to_string());
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
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => Ok(()),

        // Node-label deltas live under the fixed sentinel key, not a
        // per-collection post-image — nothing to collect here.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => Ok(()),
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
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
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
        let result =
            serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops);
        assert!(
            result.is_err(),
            "a staged put with no matching plan-carried surrogates must error, not invent one"
        );
    }

    #[test]
    fn edge_delete_emits_timestamped_redo() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(coll_key("g"), "a", "knows", "b");

        let surrogates = BTreeMap::new();
        let mut ops = Vec::new();
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
            .expect("serialize edge delete");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::Delete as u32);

        let decoded = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(&ops[0].payload)
            .expect("decode edge delete redo");
        assert_eq!(decoded.collection, "g");
        assert_eq!(decoded.src_id, "a");
        assert_eq!(decoded.label, "knows");
        assert_eq!(decoded.dst_id, "b");
        assert_eq!(
            decoded.system_from,
            Some(123),
            "delete must carry the frozen system-time ordinal for deterministic replay"
        );
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
        serialize_graph_collection(&overlay, &coll_key("g"), "g", &surrogates, 123, &mut ops)
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
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelSet as u32);

        let (node_id, labels) =
            zerompk::from_msgpack::<(String, Vec<String>)>(&ops[0].payload).expect("decode");
        assert_eq!(node_id, "n1");
        assert_eq!(labels, vec!["Person".to_string()]);
    }

    #[test]
    fn node_label_remove_emits_graph_node_label_remove_subrecord() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].record_type, RecordType::GraphNodeLabelRemove as u32);

        let (node_id, labels) =
            zerompk::from_msgpack::<(String, Vec<String>)>(&ops[0].payload).expect("decode");
        assert_eq!(node_id, "n1");
        assert_eq!(labels, vec!["Person".to_string()]);
    }

    #[test]
    fn node_label_added_and_removed_on_same_node_emit_both_subrecords() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_node_labels_set(coll_key("g"), "n1", &["Robot".to_string()]);
        overlay.stage_node_labels_remove(coll_key("g"), "n1", &["Person".to_string()]);

        let mut ops = Vec::new();
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
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
        serialize_node_label_deltas(&overlay_a, &coll_key("g"), &mut ops_a).expect("serialize a");
        let mut ops_b = Vec::new();
        serialize_node_label_deltas(&overlay_b, &coll_key("g"), &mut ops_b).expect("serialize b");

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
        serialize_node_label_deltas(&overlay, &coll_key("g"), &mut ops).expect("serialize");
        assert!(ops.is_empty());
    }
}
