// SPDX-License-Identifier: BUSL-1.1

//! Resolve a RESTORE batch into the redo record of its Calvin transaction.
//!
//! A batch stages nothing. Its resolve appends the rows' sub-records and
//! change events as the backup holds them, and one sub-record per edge
//! version. Each edge version keeps its historical `system_from` and is
//! applied at the transaction's ordinal: a TRUNCATE sequenced before the
//! RESTORE leaves it visible, and one sequenced after it hides it.
//!
//! Each restored node's label versions resolve to exact label sets, each at
//! its historical `system_from`. A node whose newest version is not its
//! current label set gets one more exact set, above both that version and the
//! transaction's ordinal.

use nodedb_physical::physical_plan::{
    MetaOp, PhysicalPlan, RestoredEdgeVersion, RestoredNodeLabels,
};
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::wal::{
    EdgeDeleteRedo, EdgePutRedo, NodeLabelRedo, RedoRecord, RedoRowChange, RedoSubRecord,
};

impl CoreLoop {
    /// Append the rows, edge versions and node labels of every RESTORE batch
    /// among `plans` to `ops`, and their rows' change events to
    /// `row_changes`.
    pub(super) fn serialize_restored_batches(
        &self,
        plans: &[PhysicalPlan],
        ops: &mut Vec<RedoSubRecord>,
        row_changes: &mut Vec<RedoRowChange>,
    ) -> crate::Result<()> {
        for plan in plans {
            let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan else {
                continue;
            };
            let applied =
                self.apply_scope
                    .calvin_txn_ordinal
                    .ok_or_else(|| crate::Error::Internal {
                        detail: "a RESTORE batch resolved outside a Calvin transaction".into(),
                    })?;
            if !batch.rows_redo.is_empty() {
                let rows = RedoRecord::from_bytes(&batch.rows_redo)?;
                ops.extend(rows.ops);
                row_changes.extend(rows.row_changes);
            }
            for edge in &batch.edges {
                ops.push(restored_edge(edge, applied)?);
            }
            for node in &batch.node_labels {
                restored_labels(node, applied, ops)?;
            }
        }
        Ok(())
    }
}

/// The sub-record of one restored edge version, applied at `applied`, the
/// transaction's ordinal. A backup taken on a cluster whose clock ran ahead
/// of this sequence holds versions above that ordinal: each keeps its
/// system time and is applied at the ordinal all the same, so a TRUNCATE
/// compares it by its place in the sequence.
fn restored_edge(edge: &RestoredEdgeVersion, applied: i64) -> crate::Result<RedoSubRecord> {
    let applied = (applied != edge.system_from).then_some(applied);
    let encoded = match &edge.properties {
        Some(properties) => zerompk::to_msgpack_vec(&EdgePutRedo {
            collection: edge.collection.clone(),
            src_id: edge.src_id.clone(),
            label: edge.label.clone(),
            dst_id: edge.dst_id.clone(),
            properties: properties.clone(),
            src_surrogate: edge.src_surrogate,
            dst_surrogate: edge.dst_surrogate,
            system_from: Some(edge.system_from),
            applied,
            valid_from_ms: edge.valid_time.map(|(from, _)| from),
            valid_until_ms: edge.valid_time.map(|(_, until)| until),
        })
        .map(|payload| (RecordType::Put, payload)),
        None => zerompk::to_msgpack_vec(&EdgeDeleteRedo {
            collection: edge.collection.clone(),
            src_id: edge.src_id.clone(),
            label: edge.label.clone(),
            dst_id: edge.dst_id.clone(),
            src_surrogate: edge.src_surrogate,
            dst_surrogate: edge.dst_surrogate,
            system_from: Some(edge.system_from),
            applied,
        })
        .map(|payload| (RecordType::Delete, payload)),
    };
    let (record_type, payload) = encoded.map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restored edge version of '{}': {e}", edge.collection),
    })?;
    Ok(RedoSubRecord {
        record_type: record_type as u32,
        payload,
    })
}

/// The exact label sets of one restored node: each version at its system
/// time, then the current label set when the newest version is not it.
fn restored_labels(
    node: &RestoredNodeLabels,
    applied: i64,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let exact = |labels: &[String], system_from: i64| -> crate::Result<RedoSubRecord> {
        let payload = NodeLabelRedo {
            node_id: node.node_id.clone(),
            labels: sorted_set(labels),
            system_from: Some(system_from),
            exact: true,
        }
        .encode()?;
        Ok(RedoSubRecord {
            record_type: RecordType::GraphNodeLabelSet as u32,
            payload,
        })
    };
    for (system_from, labels) in &node.versions {
        ops.push(exact(labels, *system_from)?);
    }
    let current = sorted_set(&node.current);
    let newest = node.versions.last();
    let ends_current = match newest {
        Some((_, labels)) => sorted_set(labels) == current,
        None => current.is_empty(),
    };
    if !ends_current {
        let above = newest.map_or(applied, |(system_from, _)| {
            applied.max(system_from.saturating_add(1))
        });
        ops.push(exact(&node.current, above)?);
    }
    Ok(())
}

/// `labels` sorted, each once.
fn sorted_set(labels: &[String]) -> Vec<String> {
    let mut sorted = labels.to_vec();
    sorted.sort();
    sorted.dedup();
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(system_from: i64, properties: Option<Vec<u8>>) -> RestoredEdgeVersion {
        RestoredEdgeVersion {
            collection: "g".into(),
            src_id: "a".into(),
            label: "L".into(),
            dst_id: "b".into(),
            src_surrogate: 1,
            dst_surrogate: 2,
            system_from,
            valid_time: properties.as_ref().map(|_| (0, i64::MAX)),
            properties,
        }
    }

    /// A restored version keeps its system time and carries the
    /// transaction's ordinal as its applied ordinal. A tombstone does too.
    #[test]
    fn a_restored_version_is_applied_at_the_transactions_ordinal() {
        let put = restored_edge(&version(100, Some(b"p".to_vec())), 5_000).expect("put");
        assert_eq!(put.record_type, RecordType::Put as u32);
        let put: EdgePutRedo = zerompk::from_msgpack(&put.payload).expect("decode put");
        assert_eq!((put.system_from, put.applied), (Some(100), Some(5_000)));

        let tombstone = restored_edge(&version(200, None), 5_000).expect("tombstone");
        assert_eq!(tombstone.record_type, RecordType::Delete as u32);
        let tombstone: EdgeDeleteRedo =
            zerompk::from_msgpack(&tombstone.payload).expect("decode tombstone");
        assert_eq!(
            (tombstone.system_from, tombstone.applied),
            (Some(200), Some(5_000))
        );
    }

    /// A version above the transaction's ordinal keeps its system time and
    /// is applied at the ordinal.
    #[test]
    fn a_version_above_the_ordinal_is_applied_at_the_ordinal() {
        let put = restored_edge(&version(9_000, Some(Vec::new())), 5_000).expect("put");
        let put: EdgePutRedo = zerompk::from_msgpack(&put.payload).expect("decode put");
        assert_eq!((put.system_from, put.applied), (Some(9_000), Some(5_000)));
    }

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn decode(ops: &[RedoSubRecord]) -> Vec<(i64, Vec<String>)> {
        ops.iter()
            .map(|op| {
                assert_eq!(op.record_type, RecordType::GraphNodeLabelSet as u32);
                let op = NodeLabelRedo::decode(&op.payload).expect("decode label op");
                assert!(op.exact, "a restored label set is exact");
                (op.system_from.expect("a carried ordinal"), op.labels)
            })
            .collect()
    }

    /// Each version keeps its system time. A history that ends at the
    /// current set adds nothing.
    #[test]
    fn restored_label_versions_keep_their_system_time() {
        let node = RestoredNodeLabels {
            node_id: "alice".into(),
            versions: vec![(100, labels(&["Person"])), (200, labels(&["Admin"]))],
            current: labels(&["Admin"]),
        };
        let mut ops = Vec::new();
        restored_labels(&node, 5_000, &mut ops).expect("resolve");
        assert_eq!(
            decode(&ops),
            vec![(100, labels(&["Person"])), (200, labels(&["Admin"]))]
        );
    }

    /// A current set the history does not end at lands above the newest
    /// version and the transaction's ordinal.
    #[test]
    fn a_current_set_past_the_history_lands_above_it() {
        let node = RestoredNodeLabels {
            node_id: "alice".into(),
            versions: vec![(9_000, labels(&["Person"]))],
            current: labels(&["Robot"]),
        };
        let mut ops = Vec::new();
        restored_labels(&node, 5_000, &mut ops).expect("resolve");
        assert_eq!(
            decode(&ops),
            vec![(9_000, labels(&["Person"])), (9_001, labels(&["Robot"]))]
        );
        let unversioned = RestoredNodeLabels {
            node_id: "bob".into(),
            versions: Vec::new(),
            current: labels(&["City"]),
        };
        let mut ops = Vec::new();
        restored_labels(&unversioned, 5_000, &mut ops).expect("resolve");
        assert_eq!(decode(&ops), vec![(5_000, labels(&["City"]))]);
    }
}
