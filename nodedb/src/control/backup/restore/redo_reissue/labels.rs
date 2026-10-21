// SPDX-License-Identifier: BUSL-1.1

//! Restored node labels as label units.
//!
//! A backup carries every node's current label set and every label version.
//! Each node re-issues to the owner home of its labels
//! ([`HomedRecord::NodeLabels`]), where every label write of the node
//! routes. Its versions re-issue in ascending system time, each at its
//! original system time, so the restored node keeps its label history.
//! Labels belong to no collection: a node keeps its id across the move into
//! the destination database.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::RestoredNodeLabels;

use crate::types::{HomedRecord, NodeLabelVersionEntry, NodeLabelsEntry, RecordHomes, VShardId};

/// The restored nodes of each home vShard, in node order.
pub(super) type HomedLabels = BTreeMap<VShardId, Vec<RestoredNodeLabels>>;

fn foreign_tenant(found: u64, tenant_id: u64) -> crate::Error {
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!(
            "restore: a node label section of tenant {found} is in the backup of tenant \
             {tenant_id}"
        ),
    }
}

/// Every node `current` or `versions` names, with its versions and its
/// current label set, grouped by home vShard. Every entry must belong to
/// `tenant_id`.
pub(super) fn label_units(
    tenant_id: u64,
    current: Vec<NodeLabelsEntry>,
    versions: Vec<NodeLabelVersionEntry>,
) -> crate::Result<HomedLabels> {
    let mut nodes: BTreeMap<String, RestoredNodeLabels> = BTreeMap::new();
    let mut by_ordinal: BTreeMap<String, BTreeMap<i64, Vec<String>>> = BTreeMap::new();
    for (_, tid, node, ordinal, labels) in versions {
        if tid != tenant_id {
            return Err(foreign_tenant(tid, tenant_id));
        }
        by_ordinal.entry(node).or_default().insert(ordinal, labels);
    }
    for (_, tid, node, labels) in current {
        if tid != tenant_id {
            return Err(foreign_tenant(tid, tenant_id));
        }
        nodes
            .entry(node.clone())
            .or_insert_with(|| empty(node))
            .current = labels;
    }
    for (node, ordinals) in by_ordinal {
        nodes
            .entry(node.clone())
            .or_insert_with(|| empty(node))
            .versions = ordinals.into_iter().collect();
    }
    let mut homed = HomedLabels::new();
    for (node, restored) in nodes {
        let home = RecordHomes::of(HomedRecord::NodeLabels { node: &node }).owner();
        homed.entry(home).or_default().push(restored);
    }
    Ok(homed)
}

fn empty(node_id: String) -> RestoredNodeLabels {
    RestoredNodeLabels {
        node_id,
        versions: Vec::new(),
        current: Vec::new(),
    }
}

/// The encoded weight of one restored node, for sizing batches: its
/// versions plus its current set as units, and their bytes.
pub(super) fn label_weight(node: &RestoredNodeLabels) -> (usize, usize) {
    let labels_len = |labels: &[String]| labels.iter().map(String::len).sum::<usize>();
    let bytes = node.node_id.len()
        + labels_len(&node.current)
        + node
            .versions
            .iter()
            .map(|(_, labels)| 8 + labels_len(labels))
            .sum::<usize>();
    (node.versions.len() + 1, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn each_node_restores_on_its_home_with_its_history() {
        let homed = label_units(
            7,
            vec![(1, 7, "alice".into(), labels(&["Admin"]))],
            vec![
                (1, 7, "alice".into(), 200, labels(&["Admin"])),
                (1, 7, "alice".into(), 100, labels(&["Person"])),
                (1, 7, "bob".into(), 150, labels(&[])),
            ],
        )
        .expect("units");
        let alice = &homed[&VShardId::from_key(b"alice")]
            .iter()
            .find(|n| n.node_id == "alice")
            .expect("alice restores on her home");
        assert_eq!(
            alice.versions,
            vec![(100, labels(&["Person"])), (200, labels(&["Admin"]))]
        );
        assert_eq!(alice.current, labels(&["Admin"]));
        let bob = &homed[&VShardId::from_key(b"bob")]
            .iter()
            .find(|n| n.node_id == "bob")
            .expect("bob restores on his home");
        assert!(bob.current.is_empty());
        assert!(
            label_units(7, vec![(1, 8, "x".into(), labels(&["L"]))], Vec::new()).is_err(),
            "another tenant's labels are refused"
        );
    }
}
