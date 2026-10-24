// SPDX-License-Identifier: BUSL-1.1

//! Install a snapshot's node labels: every label version, and each node's
//! current label set.
//!
//! A data-group install replaces the labels of every node its vShards home:
//! it drops their label versions and current labels before it installs the
//! snapshot's. Labels of nodes outside the group stay. A CSR rebuild from the
//! edge store keeps no label, so every label the install keeps goes back onto
//! the rebuilt CSR.

use std::collections::HashSet;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::edge_store::NodeLabelVersion;
use crate::types::{
    DatabaseId, HomedRecord, NodeLabelsEntry, RecordHomes, TenantDataSnapshot, TenantId,
};

/// Whether the labels of `node` home on one of `vshards`.
fn labels_owned_by(node: &str, vshards: &HashSet<u32>) -> bool {
    RecordHomes::of(HomedRecord::NodeLabels { node }).owned_by(vshards)
}

impl CoreLoop {
    /// Every labeled node's labels on this core. With `outside`, only the
    /// nodes no vShard of `outside` homes.
    pub(in crate::data::executor::handlers::snapshot) fn held_node_labels(
        &self,
        outside: Option<&HashSet<u32>>,
    ) -> Vec<NodeLabelsEntry> {
        let mut held = Vec::new();
        for (&(db, tid), partition) in self.csr.iter() {
            for (node, labels) in partition.labeled_nodes() {
                if outside.is_some_and(|group| labels_owned_by(node, group)) {
                    continue;
                }
                held.push((
                    db.as_u64(),
                    tid.as_u64(),
                    node.to_string(),
                    labels.into_iter().map(str::to_string).collect(),
                ));
            }
        }
        held
    }

    /// Drop the label versions of every node `group` homes, in every
    /// database and tenant.
    pub(super) fn purge_group_node_label_versions(
        &self,
        group: &HashSet<u32>,
    ) -> crate::Result<usize> {
        self.edge_store.purge_homed_node_labels(group)
    }

    /// Install the node labels of `snap` after the edge install.
    ///
    /// `kept` are the labels the install keeps. When the CSR was rebuilt
    /// they go back onto it. When it was not, a group install takes the
    /// labels of the group's nodes off it. Each node `snap` names then
    /// carries exactly its snapshot label set. Returns the label versions
    /// installed.
    pub(in crate::data::executor::handlers::snapshot) fn install_snapshot_node_labels(
        &mut self,
        kept: &[NodeLabelsEntry],
        csr_rebuilt: bool,
        group: Option<&HashSet<u32>>,
        snap: &TenantDataSnapshot,
    ) -> crate::Result<u64> {
        if csr_rebuilt {
            self.add_label_entries(kept)?;
        } else if let Some(group) = group {
            let replaced: Vec<NodeLabelsEntry> = self
                .held_node_labels(None)
                .into_iter()
                .filter(|(_, _, node, _)| labels_owned_by(node, group))
                .collect();
            self.remove_label_entries(&replaced);
        }
        for (db, tid, node, labels) in &snap.node_labels {
            let stale: Vec<String> = self
                .current_node_labels(*db, *tid, node)
                .into_iter()
                .filter(|label| !labels.contains(label))
                .collect();
            self.remove_label_entries(&[(*db, *tid, node.clone(), stale)]);
        }
        self.add_label_entries(&snap.node_labels)?;
        self.historical_csr.clear();

        let versions: Vec<NodeLabelVersion> = snap
            .node_label_versions
            .iter()
            .map(|(db, tid, node, ordinal, labels)| NodeLabelVersion {
                db: DatabaseId::new(*db),
                tid: TenantId::new(*tid),
                node: node.clone(),
                ordinal: *ordinal,
                labels: labels.clone(),
            })
            .collect();
        if !versions.is_empty() {
            self.edge_store.put_node_label_versions(&versions, None)?;
        }
        Ok(versions.len() as u64)
    }

    /// Set every label of `entries` on its node.
    fn add_label_entries(&mut self, entries: &[NodeLabelsEntry]) -> crate::Result<()> {
        for (db, tid, node, labels) in entries {
            let partition = self.csr_partition_mut(*db, *tid);
            for label in labels {
                match partition.add_node_label(node, label) {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(crate::Error::Storage {
                            engine: "graph".into(),
                            detail: format!(
                                "snapshot install: label '{label}' of node '{node}' needs more \
                                 distinct node labels than one partition holds"
                            ),
                        });
                    }
                    Err(e) => return Err(crate::Error::from(e)),
                }
            }
        }
        Ok(())
    }

    /// Take every label of `entries` off its node.
    fn remove_label_entries(&mut self, entries: &[NodeLabelsEntry]) {
        for (db, tid, node, labels) in entries {
            if labels.is_empty() {
                continue;
            }
            let partition = self.csr_partition_mut(*db, *tid);
            for label in labels {
                partition.remove_node_label(node, label);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::graph::edge_store::EdgeRef;
    use crate::types::{DatabaseId, TenantDataSnapshot, TenantId, VShardId};

    const TID: u64 = 1;
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Two node ids on different vShards: the moved one and a kept one.
    fn moved_and_kept() -> (String, String) {
        let moved = "n0".to_string();
        let home = VShardId::from_key(moved.as_bytes());
        let kept = (1..4096)
            .map(|i| format!("n{i}"))
            .find(|n| VShardId::from_key(n.as_bytes()) != home)
            .expect("a node on another vShard");
        (moved, kept)
    }

    /// Write `node`'s label `history`, then set its newest labels on the CSR.
    fn label(core: &mut CoreLoop, node: &str, history: &[(i64, &[&str])]) {
        for (ordinal, names) in history {
            core.edge_store
                .put_node_label_version(
                    DB,
                    TenantId::new(TID),
                    node,
                    *ordinal,
                    None,
                    &labels(names),
                )
                .expect("put label version");
        }
        if let Some((_, names)) = history.last() {
            let partition = core.csr_partition_mut(DB.as_u64(), TID);
            for name in names.iter() {
                partition.add_node_label(node, name).expect("set label");
            }
        }
    }

    /// A data-group install of `group` on `core` from `snap`.
    fn install(core: &mut CoreLoop, group: &HashSet<u32>, snap: &TenantDataSnapshot) {
        let task = CoreLoop::replay_vector_task(
            TenantId::new(0),
            DB,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            None,
        );
        let bytes = zerompk::to_msgpack_vec(snap).expect("encode snapshot");
        let group: Vec<u32> = group.iter().copied().collect();
        let response = core.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &group);
        assert_eq!(response.status, Status::Ok, "install: {response:?}");
    }

    /// The group snapshot of `group` built from `core`, as the Raft snapshot
    /// builder filters it.
    fn group_snapshot(core: &CoreLoop, group: &HashSet<u32>) -> TenantDataSnapshot {
        let (current, versions) = core
            .capture_node_labels(DB.as_u64(), TenantId::new(TID))
            .expect("capture labels");
        let mut snap = TenantDataSnapshot::default();
        crate::control::cluster::snapshot_builder_filter::push_group_node_labels(
            current, versions, group, &mut snap,
        );
        snap
    }

    fn current(core: &CoreLoop, node: &str) -> Vec<String> {
        core.current_node_labels(DB.as_u64(), TID, node)
    }

    fn at(core: &CoreLoop, cut: i64) -> Vec<(String, Vec<String>)> {
        core.edge_store
            .node_labels_at(DB, TenantId::new(TID), cut)
            .expect("labels at cut")
    }

    /// A vShard moves from `old` to `new`. The new owner's install carries
    /// the moved node's labels and history, drops the stale labels it held
    /// for the vShard, and keeps every other label across its CSR rebuild.
    /// A later install of the vShard that no longer carries the node drops
    /// the node's labels and versions from the core that held them.
    #[test]
    fn a_vshard_move_carries_its_labels_and_drops_them_by_vshard() {
        let (moved, kept) = moved_and_kept();
        let group: HashSet<u32> = [VShardId::from_key(moved.as_bytes()).as_u32()].into();

        let old_dir = tempfile::tempdir().expect("tempdir");
        let mut old = open_core(old_dir.path());
        label(
            &mut old,
            &moved,
            &[(100, &["Person"][..]), (200, &["Admin"][..])],
        );
        label(&mut old, &kept, &[(150, &["City"][..])]);
        let snap = group_snapshot(&old, &group);
        assert_eq!(
            snap.node_labels.len(),
            1,
            "only the moved node's labels ship"
        );

        let new_dir = tempfile::tempdir().expect("tempdir");
        let mut new = open_core(new_dir.path());
        label(&mut new, &moved, &[(50, &["Stale"][..])]);
        label(&mut new, &kept, &[(75, &["Other"][..])]);
        // An edge homed on the moved vShard: its purge rebuilds the CSR.
        new.edge_store
            .put_edge_versioned(
                EdgeRef::new(DB, TenantId::new(TID), "people", &moved, "knows", &kept),
                b"{}",
                10,
                10,
                i64::MAX,
            )
            .expect("put edge");
        install(&mut new, &group, &snap);

        assert_eq!(current(&new, &moved), labels(&["Admin"]));
        assert_eq!(
            current(&new, &kept),
            labels(&["Other"]),
            "kept across the rebuild"
        );
        // Node ids sort `n0` first.
        assert_eq!(
            at(&new, 120),
            vec![
                (moved.clone(), labels(&["Person"])),
                (kept.clone(), labels(&["Other"]))
            ],
            "the moved node's history reads at a cut before its label change"
        );
        assert!(
            at(&new, 60).iter().all(|(node, _)| node != &moved),
            "the stale version of the moved node is gone"
        );

        // The vShard's install no longer carries the node: the core drops it.
        install(&mut old, &group, &TenantDataSnapshot::default());
        assert!(current(&old, &moved).is_empty());
        assert!(at(&old, i64::MAX).iter().all(|(node, _)| node != &moved));
        assert_eq!(current(&old, &kept), labels(&["City"]));
        assert_eq!(at(&old, i64::MAX), vec![(kept, labels(&["City"]))]);
    }
}
