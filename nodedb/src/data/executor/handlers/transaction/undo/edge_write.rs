// SPDX-License-Identifier: BUSL-1.1

//! Undo of one graph edge write: a put, a delete, or one edge of a node-delete
//! cascade.
//!
//! The write adds one version to the bitemporal edge store. The undo removes
//! that version, so no read at any system time sees the rolled-back write, as
//! if it never ran. It also puts the CSR back: the edge's presence and
//! weight, each endpoint surrogate the write rebound, and each node the write
//! created.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::edge_store::{EdgeRef, EdgeVersionWrite};
use crate::types::{DatabaseId, TenantId};

/// The pre-image of one edge write.
pub(in crate::data::executor) struct EdgeWriteUndo {
    pub database_id: u64,
    pub tid: u64,
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    /// The version the write added to the edge store.
    pub version: EdgeVersionWrite,
    /// The CSR state the write found.
    pub csr: EdgeCsrPrior,
    /// Whether the write dropped the endpoints' durable identity bindings. A
    /// node-delete cascade does. The undo binds them again from the CSR.
    pub rebind_endpoints: bool,
}

/// The CSR state one edge write found.
#[derive(Debug, Clone, Default, PartialEq)]
pub(in crate::data::executor) struct EdgeCsrPrior {
    /// The edge's weight, `None` when the edge was not live.
    pub weight: Option<f64>,
    /// The surrogate each existing endpoint held, `0` for none.
    pub surrogates: Vec<(String, u32)>,
    /// The endpoints the CSR did not hold, in the order a write interns them.
    pub created_nodes: Vec<String>,
}

/// One edge a write targets.
pub(in crate::data::executor) struct EdgeTarget<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
}

impl EdgeTarget<'_> {
    /// The undo of a write to this edge that added `version`.
    pub(in crate::data::executor) fn undo(
        &self,
        version: EdgeVersionWrite,
        csr: EdgeCsrPrior,
    ) -> EdgeWriteUndo {
        EdgeWriteUndo {
            database_id: self.database_id,
            tid: self.tid,
            collection: self.collection.to_string(),
            src_id: self.src_id.to_string(),
            label: self.label.to_string(),
            dst_id: self.dst_id.to_string(),
            version,
            csr,
            rebind_endpoints: false,
        }
    }
}

impl CoreLoop {
    /// The CSR state a write to `target` finds now.
    pub(in crate::data::executor) fn capture_edge_csr(
        &self,
        target: &EdgeTarget<'_>,
    ) -> EdgeCsrPrior {
        let partition = self.csr_partition(target.database_id, target.tid);
        let mut prior = EdgeCsrPrior {
            weight: partition.and_then(|p| {
                p.edge_weight_in_collection(
                    target.src_id,
                    target.label,
                    target.dst_id,
                    target.collection,
                )
            }),
            ..EdgeCsrPrior::default()
        };
        let endpoints = if target.src_id == target.dst_id {
            vec![target.src_id]
        } else {
            vec![target.src_id, target.dst_id]
        };
        for node in endpoints {
            match partition.filter(|p| p.contains_node(node)) {
                Some(p) => prior.surrogates.push((
                    node.to_string(),
                    p.node_surrogate(node).map_or(0, |s| s.as_u32()),
                )),
                None => prior.created_nodes.push(node.to_string()),
            }
        }
        prior
    }

    /// Reverse one edge write.
    pub(super) fn apply_undo_edge_write(
        &mut self,
        entry_index: usize,
        undo: EdgeWriteUndo,
    ) -> Result<(), (usize, String)> {
        let EdgeWriteUndo {
            database_id,
            tid,
            collection,
            src_id,
            label,
            dst_id,
            version,
            csr,
            rebind_endpoints,
        } = undo;
        let core = self.core_id;
        let fail = |detail: String| {
            error!(
                core,
                entry_index,
                error = %detail,
                "transaction undo: edge rollback failed; shard state unknown"
            );
            (entry_index, detail)
        };
        let edge_name = format!("{collection} {src_id}-[{label}]->{dst_id}");
        let database = DatabaseId::new(database_id);
        let tenant = TenantId::new(tid);

        self.edge_store
            .remove_edge_version(
                EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id),
                &version,
            )
            .map_err(|e| fail(format!("removing the version of {edge_name}: {e}")))?;

        if rebind_endpoints {
            let bindings: Vec<(&str, u32)> = match self.csr_partition(database_id, tid) {
                Some(p) => [src_id.as_str(), dst_id.as_str()]
                    .into_iter()
                    .filter_map(|node| p.node_surrogate(node).map(|s| (node, s.as_u32())))
                    .collect(),
                None => Vec::new(),
            };
            for (node, raw) in bindings {
                self.edge_store
                    .bind_node_surrogate(database, tenant, node, raw)
                    .map_err(|e| fail(format!("binding node '{node}' again: {e}")))?;
            }
        }

        let partition = self.csr_partition_mut(database_id, tid);
        partition
            .restore_edge_in_collection(&src_id, &label, &dst_id, &collection, csr.weight)
            .map_err(|e| fail(format!("restoring the CSR edge {edge_name}: {e}")))?;
        for (node, prior) in &csr.surrogates {
            partition.restore_node_surrogate(node, *prior);
        }
        for node in csr.created_nodes.iter().rev() {
            partition
                .withdraw_newest_node(node)
                .map_err(|e| fail(format!("withdrawing node '{node}': {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::graph::csr::Direction;
    use crate::engine::graph::csr::extract_weight_from_properties;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn tenant() -> TenantId {
        TenantId::new(TID)
    }

    fn edge<'a>(src: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DatabaseId::new(DB), tenant(), "c", src, "KNOWS", dst)
    }

    fn target<'a>(src: &'a str, dst: &'a str) -> EdgeTarget<'a> {
        EdgeTarget {
            database_id: DB,
            tid: TID,
            collection: "c",
            src_id: src,
            label: "KNOWS",
            dst_id: dst,
        }
    }

    fn weighted(weight: f64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ "weight": weight }))
            .expect("encode edge properties")
    }

    /// Put an edge the way the put handler does, and return its undo.
    fn put(core: &mut CoreLoop, src: &str, dst: &str, props: &[u8], ord: i64) -> EdgeWriteUndo {
        let target = target(src, dst);
        let csr = core.capture_edge_csr(&target);
        let version = core
            .edge_store
            .put_edge_version_recorded(
                edge(src, dst).with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                props,
                ord,
                ord,
                i64::MAX,
                true,
            )
            .expect("put edge version");
        let partition = core.csr_partition_mut(DB, TID);
        partition
            .put_edge_in_collection(
                src,
                "KNOWS",
                dst,
                "c",
                extract_weight_from_properties(props),
            )
            .expect("put CSR edge");
        partition.set_node_surrogate(src, Surrogate::new(10));
        partition.set_node_surrogate(dst, Surrogate::new(20));
        target.undo(version, csr)
    }

    fn resolve(core: &CoreLoop, src: &str, dst: &str, as_of: i64) -> Option<Vec<u8>> {
        core.edge_store
            .ceiling_resolve_edge(edge(src, dst), as_of, None)
            .expect("resolve edge")
    }

    /// An `AS OF` read at a system time between the write and its rollback
    /// sees what it saw before the write: the rolled-back version is gone,
    /// not shadowed by a newer compensating one.
    #[test]
    fn a_rolled_back_edge_update_leaves_no_version_at_any_system_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let _seed = put(&mut core, "alice", "bob", &weighted(2.5), 100);

        let undo = put(&mut core, "alice", "bob", &weighted(9.0), 200);
        core.apply_undo_edge_write(0, undo).expect("undo update");

        for as_of in [150, 200, 250, i64::MAX] {
            assert_eq!(
                resolve(&core, "alice", "bob", as_of),
                Some(weighted(2.5)),
                "system time {as_of} reads the edge as it was before the update"
            );
        }
        assert_eq!(
            core.csr_partition(DB, TID)
                .and_then(|p| p.edge_weight_in_collection("alice", "KNOWS", "bob", "c")),
            Some(2.5),
            "the CSR keeps the committed weight"
        );
    }

    #[test]
    fn a_rolled_back_edge_insert_withdraws_the_edge_and_the_nodes_it_created() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let undo = put(&mut core, "alice", "bob", b"", 100);
        core.apply_undo_edge_write(0, undo).expect("undo insert");

        assert_eq!(resolve(&core, "alice", "bob", 100), None);
        assert_eq!(resolve(&core, "alice", "bob", i64::MAX), None);
        let partition = core.csr_partition(DB, TID).expect("partition");
        assert!(!partition.contains_node("alice"));
        assert!(!partition.contains_node("bob"));
        assert_eq!(partition.node_count(), 0);
        assert!(
            core.edge_store
                .scan_all_node_surrogates()
                .expect("scan bindings")
                .is_empty(),
            "the identity bindings the insert made are gone"
        );
    }

    #[test]
    fn a_rolled_back_delete_removes_its_tombstone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let _seed = put(&mut core, "alice", "bob", &weighted(2.5), 100);

        let target = target("alice", "bob");
        let csr = core.capture_edge_csr(&target);
        let tombstone = core
            .edge_store
            .soft_delete_edge_recorded(edge("alice", "bob"), 200, true)
            .expect("tombstone");
        core.csr_partition_mut(DB, TID)
            .remove_edge_in_collection("alice", "KNOWS", "bob", "c");
        core.apply_undo_edge_write(0, target.undo(tombstone, csr))
            .expect("undo delete");

        assert_eq!(resolve(&core, "alice", "bob", 250), Some(weighted(2.5)));
        assert_eq!(
            core.csr_partition(DB, TID)
                .map(|p| p.neighbors("alice", None, Direction::Out)),
            Some(vec![("KNOWS".to_string(), "bob".to_string())])
        );
    }

    /// A rolled-back node delete puts back every edge the cascade tombstoned
    /// and the binding it dropped, in the edge store and in the CSR.
    #[test]
    fn a_rolled_back_node_delete_cascade_restores_edges_and_bindings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let _seed = put(&mut core, "alice", "bob", &weighted(2.5), 100);

        core.csr_partition_mut(DB, TID).remove_node_edges("alice");
        let removed = core
            .edge_store
            .delete_edges_for_node(DB, tenant(), "alice", 200)
            .expect("cascade");
        assert_eq!(removed.len(), 1);
        for (idx, restore) in removed.into_iter().enumerate() {
            let weight = extract_weight_from_properties(&restore.old_properties);
            let mut undo = EdgeTarget {
                database_id: DB,
                tid: TID,
                collection: &restore.collection,
                src_id: &restore.src,
                label: &restore.label,
                dst_id: &restore.dst,
            }
            .undo(
                restore.tombstone.clone(),
                EdgeCsrPrior {
                    weight: Some(weight),
                    ..EdgeCsrPrior::default()
                },
            );
            undo.rebind_endpoints = true;
            core.apply_undo_edge_write(idx, undo).expect("undo cascade");
        }

        assert_eq!(resolve(&core, "alice", "bob", 250), Some(weighted(2.5)));
        assert_eq!(
            core.csr_partition(DB, TID)
                .and_then(|p| p.edge_weight_in_collection("alice", "KNOWS", "bob", "c")),
            Some(2.5)
        );
        let mut bindings: Vec<(String, u32)> = core
            .edge_store
            .scan_all_node_surrogates()
            .expect("scan bindings")
            .into_iter()
            .map(|record| (record.2, record.3))
            .collect();
        bindings.sort();
        assert_eq!(
            bindings,
            vec![("alice".to_string(), 10), ("bob".to_string(), 20)]
        );
    }
}
