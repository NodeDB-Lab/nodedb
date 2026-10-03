// SPDX-License-Identifier: Apache-2.0

//! Finishing a rebuild on the owning thread: restore the compacted copy
//! and replay the journal onto it.

use nodedb_mem::ScopedMemory;

use super::seed::{CsrRebuilt, NodeLabelState, restore_checkpoint};
use crate::GraphError;
use crate::csr::index::CsrIndex;

/// Most node labels one index interns. The label bitset is a `u64`.
const MAX_NODE_LABELS: usize = 64;

impl CsrIndex {
    /// Close the journal of `rebuilt` and return the copy that replaces
    /// this index.
    ///
    /// The copy holds the snapshot, compacted, plus every mutation this
    /// index took since `begin_rebuild`, in order. It keeps this index's
    /// partition tag, so node ids handed out before the swap stay valid.
    /// The caller installs it in one step. On error the journal is closed,
    /// this index stays as it is, and the copy is dropped:
    ///
    /// - [`GraphError::RebuildSuperseded`]: no journal of this rebuild is open.
    /// - [`GraphError::RebuildJournalOverflow`]: the journal hit its bound.
    /// - [`GraphError::RebuildReplayDiverged`]: a replayed mutation returned
    ///   another outcome than it did here.
    /// - [`GraphError::RebuildSnapshotInvalid`]: the copy does not decode.
    pub fn finish_rebuild(
        &mut self,
        rebuilt: CsrRebuilt,
        memory: ScopedMemory,
    ) -> Result<CsrIndex, GraphError> {
        let journal = match self.rebuild_journal.take() {
            Some(journal) if journal.token == rebuilt.token => journal,
            other => {
                self.rebuild_journal = other;
                return Err(GraphError::RebuildSuperseded);
            }
        };
        let ops = journal.into_ops()?;
        let mut copy = restore_checkpoint(&rebuilt.checkpoint, memory)?;
        copy.install_node_labels(rebuilt.node_labels)?;
        for (op, live_outcome) in &ops {
            if copy.replay_op(op) != *live_outcome {
                return Err(GraphError::RebuildReplayDiverged { op: op.kind() });
            }
        }
        copy.partition_tag = self.partition_tag;
        Ok(copy)
    }

    /// Put the node labels a rebuild carried onto a freshly restored copy.
    fn install_node_labels(&mut self, labels: NodeLabelState) -> Result<(), GraphError> {
        if labels.bits.len() != self.id_to_node.len() {
            return Err(GraphError::RebuildSnapshotInvalid {
                detail: format!(
                    "node label bitsets cover {} nodes, the snapshot holds {}",
                    labels.bits.len(),
                    self.id_to_node.len()
                ),
            });
        }
        if labels.names.len() > MAX_NODE_LABELS {
            return Err(GraphError::RebuildSnapshotInvalid {
                detail: format!(
                    "{} node labels exceed the {MAX_NODE_LABELS}-label bitset",
                    labels.names.len()
                ),
            });
        }
        self.node_label_to_id = labels
            .names
            .iter()
            .enumerate()
            .map(|(id, name)| (name.clone(), id as u8))
            .collect();
        self.node_label_names = labels.names;
        self.node_label_bits = labels.bits;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::GraphError;
    use crate::csr::index::{CsrIndex, Direction};
    use crate::test_support::test_memory;

    const JOURNAL_BYTES: usize = 1 << 20;

    fn seeded() -> CsrIndex {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_in_collection("a", "L", "b", "c").unwrap();
        csr.add_edge_in_collection("b", "L", "c", "c").unwrap();
        csr.add_node_label("a", "Person").unwrap();
        csr.set_node_surrogate("a", Surrogate::new(7));
        csr
    }

    fn out_of(csr: &CsrIndex, node: &str) -> Vec<String> {
        let mut dsts: Vec<String> = csr
            .neighbors(node, &[], Direction::Out)
            .into_iter()
            .map(|(_, d)| d)
            .collect();
        dsts.sort();
        dsts
    }

    #[test]
    fn writes_during_the_build_reach_the_installed_copy() {
        let mut live = seeded();
        let seed = live.begin_rebuild(JOURNAL_BYTES).unwrap();

        // Writes that land after the snapshot.
        live.add_edge_in_collection("a", "L", "d", "c").unwrap();
        live.remove_edge_in_collection("b", "L", "c", "c");
        live.put_edge_in_collection("x", "L", "y", "c", 2.5)
            .unwrap();
        live.add_node_label("x", "Person").unwrap();
        live.set_node_surrogate("x", Surrogate::new(9));

        let rebuilt = seed.build(test_memory()).unwrap();
        let tag_before = live.partition_tag;
        let copy = live.finish_rebuild(rebuilt, test_memory()).unwrap();

        assert_eq!(out_of(&copy, "a"), vec!["b".to_string(), "d".to_string()]);
        assert!(
            out_of(&copy, "b").is_empty(),
            "a delete during the build carries over"
        );
        assert_eq!(
            copy.edge_weight_in_collection("x", "L", "y", "c"),
            Some(2.5)
        );
        let x = copy.node_id_raw("x").unwrap();
        assert!(copy.node_has_label(x, "Person"));
        let a = copy.node_id_raw("a").unwrap();
        assert!(copy.node_has_label(a, "Person"), "snapshot labels survive");
        assert_eq!(copy.node_id_for_surrogate(Surrogate::new(9)), Some("x"));
        assert_eq!(copy.partition_tag, tag_before);
        assert!(!live.rebuild_in_progress(), "finishing closes the journal");
    }

    #[test]
    fn a_result_from_another_rebuild_is_refused() {
        let mut live = seeded();
        let seed = live.begin_rebuild(JOURNAL_BYTES).unwrap();
        live.abort_rebuild(seed.token());
        let _second = live.begin_rebuild(JOURNAL_BYTES).unwrap();
        let rebuilt = seed.build(test_memory()).unwrap();
        assert!(matches!(
            live.finish_rebuild(rebuilt, test_memory()),
            Err(GraphError::RebuildSuperseded)
        ));
        assert!(live.rebuild_in_progress(), "the newer journal stays open");
    }

    #[test]
    fn a_second_rebuild_is_refused_while_one_runs() {
        let mut live = seeded();
        let _seed = live.begin_rebuild(JOURNAL_BYTES).unwrap();
        assert!(matches!(
            live.begin_rebuild(JOURNAL_BYTES),
            Err(GraphError::RebuildInProgress)
        ));
    }

    #[test]
    fn journal_overflow_refuses_the_copy_and_keeps_the_live_writes() {
        let mut live = seeded();
        let seed = live.begin_rebuild(1).unwrap();
        live.add_edge_in_collection("a", "L", "z", "c").unwrap();
        let rebuilt = seed.build(test_memory()).unwrap();
        assert!(matches!(
            live.finish_rebuild(rebuilt, test_memory()),
            Err(GraphError::RebuildJournalOverflow { cap_bytes: 1 })
        ));
        assert!(out_of(&live, "a").contains(&"z".to_string()));
    }
}
