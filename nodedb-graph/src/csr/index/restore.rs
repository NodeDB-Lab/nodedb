// SPDX-License-Identifier: Apache-2.0

//! Exact edge writes, and the reversal primitives a rollback of a graph
//! write uses.
//!
//! A rollback puts the index back to what it held before the write: the
//! edge's presence and weight, each node surrogate the write rebound, and
//! each node or node label the write interned. Interning appends, so a
//! rollback withdraws the newest entry first and refuses any other.

use super::types::CsrIndex;
use crate::GraphError;

impl CsrIndex {
    /// Weight of the live `(src, label, dst)` edge in `collection`, `None`
    /// when that edge is not live.
    pub fn edge_weight_in_collection(
        &self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
    ) -> Option<f64> {
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        let label_id = *self.label_to_id.get(label)?;
        let coll_id = *self.collection_to_id.get(collection)?;
        let idx = src_id as usize;

        if let (Some(edges), Some(colls)) = (
            self.buffer_out.get(idx),
            self.buffer_out_collections.get(idx),
        ) {
            let position = edges
                .iter()
                .zip(colls.iter())
                .position(|(&(l, d), &c)| l == label_id && d == dst_id && c == coll_id);
            if let Some(k) = position {
                let weight = if self.has_weights {
                    self.buffer_out_weights
                        .get(idx)
                        .and_then(|weights| weights.get(k))
                        .copied()
                        .unwrap_or(1.0)
                } else {
                    1.0
                };
                return Some(weight);
            }
        }

        if self
            .deleted_edges
            .contains(&(src_id, label_id, dst_id, coll_id))
            || idx + 1 >= self.out_offsets.len()
        {
            return None;
        }
        let start = self.out_offsets[idx] as usize;
        let end = self.out_offsets[idx + 1] as usize;
        (start..end)
            .find(|&i| {
                self.out_labels[i] == label_id
                    && self.out_targets[i] == dst_id
                    && self.out_collections.get(i).copied().unwrap_or(0) == coll_id
            })
            .map(|i| self.out_edge_weight(i))
    }

    /// Make the `(src, label, dst)` edge in `collection` live with `weight`.
    ///
    /// A live edge with another weight takes the new one. Returns the weight
    /// the edge had while live before, `None` when it was not live, so a
    /// rollback can put it back.
    pub fn put_edge_in_collection(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
        weight: f64,
    ) -> Result<Option<f64>, GraphError> {
        let prior = self.edge_weight_in_collection(src, label, dst, collection);
        match prior {
            Some(current) if current == weight => return Ok(prior),
            Some(_) => self.remove_edge_in_collection(src, label, dst, collection),
            None => {}
        }
        let src_id = self.ensure_node(src)?;
        let dst_id = self.ensure_node(dst)?;
        let label_id = self.ensure_label(label)?;
        let coll_id = self.ensure_collection(collection);
        if weight != 1.0 && !self.has_weights {
            self.enable_weights();
        }
        // A deleted dense copy stays deleted: the live copy is the buffer one.
        // With no dense copy, a deletion mark names this edge only, so it goes.
        if !self.dense_has_edge(src_id, label_id, dst_id, coll_id) {
            self.deleted_edges
                .remove(&(src_id, label_id, dst_id, coll_id));
        }
        self.buffer_out[src_id as usize].push((label_id, dst_id));
        self.buffer_in[dst_id as usize].push((label_id, src_id));
        self.buffer_out_collections[src_id as usize].push(coll_id);
        self.buffer_in_collections[dst_id as usize].push(coll_id);
        if self.has_weights {
            self.buffer_out_weights[src_id as usize].push(weight);
            self.buffer_in_weights[dst_id as usize].push(weight);
        }
        Ok(prior)
    }

    /// Put the edge back to `prior`: live with that weight, or absent when
    /// `prior` is `None`.
    pub fn restore_edge_in_collection(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        collection: &str,
        prior: Option<f64>,
    ) -> Result<(), GraphError> {
        match prior {
            Some(weight) => self
                .put_edge_in_collection(src, label, dst, collection, weight)
                .map(drop),
            None => {
                self.remove_edge_in_collection(src, label, dst, collection);
                Ok(())
            }
        }
    }

    /// Put `node`'s surrogate back to `prior`, `0` for none.
    pub fn restore_node_surrogate(&mut self, node: &str, prior: u32) {
        let Some(&id) = self.node_to_id.get(node) else {
            return;
        };
        let Some(slot) = self.node_surrogates.get_mut(id as usize) else {
            return;
        };
        let current = *slot;
        if current == prior {
            return;
        }
        *slot = prior;
        if current != 0 && self.surrogate_to_local.get(&current) == Some(&id) {
            self.surrogate_to_local.remove(&current);
        }
        if prior != 0 {
            self.surrogate_to_local.insert(prior, id);
        }
    }

    /// Whether the node label `label` is interned.
    pub fn has_node_label_name(&self, label: &str) -> bool {
        self.node_label_to_id.contains_key(label)
    }

    /// Withdraw `node`, which a rolled-back write interned.
    ///
    /// An absent node is a no-op. The node must be the newest one, with no
    /// edge and no label: withdrawing any other would renumber or orphan live
    /// state, so that is refused.
    pub fn withdraw_newest_node(&mut self, node: &str) -> Result<(), GraphError> {
        let Some(&id) = self.node_to_id.get(node) else {
            return Ok(());
        };
        let idx = id as usize;
        let newest = idx + 1 == self.id_to_node.len();
        let no_buffered_edge = self.buffer_out.get(idx).is_none_or(Vec::is_empty)
            && self.buffer_in.get(idx).is_none_or(Vec::is_empty);
        let empty_range = |offsets: &[u32]| match (offsets.get(idx), offsets.get(idx + 1)) {
            (Some(start), Some(end)) => start == end,
            _ => true,
        };
        let no_dense_edge =
            empty_range(self.out_offsets.as_slice()) && empty_range(self.in_offsets.as_slice());
        let unlabeled = self.node_label_bits.get(idx).copied().unwrap_or(0) == 0;
        if !(newest && no_buffered_edge && no_dense_edge && unlabeled) {
            return Err(GraphError::WithdrawRefused {
                kind: "node",
                name: node.to_string(),
            });
        }

        self.node_to_id.remove(node);
        self.id_to_node.pop();
        // Offsets hold one entry more than there are nodes.
        if self.out_offsets.len() > idx + 1 {
            self.out_offsets.pop();
        }
        if self.in_offsets.len() > idx + 1 {
            self.in_offsets.pop();
        }
        self.buffer_out.truncate(idx);
        self.buffer_in.truncate(idx);
        self.buffer_out_weights.truncate(idx);
        self.buffer_in_weights.truncate(idx);
        self.buffer_out_collections.truncate(idx);
        self.buffer_in_collections.truncate(idx);
        self.node_label_bits.truncate(idx);
        self.node_surrogates.truncate(idx);
        self.surrogate_to_local.retain(|_, local| *local != id);
        // The id goes back to the pool: no deletion mark may name it.
        self.deleted_edges
            .retain(|&(src, _, dst, _)| src != id && dst != id);
        self.access_counts.truncate(idx);
        Ok(())
    }

    /// Withdraw the node label `label`, which a rolled-back write interned.
    ///
    /// An absent label is a no-op. The label must be the newest one and no
    /// node may carry it, or the withdraw is refused.
    pub fn withdraw_newest_node_label(&mut self, label: &str) -> Result<(), GraphError> {
        let Some(&id) = self.node_label_to_id.get(label) else {
            return Ok(());
        };
        let newest = usize::from(id) + 1 == self.node_label_names.len();
        let bit = 1u64 << id;
        let carried = self.node_label_bits.iter().any(|bits| bits & bit != 0);
        if !newest || carried {
            return Err(GraphError::WithdrawRefused {
                kind: "node label",
                name: label.to_string(),
            });
        }
        self.node_label_to_id.remove(label);
        self.node_label_names.pop();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csr::index::types::Direction;
    use crate::test_support::test_memory;

    #[test]
    fn put_edge_replaces_the_weight_of_a_live_edge_and_reports_the_old_one() {
        let mut csr = CsrIndex::new(test_memory());
        assert_eq!(
            csr.put_edge_in_collection("a", "L", "b", "c", 2.5)
                .expect("first put"),
            None
        );
        assert_eq!(
            csr.put_edge_in_collection("a", "L", "b", "c", 9.0)
                .expect("second put"),
            Some(2.5)
        );
        assert_eq!(csr.edge_weight_in_collection("a", "L", "b", "c"), Some(9.0));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn put_edge_replaces_the_weight_of_a_compacted_edge() {
        let mut csr = CsrIndex::new(test_memory());
        csr.put_edge_in_collection("a", "L", "b", "c", 2.5)
            .expect("put");
        csr.compact().expect("compact");
        assert_eq!(
            csr.put_edge_in_collection("a", "L", "b", "c", 9.0)
                .expect("reweigh"),
            Some(2.5)
        );
        assert_eq!(csr.edge_weight_in_collection("a", "L", "b", "c"), Some(9.0));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
        csr.compact().expect("compact again");
        assert_eq!(csr.edge_weight_in_collection("a", "L", "b", "c"), Some(9.0));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn restoring_an_edge_brings_back_a_deleted_compacted_edge() {
        let mut csr = CsrIndex::new(test_memory());
        csr.put_edge_in_collection("a", "L", "b", "c", 1.0)
            .expect("put");
        csr.compact().expect("compact");
        csr.remove_edge_in_collection("a", "L", "b", "c");
        assert_eq!(csr.edge_weight_in_collection("a", "L", "b", "c"), None);

        csr.restore_edge_in_collection("a", "L", "b", "c", Some(1.0))
            .expect("restore");
        assert_eq!(csr.edge_weight_in_collection("a", "L", "b", "c"), Some(1.0));
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn re_adding_a_deleted_compacted_edge_brings_it_back() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_in_collection("a", "L", "b", "c")
            .expect("edge");
        csr.compact().expect("compact");
        csr.remove_edge_in_collection("a", "L", "b", "c");
        csr.add_edge_in_collection("a", "L", "b", "c")
            .expect("re-add");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 1);
    }

    #[test]
    fn withdrawing_the_newest_isolated_node_removes_it() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").expect("edge");
        csr.put_edge_in_collection("b", "L", "z", "c", 1.0)
            .expect("edge to z");
        csr.remove_edge_in_collection("b", "L", "z", "c");
        csr.set_node_surrogate("z", nodedb_types::Surrogate::new(42));

        csr.withdraw_newest_node("z").expect("withdraw");

        assert!(!csr.contains_node("z"));
        assert_eq!(csr.node_count(), 2);
        assert_eq!(
            csr.node_id_for_surrogate(nodedb_types::Surrogate::new(42)),
            None
        );
        csr.add_edge("a", "L", "y")
            .expect("a later node takes the freed id");
        csr.compact().expect("compact");
        assert_eq!(csr.neighbors("a", None, Direction::Out).len(), 2);
    }

    #[test]
    fn withdrawing_a_node_that_is_not_the_newest_is_refused() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_node_label("x", "Person").expect("label x");
        csr.remove_node_label("x", "Person");
        csr.add_edge("a", "L", "b").expect("edge");
        assert!(matches!(
            csr.withdraw_newest_node("x"),
            Err(GraphError::WithdrawRefused { .. })
        ));
        assert!(csr.contains_node("x"));
    }

    #[test]
    fn withdrawing_a_node_with_an_edge_is_refused() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").expect("edge");
        assert!(matches!(
            csr.withdraw_newest_node("b"),
            Err(GraphError::WithdrawRefused { .. })
        ));
    }

    #[test]
    fn withdrawing_the_newest_unused_node_label_frees_its_slot() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_node_label("x", "Person").expect("label");
        csr.remove_node_label("x", "Person");
        csr.withdraw_newest_node_label("Person").expect("withdraw");
        assert!(!csr.has_node_label_name("Person"));
    }

    #[test]
    fn withdrawing_a_carried_node_label_is_refused() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_node_label("x", "Person").expect("label");
        assert!(matches!(
            csr.withdraw_newest_node_label("Person"),
            Err(GraphError::WithdrawRefused { .. })
        ));
    }

    #[test]
    fn restoring_a_surrogate_unbinds_the_one_a_write_set() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").expect("edge");
        csr.set_node_surrogate("a", nodedb_types::Surrogate::new(5));
        csr.restore_node_surrogate("a", 0);
        assert_eq!(csr.node_surrogate("a"), None);
        assert_eq!(
            csr.node_id_for_surrogate(nodedb_types::Surrogate::new(5)),
            None
        );
    }
}
