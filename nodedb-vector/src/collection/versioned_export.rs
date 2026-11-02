// SPDX-License-Identifier: Apache-2.0

//! Export and install of a collection's system-time history.
//!
//! A snapshot of a collection carries its live vectors (`export_snapshot`),
//! the start of each live node a cut can read below, and every ended node a
//! cut still reads. An install rebuilds each ended node soft-deleted with its
//! span, so a read at any retained cut answers as on the source.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;
use crate::error::{VectorError, check_dim};

/// One ended node a cut still reads.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EndedNodeExport {
    /// The node's FP32 vector.
    pub vector: Vec<f32>,
    /// The row the node held the vector of.
    pub surrogate: Surrogate,
    /// The ordinal of the write that inserted the node.
    pub sys_from: i64,
    /// The ordinal of the write that ended the node.
    pub sys_to: i64,
}

impl VectorCollection {
    /// `(global node id, sys_from)` of every live node a cut can read below,
    /// ascending by id. A live node not listed is visible at every cut.
    pub fn live_node_starts(&self) -> Vec<(u32, i64)> {
        let mut starts: Vec<(u32, i64)> = self
            .versions
            .to_entries()
            .into_iter()
            .filter(|(_, _, _, sys_to)| sys_to.is_none())
            .map(|(id, _, sys_from, _)| (id, sys_from))
            .collect();
        starts.sort_unstable_by_key(|(id, _)| *id);
        starts
    }

    /// Every ended node a cut still reads, ascending by node id. A node
    /// whose vector no segment holds any more fails with
    /// [`VectorError::CheckpointSerializationError`]: a snapshot that left it
    /// out answers a past cut without it.
    pub fn export_ended_nodes(&self) -> Result<Vec<EndedNodeExport>, VectorError> {
        self.versions
            .to_entries()
            .into_iter()
            .filter_map(|(id, surrogate, sys_from, sys_to)| {
                sys_to.map(|sys_to| (id, surrogate, sys_from, sys_to))
            })
            .map(|(id, surrogate, sys_from, sys_to)| {
                let vector =
                    self.stored_vector(id)
                        .ok_or(VectorError::CheckpointSerializationError {
                            detail: format!("ended vector node {id} holds no stored vector"),
                        })?;
                Ok(EndedNodeExport {
                    vector,
                    surrogate: Surrogate::new(surrogate),
                    sys_from,
                    sys_to,
                })
            })
            .collect()
    }

    /// The newest ordinal any node of row `surrogate` holds, and whether a
    /// live node holds the row. The newest ordinal is the newest `sys_from`
    /// of a live node or `sys_to` of an ended one. A live node without a span
    /// starts at `i64::MIN`: no retained cut reads below it.
    pub fn row_newest_ordinal(&self, surrogate: Surrogate) -> (Option<i64>, bool) {
        let live: Vec<u32> = match self.multi_doc_map.get(&surrogate) {
            Some(ids) => ids.clone(),
            None => self
                .surrogate_to_local
                .get(&surrogate)
                .copied()
                .into_iter()
                .collect(),
        };
        let live_from = live
            .iter()
            .map(|id| {
                self.versions
                    .span(*id)
                    .map_or(i64::MIN, |span| span.sys_from)
            })
            .max();
        let newest = live_from.max(self.versions.newest_end_of(surrogate));
        (newest, !live.is_empty())
    }

    /// Install `node` as an ended node: stored soft-deleted, bound to no
    /// row, visible at the cuts its span holds. Returns its global id. A
    /// vector without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`] before anything changes.
    pub fn install_ended_node(&mut self, node: EndedNodeExport) -> Result<u32, VectorError> {
        check_dim(self.dim, node.vector.len())?;
        let id = self.insert(node.vector)?;
        self.delete_inner(id);
        self.versions.record_live(id, node.surrogate, node.sys_from);
        self.versions.end(id, node.surrogate, node.sys_to);
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;
    use crate::hnsw::HnswParams;

    fn ids_at(coll: &VectorCollection, at: i64) -> usize {
        coll.search_at(&[1.0, 0.0], 10, 32, DistanceMetric::L2, None, at)
            .unwrap()
            .len()
    }

    #[test]
    fn exported_history_installs_with_its_spans() {
        let mut source = VectorCollection::new(2, HnswParams::default());
        let s = Surrogate::new(3);
        source.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        source.insert_at(vec![0.5, 0.5], s, 20).unwrap();

        let starts = source.live_node_starts();
        let ended = source.export_ended_nodes().unwrap();
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].sys_from, ended[0].sys_to), (10, 20));

        let mut target = VectorCollection::new(2, HnswParams::default());
        for (vid, vector, surrogate) in source.export_snapshot().unwrap() {
            let from = starts
                .iter()
                .find(|(id, _)| *id == vid)
                .map_or(i64::MIN, |(_, from)| *from);
            target
                .insert_at(vector, surrogate.unwrap_or(Surrogate::ZERO), from)
                .unwrap();
        }
        for node in ended {
            target.install_ended_node(node).unwrap();
        }

        assert_eq!(ids_at(&target, 5), 0);
        assert_eq!(ids_at(&target, 15), 1);
        assert_eq!(
            target.vector_for_surrogate_at(s, 15),
            Some(vec![1.0, 0.0]),
            "the ended node answers its cut"
        );
        assert_eq!(target.vector_for_surrogate_at(s, 25), Some(vec![0.5, 0.5]));
        assert_eq!(target.live_count(), 1);
    }

    /// A row's newest ordinal is the newest start or end of its nodes.
    #[test]
    fn a_rows_newest_ordinal_spans_live_and_ended_nodes() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        let s = Surrogate::new(3);
        assert_eq!(coll.row_newest_ordinal(s), (None, false));
        coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        assert_eq!(coll.row_newest_ordinal(s), (Some(10), true));
        coll.end_surrogate_at(s, 30);
        assert_eq!(coll.row_newest_ordinal(s), (Some(30), false));
    }

    /// A rolled-back restore of a row withdraws the ended nodes it installed
    /// and makes the row's ended live node current again: the write mark
    /// covers every node a RESTORE's vector install writes.
    #[test]
    fn a_rolled_back_restore_withdraws_its_ended_nodes() {
        let mut coll = VectorCollection::new(2, HnswParams::default());
        let s = Surrogate::new(3);
        coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        let before = coll.row_newest_ordinal(s);
        let mark = coll.write_mark(&[s], &[]);

        coll.end_surrogate_at(s, 50);
        coll.install_ended_node(EndedNodeExport {
            vector: vec![0.5, 0.5],
            surrogate: s,
            sys_from: 50,
            sys_to: 60,
        })
        .unwrap();
        coll.insert_at(vec![0.0, 1.0], s, 60).unwrap();
        assert_eq!(coll.row_newest_ordinal(s), (Some(60), true));

        assert!(coll.roll_back_to(mark));
        assert_eq!(coll.row_newest_ordinal(s), before);
        assert_eq!(coll.vector_for_surrogate_at(s, 55), Some(vec![1.0, 0.0]));
        assert_eq!(coll.live_count(), 1);
    }
}
