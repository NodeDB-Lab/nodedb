// SPDX-License-Identifier: Apache-2.0

//! Reads at a past system-time cut.
//!
//! A search at ordinal `at` admits the node of each row visible at `at`: it
//! hides the live nodes inserted after `at` and admits the ended nodes still
//! visible there (see `versions`). The cut's admission sets cost one pass
//! over the spans no purge dropped yet. A current search never builds them.

use nodedb_types::Surrogate;
use roaring::RoaringBitmap;

use super::lifecycle::VectorCollection;
use super::lifecycle_insert_ops::sealed_vector;
use super::search::{finish, push_shifted};
use crate::distance::{DistanceMetric, distance};
use crate::error::{VectorError, check_dim};
use crate::hnsw::SearchResult;
use crate::visibility::NodeFilter;

impl VectorCollection {
    /// The `top_k` nodes nearest to `query` among those visible at ordinal
    /// `at`, scored under `metric`. `allow`, when given, restricts the search
    /// to these global node ids ([`Self::nodes_at`] translates a row
    /// prefilter). A query without the collection dimension fails with
    /// [`VectorError::DimensionMismatch`].
    pub fn search_at(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        metric: DistanceMetric,
        allow: Option<&RoaringBitmap>,
        at: i64,
    ) -> Result<Vec<SearchResult>, VectorError> {
        check_dim(self.dim, query.len())?;
        let sets = self.versions.cut_sets(at);
        let filter = NodeFilter {
            allow,
            hide: Some(&sets.hide),
            revive: Some(&sets.revive),
        };
        let mut all: Vec<SearchResult> = Vec::new();
        if let Some(ivf) = &self.ivf {
            all.extend(ivf.search_admitting(query, top_k, metric, &filter)?);
        }
        push_shifted(
            &mut all,
            self.growing
                .search_admitting(query, top_k, metric, &filter, self.growing_base_id)?,
            self.growing_base_id,
        );
        for seg in &self.sealed {
            let mut results = seg
                .index
                .search_admitting(query, top_k, ef, &filter, seg.base_id)?;
            // The graph ranks by its build metric. Rescore with the
            // requested one from the stored FP32 vector.
            for r in &mut results {
                if let Some(v) = sealed_vector(seg, r.id) {
                    r.distance = distance(query, &v, metric);
                }
            }
            push_shifted(&mut all, results, seg.base_id);
        }
        for seg in &self.building {
            push_shifted(
                &mut all,
                seg.flat
                    .search_admitting(query, top_k, metric, &filter, seg.base_id)?,
                seg.base_id,
            );
        }
        Ok(finish(all, top_k))
    }

    /// The global ids of the nodes of `surrogates` visible at ordinal `at`:
    /// the live node of a row when its insert is at or below `at`, else the
    /// ended node visible there. A multi-vector document yields every node
    /// of the version visible at `at`.
    pub fn nodes_at(
        &self,
        surrogates: impl IntoIterator<Item = Surrogate>,
        at: i64,
    ) -> RoaringBitmap {
        let mut nodes = RoaringBitmap::new();
        for surrogate in surrogates {
            if let Some(&id) = self.surrogate_to_local.get(&surrogate)
                && self.versions.live_visible_at(id, at)
            {
                nodes.insert(id);
            }
            if let Some(ids) = self.multi_doc_map.get(&surrogate) {
                nodes.extend(
                    ids.iter()
                        .copied()
                        .filter(|id| self.versions.live_visible_at(*id, at)),
                );
            }
            nodes.extend(self.versions.ended_visible_at(surrogate, at));
        }
        nodes
    }

    /// The FP32 vector of `surrogate`'s node visible at ordinal `at`.
    /// `None` when the row has no vector visible there.
    pub fn vector_for_surrogate_at(&self, surrogate: Surrogate, at: i64) -> Option<Vec<f32>> {
        if let Some(&id) = self.surrogate_to_local.get(&surrogate)
            && self.versions.live_visible_at(id, at)
        {
            return self.vector_for_id(id);
        }
        self.versions
            .ended_visible_at(surrogate, at)
            .into_iter()
            .find_map(|id| self.stored_vector(id))
    }

    /// The FP32 vector stored under global `id`, live or soft-deleted.
    pub(super) fn stored_vector(&self, id: u32) -> Option<Vec<f32>> {
        if let Some(ivf) = &self.ivf
            && ivf.contains(id)
        {
            return ivf.get_vector_raw(id).map(<[f32]>::to_vec);
        }
        if id >= self.growing_base_id {
            let local = id - self.growing_base_id;
            if (local as usize) < self.growing.len() {
                return self.growing.get_vector_raw(local).map(<[f32]>::to_vec);
            }
        }
        for seg in &self.sealed {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.index.len() {
                    return sealed_vector(seg, local);
                }
            }
        }
        for seg in &self.building {
            if id >= seg.base_id {
                let local = id - seg.base_id;
                if (local as usize) < seg.flat.len() {
                    return seg.flat.get_vector_raw(local).map(<[f32]>::to_vec);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::HnswParams;

    fn collection() -> VectorCollection {
        VectorCollection::new(2, HnswParams::default())
    }

    fn ids_at(coll: &VectorCollection, at: i64) -> Vec<u32> {
        let mut ids: Vec<u32> = coll
            .search_at(&[1.0, 0.0], 10, 32, DistanceMetric::L2, None, at)
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Insert at 10, update at 20, delete at 30, re-insert at 40: each cut
    /// reads the node visible there, and the current search reads the live
    /// node only.
    #[test]
    fn each_cut_reads_the_version_visible_there() {
        let mut coll = collection();
        let s = Surrogate::new(1);
        let v1 = coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        let v2 = coll.insert_at(vec![0.9, 0.1], s, 20).unwrap();
        assert!(coll.end_surrogate_at(s, 30));
        let v3 = coll.insert_at(vec![0.8, 0.2], s, 40).unwrap();

        assert!(ids_at(&coll, 5).is_empty());
        assert_eq!(ids_at(&coll, 10), vec![v1]);
        assert_eq!(ids_at(&coll, 25), vec![v2]);
        assert!(ids_at(&coll, 35).is_empty(), "deleted at 30");
        assert_eq!(ids_at(&coll, 45), vec![v3]);

        let current: Vec<u32> = coll
            .search(&[1.0, 0.0], 10, 32)
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(current, vec![v3]);

        assert_eq!(coll.vector_for_surrogate_at(s, 25), Some(vec![0.9, 0.1]));
        assert_eq!(coll.vector_for_surrogate_at(s, 35), None);
        assert_eq!(coll.vector_for_surrogate_at(s, 45), Some(vec![0.8, 0.2]));
    }

    #[test]
    fn a_row_prefilter_reads_the_node_visible_at_the_cut() {
        let mut coll = collection();
        let a = Surrogate::new(1);
        let b = Surrogate::new(2);
        let a1 = coll.insert_at(vec![1.0, 0.0], a, 10).unwrap();
        coll.insert_at(vec![0.0, 1.0], b, 10).unwrap();
        let a2 = coll.insert_at(vec![0.5, 0.5], a, 20).unwrap();
        let at15 = coll.nodes_at([a], 15);
        assert_eq!(at15.iter().collect::<Vec<_>>(), vec![a1]);
        let at25 = coll.nodes_at([a], 25);
        assert_eq!(at25.iter().collect::<Vec<_>>(), vec![a2]);
        let hits = coll
            .search_at(&[0.0, 1.0], 10, 32, DistanceMetric::L2, Some(&at15), 15)
            .unwrap();
        assert_eq!(hits.iter().map(|r| r.id).collect::<Vec<_>>(), vec![a1]);
    }

    /// Ended nodes survive a seal, a build, and compaction while a cut still
    /// reads them. A purge lets compaction drop them.
    #[test]
    fn ended_nodes_survive_compaction_until_purged() {
        let mut coll = VectorCollection::with_seal_threshold(2, HnswParams::default(), 2);
        let s = Surrogate::new(1);
        coll.insert_at(vec![1.0, 0.0], s, 10).unwrap();
        coll.insert_at(vec![0.0, 1.0], Surrogate::new(2), 10)
            .unwrap();
        let req = coll.seal("k").expect("seal");
        let mut index = crate::hnsw::HnswIndex::with_seed(req.dim, req.params.clone(), 7);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        assert!(coll.complete_build(req.segment_id, index, crate::test_support::test_memory()));
        coll.insert_at(vec![0.9, 0.1], s, 20).unwrap();

        coll.compact();
        let at15 = coll
            .search_at(&[1.0, 0.0], 1, 32, DistanceMetric::L2, None, 15)
            .unwrap();
        assert_eq!(at15.len(), 1);
        assert_eq!(coll.get_surrogate(at15[0].id), Some(s));
        assert_eq!(coll.vector_for_surrogate_at(s, 15), Some(vec![1.0, 0.0]));

        assert_eq!(coll.purge_vector_versions_below(20), 1);
        coll.compact();
        // No snapshot below the horizon exists after a purge, so only the
        // purged version's absence is checked, never a read below it.
        assert_ne!(
            coll.vector_for_surrogate_at(s, 15),
            Some(vec![1.0, 0.0]),
            "the purged version is gone"
        );
        assert_eq!(coll.vector_for_surrogate_at(s, 20), Some(vec![0.9, 0.1]));
    }

    /// A deterministic 8-dimensional vector for `i`.
    fn vector(i: u32) -> Vec<f32> {
        let mut state = u64::from(i).wrapping_mul(6_364_136_223_846_793_005) | 1;
        (0..8)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 33) as f32 / u32::MAX as f32) - 0.25
            })
            .collect()
    }

    /// A purge frees the ended nodes for compaction. After compaction the
    /// sealed HNSW graph holds only live nodes, every live row's vector finds
    /// itself as its nearest neighbour, and a search for every node returns
    /// the whole live set: the graph stays connected and recall holds.
    #[test]
    fn compaction_after_a_purge_frees_ended_nodes_and_keeps_recall() {
        const ROWS: u32 = 200;
        let mut coll =
            VectorCollection::with_seal_threshold(8, HnswParams::default(), ROWS as usize);
        for i in 0..ROWS {
            coll.insert_at(vector(i), Surrogate::new(i + 1), 10)
                .unwrap();
        }
        let req = coll.seal("k").expect("seal");
        let mut index = crate::hnsw::HnswIndex::with_seed(req.dim, req.params.clone(), 7);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        assert!(coll.complete_build(req.segment_id, index, crate::test_support::test_memory()));
        // Half the rows move: their sealed nodes end at 20.
        for i in (0..ROWS).step_by(2) {
            coll.insert_at(vector(i + ROWS), Surrogate::new(i + 1), 20)
                .unwrap();
        }
        let before_purge = coll.resident_bytes();
        assert_eq!(
            coll.removable_tombstone_count(),
            0,
            "a cut still reads them"
        );
        assert_eq!(coll.compact(), 0, "nothing is removable before the purge");

        assert_eq!(coll.purge_vector_versions_below(20), (ROWS / 2) as usize);
        assert_eq!(coll.removable_tombstone_count(), (ROWS / 2) as usize);
        assert_eq!(coll.compact(), (ROWS / 2) as usize);
        assert_eq!(coll.removable_tombstone_count(), 0);
        assert!(
            coll.resident_bytes() < before_purge,
            "the purged nodes free memory"
        );

        let sealed: usize = coll.sealed_segments().iter().map(|s| s.index.len()).sum();
        assert_eq!(sealed, (ROWS / 2) as usize, "only live nodes stay sealed");
        let mut live = 0;
        for i in 0..ROWS {
            let current = if i % 2 == 0 {
                vector(i + ROWS)
            } else {
                vector(i)
            };
            let hits = coll.search(&current, 1, 64).unwrap();
            assert_eq!(
                hits.first().and_then(|hit| coll.get_surrogate(hit.id)),
                Some(Surrogate::new(i + 1)),
                "row {i} finds itself"
            );
            live += 1;
        }
        let everything = coll.search(&vector(0), ROWS as usize, 400).unwrap();
        let mut found: Vec<Surrogate> = everything
            .iter()
            .filter_map(|hit| coll.get_surrogate(hit.id))
            .collect();
        found.sort_unstable_by_key(|s| s.as_u32());
        found.dedup();
        assert_eq!(found.len(), live, "every live row is reachable");
    }
}
