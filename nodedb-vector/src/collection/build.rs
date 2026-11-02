// SPDX-License-Identifier: Apache-2.0

//! HNSW builds for a `VectorCollection`: the requests the owning core sends
//! to its builder thread, and installing the finished graphs.
//!
//! Every request carries one vector per local node id, soft-deleted nodes
//! included, so a built graph keeps the ids its segment had. Installing a
//! build applies the tombstones the segment holds at that moment, so a
//! delete that landed while the build ran is kept.
//!
//! - A seal build promotes a building segment to sealed.
//! - A rebuild replaces a sealed segment in place, from that segment's own
//!   vectors, and quantizes it again under the collection's config.

use nodedb_mem::ScopedMemory;
use nodedb_types::VectorQuantization;

use crate::error::VectorError;
use crate::hnsw::HnswIndex;
use crate::index_config::IndexType;

use super::lifecycle::VectorCollection;
use super::lifecycle_insert_ops::sealed_vector;
use super::segment::{BuildKind, BuildRequest, SealedSegment};

impl VectorCollection {
    /// Segment ids of the building segments, oldest first.
    pub fn building_segment_ids(&self) -> Vec<u32> {
        self.building.iter().map(|b| b.segment_id).collect()
    }

    /// A seal build request for building segment `segment_id`, read from its
    /// flat vectors. `None` when no building segment has that id.
    pub fn build_request_for(&self, key: &str, segment_id: u32) -> Option<BuildRequest> {
        let seg = self.building.iter().find(|b| b.segment_id == segment_id)?;
        let vectors = (0..seg.flat.len() as u32)
            .filter_map(|i| seg.flat.get_vector_raw(i).map(<[f32]>::to_vec))
            .collect();
        Some(BuildRequest {
            key: key.to_string(),
            segment_id,
            kind: BuildKind::Seal,
            vectors,
            dim: self.dim,
            params: self.params.clone(),
        })
    }

    /// Base ids of the non-empty sealed segments, in segment order.
    pub fn sealed_base_ids(&self) -> Vec<u32> {
        self.sealed
            .iter()
            .filter(|s| !s.index.is_empty())
            .map(|s| s.base_id)
            .collect()
    }

    /// A rebuild request for the sealed segment at `base_id`, under the
    /// current HNSW params, read from that segment only: the growing and
    /// building segments keep their own vectors. `None` when no sealed
    /// segment starts at `base_id`.
    ///
    /// Fails with [`VectorError::VectorUnavailable`] when a node's vector
    /// cannot be read.
    pub fn rebuild_request_for(
        &mut self,
        key: &str,
        base_id: u32,
    ) -> Result<Option<BuildRequest>, VectorError> {
        let Some(seg) = self.sealed.iter().find(|s| s.base_id == base_id) else {
            return Ok(None);
        };
        let len = seg.index.len();
        let mut vectors = Vec::with_capacity(len);
        for local in 0..len as u32 {
            let v = sealed_vector(seg, local).ok_or(VectorError::VectorUnavailable {
                id: base_id + local,
            })?;
            vectors.push(v);
        }
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;
        Ok(Some(BuildRequest {
            key: key.to_string(),
            segment_id,
            kind: BuildKind::Rebuild { base_id, len },
            vectors,
            dim: self.dim,
            params: self.params.clone(),
        }))
    }

    /// Install a seal build: promote building segment `segment_id` to sealed
    /// with its current tombstones. Returns `false`, changing nothing, when
    /// no building segment has that id (a truncate dropped it) or the graph
    /// does not hold one node per vector of the segment.
    pub fn complete_build(
        &mut self,
        segment_id: u32,
        index: HnswIndex,
        memory: ScopedMemory,
    ) -> bool {
        let Some(pos) = self
            .building
            .iter()
            .position(|b| b.segment_id == segment_id)
        else {
            return false;
        };
        let mut index = index;
        if index.len() != self.building[pos].flat.len() {
            tracing::error!(
                segment_id,
                built = index.len(),
                expected = self.building[pos].flat.len(),
                "HNSW build does not match its segment; segment stays on brute force"
            );
            return false;
        }
        let building = self.building.remove(pos);
        for local in 0..building.flat.len() as u32 {
            if building.flat.is_deleted(local) {
                index.delete(local);
            }
        }
        let seg = self.sealed_segment_from(segment_id, building.base_id, index, &memory);
        self.sealed.push(seg);
        self.builds_completed += 1;
        self.refresh_codec_dispatch();
        true
    }

    /// Install a rebuild of the sealed segment at `base_id`, read when it held
    /// `len` nodes. The old segment's tombstones carry over and the segment is
    /// quantized again under the collection's config. Returns `false`,
    /// changing nothing, when no sealed segment at `base_id` still holds
    /// `len` nodes, or the graph does not hold `len` nodes.
    pub fn complete_rebuild(
        &mut self,
        segment_id: u32,
        base_id: u32,
        len: usize,
        index: HnswIndex,
        memory: ScopedMemory,
    ) -> bool {
        let Some(pos) = self
            .sealed
            .iter()
            .position(|s| s.base_id == base_id && s.index.len() == len)
        else {
            return false;
        };
        if index.len() != len {
            tracing::error!(
                base_id,
                built = index.len(),
                expected = len,
                "HNSW rebuild does not match its segment; the old segment stays"
            );
            return false;
        }
        let mut index = index;
        for local in 0..len as u32 {
            if self.sealed[pos].index.is_deleted(local) {
                index.delete(local);
            }
        }
        let seg = self.sealed_segment_from(segment_id, base_id, index, &memory);
        let old = std::mem::replace(&mut self.sealed[pos], seg);
        self.drop_sealed(old);
        self.builds_completed += 1;
        self.refresh_codec_dispatch();
        true
    }

    /// Record a build that failed. The segment stays as it was.
    pub fn note_build_failed(&mut self) {
        self.builds_failed += 1;
    }

    /// A sealed segment for `index`: quantized under the collection's config
    /// and placed on the storage tier the memory budget allows.
    fn sealed_segment_from(
        &mut self,
        segment_id: u32,
        base_id: u32,
        index: HnswIndex,
        memory: &ScopedMemory,
    ) -> SealedSegment {
        let use_codec_dispatch = self.codec_dispatch_tag().is_some();
        let use_pq = !use_codec_dispatch && self.index_config.index_type == IndexType::HnswPq;
        let (sq8, pq) = if use_codec_dispatch {
            (None, None)
        } else if use_pq {
            (
                None,
                Self::build_pq_for_index(&index, self.index_config.pq_m, memory.clone()),
            )
        } else {
            (Self::build_sq8_for_index(&index), None)
        };
        let (tier, mmap_vectors) = self.resolve_tier_for_build(segment_id, base_id, &index, memory);
        let resident_bytes = SealedSegment::measure(&index, sq8.as_ref());
        SealedSegment {
            index,
            base_id,
            sq8,
            pq,
            tier,
            mmap_vectors,
            resident_bytes,
        }
    }

    /// Drop a replaced sealed segment, removing its mmap file.
    fn drop_sealed(&mut self, seg: SealedSegment) {
        let mmap_path = seg.mmap_vectors.as_ref().map(|m| m.path().to_path_buf());
        // Unmap before the file goes.
        drop(seg);
        if let Some(path) = mmap_path {
            self.mmap_segment_count = self.mmap_segment_count.saturating_sub(1);
            if let Err(e) = std::fs::remove_file(&path) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "vector rebuild: replaced mmap segment file not removed"
                );
            }
        }
    }

    /// The codec-dispatch tag the collection's quantization selects.
    fn codec_dispatch_tag(&self) -> Option<&'static str> {
        match self.quantization {
            VectorQuantization::RaBitQ => Some("rabitq"),
            VectorQuantization::Bbq => Some("bbq"),
            _ => None,
        }
    }

    /// Rebuild the collection-level codec-dispatch index over the sealed
    /// segments when the quantization selects one.
    fn refresh_codec_dispatch(&mut self) {
        let Some(tag) = self.codec_dispatch_tag() else {
            return;
        };
        if let Err(e) = self.build_codec_dispatch(tag).map(|_| ()) {
            // Without the codec index the sealed segments are searched by
            // their own HNSW graphs, which answer the same queries.
            tracing::error!(error = %e, tag, "codec-dispatch build failed; searching sealed segments directly");
            self.codec_dispatch = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::hnsw::HnswParams;
    use crate::test_support::test_memory;

    fn vector(i: usize) -> Vec<f32> {
        vec![(i + 1) as f32, (i % 7 + 1) as f32, (i % 11 + 1) as f32]
    }

    fn l2() -> HnswParams {
        HnswParams {
            metric: crate::distance::DistanceMetric::L2,
            ..HnswParams::default()
        }
    }

    fn build(req: &BuildRequest) -> HnswIndex {
        let mut index = HnswIndex::with_seed(req.dim, req.params.clone(), 7);
        for v in &req.vectors {
            index.insert(v.clone()).unwrap();
        }
        index
    }

    /// Ten L2 vectors bound to surrogates 1..=10, with a seal threshold of 10.
    fn collection() -> VectorCollection {
        let mut coll = VectorCollection::with_seal_threshold(3, l2(), 10);
        for i in 0..10 {
            coll.insert_with_surrogate(vector(i), Surrogate::new(i as u32 + 1))
                .unwrap();
        }
        coll
    }

    #[test]
    fn a_seal_build_keeps_ids_and_the_deletes_made_meanwhile() {
        let mut coll = collection();
        coll.delete(2);
        let req = coll.seal("k").unwrap();
        assert_eq!(req.vectors.len(), 10, "deleted vectors keep their slot");
        coll.delete(4);

        assert!(coll.complete_build(req.segment_id, build(&req), test_memory()));

        assert!(coll.building.is_empty());
        assert!(!coll.is_live(2) && !coll.is_live(4) && coll.is_live(9));
        assert_eq!(coll.vector_for_id(7), Some(vector(7)));
        assert_eq!(coll.search(&vector(7), 1, 64).unwrap()[0].id, 7);
        assert_eq!(coll.get_surrogate(7), Some(Surrogate::new(8)));
    }

    #[test]
    fn a_build_of_the_wrong_size_is_refused() {
        let mut coll = collection();
        let req = coll.seal("k").unwrap();
        let mut short = HnswIndex::new(3, l2());
        short.insert(vector(0)).unwrap();
        assert!(!coll.complete_build(req.segment_id, short, test_memory()));
        assert_eq!(coll.building.len(), 1, "the segment stays on brute force");
    }

    #[test]
    fn a_rebuild_keeps_ids_codes_and_tombstones() {
        let mut coll = VectorCollection::with_pq_config(3, l2(), 3);
        coll.set_seal_threshold(10);
        for i in 0..10 {
            coll.insert_with_surrogate(vector(i), Surrogate::new(i as u32 + 1))
                .unwrap();
        }
        let req = coll.seal("k").unwrap();
        coll.complete_build(req.segment_id, build(&req), test_memory());
        coll.delete(1);

        let req = coll.rebuild_request_for("k", 0).unwrap().unwrap();
        assert_eq!(
            req.kind,
            BuildKind::Rebuild {
                base_id: 0,
                len: 10
            }
        );
        coll.delete(6);
        let BuildKind::Rebuild { base_id, len } = req.kind else {
            panic!("a rebuild request");
        };
        assert!(coll.complete_rebuild(req.segment_id, base_id, len, build(&req), test_memory()));

        assert_eq!(coll.sealed.len(), 1);
        assert!(
            coll.sealed[0].pq.is_some(),
            "the segment is quantized again"
        );
        assert!(!coll.is_live(1) && !coll.is_live(6));
        for i in [0usize, 3, 9] {
            assert_eq!(coll.search(&vector(i), 1, 64).unwrap()[0].id, i as u32);
            assert_eq!(
                coll.get_surrogate(i as u32),
                Some(Surrogate::new(i as u32 + 1))
            );
        }
    }

    #[test]
    fn a_rebuild_of_a_compacted_segment_is_refused() {
        let mut coll = collection();
        let req = coll.seal("k").unwrap();
        coll.complete_build(req.segment_id, build(&req), test_memory());
        coll.delete(3);
        let req = coll.rebuild_request_for("k", 0).unwrap().unwrap();
        assert_eq!(coll.compact(), 1);
        let BuildKind::Rebuild { base_id, len } = req.kind else {
            panic!("a rebuild request");
        };
        assert!(!coll.complete_rebuild(req.segment_id, base_id, len, build(&req), test_memory()));
        assert_eq!(coll.sealed[0].index.len(), 9, "the compacted segment stays");
    }

    #[test]
    fn an_unbuilt_segment_survives_a_checkpoint_as_building() {
        let mut coll = collection();
        coll.delete(5);
        coll.seal("k").unwrap();
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();

        let ids = restored.building_segment_ids();
        assert_eq!(ids.len(), 1);
        let req = restored.build_request_for("k", ids[0]).unwrap();
        assert_eq!(req.vectors.len(), 10);
        assert!(!restored.is_live(5));
        assert_eq!(restored.search(&vector(8), 1, 64).unwrap()[0].id, 8);
    }
}
