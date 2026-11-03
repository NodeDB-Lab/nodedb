// SPDX-License-Identifier: Apache-2.0

//! HNSW builds for a `VectorCollection`: the requests the owning core sends
//! to its builder thread, and installing the finished segments.
//!
//! Every request carries one vector per local node id, soft-deleted nodes
//! included, so a built graph keeps the ids its segment had. The builder
//! thread builds the graph and then the codec the collection's plan names,
//! so codec training never runs on the owning core. Installing a build
//! applies the tombstones the segment holds at that moment, so a delete that
//! landed while the build ran is kept.
//!
//! - A seal build promotes a building segment to sealed.
//! - A rebuild replaces a sealed segment in place, from that segment's own
//!   vectors, with a codec built under the collection's current plan.
//!
//! A build whose codec failed installs its graph with no codec, so the
//! segment answers exactly. The install outcome carries the codec error to
//! the owning core, which reports it and decides whether to retry.

use nodedb_mem::ScopedMemory;
use nodedb_types::VectorQuantization;

use crate::error::VectorError;
use crate::hnsw::HnswIndex;
use crate::index_config::IndexType;

use super::lifecycle::VectorCollection;
use super::lifecycle_insert_ops::sealed_vector;
use super::segment::{BuildKind, BuildRequest, BuiltSegment, InstallOutcome, SealedSegment};
use super::segment_codec::{CodecPlan, GraphCodecKind, SegmentCodec};

impl VectorCollection {
    /// Segment ids of the building segments, oldest first.
    pub fn building_segment_ids(&self) -> Vec<u32> {
        self.building.iter().map(|b| b.segment_id).collect()
    }

    /// The codec this collection searches and builds its segments with.
    /// `HnswPq` and `IvfPq` select PQ whatever the quantization says. An
    /// `IvfPq` collection searches its trained index by PQ codes, so a
    /// search hint is checked against PQ.
    pub fn codec_plan(&self) -> CodecPlan {
        match self.index_config.index_type {
            IndexType::HnswPq | IndexType::IvfPq => {
                return CodecPlan::Pq {
                    m: self.index_config.pq_m,
                };
            }
            IndexType::Hnsw => {}
        }
        match self.quantization {
            VectorQuantization::None => CodecPlan::None,
            VectorQuantization::Sq8 => CodecPlan::Sq8,
            VectorQuantization::Pq => CodecPlan::Pq {
                m: self.index_config.pq_m,
            },
            VectorQuantization::RaBitQ => CodecPlan::Graph(GraphCodecKind::RaBitQ),
            VectorQuantization::Bbq => CodecPlan::Graph(GraphCodecKind::Bbq),
            VectorQuantization::Binary => CodecPlan::Graph(GraphCodecKind::Binary),
            VectorQuantization::Ternary => CodecPlan::Graph(GraphCodecKind::Ternary),
            VectorQuantization::Opq => CodecPlan::Graph(GraphCodecKind::Opq {
                m: self.index_config.pq_m,
            }),
        }
    }

    /// Base ids of the non-empty sealed segments whose codec differs from
    /// the plan: a graph codec after a restore, a codec build that failed,
    /// or a graph codec compaction dropped. Each needs a rebuild. A segment
    /// whose codec failed with an error a retry does not fix is skipped.
    pub fn sealed_ids_missing_codec(&self) -> Vec<u32> {
        let want = self.codec_plan().quantization();
        self.sealed
            .iter()
            .filter(|s| {
                !s.index.is_empty()
                    && s.codec.serves() != want
                    && !self.codec_refused.contains(&s.base_id)
            })
            .map(|s| s.base_id)
            .collect()
    }

    /// Record that the codec build of the sealed segment at `base_id` failed
    /// with an error a retry does not fix. The segment keeps answering with
    /// no codec and is not queued again.
    pub fn note_codec_refused(&mut self, base_id: u32) {
        if !self.codec_refused.contains(&base_id) {
            self.codec_refused.push(base_id);
        }
    }

    /// A seal build request for building segment `segment_id`, read from its
    /// flat vectors, with codec training charged to `memory`. `None` when no
    /// building segment has that id.
    pub fn build_request_for(
        &self,
        key: &str,
        segment_id: u32,
        memory: ScopedMemory,
    ) -> Option<BuildRequest> {
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
            codec: self.codec_plan(),
            memory,
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
    /// current HNSW params and codec plan, read from that segment only: the
    /// growing and building segments keep their own vectors. Codec training
    /// is charged to `memory`. `None` when no sealed segment starts at
    /// `base_id`.
    ///
    /// Fails with [`VectorError::VectorUnavailable`] when a node's vector
    /// cannot be read.
    pub fn rebuild_request_for(
        &mut self,
        key: &str,
        base_id: u32,
        memory: ScopedMemory,
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
            codec: self.codec_plan(),
            memory,
        }))
    }

    /// Install a seal build: promote building segment `segment_id` to sealed
    /// with its current tombstones. Refused, changing nothing, when no
    /// building segment has that id (a truncate dropped it) or the graph does
    /// not hold one node per vector of the segment.
    pub fn complete_build(&mut self, segment_id: u32, built: BuiltSegment) -> InstallOutcome {
        let Some(pos) = self
            .building
            .iter()
            .position(|b| b.segment_id == segment_id)
        else {
            return InstallOutcome::Refused;
        };
        if built.index.len() != self.building[pos].flat.len() {
            tracing::error!(
                segment_id,
                built = built.index.len(),
                expected = self.building[pos].flat.len(),
                "HNSW build does not match its segment; segment stays on brute force"
            );
            return InstallOutcome::Refused;
        }
        let building = self.building.remove(pos);
        let base_id = building.base_id;
        let (mut index, mut codec, codec_error, memory) = take_build(built);
        for local in 0..building.flat.len() as u32 {
            if building.flat.is_deleted(local) {
                index.delete(local);
                codec.set_deleted(local, true);
            }
        }
        if codec_error.is_none() {
            self.codec_refused.retain(|&b| b != base_id);
        }
        let seg = self.sealed_segment_from(segment_id, base_id, index, codec, &memory);
        self.sealed.push(seg);
        self.builds_completed += 1;
        InstallOutcome::Installed {
            base_id,
            codec_error,
        }
    }

    /// Install a rebuild of the sealed segment at `base_id`, read when it held
    /// `len` nodes. The old segment's tombstones carry over. Refused,
    /// changing nothing, when no sealed segment at `base_id` still holds
    /// `len` nodes, or the graph does not hold `len` nodes.
    pub fn complete_rebuild(
        &mut self,
        segment_id: u32,
        base_id: u32,
        len: usize,
        built: BuiltSegment,
    ) -> InstallOutcome {
        let Some(pos) = self
            .sealed
            .iter()
            .position(|s| s.base_id == base_id && s.index.len() == len)
        else {
            return InstallOutcome::Refused;
        };
        if built.index.len() != len {
            tracing::error!(
                base_id,
                built = built.index.len(),
                expected = len,
                "HNSW rebuild does not match its segment; the old segment stays"
            );
            return InstallOutcome::Refused;
        }
        let (mut index, mut codec, codec_error, memory) = take_build(built);
        for local in 0..len as u32 {
            if self.sealed[pos].index.is_deleted(local) {
                index.delete(local);
                codec.set_deleted(local, true);
            }
        }
        if codec_error.is_none() {
            self.codec_refused.retain(|&b| b != base_id);
        }
        let seg = self.sealed_segment_from(segment_id, base_id, index, codec, &memory);
        let old = std::mem::replace(&mut self.sealed[pos], seg);
        self.drop_sealed(old);
        self.builds_completed += 1;
        InstallOutcome::Installed {
            base_id,
            codec_error,
        }
    }

    /// Record a build that failed. The segment stays as it was.
    pub fn note_build_failed(&mut self) {
        self.builds_failed += 1;
    }

    /// A sealed segment for `index` and its `codec`, placed on the storage
    /// tier the memory budget allows. An mmap spill is charged to `memory`.
    fn sealed_segment_from(
        &mut self,
        segment_id: u32,
        base_id: u32,
        index: HnswIndex,
        codec: SegmentCodec,
        memory: &ScopedMemory,
    ) -> SealedSegment {
        let (tier, mmap_vectors) = self.resolve_tier_for_build(segment_id, base_id, &index, memory);
        let resident_bytes = SealedSegment::measure(&index, &codec);
        SealedSegment {
            index,
            base_id,
            codec,
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
}

/// The parts of `built` a segment installs: its graph, its codec or none
/// with the codec error, and its memory scope. The codec charge drops here:
/// the installed segment's resident bytes carry the charge after install.
fn take_build(built: BuiltSegment) -> (HnswIndex, SegmentCodec, Option<VectorError>, ScopedMemory) {
    let BuiltSegment {
        index,
        codec,
        charge,
        memory,
    } = built;
    drop(charge);
    match codec {
        Ok(codec) => (index, codec, None, memory),
        Err(e) => (index, SegmentCodec::None, Some(e), memory),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::builder::build_segment;
    use crate::hnsw::HnswParams;
    use crate::test_support::{build_now as build, test_memory};

    fn vector(i: usize) -> Vec<f32> {
        vec![(i + 1) as f32, (i % 7 + 1) as f32, (i % 11 + 1) as f32]
    }

    fn l2() -> HnswParams {
        HnswParams {
            metric: crate::distance::DistanceMetric::L2,
            ..HnswParams::default()
        }
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
        let req = coll.seal("k", test_memory()).unwrap();
        assert_eq!(req.vectors.len(), 10, "deleted vectors keep their slot");
        coll.delete(4);

        assert!(coll.complete_build(req.segment_id, build(&req)).installed());

        assert!(coll.building.is_empty());
        assert!(!coll.is_live(2) && !coll.is_live(4) && coll.is_live(9));
        assert_eq!(coll.vector_for_id(7), Some(vector(7)));
        assert_eq!(coll.search(&vector(7), 1, 64).unwrap()[0].id, 7);
        assert_eq!(coll.get_surrogate(7), Some(Surrogate::new(8)));
    }

    #[test]
    fn a_build_of_the_wrong_size_is_refused() {
        let mut coll = collection();
        let req = coll.seal("k", test_memory()).unwrap();
        let mut short = HnswIndex::new(3, l2());
        short.insert(vector(0)).unwrap();
        let short = BuiltSegment {
            index: short,
            codec: Ok(SegmentCodec::None),
            charge: super::super::segment_codec::CodecCharge::default(),
            memory: test_memory(),
        };
        assert!(!coll.complete_build(req.segment_id, short).installed());
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
        let req = coll.seal("k", test_memory()).unwrap();
        assert!(coll.complete_build(req.segment_id, build(&req)).installed());
        coll.delete(1);

        let req = coll
            .rebuild_request_for("k", 0, test_memory())
            .unwrap()
            .unwrap();
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
        assert!(
            coll.complete_rebuild(req.segment_id, base_id, len, build(&req))
                .installed()
        );

        assert_eq!(coll.sealed.len(), 1);
        assert!(
            matches!(coll.sealed[0].codec, SegmentCodec::Pq { .. }),
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
        let req = coll.seal("k", test_memory()).unwrap();
        assert!(coll.complete_build(req.segment_id, build(&req)).installed());
        coll.delete(3);
        let req = coll
            .rebuild_request_for("k", 0, test_memory())
            .unwrap()
            .unwrap();
        assert_eq!(coll.compact(), 1);
        let BuildKind::Rebuild { base_id, len } = req.kind else {
            panic!("a rebuild request");
        };
        assert!(
            !coll
                .complete_rebuild(req.segment_id, base_id, len, build(&req))
                .installed()
        );
        assert_eq!(coll.sealed[0].index.len(), 9, "the compacted segment stays");
    }

    #[test]
    fn an_unbuilt_segment_survives_a_checkpoint_as_building() {
        let mut coll = collection();
        coll.delete(5);
        coll.seal("k", test_memory()).unwrap();
        let bytes = coll.checkpoint_to_bytes(None).unwrap();
        let restored = VectorCollection::from_checkpoint(&bytes, None, test_memory()).unwrap();

        let ids = restored.building_segment_ids();
        assert_eq!(ids.len(), 1);
        let req = restored
            .build_request_for("k", ids[0], test_memory())
            .unwrap();
        assert_eq!(req.vectors.len(), 10);
        assert!(!restored.is_live(5));
        assert_eq!(restored.search(&vector(8), 1, 64).unwrap()[0].id, 8);
    }

    #[test]
    fn a_failed_codec_installs_the_graph_and_reports_the_error() {
        let mut coll = VectorCollection::with_pq_config(4, l2(), 3);
        coll.set_seal_threshold(10);
        for i in 0..10u32 {
            coll.insert(vec![i as f32, 1.0, 2.0, 3.0]).unwrap();
        }
        let req = coll.seal("k", test_memory()).unwrap();
        assert_eq!(req.codec, CodecPlan::Pq { m: 3 });
        let segment_id = req.segment_id;
        let built = build_segment(7, req).expect("the graph builds");
        assert!(
            matches!(built.codec, Err(VectorError::InvalidInput { .. })),
            "pq_m 3 does not divide dim 4"
        );

        let InstallOutcome::Installed {
            base_id,
            codec_error: Some(err),
        } = coll.complete_build(segment_id, built)
        else {
            panic!("the graph installs and the codec error is reported");
        };
        assert!(!err.is_transient(), "{err}");
        assert!(coll.building.is_empty(), "the segment leaves brute force");
        assert!(matches!(coll.sealed[0].codec, SegmentCodec::None));
        assert_eq!(coll.sealed_ids_missing_codec(), vec![base_id]);
        coll.note_codec_refused(base_id);
        assert!(
            coll.sealed_ids_missing_codec().is_empty(),
            "a refused codec is not queued again"
        );
        let hit = &coll.search(&[4.0, 1.0, 2.0, 3.0], 1, 16).unwrap()[0];
        assert_eq!(hit.id, 4);
        assert_eq!(hit.distance, 0.0, "the graph answers exactly");
    }

    #[test]
    fn a_codec_over_budget_installs_the_graph_with_a_transient_error() {
        let mut coll = collection();
        let req = coll.seal("k", tight_memory(64)).unwrap();
        let segment_id = req.segment_id;
        let built = build_segment(7, req).expect("the graph builds");
        let InstallOutcome::Installed {
            codec_error: Some(err),
            ..
        } = coll.complete_build(segment_id, built)
        else {
            panic!("the graph installs and the codec error is reported");
        };
        assert!(err.is_transient(), "{err}");
        assert_eq!(coll.sealed_ids_missing_codec(), vec![0]);
        assert_eq!(coll.search(&vector(5), 1, 64).unwrap()[0].id, 5);
    }

    /// A memory scope whose vector budget holds `bytes`.
    fn tight_memory(bytes: usize) -> ScopedMemory {
        use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};
        let governor = std::sync::Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: bytes * EngineId::ALL.len(),
                engine_limits: EngineLimits::uniform(bytes),
            })
            .expect("tight governor"),
        );
        ScopedMemory::new(
            governor,
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::TenantId::new(0),
            EngineId::Vector,
        )
    }

    #[test]
    fn codec_plan_follows_quantization_and_index_type() {
        let mut coll = VectorCollection::new(8, l2());
        assert_eq!(
            coll.codec_plan(),
            CodecPlan::Sq8,
            "the default quantization"
        );
        coll.set_quantization(VectorQuantization::None);
        assert_eq!(coll.codec_plan(), CodecPlan::None);
        coll.set_quantization(VectorQuantization::Opq);
        assert_eq!(
            coll.codec_plan(),
            CodecPlan::Graph(GraphCodecKind::Opq {
                m: crate::index_config::DEFAULT_PQ_M
            })
        );
        let pq = VectorCollection::with_pq_config(8, l2(), 4);
        assert_eq!(pq.codec_plan(), CodecPlan::Pq { m: 4 });
        let mut ivf = VectorCollection::new(8, l2());
        ivf.set_index_config(crate::index_config::IndexConfig {
            index_type: IndexType::IvfPq,
            pq_m: 2,
            ..crate::index_config::IndexConfig::default()
        });
        assert_eq!(
            ivf.codec_plan(),
            CodecPlan::Pq { m: 2 },
            "an IVF-PQ collection searches by PQ codes"
        );
    }
}
