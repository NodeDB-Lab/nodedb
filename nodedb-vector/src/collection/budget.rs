// SPDX-License-Identifier: Apache-2.0

//! RAM budget enforcement and mmap spillover for `VectorCollection`.

use nodedb_mem::ScopedMemory;

use crate::collection::tier::StorageTier;
use crate::hnsw::HnswIndex;
use crate::mmap_segment::MmapVectorSegment;

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Set the data directory for mmap segment files.
    pub fn set_data_dir(&mut self, dir: std::path::PathBuf) {
        self.data_dir = Some(dir);
    }

    /// Set the RAM budget for vector data (FP32 in sealed segments).
    pub fn set_ram_budget(&mut self, bytes: usize) {
        self.ram_budget_bytes = bytes;
    }

    /// Estimate current RAM usage for vector data.
    pub fn ram_usage_bytes(&self) -> usize {
        let bytes_per_vector = self.dim * std::mem::size_of::<f32>();
        let growing = self.growing.len() * bytes_per_vector;
        let building: usize = self
            .building
            .iter()
            .map(|b| b.flat.len() * bytes_per_vector)
            .sum();
        let sealed_ram: usize = self
            .sealed
            .iter()
            .filter(|s| s.tier == StorageTier::L0Ram)
            .map(|s| s.index.len() * bytes_per_vector)
            .sum();
        let ivf = self.ivf.as_ref().map_or(0, |ivf| ivf.memory_bytes());
        growing + building + sealed_ram + ivf
    }

    /// Whether the RAM budget is exceeded.
    pub fn is_budget_exceeded(&self) -> bool {
        self.ram_budget_bytes > 0 && self.ram_usage_bytes() >= self.ram_budget_bytes
    }

    /// Number of segments that fell back to mmap.
    pub fn mmap_fallback_count(&self) -> u32 {
        self.mmap_fallback_count
    }

    /// Number of currently active mmap segments.
    pub fn mmap_segment_count(&self) -> u32 {
        self.mmap_segment_count
    }

    /// Determine storage tier and optionally create mmap segment for a completed build.
    ///
    /// `base_id` is the global vector ID offset for the first vector in `index`.
    /// Surrogate IDs are looked up from `self.surrogate_map` for rows
    /// `[base_id, base_id + N)` and written into the segment's surrogate block.
    pub(crate) fn resolve_tier_for_build(
        &mut self,
        segment_id: u32,
        base_id: u32,
        index: &HnswIndex,
        memory: &ScopedMemory,
    ) -> (StorageTier, Option<MmapVectorSegment>) {
        if !self.is_budget_exceeded() {
            return (StorageTier::L0Ram, None);
        }

        let Some(dir) = &self.data_dir else {
            return (StorageTier::L0Ram, None);
        };

        let seg_name = format!("seg-{segment_id}.vseg");
        let seg_path = dir.join(&seg_name);
        let count = index.len();

        // One row per local node id, in any storage dtype: the mmap tier is
        // read by local id.
        let refs: Vec<Vec<f32>> = match (0..count as u32)
            .map(|i| index.materialize_vector(i))
            .collect::<Result<_, _>>()
        {
            Ok(refs) => refs,
            Err(e) => {
                tracing::warn!(
                    segment_id,
                    error = %e,
                    "mmap spill failed to read the segment vectors, keeping vectors in RAM"
                );
                return (StorageTier::L0Ram, None);
            }
        };
        let ref_slices: Vec<&[f32]> = refs.iter().map(|v| v.as_slice()).collect();

        // Build the parallel surrogate ID array (u64 per row).
        let surrogate_ids: Vec<u64> = (0..count as u32)
            .map(|local_id| {
                let global_id = base_id + local_id;
                self.surrogate_map
                    .get(&global_id)
                    .map(|s| s.as_u32() as u64)
                    .unwrap_or(0)
            })
            .collect();

        match MmapVectorSegment::create_with_surrogates(
            dir,
            &seg_name,
            self.dim,
            &ref_slices,
            &surrogate_ids,
            memory,
        ) {
            Ok(mmap) => {
                self.mmap_fallback_count += 1;
                self.mmap_segment_count += 1;
                tracing::info!(
                    segment_id,
                    vectors = count,
                    path = %seg_path.display(),
                    "vector segment spilled to mmap (L1 NVMe)"
                );
                (StorageTier::L1Nvme, Some(mmap))
            }
            Err(e) => {
                tracing::warn!(
                    segment_id,
                    error = %e,
                    "mmap fallback failed, keeping vectors in RAM"
                );
                (StorageTier::L0Ram, None)
            }
        }
    }
}
