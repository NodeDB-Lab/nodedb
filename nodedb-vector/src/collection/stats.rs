// SPDX-License-Identifier: Apache-2.0

//! Live statistics aggregation for `VectorCollection`.

use crate::index_config::IndexType;

use super::lifecycle::VectorCollection;

impl VectorCollection {
    /// Collect live statistics from all segments.
    pub fn stats(&self) -> nodedb_types::VectorIndexStats {
        let growing_vectors = self.growing.len();
        let sealed_vectors: usize = self.sealed.iter().map(|s| s.index.len()).sum();
        let building_vectors: usize = self.building.iter().map(|s| s.flat.len()).sum();

        let ivf_vectors = self.ivf.as_ref().map_or(0, |ivf| ivf.len());
        let tombstone_count: usize = self.ivf.as_ref().map_or(0, |ivf| ivf.tombstone_count())
            + self
                .sealed
                .iter()
                .map(|s| s.index.tombstone_count())
                .sum::<usize>()
            + self.growing.tombstone_count()
            + self
                .building
                .iter()
                .map(|s| s.flat.tombstone_count())
                .sum::<usize>();

        let total = growing_vectors + sealed_vectors + building_vectors + ivf_vectors;
        let tombstone_ratio = if total > 0 {
            tombstone_count as f64 / total as f64
        } else {
            0.0
        };

        let quantization = if let Some(ref dispatch) = self.codec_dispatch {
            match dispatch.quantization() {
                "rabitq" => nodedb_types::VectorIndexQuantization::RaBitQ,
                "bbq" => nodedb_types::VectorIndexQuantization::Bbq,
                _ => nodedb_types::VectorIndexQuantization::None,
            }
        } else if self.ivf.is_some() || self.sealed.iter().any(|s| s.pq.is_some()) {
            nodedb_types::VectorIndexQuantization::Pq
        } else if self.sealed.iter().any(|s| s.sq8.is_some()) {
            nodedb_types::VectorIndexQuantization::Sq8
        } else {
            nodedb_types::VectorIndexQuantization::None
        };

        let index_type = match self.index_config.index_type {
            IndexType::HnswPq => nodedb_types::VectorIndexType::HnswPq,
            IndexType::IvfPq => nodedb_types::VectorIndexType::IvfPq,
            IndexType::Hnsw => nodedb_types::VectorIndexType::Hnsw,
        };

        let memory_bytes = self.index_memory_bytes();

        let disk_bytes: usize = self
            .sealed
            .iter()
            .filter_map(|s| s.mmap_vectors.as_ref().map(|m| m.file_size()))
            .sum();

        let metric_name = format!("{:?}", self.params.metric).to_lowercase();

        nodedb_types::VectorIndexStats {
            sealed_count: self.sealed.len(),
            building_count: self.building.len(),
            growing_vectors,
            sealed_vectors,
            live_count: self.live_count(),
            tombstone_count,
            tombstone_ratio,
            quantization,
            memory_bytes,
            disk_bytes,
            build_in_progress: !self.building.is_empty(),
            index_type,
            hnsw_m: self.params.m,
            hnsw_m0: self.params.m0,
            hnsw_ef_construction: self.params.ef_construction,
            metric: metric_name,
            dimensions: self.dim,
            seal_threshold: self.seal_threshold,
            mmap_segment_count: self.mmap_segment_count,
            // `arena_bytes` is populated by the Data Plane handler which
            // has access to `nodedb_mem::CollectionArenaHandle`. The field
            // is always `None` here; callers overwrite it after calling
            // `stats()` when a dedicated arena handle is available.
            arena_bytes: None,
            ivf: self.is_ivf().then(|| nodedb_types::VectorIvfStats {
                training_threshold: self.ivf_training_threshold(),
                trained: self.ivf.is_some(),
                trained_on: self.ivf.as_ref().map_or(0, |ivf| ivf.trained_on()),
                trained_at_ms: self.ivf.as_ref().map_or(0, |ivf| ivf.trained_at_ms()),
                indexed_vectors: ivf_vectors,
                cells: self.ivf.as_ref().map_or(0, |ivf| ivf.n_cells()),
                nprobe: self.index_config.ivf_nprobe,
            }),
            // The owning core fills this from its build queue.
            builds_queued: 0,
            builds_completed: self.builds_completed,
            builds_failed: self.builds_failed,
        }
    }
}

impl VectorCollection {
    /// Bytes the index holds in RAM: sealed HNSW graphs, SQ8 codes, the
    /// growing and building segments, and the IVF-PQ index.
    fn index_memory_bytes(&self) -> usize {
        let sealed_mem: usize = self.sealed.iter().map(|s| s.resident_bytes).sum();
        let growing_vectors = self.growing.len();
        let building_vectors: usize = self.building.iter().map(|s| s.flat.len()).sum();
        let growing_mem = growing_vectors * self.dim * std::mem::size_of::<f32>();
        let building_mem = building_vectors * self.dim * std::mem::size_of::<f32>();
        let ivf_mem = self.ivf.as_ref().map_or(0, |ivf| ivf.memory_bytes());
        sealed_mem + growing_mem + building_mem + ivf_mem
    }

    /// Bytes the collection holds resident, live and history: the index
    /// (ended nodes stay in it until compaction removes them) and the node
    /// spans. It reads counts the writes keep, never walks a graph, so a
    /// governor charge follows it at every write.
    pub fn resident_bytes(&self) -> usize {
        self.index_memory_bytes() + self.versions.memory_bytes()
    }
}
