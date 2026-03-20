//! WAL vector replay for CoreLoop startup recovery.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay WAL vector records to rebuild in-memory HNSW indexes after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `VectorPut` and `VectorDelete` records, ignoring records
    /// for other vShards (each core only replays records routed to it).
    ///
    /// Records are replayed in LSN order (WAL guarantees this). For batch
    /// inserts, the payload contains multiple vectors in a single record.
    pub fn replay_vector_wal(&mut self, records: &[nodedb_wal::WalRecord], num_cores: usize) {
        use crate::engine::vector::hnsw::{HnswIndex, HnswParams};
        use nodedb_wal::record::RecordType;

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();

            let record_type = RecordType::from_raw(logical_type);
            let is_vector_put = record_type == Some(RecordType::VectorPut);
            let is_vector_delete = record_type == Some(RecordType::VectorDelete);
            let is_vector_params = record_type == Some(RecordType::VectorParams);
            if !is_vector_put && !is_vector_delete && !is_vector_params {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            let tenant_id = record.header.tenant_id;

            if is_vector_params {
                if let Ok((collection, m, ef_construction, metric)) =
                    rmp_serde::from_slice::<(String, usize, usize, String)>(&record.payload)
                {
                    let index_key = CoreLoop::vector_index_key(tenant_id, &collection, "");
                    let metric_enum = match metric.as_str() {
                        "l2" | "euclidean" => crate::engine::vector::distance::DistanceMetric::L2,
                        "cosine" => crate::engine::vector::distance::DistanceMetric::Cosine,
                        "inner_product" | "ip" | "dot" => {
                            crate::engine::vector::distance::DistanceMetric::InnerProduct
                        }
                        _ => crate::engine::vector::distance::DistanceMetric::Cosine,
                    };
                    let params = HnswParams {
                        m,
                        m0: m * 2,
                        ef_construction,
                        metric: metric_enum,
                    };
                    self.vector_params.insert(index_key, params);
                    tracing::debug!(
                        core = self.core_id,
                        %collection,
                        m,
                        ef_construction,
                        %metric,
                        "WAL replay: restored vector params"
                    );
                }
                continue;
            }

            if is_vector_put {
                if let Ok((collection, vector, dim)) =
                    rmp_serde::from_slice::<(String, Vec<f32>, usize)>(&record.payload)
                {
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(tenant_id, &collection, "");
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self.vector_indexes.entry(index_key).or_insert_with(|| {
                        HnswIndex::with_seed(dim, params, self.core_id as u64 + 1)
                    });
                    if index.dim() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            index_dim = index.dim(),
                            record_dim = dim,
                            "skipping WAL vector record: index dimension mismatch"
                        );
                        continue;
                    }
                    index.insert(vector);
                    inserted += 1;
                } else if let Ok((collection, vectors, dim)) =
                    rmp_serde::from_slice::<(String, Vec<Vec<f32>>, usize)>(&record.payload)
                {
                    let index_key = CoreLoop::vector_index_key(tenant_id, &collection, "");
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found for batch replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self.vector_indexes.entry(index_key).or_insert_with(|| {
                        HnswIndex::with_seed(dim, params, self.core_id as u64 + 1)
                    });
                    for vector in vectors {
                        index.insert(vector);
                    }
                    inserted += 1;
                }
            } else if is_vector_delete {
                if let Ok((collection, vector_id)) =
                    rmp_serde::from_slice::<(String, u32)>(&record.payload)
                {
                    let index_key = CoreLoop::vector_index_key(tenant_id, &collection, "");
                    if let Some(index) = self.vector_indexes.get_mut(&index_key) {
                        index.delete(vector_id);
                        deleted += 1;
                    }
                }
            }
        }

        if inserted > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                inserted,
                deleted,
                skipped,
                indexes = self.vector_indexes.len(),
                "WAL vector replay complete"
            );
        }
    }
}
