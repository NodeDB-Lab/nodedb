//! WAL replay for CoreLoop startup recovery: vector + KV engines.

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
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;
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
                    use crate::engine::vector::distance::DistanceMetric;
                    let metric_enum = match metric.as_str() {
                        "l2" | "euclidean" => DistanceMetric::L2,
                        "cosine" => DistanceMetric::Cosine,
                        "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
                        "manhattan" | "l1" => DistanceMetric::Manhattan,
                        "chebyshev" | "linf" => DistanceMetric::Chebyshev,
                        "hamming" => DistanceMetric::Hamming,
                        "jaccard" => DistanceMetric::Jaccard,
                        "pearson" => DistanceMetric::Pearson,
                        _ => DistanceMetric::Cosine,
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
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
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
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    for vector in vectors {
                        index.insert(vector);
                    }
                    inserted += 1;
                }
            } else if is_vector_delete
                && let Ok((collection, vector_id)) =
                    rmp_serde::from_slice::<(String, u32)>(&record.payload)
            {
                let index_key = CoreLoop::vector_index_key(tenant_id, &collection, "");
                if let Some(index) = self.vector_collections.get_mut(&index_key) {
                    index.delete(vector_id);
                    deleted += 1;
                }
            }
        }

        if inserted > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                inserted,
                deleted,
                skipped,
                collections = self.vector_collections.len(),
                "WAL vector replay complete"
            );
        }
    }

    /// Replay WAL KV records to rebuild in-memory hash tables after crash.
    ///
    /// KV records use generic `RecordType::Put` and `RecordType::Delete` with
    /// a discriminator prefix in the MessagePack payload: `("kv_put", ...)`
    /// or `("kv_delete", ...)`.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Each core only replays records routed to its vShard.
    pub fn replay_kv_wal(&mut self, records: &[nodedb_wal::WalRecord], num_cores: usize) {
        use nodedb_wal::record::RecordType;

        let mut puts = 0usize;
        let mut deletes = 0usize;

        let now_ms = crate::engine::kv::current_ms();

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);
            let is_put = record_type == Some(RecordType::Put);
            let is_delete = record_type == Some(RecordType::Delete);
            if !is_put && !is_delete {
                continue;
            }

            // Route to the correct core by vShard.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tenant_id = record.header.tenant_id;

            // Try to detect KV records by discriminator prefix in the payload.
            if is_put {
                // kv_put: ("kv_put", collection, key, value, ttl_ms)
                if let Ok((disc, collection, key, value, ttl_ms)) =
                    rmp_serde::from_slice::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&record.payload)
                    && disc == "kv_put"
                {
                    self.kv_engine
                        .put(tenant_id, &collection, key, value, ttl_ms, now_ms);
                    puts += 1;
                    continue;
                }

                // kv_batch_put: ("kv_batch_put", collection, entries, ttl_ms)
                if let Ok((disc, collection, entries, ttl_ms)) =
                    rmp_serde::from_slice::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_batch_put"
                {
                    self.kv_engine
                        .batch_put(tenant_id, &collection, &entries, ttl_ms, now_ms);
                    puts += entries.len();
                    continue;
                }

                // kv_field_set: ("kv_field_set", collection, key, updates)
                // Replay as a full PUT (the value is the updated document).
                // We skip field_set replay because it requires the current value
                // which may not exist yet. The WAL should have a kv_put after.
            }

            if is_delete {
                // kv_delete: ("kv_delete", collection, keys)
                if let Ok((disc, collection, keys)) =
                    rmp_serde::from_slice::<(&str, String, Vec<Vec<u8>>)>(&record.payload)
                    && disc == "kv_delete"
                {
                    self.kv_engine.delete(tenant_id, &collection, &keys, now_ms);
                    deletes += keys.len();
                    continue;
                }

                // kv_truncate: ("kv_truncate", collection)
                if let Ok((disc, collection)) =
                    rmp_serde::from_slice::<(&str, String)>(&record.payload)
                    && disc == "kv_truncate"
                {
                    self.kv_engine.truncate(tenant_id, &collection);
                    deletes += 1;
                }
            }
        }

        if puts > 0 || deletes > 0 {
            tracing::info!(
                core = self.core_id,
                puts,
                deletes,
                collections = self.kv_engine.stats().collection_count,
                "WAL KV replay complete"
            );
        }
    }
}
