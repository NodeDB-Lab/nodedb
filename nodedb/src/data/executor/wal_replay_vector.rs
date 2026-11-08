// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for vector engine startup recovery.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;

use super::core_loop::CoreLoop;
use super::wal_replay_vector_redo::RedoVectorWrite;

impl CoreLoop {
    /// Replay WAL vector records to rebuild in-memory HNSW indexes after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `VectorPut` and `VectorDelete` records, ignoring records
    /// for other vShards (each core only replays records routed to it).
    ///
    /// Records are replayed in LSN order (WAL guarantees this). For batch
    /// inserts, the payload contains multiple vectors in a single record.
    pub fn replay_vector_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        // Each record applies at the system ordinal its live apply stamped,
        // so a replayed node starts and ends where the live one did.
        let outer_ordinal = self.apply_scope.replicated_ordinal;
        for record in records {
            if self.replay_halted() {
                break;
            }
            self.apply_scope.replicated_ordinal = record.system_ordinal().or(outer_ordinal);
            let logical_type = record.logical_record_type();

            let record_type = RecordType::from_raw(logical_type);
            let is_vector_put = record_type == Some(RecordType::VectorPut);
            let is_vector_delete = record_type == Some(RecordType::VectorDelete);
            let is_vector_params = record_type == Some(RecordType::VectorParams);
            let is_index_drop = record_type == Some(RecordType::VectorIndexDrop);
            if !is_vector_put && !is_vector_delete && !is_vector_params && !is_index_drop {
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
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;
            let tombstones = tombstones.for_database(database_id);

            // A committed redo record carries no index DDL; left unclaimed,
            // the validate pass refuses a record that does.
            if (is_index_drop || is_vector_params) && self.applying_committed_redo() {
                continue;
            }

            // The restored vector checkpoint holds this put, delete or index
            // drop. Its stamp names exactly the records the live core applied
            // before the checkpoint, so every other record replays, in LSN
            // order, on top of it: a lower-LSN record still in flight at the
            // checkpoint applies here as it applied live, after the higher
            // ones. `VectorParams` is configuration the catalog seeds at boot,
            // not index content the checkpoint holds, so it always replays.
            // An index drop the checkpoint holds still removes that
            // configuration: the `VectorParams` it cancels replayed above
            // it, and left in place it lets a later document put rebuild the
            // index the user dropped. The index content stays as the
            // checkpoint restored it.
            if !is_vector_params && self.vector_replay_skips(record_lsn) {
                if is_index_drop {
                    self.forget_vector_index_config_record(database_id, tenant_id, &record.payload);
                }
                skipped += 1;
                continue;
            }

            if is_index_drop {
                // Applied in LSN order, so it wipes the params / puts that
                // preceded it and leaves a later re-CREATE to rebuild.
                self.restore_vector_index_drop_record(
                    database_id,
                    tenant_id,
                    &record.payload,
                    &mut skipped,
                );
                continue;
            }

            if is_vector_params {
                self.restore_vector_params_record(
                    database_id,
                    tenant_id,
                    record_lsn,
                    &record.payload,
                    &tombstones,
                    &mut skipped,
                );
                continue;
            }

            if is_vector_put {
                // A RESTORE's vector rows (`vector_restore`) open with their
                // own discriminator pair, so no insert shape decodes as one.
                if let Some(applied) = self.try_replay_vector_restore(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    &tombstones,
                ) {
                    if applied {
                        inserted += 1;
                    } else {
                        skipped += 1;
                    }
                    continue;
                }
                // A `VectorPut` payload has two shapes, one per writer
                // (`wal_dispatch::vector::encode`): the 6-element single
                // insert and the 4-element batch. Both carry every vector's
                // surrogate. The single-insert arm threads provenance into
                // `execute_vector_insert` so the idempotency gate runs on
                // replay exactly as it does on the live path. A payload of
                // any other shape stops replay.
                if let Ok((collection, vector, dim, field_name, surrogate_u32, provenance)) =
                    zerompk::from_msgpack::<(
                        String,
                        Vec<f32>,
                        usize,
                        String,
                        u32,
                        Option<nodedb_types::sync::wire::SyncProvenance>,
                    )>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim && provenance.is_none() {
                        // Sync input reaches the admitted validator below: a
                        // crash can retain its record before refusal markers.
                        // `dim` and the vector both come out of the SAME
                        // payload, so a disagreement is not a schema change the
                        // record predates — it is a record whose two halves
                        // cannot both be what the writer wrote.
                        self.replay_record_unapplied(
                            "vector",
                            "dim",
                            record_lsn,
                            &format!(
                                "record for '{collection}' declares dim {dim} but carries {} \
                                 components",
                                vector.len()
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    let insert_index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
                    if !self.redo_vector_prelude(
                        RedoVectorWrite {
                            index_key: &insert_index_key,
                            tid: tenant_id,
                            collection: &collection,
                            dim,
                            surrogates: &[surrogate],
                            ids: &[],
                            sidecars: false,
                        },
                        provenance.as_ref(),
                        record_lsn,
                    ) {
                        skipped += 1;
                        continue;
                    }
                    // Local replay rebinds by the carried surrogate.
                    let Some(vshard) = self.replay_vshard(
                        "vector",
                        record_lsn,
                        DatabaseId::new(database_id),
                        &collection,
                    ) else {
                        skipped += 1;
                        continue;
                    };
                    let Some(qualified) = self.replay_qualified(
                        "vector",
                        record_lsn,
                        DatabaseId::new(database_id),
                        collection.clone(),
                    ) else {
                        skipped += 1;
                        continue;
                    };
                    let task = Self::replay_vector_task(
                        nodedb_types::TenantId::new(tenant_id),
                        DatabaseId::new(database_id),
                        vshard,
                        PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::Insert {
                            collection: qualified,
                            vector: vector.clone(),
                            dim,
                            field_name: field_name.clone(),
                            surrogate,
                            pk_bytes: None,
                            provenance: provenance.clone(),
                        }),
                        Self::replay_record_lsn(record_lsn),
                    );
                    let response = self.execute_vector_insert(
                        crate::data::executor::handlers::vector::VectorInsertParams {
                            task: &task,
                            tid: tenant_id,
                            collection: &collection,
                            vector: &vector,
                            dim,
                            field_name: &field_name,
                            surrogate,
                            provenance: provenance.as_ref(),
                        },
                    );
                    if response.status != crate::bridge::envelope::Status::Ok {
                        // A crash before the funnel journals refusal markers
                        // leaves the original sync input. Its deterministic
                        // refusal rebuilds the consumed mark without a vector.
                        if matches!(
                            response.error_code.as_deref(),
                            Some(crate::bridge::envelope::ErrorCode::SyncRejected {
                                provenance: rejected, applied_seq, ..
                            }) if provenance.as_ref() == Some(rejected) && *applied_seq >= rejected.seq
                        ) {
                            skipped += 1;
                            continue;
                        }
                        self.replay_record_unapplied(
                            "vector",
                            "insert_handler",
                            record_lsn,
                            &format!(
                                "the vector insert handler rejected a committed write into \
                                 '{collection}': {:?}",
                                response.error_code
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    inserted += 1;
                } else if let Ok((collection, vectors, dim, surrogates_u32)) =
                    zerompk::from_msgpack::<(String, Vec<Vec<f32>>, usize, Vec<u32>)>(
                        &record.payload,
                    )
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if surrogates_u32.len() != vectors.len()
                        || surrogates_u32.contains(&nodedb_types::Surrogate::ZERO.as_u32())
                    {
                        self.replay_record_unapplied(
                            "vector",
                            "batch_identity",
                            record_lsn,
                            &format!(
                                "batch record for '{collection}' carries {} surrogates for {} \
                                 vectors, or an unbound one",
                                surrogates_u32.len(),
                                vectors.len()
                            ),
                        );
                        skipped += 1;
                        continue;
                    }
                    let surrogates: Vec<nodedb_types::Surrogate> = surrogates_u32
                        .into_iter()
                        .map(nodedb_types::Surrogate::new)
                        .collect();
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    if !self.redo_vector_prelude(
                        RedoVectorWrite {
                            index_key: &index_key,
                            tid: tenant_id,
                            collection: &collection,
                            dim,
                            surrogates: &surrogates,
                            ids: &[],
                            sidecars: false,
                        },
                        None,
                        record_lsn,
                    ) {
                        skipped += 1;
                        continue;
                    }
                    let ordinal = self.bitemporal_ordinal();
                    let index = match self.ensure_vector_collection(&index_key, &index_key, dim) {
                        Ok(index) => index,
                        Err(e) => {
                            self.replay_record_rejected(
                                "vector",
                                record_lsn,
                                None,
                                &format!("vector batch record for '{collection}': {e}"),
                            );
                            continue;
                        }
                    };
                    // Checked as a whole before any vector lands, so a record
                    // holding one vector of another width applies nothing.
                    if let Err(e) = index.insert_batch_at(&vectors, &surrogates, ordinal) {
                        self.replay_record_rejected(
                            "vector",
                            record_lsn,
                            None,
                            &format!("vector batch record for '{collection}': {e}"),
                        );
                        continue;
                    }
                    inserted += 1;
                } else {
                    self.replay_record_unapplied(
                        "vector",
                        "put_decode",
                        record_lsn,
                        "VectorPut payload matched none of its record shapes",
                    );
                    skipped += 1;
                }
            } else if is_vector_delete {
                if self.replay_vector_delete_record(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    &tombstones,
                ) {
                    deleted += 1;
                } else {
                    skipped += 1;
                }
            }
        }
        self.apply_scope.replicated_ordinal = outer_ordinal;

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
}

#[cfg(test)]
#[path = "wal_replay_vector_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wal_replay_vector_sync_tests.rs"]
mod sync_tests;
