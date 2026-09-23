// SPDX-License-Identifier: BUSL-1.1

//! KV WAL replay: rebuilds in-memory hash tables after crash.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::wal_replay::kv_put::KvReplayRecord;

impl CoreLoop {
    /// Whether a KV WAL record must not be re-applied: tombstoned, or below
    /// the checkpoint floor (most records are deltas, so re-applying one
    /// already folded in double-counts it). Floor is engine-wide, not
    /// per-collection — a checkpoint publishes all collections at one LSN.
    pub(in crate::data::executor) fn skip_kv_replay_record(
        &self,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
        tenant_id: u64,
        collection: &str,
        record_lsn: u64,
    ) -> bool {
        tombstones.is_tombstoned(tenant_id, collection, record_lsn)
            || self.replay_watermark_skips(self.floors.replay_floors.kv.covers(record_lsn))
    }

    /// Replay WAL KV records to rebuild in-memory hash tables after crash.
    /// Records use generic `RecordType::Put`/`Delete` with a discriminator
    /// prefix in the payload. Called once at startup; each core replays
    /// only records routed to its vShard.
    pub fn replay_kv_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
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
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;
            let tombstones = &tombstones.for_database(database_id);

            crate::fail_point!("replay::kv_mid_pass");

            if is_put {
                // Absolute-overwrite puts — see `kv_put.rs` for the record
                // shapes and why the surrogate travels in the record.
                let kv_record = KvReplayRecord {
                    payload: &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                };

                if let Some(applied) = self.try_replay_kv_put(&kv_record, tombstones) {
                    puts += applied;
                    continue;
                }
                // A committed redo record carries only absolute `kv_put`
                // post-images. Every other shape is left unclaimed, so the
                // validate pass refuses the record before any arm writes.
                if self.applying_committed_redo() {
                    continue;
                }

                if let Some(applied) = self.try_replay_kv_batch_put(&kv_record, tombstones) {
                    puts += applied;
                    continue;
                }

                // kv_transfer (delta, not a post-image): re-executes
                // `compute_transfer` against current state in LSN order.
                if let Some(applied) = self.try_replay_kv_transfer(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_transfer_item: re-verifies source ownership and
                // re-executes the delete+insert pair.
                if let Some((item_puts, item_deletes)) = self.try_replay_kv_transfer_item(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += item_puts;
                    deletes += item_deletes;
                    continue;
                }

                // kv_cas / kv_incr_float / kv_getset (deltas): re-run the
                // same live computation against current state.
                if let Some(applied) = self.try_replay_kv_atomic(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_field_set (delta): re-runs the same field merge against
                // current state.
                if let Some(applied) = self.try_replay_kv_field_set(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_insert_on_conflict_update (delta): re-runs the same
                // `merge_kv_conflict_body` RMW merge against current state.
                if let Some(applied) = self.try_replay_kv_insert_on_conflict_update(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_register_index / kv_drop_index — see `wal_replay_kv_index.rs`.
                if let Some(applied) = self.try_replay_kv_index(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_register_sorted_index — see `wal_replay_kv_sorted_index.rs`.
                if let Some(applied) = self.try_replay_kv_register_sorted_index(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_expire — see `wal_replay_kv_expiry.rs`.
                if let Some(applied) = self.try_replay_kv_expire(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_persist — see `wal_replay_kv_expiry.rs`.
                if let Some(applied) = self.try_replay_kv_persist(
                    &record.payload,
                    tenant_id,
                    database_id,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_incr (delta): re-runs the same integer increment
                // against current state.
                if let Some(applied) = self.try_replay_kv_incr(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }

                // kv_predicate_update (delta): re-resolves the row set against
                // current state and re-runs the same merge.
                if let Some(applied) = self.try_replay_kv_predicate_update(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    puts += applied;
                    continue;
                }
            }

            if is_delete {
                // kv_delete: ("kv_delete", collection, keys)
                if let Ok((disc, collection, keys)) =
                    zerompk::from_msgpack::<(&str, String, Vec<Vec<u8>>)>(&record.payload)
                    && disc == "kv_delete"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    if self.claim_for_validation() {
                        continue;
                    }
                    if self.recording_redo_undo() {
                        let undo: Vec<UndoEntry> = keys
                            .iter()
                            .filter_map(|key| {
                                let prior = self.kv_engine.entry_image(
                                    database_id,
                                    tenant_id,
                                    &collection,
                                    key,
                                    now_ms,
                                )?;
                                Some(UndoEntry::KvDelete {
                                    collection: collection.clone(),
                                    key: key.clone(),
                                    prior,
                                })
                            })
                            .collect();
                        self.record_redo_undo(undo);
                    }
                    self.kv_engine
                        .delete(database_id, tenant_id, &collection, &keys, now_ms);
                    for deleted_key in &keys {
                        self.note_replay_write_lsn(
                            database_id,
                            tenant_id,
                            &collection,
                            Some(KeyRepr::KvKey(Box::from(deleted_key.as_slice()))),
                            record_lsn,
                        );
                    }
                    deletes += keys.len();
                    continue;
                }

                // kv_truncate: ("kv_truncate", collection)
                if let Ok((disc, collection)) =
                    zerompk::from_msgpack::<(&str, String)>(&record.payload)
                    && disc == "kv_truncate"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    if self.claim_for_validation() {
                        continue;
                    }
                    if self.recording_redo_undo() {
                        let rows =
                            self.kv_engine
                                .export_collection(database_id, tenant_id, &collection);
                        self.record_redo_undo([UndoEntry::KvTruncate {
                            collection: collection.clone(),
                            rows,
                        }]);
                    }
                    self.kv_engine.truncate(database_id, tenant_id, &collection);
                    self.note_replay_write_lsn(
                        database_id,
                        tenant_id,
                        &collection,
                        None,
                        record_lsn,
                    );
                    deletes += 1;
                    continue;
                }
                if self.applying_committed_redo() {
                    continue;
                }

                // kv_predicate_delete (delta): re-resolves the row set
                // against current state.
                if let Some(removed) = self.try_replay_kv_predicate_delete(
                    &record.payload,
                    tenant_id,
                    database_id,
                    now_ms,
                    record_lsn,
                    tombstones,
                ) {
                    deletes += removed;
                    continue;
                }

                // No tombstone gate: the record carries only `index_name`,
                // no collection to gate on.
                if let Some(applied) =
                    self.try_replay_kv_drop_sorted_index(&record.payload, tenant_id, database_id)
                {
                    deletes += applied;
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
