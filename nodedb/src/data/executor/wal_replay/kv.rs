// SPDX-License-Identifier: BUSL-1.1

//! KV WAL replay: rebuilds in-memory hash tables after crash.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;

impl CoreLoop {
    /// Whether a KV WAL record must NOT be re-applied during boot replay.
    ///
    /// Two independent reasons to skip, each a correctness bug if missed:
    ///
    /// * **Tombstoned** — the collection was dropped at or after this LSN, so
    ///   the record is shadowed by a delete that may itself have fallen out of
    ///   the live WAL.
    /// * **Below the checkpoint floor** — the record's effect is already inside
    ///   the KV checkpoint restored before replay. Skipping is mandatory rather
    ///   than merely wasteful: most KV records are DELTAS (`kv_incr`, `kv_cas`,
    ///   `kv_field_set`, `kv_transfer`, `kv_insert_on_conflict_update`) whose
    ///   replay re-executes against current state instead of overwriting it, so
    ///   re-applying one already folded into the checkpoint double-counts it.
    ///
    /// Records ABOVE the floor are safe to replay on top of the restored table:
    /// the checkpoint reproduces exactly the state that existed at its stamped
    /// LSN, so applying the remaining records in LSN order reaches the same
    /// state a full from-zero replay would.
    ///
    /// The floor is engine-wide rather than per-collection because a KV
    /// checkpoint publishes every collection at ONE LSN atomically — see
    /// `kv_checkpoint.rs` for why a per-collection floor is unsound for the
    /// records that span two collections.
    pub(in crate::data::executor) fn skip_kv_replay_record(
        &self,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
        tenant_id: u64,
        collection: &str,
        record_lsn: u64,
    ) -> bool {
        tombstones.is_tombstoned(tenant_id, collection, record_lsn)
            || self.floors.replay_floors.kv.covers(record_lsn)
    }

    /// Replay WAL KV records to rebuild in-memory hash tables after crash.
    ///
    /// KV records use generic `RecordType::Put` and `RecordType::Delete` with
    /// a discriminator prefix in the MessagePack payload: `("kv_put", ...)`
    /// or `("kv_delete", ...)`.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Each core only replays records routed to its vShard.
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

            // Try to detect KV records by discriminator prefix in the payload.
            if is_put {
                // kv_put with absolute expiry (redo sub-record):
                //   ("kv_put", collection, key, value, ttl_ms, expire_at_ms)
                //
                // zerompk enforces a strict array length, so this six-element
                // tuple decodes ONLY the extended shape and never the historical
                // five-element one below (and vice versa). When present, the
                // resolved absolute instant is installed verbatim instead of
                // recomputing `now_ms + ttl_ms`, which would drift the expiry
                // forward by the crash-to-restart delay.
                if let Ok((disc, collection, key, value, ttl_ms, expire_at_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_put"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.put_with_absolute_expiry(
                        crate::engine::kv::KvPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            key: &key,
                            value: &value,
                            ttl_ms,
                            now_ms,
                            surrogate: nodedb_types::Surrogate::ZERO,
                        },
                        expire_at_ms,
                    );
                    self.note_replay_write_lsn(
                        database_id,
                        tenant_id,
                        &collection,
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        record_lsn,
                    );
                    puts += 1;
                    continue;
                }

                // kv_put: ("kv_put", collection, key, value, ttl_ms)
                if let Ok((disc, collection, key, value, ttl_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(&record.payload)
                    && disc == "kv_put"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    self.kv_engine.put(crate::engine::kv::KvPutParams {
                        database_id,
                        tenant_id,
                        collection: &collection,
                        key: &key,
                        value: &value,
                        ttl_ms,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    });
                    self.note_replay_write_lsn(
                        database_id,
                        tenant_id,
                        &collection,
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        record_lsn,
                    );
                    puts += 1;
                    continue;
                }

                // kv_batch_put with absolute expiry (redo sub-record):
                //   ("kv_batch_put", collection, entries, ttl_ms, expire_at_ms)
                //
                // Same rationale as the six-element `kv_put` arm above: zerompk's
                // strict array-length check means this five-element tuple decodes
                // ONLY the extended shape, never the historical four-element one
                // below (and vice versa). The resolved absolute instant is
                // installed verbatim on every entry instead of recomputing
                // `now_ms + ttl_ms`, which would drift the expiry forward by the
                // crash-to-restart delay.
                if let Ok((disc, collection, entries, ttl_ms, expire_at_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_batch_put"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    let surrogates = vec![nodedb_types::Surrogate::ZERO; entries.len()];
                    self.kv_engine.batch_put_with_absolute_expiry(
                        crate::engine::kv::KvBatchPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            entries: &entries,
                            ttl_ms,
                            now_ms,
                            surrogates: &surrogates,
                        },
                        expire_at_ms,
                    );
                    for (entry_key, _entry_value) in &entries {
                        self.note_replay_write_lsn(
                            database_id,
                            tenant_id,
                            &collection,
                            Some(KeyRepr::KvKey(Box::from(entry_key.as_slice()))),
                            record_lsn,
                        );
                    }
                    puts += entries.len();
                    continue;
                }

                // kv_batch_put: ("kv_batch_put", collection, entries, ttl_ms)
                if let Ok((disc, collection, entries, ttl_ms)) =
                    zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(
                        &record.payload,
                    )
                    && disc == "kv_batch_put"
                {
                    if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
                        continue;
                    }
                    // Same as the `kv_put` replay arm above: this local WAL
                    // record does not carry the surrogate (it lives in the
                    // separately-durable, redb-backed surrogate catalog, not
                    // this per-core WAL), so replay passes `Surrogate::ZERO`
                    // for every entry, matching single-`Put` replay exactly.
                    let surrogates = vec![nodedb_types::Surrogate::ZERO; entries.len()];
                    self.kv_engine
                        .batch_put(crate::engine::kv::KvBatchPutParams {
                            database_id,
                            tenant_id,
                            collection: &collection,
                            entries: &entries,
                            ttl_ms,
                            now_ms,
                            surrogates: &surrogates,
                        });
                    for (entry_key, _entry_value) in &entries {
                        self.note_replay_write_lsn(
                            database_id,
                            tenant_id,
                            &collection,
                            Some(KeyRepr::KvKey(Box::from(entry_key.as_slice()))),
                            record_lsn,
                        );
                    }
                    puts += entries.len();
                    continue;
                }

                // kv_transfer (delta record, not a post-image): re-executes
                // `compute_transfer` against whatever source/dest values are
                // present in this core's KV engine at this point in LSN
                // order — see `wal_replay_kv_transfer.rs` for the full
                // rationale and the missing-source / compute-error policy.
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

                // kv_transfer_item (delta record): re-verifies source
                // ownership and re-executes the delete+insert pair — see
                // `wal_replay_kv_transfer.rs`.
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

                // kv_cas / kv_incr_float / kv_getset (delta records, not
                // post-images): re-run the same live computation against
                // whatever value is present in this core's KV engine at this
                // point in LSN order — see `wal_replay_kv_atomic.rs`.
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

                // kv_field_set (delta record, not a post-image): re-runs the
                // same field merge against whatever value is present in this
                // core's KV engine at this point in LSN order — see
                // `wal_replay_kv_field.rs`.
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

                // kv_insert_on_conflict_update (delta record, not a
                // post-image): re-runs the same `apply_on_conflict_updates`
                // RMW merge against whatever value is present in this core's
                // KV engine at this point in LSN order — see
                // `wal_replay_kv_insert_conflict.rs`.
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

                // kv_incr (delta record, not a post-image): re-runs the same
                // integer increment against whatever value is present in
                // this core's KV engine at this point in LSN order — see
                // `wal_replay_kv_incr.rs`.
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

                // kv_drop_sorted_index — see `wal_replay_kv_sorted_index.rs`.
                // No tombstone gate here: the record carries only
                // `index_name`, no collection to gate on. See that module's
                // doc comment for why this is safe.
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
