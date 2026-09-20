// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV predicate DML records (`kv_predicate_update`,
//! `kv_predicate_delete`). The log carries only the predicate and
//! assignments, never a row set, so replay re-resolves the row set with the
//! same scan and merge the live handlers use.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::kv::field_compute::merge_field_updates;

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_predicate_update` record.
    ///
    /// Returns `None` when `payload` is not that record shape (the caller
    /// tries the next candidate arm), otherwise the number of rows written.
    pub(super) fn try_replay_kv_predicate_update(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, filters, updates) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<(String, Vec<u8>)>)>(payload)
                .ok()?;
        if disc != "kv_predicate_update" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let matched = match self.kv_predicate_matches(
            database_id,
            tenant_id,
            &collection,
            &filters,
            now_ms,
        ) {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    ?e,
                    "WAL kv_predicate_update replay: predicate scan failed, skipping record"
                );
                return Some(0);
            }
        };

        let mut written = 0usize;
        for (key, body) in matched {
            let computed = match merge_field_updates(&collection, Some(body.as_slice()), &updates) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        core = self.core_id,
                        %collection,
                        key = %String::from_utf8_lossy(&key),
                        ?e,
                        "WAL kv_predicate_update replay: field merge failed, skipping row"
                    );
                    continue;
                }
            };
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                value: &computed.new_value,
                ttl_ms: 0,
                now_ms,
                // The row exists, so `ZERO` leaves its bound identity alone —
                // the same surrogate the live merge preserved.
                surrogate: nodedb_types::Surrogate::ZERO,
            });
            written += 1;
        }
        // The key set is resolved at replay time rather than carried, so the
        // collection floor is the version this can record.
        self.note_replay_write_lsn(database_id, tenant_id, &collection, None, record_lsn);
        Some(written)
    }

    /// Decode + tombstone-gate + replay one `kv_predicate_delete` record.
    pub(super) fn try_replay_kv_predicate_delete(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, filters) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>)>(payload).ok()?;
        if disc != "kv_predicate_delete" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let matched = match self.kv_predicate_matches(
            database_id,
            tenant_id,
            &collection,
            &filters,
            now_ms,
        ) {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    %collection,
                    ?e,
                    "WAL kv_predicate_delete replay: predicate scan failed, skipping record"
                );
                return Some(0);
            }
        };
        let keys: Vec<Vec<u8>> = matched.into_iter().map(|(key, _body)| key).collect();
        let removed = self
            .kv_engine
            .delete(database_id, tenant_id, &collection, &keys, now_ms);
        self.note_replay_write_lsn(database_id, tenant_id, &collection, None, record_lsn);
        Some(removed)
    }
}
