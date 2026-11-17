// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for timeseries records.
//!
//! On startup, replays `TimeseriesBatch` records into the per-core
//! columnar memtable. A record the collection's replay stamp names is in a
//! partition already, and is skipped
//! (`timeseries_checkpoint::stamp`). A committed-redo apply runs the same arm
//! without that skip (`replay_policy`).
//!
//! Restart replay walks the records in LSN order and sets the replay cursor
//! to the last LSN it passed. A flush or truncate during the pass stamps
//! through the cursor, never past a record replay has not reached.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::DatabaseId;

use super::timeseries_wal_decode::{ColumnarReplayArgs, TimeseriesReplayArgs};
use crate::wal::{DecodedBatchRecord, decode_batch_record};

impl CoreLoop {
    /// Apply the ended-partition sub-record `redo` of `record`. Returns
    /// whether it applied. A dropped collection skips it, and an install
    /// that fails reports the record unapplied.
    fn replay_ts_ended_partition(
        &mut self,
        record: &nodedb_wal::WalRecord,
        redo: crate::wal::TimeseriesEndedPartitionRedo,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let lsn = record.header.lsn;
        let database_id = DatabaseId::new(record.header.database_id);
        if tombstones.is_tombstoned(
            database_id.as_u64(),
            record.header.tenant_id,
            &redo.collection,
            lsn,
        ) {
            return false;
        }
        let key = (
            database_id,
            crate::types::TenantId::new(record.header.tenant_id),
            redo.collection,
        );
        if redo.meta.ended_at.is_none() {
            self.replay_record_unapplied(
                "timeseries",
                "ended_partition",
                lsn,
                &format!("ended partition of '{}' carries no ended_at", key.2),
            );
            return false;
        }
        // The validate pass writes nothing.
        if self.claim_for_validation() {
            return false;
        }
        let partition = super::timeseries::ended_install::EndedPartitionInstall {
            meta: redo.meta,
            files: redo.files,
        };
        // A committed-redo install records the undo that removes the
        // partition when the record rolls back.
        let mut undo = Vec::new();
        let undo_log = self.recording_redo_undo().then_some(&mut undo);
        let installed = self.install_ts_ended_partition(&key, partition, undo_log);
        self.record_redo_undo(undo);
        match installed {
            Ok(_) => true,
            Err(e) => {
                self.replay_record_unapplied("timeseries", "ended_partition", lsn, &e.to_string());
                false
            }
        }
    }

    /// Replay WAL timeseries records to rebuild in-memory memtable state after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `TimeseriesBatch` records and the columnar-family truncate
    /// records, ignoring records for other vShards. Skips every record its
    /// collection's replay stamp names, and never replays a record a later
    /// erasing truncate of its collection already removed.
    pub fn replay_timeseries_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        self.replay_timeseries_wal_with_origins(
            records,
            num_cores,
            tombstones,
            &crate::wal::redo::replay_origin::GroupedReplayOrigins::default(),
        );
    }

    pub(crate) fn replay_timeseries_wal_with_origins(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
        grouped_origins: &crate::wal::redo::replay_origin::GroupedReplayOrigins,
    ) {
        use crate::data::executor::wal_replay_columnar_truncate::TruncateFloors;
        use nodedb_wal::record::RecordType;

        let truncate_floors = TruncateFloors::collect(records, num_cores, self.core_id);
        let mut replayed = 0usize;
        let mut skipped = 0usize;
        // Records replayed below the highest LSN their collection's stamp
        // names: in flight when that stamp was written.
        let mut in_flight = 0usize;
        let restart = !self.applying_committed_redo();
        // The record whose sub-records the arm is applying. A record installs
        // into a collection as one unit (`group_flush`).
        let mut current = None;

        for (index, record) in records.iter().enumerate() {
            if self.replay_halted() {
                break;
            }
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_ts_batch = record_type == Some(RecordType::TimeseriesBatch);
            let is_truncate = matches!(
                record_type,
                Some(RecordType::ColumnarTruncate) | Some(RecordType::TimeseriesTruncate)
            );
            if !is_ts_batch && !is_truncate {
                continue;
            }

            // Route by vShard to the correct core.
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
            // Every record below this one is passed.
            if restart {
                self.ts_replay.cursor = Some(record.header.lsn.saturating_sub(1));
            }
            self.begin_ts_record(records, index, num_cores, &mut current);

            if is_truncate {
                if self.replay_truncate_record(record, tombstones) {
                    replayed += 1;
                } else {
                    skipped += 1;
                }
                continue;
            }

            // A RESTORE's ended timeseries partition opens with its own
            // discriminator pair. Its install is idempotent by the
            // partition's identity, so a restart replays it as a no-op.
            // A RESTORE's series catalog installs ahead of the rows it
            // serves, in log order.
            if let Some(redo) = crate::wal::TimeseriesSeriesCatalogRedo::decode(&record.payload) {
                if self.replay_ts_series_catalog(
                    DatabaseId::new(record.header.database_id),
                    record.header.tenant_id,
                    record.header.lsn,
                    redo,
                    tombstones,
                ) {
                    replayed += 1;
                } else {
                    skipped += 1;
                }
                continue;
            }
            if let Some(redo) = crate::wal::TimeseriesEndedPartitionRedo::decode(&record.payload) {
                if self.replay_ts_ended_partition(record, redo, tombstones) {
                    replayed += 1;
                } else {
                    skipped += 1;
                }
                continue;
            }

            // A RESTORE's columnar keys (`columnar_restore`) open with their
            // own discriminator pair, so no other shape decodes as one.
            if let Some(applied) = self.try_replay_columnar_restore(
                &record.payload,
                record.header.tenant_id,
                DatabaseId::new(record.header.database_id),
                record.header.lsn,
                tombstones,
                &truncate_floors,
            ) {
                replayed += applied;
                continue;
            }
            // A transaction's columnar row images (`columnar_image`) are a
            // disjoint map shape too, tried first for the same reason as the
            // DML shapes below.
            if let Some(applied) = self.try_replay_columnar_image(
                &record.payload,
                record.header.tenant_id,
                DatabaseId::new(record.header.database_id),
                record.header.lsn,
                tombstones,
                &truncate_floors,
            ) {
                replayed += applied;
                continue;
            }
            // A committed redo record carries columnar rows only as images,
            // and timeseries rows only as an ingest. Every other shape is
            // left unclaimed, so the validate pass refuses the record.
            let redo_apply = self.applying_committed_redo();

            // Predicate DML (`columnar_dml`) rides the same `TimeseriesBatch`
            // record type but a map shape disjoint from `ColumnarWalRecord`
            // and the ingest tuple (see `ColumnarDmlWalRecord`'s doc comment),
            // so it must be tried BEFORE `decode_batch_record` below. That
            // decoder refuses it, and the record would be dropped.
            if !redo_apply
                && let Some(applied) = self.try_replay_columnar_predicate_dml(
                    &record.payload,
                    record.header.tenant_id,
                    DatabaseId::new(record.header.database_id),
                    record.header.lsn,
                    tombstones,
                    &truncate_floors,
                )
            {
                replayed += applied;
                continue;
            }

            // Resolved-row-set DML (`columnar_resolved_dml`) is likewise a
            // disjoint map shape and must be tried before the generic decode
            // below for the same reason as the predicate-DML check above.
            if !redo_apply
                && let Some(applied) = self.try_replay_columnar_resolved_predicate_dml(
                    &record.payload,
                    record.header.tenant_id,
                    DatabaseId::new(record.header.database_id),
                    record.header.lsn,
                    tombstones,
                    &truncate_floors,
                )
            {
                replayed += applied;
                continue;
            }

            // Decode the record. A columnar insert is a map-shaped
            // `ColumnarWalRecord` carrying per-row surrogates. A timeseries
            // ingest is the seven-element tuple.
            // Records iterate in LSN order (guaranteed by the WAL segment
            // layout), so provenance-aware replay processes seq in order.
            let Ok(DecodedBatchRecord {
                kind,
                collection: raw_collection,
                payload,
                provenance: record_provenance,
                rls_write_check,
                format: record_format,
                surrogates: record_surrogates,
                conflict_policy,
                default_timestamp_ms,
                system_from,
                restored_versions,
            }) = decode_batch_record(&record.payload)
            else {
                self.replay_record_unapplied(
                    "timeseries",
                    "decode_batch",
                    record.header.lsn,
                    "TimeseriesBatch payload matched none of the columnar / timeseries \
                     record shapes",
                );
                skipped += 1;
                continue;
            };

            let tenant_id = record.header.tenant_id;
            let tid_id = crate::types::TenantId::new(tenant_id);
            let db_id = DatabaseId::new(record.header.database_id);
            let collection = raw_collection.as_str();
            let key = (db_id, tid_id, raw_collection.clone());

            let record_lsn = record.header.lsn;

            // Skip records for collections that were hard-deleted after
            // this write. Otherwise the purged memtable would resurrect.
            if tombstones.is_tombstoned(db_id.as_u64(), tenant_id, collection, record_lsn) {
                skipped += 1;
                continue;
            }
            // A later erasing truncate of this collection removed this row.
            if truncate_floors.covers(&key, record_lsn) {
                skipped += 1;
                continue;
            }

            // A partition holds this record. A
            // restart gate only: see `replay_policy`.
            if self.ts_replay_skips(&key, record_lsn) {
                skipped += 1;
                continue;
            }

            if redo_apply && kind == "columnar" {
                skipped += 1;
                continue;
            }
            if self.claim_for_validation() {
                continue;
            }
            let passed = self
                .ts_replay
                .stamps
                .get(&key)
                .is_some_and(|stamp| stamp.rows.highest() > record_lsn);

            let accepted = match kind.as_str() {
                // The columnar floor is consulted HERE and not above the `kind`
                // match, because it is the columnar engines' floor and this
                // record type is shared: a `timeseries` record routes to
                // `columnar_memtables` / `ts_registries`, which this checkpoint
                // does not cover and whose replay it must therefore not gate.
                // Gating one engine's records on another engine's durability
                // would drop the writes outright.
                "columnar"
                    if self.replay_watermark_skips(
                        self.floors.replay_floors.columnar.covers(record_lsn),
                    ) =>
                {
                    // Already folded into the restored generation. Replaying it
                    // would re-insert every row: an upsert masks the duplicate
                    // on a plain collection, but a `bitemporal=true` collection
                    // deliberately retains every version, so the duplicate
                    // becomes a second version visible to `AS OF` queries.
                    skipped += 1;
                    continue;
                }
                "columnar" => self.replay_columnar_payload(
                    tid_id,
                    db_id,
                    ColumnarReplayArgs {
                        collection,
                        payload: &payload,
                        record_lsn,
                        provenance: record_provenance,
                        surrogates: record_surrogates,
                        conflict_policy,
                        system_from,
                        restored_versions,
                        grouped: grouped_origins.contains(record_lsn),
                    },
                ),
                "timeseries" => self.replay_timeseries_payload(
                    tid_id,
                    db_id,
                    TimeseriesReplayArgs {
                        collection,
                        payload: &payload,
                        record_lsn,
                        provenance: record_provenance,
                        format: record_format.as_deref(),
                        default_timestamp_ms,
                        system_ordinal: record.system_ordinal(),
                        rls_write_check: &rls_write_check,
                        grouped: grouped_origins.contains(record_lsn),
                    },
                ),
                other => {
                    self.replay_record_rejected(
                        "timeseries",
                        record_lsn,
                        None,
                        &format!("unknown TimeseriesBatch WAL kind '{other}'"),
                    );
                    0
                }
            };
            if accepted == 0 {
                continue;
            }

            // Every row of the record landed; a flush from here on names it.
            // A committed-redo apply notes its record once the whole record
            // installed.
            if restart {
                self.note_ts_record_applied(record_lsn);
            }
            replayed += accepted;
            in_flight += usize::from(passed);
        }
        if let Some(record) = current {
            self.end_ts_record(record);
        }
        if restart {
            self.ts_replay.cursor = None;
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                skipped,
                in_flight,
                collections = self.columnar_memtables.len(),
                "WAL timeseries replay complete"
            );
        }
    }
}

#[cfg(test)]
#[path = "timeseries_wal_tests.rs"]
mod tests;
