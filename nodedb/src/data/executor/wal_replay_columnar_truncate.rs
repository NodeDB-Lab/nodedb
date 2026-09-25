// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the columnar-family `TRUNCATE` records
//! (`RecordType::ColumnarTruncate`, `RecordType::TimeseriesTruncate`) and
//! the per-collection floor they impose on the `TimeseriesBatch` records
//! written before them.
//!
//! Both records route through the live handlers, so replay leaves the
//! engines exactly as the live truncate did. A `TimeseriesBatch` record
//! below a collection's truncate LSN describes a row the truncate removed;
//! `replay_timeseries_wal` skips it through [`TruncateFloors`] rather than
//! ingesting it only to wipe it again at the truncate's LSN.
//!
//! A timeseries truncate that took effect before the crash is named by its
//! collection's replay stamp. Replay never re-applies it: it would remove
//! rows written after it. It imposes no floor either. The stamp names every
//! record it removed, and a record it does not name applied after the
//! truncate and survives it.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarTruncateWalRecord;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use super::timeseries_checkpoint::stamp::TsReplayStamp;
use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};

/// The key of one columnar-family collection on a core.
type CollectionKey = (DatabaseId, TenantId, String);

/// The truncates a replay pass must honour, read off the WAL before the
/// engine pass replays it.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct TruncateFloors {
    /// Highest LSN of a truncate that has not taken effect, per collection.
    /// Its replay removes every earlier record of the collection.
    floors: HashMap<CollectionKey, u64>,
    /// Timeseries truncates the collection stamp names and the pass has not
    /// reached yet. A sub-record at such an LSN precedes the truncate in its
    /// redo group, so the truncate removed its rows.
    named_ahead: HashMap<CollectionKey, Vec<u64>>,
}

/// The collection a truncate record names, or `None` for any other record.
fn truncate_key(record: &WalRecord) -> Option<CollectionKey> {
    let is_truncate = matches!(
        RecordType::from_raw(record.logical_record_type()),
        Some(RecordType::ColumnarTruncate) | Some(RecordType::TimeseriesTruncate)
    );
    if !is_truncate {
        return None;
    }
    let payload = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(&record.payload).ok()?;
    Some((
        DatabaseId::new(record.header.database_id),
        TenantId::new(record.header.tenant_id),
        payload.collection,
    ))
}

impl TruncateFloors {
    /// Collect the truncate records that route to `core_id`. `stamps` holds
    /// the timeseries collection stamps restored from disk.
    pub(in crate::data::executor) fn collect(
        records: &[WalRecord],
        num_cores: usize,
        core_id: usize,
        stamps: &HashMap<CollectionKey, TsReplayStamp>,
    ) -> Self {
        let mut this = Self::default();
        for record in records {
            let target_core = if num_cores > 0 {
                record.header.vshard_id as usize % num_cores
            } else {
                0
            };
            if target_core != core_id {
                continue;
            }
            let Some(key) = truncate_key(record) else {
                continue;
            };
            let lsn = record.header.lsn;
            let took_effect = stamps
                .get(&key)
                .is_some_and(|stamp| stamp.truncates.skips(lsn));
            if took_effect {
                this.named_ahead.entry(key).or_default().push(lsn);
            } else {
                let floor = this.floors.entry(key).or_insert(0u64);
                *floor = (*floor).max(lsn);
            }
        }
        this
    }

    /// The pass reached the truncate `record`: sub-records after it in its
    /// redo group apply.
    pub(in crate::data::executor) fn pass(&mut self, record: &WalRecord) {
        let Some(key) = truncate_key(record) else {
            return;
        };
        if let Some(ahead) = self.named_ahead.get_mut(&key) {
            ahead.retain(|lsn| *lsn != record.header.lsn);
        }
    }

    /// Whether a record at `lsn` for `key` is removed by a truncate of that
    /// collection.
    ///
    /// Strictly below a truncate that has not taken effect: a record at the
    /// truncate's own LSN is a sibling sub-record of the same transaction
    /// redo group. Replay applies a group's sub-records in the order the
    /// transaction wrote them, so a row staged before the truncate is removed
    /// by the truncate's own replay, and a row staged after it must apply. A
    /// truncate that took effect is not replayed, so a sibling ahead of it is
    /// skipped here instead.
    pub(in crate::data::executor) fn covers(&self, key: &CollectionKey, lsn: u64) -> bool {
        self.floors.get(key).is_some_and(|floor| lsn < *floor)
            || self
                .named_ahead
                .get(key)
                .is_some_and(|ahead| ahead.contains(&lsn))
    }

    /// Same as [`Self::covers`], keyed by the collection's parts. The key
    /// is only built when a truncate exists at all, so a WAL with none
    /// costs no allocation per record.
    pub(in crate::data::executor) fn covers_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        lsn: u64,
    ) -> bool {
        if self.floors.is_empty() && self.named_ahead.is_empty() {
            return false;
        }
        self.covers(&(database_id, tenant_id, collection.to_string()), lsn)
    }
}

impl CoreLoop {
    /// Replay one truncate record of either columnar-family engine. `record`
    /// is a `ColumnarTruncate` or `TimeseriesTruncate` record already routed
    /// to this core; the record type picks the engine.
    pub(in crate::data::executor) fn replay_truncate_record(
        &mut self,
        record: &WalRecord,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let database_id = DatabaseId::new(record.header.database_id);
        let columnar = RecordType::from_raw(record.logical_record_type())
            == Some(RecordType::ColumnarTruncate);
        if columnar {
            self.replay_columnar_truncate(
                &record.payload,
                record.header.tenant_id,
                database_id,
                record.header.lsn,
                tombstones,
            )
        } else {
            self.replay_timeseries_truncate(
                &record.payload,
                record.header.tenant_id,
                database_id,
                record.header.lsn,
                tombstones,
            )
        }
    }

    /// Replay one `ColumnarTruncate` record through the live handler. Gated
    /// by the restored columnar checkpoint: a generation stamped at or above
    /// this LSN already reflects the truncate, and re-applying it would wipe
    /// the rows written after it that the checkpoint holds.
    pub(in crate::data::executor) fn replay_columnar_truncate(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let record = match zerompk::from_msgpack::<ColumnarTruncateWalRecord>(payload) {
            Ok(record) => record,
            Err(e) => {
                self.replay_record_unapplied(
                    "columnar",
                    "truncate_decode",
                    record_lsn,
                    &format!("truncate record does not decode: {e}"),
                );
                return false;
            }
        };
        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &record.collection,
            record_lsn,
        ) {
            return false;
        }
        if self.replay_watermark_skips(self.floors.replay_floors.columnar.covers(record_lsn)) {
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        let task = Self::replay_task(
            TenantId::new(tenant_id),
            database_id,
            VShardId::from_collection_in_database(database_id, &record.collection),
            PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                restart_identity: false,
            }),
            Some(Lsn::new(record_lsn)),
        );
        let mut undo = Vec::new();
        let recording = self.recording_redo_undo();
        let response = self.execute_columnar_truncate(
            &task,
            &record.collection,
            recording.then_some(&mut undo),
        );
        self.record_redo_undo(undo);
        if response.status != Status::Ok {
            self.replay_record_rejected(
                "columnar",
                record_lsn,
                response.error_code,
                &format!(
                    "columnar truncate handler rejected truncate of '{}'",
                    record.collection
                ),
            );
            return false;
        }
        true
    }

    /// Replay one `TimeseriesTruncate` record through the live handler.
    /// Gated by the collection's replay stamp: a truncate it names already
    /// took effect, and re-applying it would remove rows written after it.
    pub(in crate::data::executor) fn replay_timeseries_truncate(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let record = match zerompk::from_msgpack::<ColumnarTruncateWalRecord>(payload) {
            Ok(record) => record,
            Err(e) => {
                self.replay_record_unapplied(
                    "timeseries",
                    "truncate_decode",
                    record_lsn,
                    &format!("truncate record does not decode: {e}"),
                );
                return false;
            }
        };
        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &record.collection,
            record_lsn,
        ) {
            return false;
        }
        let tid = TenantId::new(tenant_id);
        let key = (database_id, tid, record.collection.clone());
        if self.replay_watermark_skips(self.ts_truncate_named(&key, record_lsn)) {
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        let task = Self::replay_task(
            tid,
            database_id,
            VShardId::from_collection_in_database(database_id, &record.collection),
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                restart_identity: false,
            }),
            Some(Lsn::new(record_lsn)),
        );
        // The install keeps the partition directory aside until the whole
        // record landed, so a rollback can rename it back.
        let mut undo = Vec::new();
        let recording = self.recording_redo_undo();
        let response = self.execute_timeseries_truncate(
            &task,
            &record.collection,
            recording.then_some(&mut undo),
        );
        self.record_redo_undo(undo);
        if response.status != Status::Ok {
            self.replay_record_rejected(
                "timeseries",
                record_lsn,
                response.error_code,
                &format!(
                    "timeseries truncate handler rejected truncate of '{}'",
                    record.collection
                ),
            );
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::record::WalRecordArgs;

    fn truncate_record(record_type: RecordType, lsn: u64, vshard: u32, coll: &str) -> WalRecord {
        let payload = zerompk::to_msgpack_vec(&ColumnarTruncateWalRecord {
            collection: coll.to_string(),
        })
        .expect("encode");
        WalRecord::new(WalRecordArgs {
            record_type: record_type as u32,
            lsn,
            tenant_id: 1,
            vshard_id: vshard,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn a_row_at_the_truncates_own_lsn_is_not_covered() {
        let records = vec![truncate_record(RecordType::ColumnarTruncate, 9, 0, "c")];
        let floors = TruncateFloors::collect(&records, 1, 0, &HashMap::new());
        let c = (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string());
        assert!(floors.covers(&c, 8));
        assert!(
            !floors.covers(&c, 9),
            "a sibling sub-record of the truncate's redo group applies in group order"
        );
    }

    #[test]
    fn floors_keep_the_highest_truncate_lsn_per_collection_on_this_core() {
        let records = vec![
            truncate_record(RecordType::ColumnarTruncate, 5, 0, "c"),
            truncate_record(RecordType::ColumnarTruncate, 9, 0, "c"),
            truncate_record(RecordType::TimeseriesTruncate, 7, 0, "ts"),
            // Routes to another core.
            truncate_record(RecordType::TimeseriesTruncate, 50, 1, "ts"),
        ];
        let floors = TruncateFloors::collect(&records, 2, 0, &HashMap::new());
        let c = (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string());
        let ts = (DatabaseId::DEFAULT, TenantId::new(1), "ts".to_string());
        assert!(floors.covers(&c, 8));
        assert!(!floors.covers(&c, 10));
        assert!(floors.covers(&ts, 6));
        assert!(
            !floors.covers(&ts, 8),
            "the other core's truncate is not ours"
        );
        assert!(floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "c", 1));
        assert!(!floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "other", 1));
    }

    /// A truncate the collection stamp names took effect before the crash.
    /// It sets no floor, so a record below it that applied after it
    /// replays. A sibling sub-record at its own LSN is skipped until the pass
    /// reaches the truncate, and applies after it.
    #[test]
    fn a_named_truncate_sets_no_floor_and_skips_only_the_siblings_ahead_of_it() {
        let ts = (DatabaseId::DEFAULT, TenantId::new(1), "ts".to_string());
        let stamps = HashMap::from([(
            ts.clone(),
            TsReplayStamp {
                rows: crate::types::replay_stamp::ReplayStamp::default(),
                truncates: crate::types::replay_stamp::ReplayStamp::naming(20),
            },
        )]);
        let truncate = truncate_record(RecordType::TimeseriesTruncate, 20, 0, "ts");
        let mut floors = TruncateFloors::collect(std::slice::from_ref(&truncate), 1, 0, &stamps);

        assert!(
            !floors.covers(&ts, 15),
            "a record in flight at the truncate replays"
        );
        assert!(
            floors.covers(&ts, 20),
            "a sibling ahead of the truncate is skipped"
        );
        floors.pass(&truncate);
        assert!(
            !floors.covers(&ts, 20),
            "a sibling after the truncate applies"
        );

        let unnamed =
            TruncateFloors::collect(std::slice::from_ref(&truncate), 1, 0, &HashMap::new());
        assert!(
            unnamed.covers(&ts, 15),
            "a truncate that did not take effect floors"
        );
    }
}
