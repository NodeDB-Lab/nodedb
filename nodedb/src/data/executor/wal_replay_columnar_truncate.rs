// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the columnar-family `TRUNCATE` records
//! (`RecordType::ColumnarTruncate`, `RecordType::TimeseriesTruncate`) and
//! the per-collection floor an erasing truncate imposes on the
//! `TimeseriesBatch` records written before it.
//!
//! Both records route through the live handlers, so replay leaves the
//! engines exactly as the live truncate did.
//!
//! A columnar truncate ends every row at its ordinal and keeps the rows as
//! history. A timeseries truncate ends every partition at its ordinal and
//! keeps the partitions as history. Replay applies every record below
//! either one, then the truncate, so the same versions end at the same
//! ordinals. Only a columnar truncate marked `erase_history` floors the
//! records below it: `replay_timeseries_wal` skips them through
//! [`TruncateFloors`] rather than ingesting rows the truncate wipes again.
//!
//! A timeseries truncate that took effect before the crash is named by its
//! collection's replay stamp. Replay never re-applies it: a record below its
//! LSN that applied after it sits in the memtable as a current row, and a
//! second apply would end it.

use crate::data::executor::handlers::columnar_truncate::TruncateHistory;
use std::collections::HashMap;

use nodedb_types::columnar::ColumnarTruncateWalRecord;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::types::{DatabaseId, Lsn, TenantId};
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};

/// The key of one columnar-family collection on a core.
type CollectionKey = (DatabaseId, TenantId, String);

/// The erasing truncates a replay pass must honour, read off the WAL before
/// the engine pass replays it.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct TruncateFloors {
    /// Highest LSN of an erasing truncate, per collection. Its replay
    /// removes every earlier record of the collection.
    floors: HashMap<CollectionKey, u64>,
}

/// The collection an erasing truncate record names: a columnar truncate the
/// record marks `erase_history`. `None` for any other record, a truncate
/// that ends its rows as history included.
fn erasing_truncate_target(record: &WalRecord) -> Option<CollectionKey> {
    let record_type = RecordType::from_raw(record.logical_record_type());
    if record_type != Some(RecordType::ColumnarTruncate) {
        return None;
    }
    let payload = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(&record.payload).ok()?;
    payload.erase_history.then(|| {
        (
            DatabaseId::new(record.header.database_id),
            TenantId::new(record.header.tenant_id),
            payload.collection,
        )
    })
}

impl TruncateFloors {
    /// Collect the erasing truncate records that route to `core_id`. A
    /// truncate that ends its rows imposes no floor: every earlier record
    /// replays, and the truncate's replay ends the rows as the live truncate
    /// did, so a read before it still sees them.
    pub(in crate::data::executor) fn collect(
        records: &[WalRecord],
        num_cores: usize,
        core_id: usize,
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
            let Some(key) = erasing_truncate_target(record) else {
                continue;
            };
            let floor = this.floors.entry(key).or_insert(0u64);
            *floor = (*floor).max(record.header.lsn);
        }
        this
    }

    /// Whether a record at `lsn` for `key` is removed by an erasing truncate
    /// of that collection.
    ///
    /// Strictly below the truncate: a record at the truncate's own LSN is a
    /// sibling sub-record of the same transaction redo group. Replay applies
    /// a group's sub-records in the order the transaction wrote them, so a
    /// row staged before the truncate is removed by the truncate's own
    /// replay, and a row staged after it must apply.
    pub(in crate::data::executor) fn covers(&self, key: &CollectionKey, lsn: u64) -> bool {
        self.floors.get(key).is_some_and(|floor| lsn < *floor)
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
        if self.floors.is_empty() {
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
            self.replay_timeseries_truncate(record, database_id, tombstones)
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
        let Some(vshard) =
            self.replay_vshard("columnar", record_lsn, database_id, &record.collection)
        else {
            return false;
        };
        let Some(qualified) = self.replay_qualified(
            "columnar",
            record_lsn,
            database_id,
            record.collection.clone(),
        ) else {
            return false;
        };
        let task = Self::replay_task(
            TenantId::new(tenant_id),
            database_id,
            vshard,
            PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: qualified,
                restart_identity: false,
                system_from: record.system_from,
                erase_history: record.erase_history,
            }),
            Some(Lsn::new(record_lsn)),
        );
        let mut undo = Vec::new();
        let recording = self.recording_redo_undo();
        // The stamp the live truncate's tombstones took.
        let response = self.execute_columnar_truncate(
            &task,
            &record.collection,
            TruncateHistory::of(record.erase_history, record.system_from),
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

    /// Replay one `TimeseriesTruncate` record through the live handler, at
    /// the ordinal the record carries. Gated by the collection's replay
    /// stamp: a truncate it names already took effect, and re-applying it
    /// would end rows that applied after it.
    pub(in crate::data::executor) fn replay_timeseries_truncate(
        &mut self,
        wal_record: &WalRecord,
        database_id: DatabaseId,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tenant_id = wal_record.header.tenant_id;
        let record_lsn = wal_record.header.lsn;
        let record = match zerompk::from_msgpack::<ColumnarTruncateWalRecord>(&wal_record.payload) {
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
        let Some(vshard) =
            self.replay_vshard("timeseries", record_lsn, database_id, &record.collection)
        else {
            return false;
        };
        let Some(qualified) = self.replay_qualified(
            "timeseries",
            record_lsn,
            database_id,
            record.collection.clone(),
        ) else {
            return false;
        };
        let task = Self::replay_task(
            tid,
            database_id,
            vshard,
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
                collection: qualified,
                restart_identity: false,
            }),
            Some(Lsn::new(record_lsn)),
        );
        // The install records what the truncate changed until the whole
        // record landed, so a rollback can reverse it.
        let mut undo = Vec::new();
        let recording = self.recording_redo_undo();
        let outer = self.enter_record_ordinal(wal_record);
        let response = self.execute_timeseries_truncate(
            &task,
            &record.collection,
            recording.then_some(&mut undo),
        );
        self.apply_scope.replicated_ordinal = outer;
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
        erasing_truncate_record(record_type, lsn, vshard, coll, false)
    }

    /// A truncate record that names `erase_history`. Only a columnar
    /// truncate reads the flag.
    fn erasing_truncate_record(
        record_type: RecordType,
        lsn: u64,
        vshard: u32,
        coll: &str,
        erase_history: bool,
    ) -> WalRecord {
        let payload = zerompk::to_msgpack_vec(&ColumnarTruncateWalRecord {
            collection: coll.to_string(),
            system_from: None,
            erase_history,
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
        let records = vec![erasing_truncate_record(
            RecordType::ColumnarTruncate,
            9,
            0,
            "c",
            true,
        )];
        let floors = TruncateFloors::collect(&records, 1, 0);
        let c = (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string());
        assert!(floors.covers(&c, 8));
        assert!(
            !floors.covers(&c, 9),
            "a sibling sub-record of the truncate's redo group applies in group order"
        );
    }

    /// A columnar truncate ends every earlier row, and a timeseries truncate
    /// every earlier partition. Both keep them as history, so neither
    /// imposes a floor: the rows replay, and the truncate's replay ends
    /// them. Only a columnar truncate that erases history floors.
    #[test]
    fn only_an_erasing_truncate_imposes_a_floor() {
        let records = vec![
            truncate_record(RecordType::ColumnarTruncate, 9, 0, "c"),
            erasing_truncate_record(RecordType::ColumnarTruncate, 9, 0, "erased", true),
            truncate_record(RecordType::TimeseriesTruncate, 9, 0, "ts"),
        ];
        let floors = TruncateFloors::collect(&records, 1, 0);
        let key = |name: &str| (DatabaseId::DEFAULT, TenantId::new(1), name.to_string());
        assert!(!floors.covers(&key("c"), 8));
        assert!(floors.covers(&key("erased"), 8));
        assert!(!floors.covers(&key("ts"), 8));
    }

    #[test]
    fn floors_keep_the_highest_truncate_lsn_per_collection_on_this_core() {
        let records = vec![
            erasing_truncate_record(RecordType::ColumnarTruncate, 5, 0, "c", true),
            erasing_truncate_record(RecordType::ColumnarTruncate, 9, 0, "c", true),
            erasing_truncate_record(RecordType::ColumnarTruncate, 7, 0, "e", true),
            // Routes to another core.
            erasing_truncate_record(RecordType::ColumnarTruncate, 50, 1, "e", true),
        ];
        let floors = TruncateFloors::collect(&records, 2, 0);
        let c = (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string());
        let e = (DatabaseId::DEFAULT, TenantId::new(1), "e".to_string());
        assert!(floors.covers(&c, 8));
        assert!(!floors.covers(&c, 10));
        assert!(floors.covers(&e, 6));
        assert!(
            !floors.covers(&e, 8),
            "the other core's truncate is not ours"
        );
        assert!(floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "c", 1));
        assert!(!floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "other", 1));
    }

    /// A `TimeseriesBatch` record carrying one columnar insert of
    /// `{id, v}` stamped `system_from`, at `lsn`.
    fn insert_record(lsn: u64, id: i64, v: i64, system_from: i64) -> WalRecord {
        use nodedb_types::Value;
        let row = Value::Object(HashMap::from([
            ("id".to_string(), Value::Integer(id)),
            ("v".to_string(), Value::Integer(v)),
        ]));
        let rec = nodedb_types::columnar::ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: "c".to_string(),
            payload: nodedb_types::value_to_msgpack(&Value::Array(vec![row])).expect("encode row"),
            provenance: None,
            surrogates: Vec::new(),
            conflict_policy: Vec::new(),
            system_from: Some(system_from),
            restored_versions: Vec::new(),
        };
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: zerompk::to_msgpack_vec(&rec).expect("encode record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// Restart replay with no checkpoint reproduces the live truncate: the
    /// rows written before it replay and end at its ordinal, so a read
    /// before the truncate still sees them, and a read after sees only the
    /// rows written after it.
    #[test]
    fn replay_keeps_the_history_a_truncate_ended() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());
        let truncate = {
            let payload = zerompk::to_msgpack_vec(&ColumnarTruncateWalRecord {
                collection: "c".to_string(),
                system_from: Some(200),
                erase_history: false,
            })
            .expect("encode");
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::ColumnarTruncate as u32,
                lsn: 30,
                tenant_id: 1,
                vshard_id: 0,
                database_id: 0,
                payload,
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("wal record")
        };
        let records = vec![
            insert_record(10, 1, 10, 100),
            insert_record(20, 2, 20, 110),
            truncate,
            insert_record(40, 3, 30, 300),
        ];
        core.replay_timeseries_wal(&records, 1, &nodedb_wal::TombstoneSet::new());

        let engine = core
            .columnar_engines
            .get(&(DatabaseId::DEFAULT, TenantId::new(1), "c".to_string()))
            .expect("engine");
        let id_idx = engine
            .schema()
            .columns
            .iter()
            .position(|c| c.name == "id")
            .expect("id column");
        let ids = |cut| {
            let mut ids: Vec<i64> = engine
                .scan_memtable_versions(cut)
                .map(|row| match row.expect("read").2.get(id_idx) {
                    Some(nodedb_types::Value::Integer(id)) => *id,
                    other => panic!("no id cell: {other:?}"),
                })
                .collect();
            ids.sort_unstable();
            ids
        };
        use nodedb_columnar::VersionCut;
        assert_eq!(ids(VersionCut::At(150)), vec![1, 2], "before the truncate");
        assert_eq!(ids(VersionCut::At(105)), vec![1]);
        assert!(ids(VersionCut::At(250)).is_empty(), "after the truncate");
        assert_eq!(ids(VersionCut::Current), vec![3]);
        assert_eq!(ids(VersionCut::All), vec![1, 2, 3], "the ended rows stay");
    }
}
