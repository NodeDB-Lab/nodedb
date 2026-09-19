// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the columnar-family `TRUNCATE` records
//! (`RecordType::ColumnarTruncate`, `RecordType::TimeseriesTruncate`) and
//! the per-collection floor they impose on the `TimeseriesBatch` records
//! written before them.
//!
//! Both records route through the live handlers, so replay leaves the
//! engines exactly as the live truncate did. A `TimeseriesBatch` record
//! at or below a collection's truncate LSN describes a row the truncate
//! removed; `replay_timeseries_wal` skips it through [`TruncateFloors`]
//! rather than ingesting it only to wipe it again at the truncate's LSN.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarTruncateWalRecord;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use super::core_loop::CoreLoop;
use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};

/// Highest truncate LSN per collection on one core, read off the WAL before
/// the engine pass replays it.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct TruncateFloors {
    floors: HashMap<(DatabaseId, TenantId, String), u64>,
}

impl TruncateFloors {
    /// Collect the truncate records that route to `core_id`.
    pub(in crate::data::executor) fn collect(
        records: &[WalRecord],
        num_cores: usize,
        core_id: usize,
    ) -> Self {
        let mut floors = HashMap::new();
        for record in records {
            let is_truncate = matches!(
                RecordType::from_raw(record.logical_record_type()),
                Some(RecordType::ColumnarTruncate) | Some(RecordType::TimeseriesTruncate)
            );
            if !is_truncate {
                continue;
            }
            let target_core = if num_cores > 0 {
                record.header.vshard_id as usize % num_cores
            } else {
                0
            };
            if target_core != core_id {
                continue;
            }
            let Ok(payload) = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(&record.payload)
            else {
                continue;
            };
            let key = (
                DatabaseId::new(record.header.database_id),
                TenantId::new(record.header.tenant_id),
                payload.collection,
            );
            let floor = floors.entry(key).or_insert(0u64);
            *floor = (*floor).max(record.header.lsn);
        }
        Self { floors }
    }

    /// Whether a record at `lsn` for `key` precedes a truncate of that
    /// collection and is therefore already removed.
    pub(in crate::data::executor) fn covers(
        &self,
        key: &(DatabaseId, TenantId, String),
        lsn: u64,
    ) -> bool {
        self.floors.get(key).is_some_and(|floor| lsn <= *floor)
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
        let Ok(record) = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(payload) else {
            return false;
        };
        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &record.collection,
            record_lsn,
        ) {
            return false;
        }
        if self.floors.replay_floors.columnar.covers(record_lsn) {
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
        let response = self.execute_columnar_truncate(&task, &record.collection, None);
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                collection = %record.collection,
                lsn = record_lsn,
                error = ?response.error_code,
                "WAL replay: columnar truncate handler returned error; skipping"
            );
            return false;
        }
        true
    }

    /// Replay one `TimeseriesTruncate` record through the live handler.
    /// Gated by the partitions on disk: a partition flushed at or above this
    /// LSN was written after the truncate, so the truncate already happened
    /// and re-applying it would remove rows written after it.
    pub(in crate::data::executor) fn replay_timeseries_truncate(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let Ok(record) = zerompk::from_msgpack::<ColumnarTruncateWalRecord>(payload) else {
            return false;
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
        let flushed_after_truncate = self.ts_registries.get(&key).is_some_and(|registry| {
            registry
                .iter()
                .any(|(_, e)| e.meta.last_flushed_wal_lsn >= record_lsn)
        });
        if flushed_after_truncate {
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
        let response = self.execute_timeseries_truncate(&task, &record.collection, None);
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                collection = %record.collection,
                lsn = record_lsn,
                error = ?response.error_code,
                "WAL replay: timeseries truncate handler returned error; skipping"
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
    fn floors_keep_the_highest_truncate_lsn_per_collection_on_this_core() {
        let records = vec![
            truncate_record(RecordType::ColumnarTruncate, 5, 0, "c"),
            truncate_record(RecordType::ColumnarTruncate, 9, 0, "c"),
            truncate_record(RecordType::TimeseriesTruncate, 7, 0, "ts"),
            // Routes to another core.
            truncate_record(RecordType::TimeseriesTruncate, 50, 1, "ts"),
        ];
        let floors = TruncateFloors::collect(&records, 2, 0);
        let c = (DatabaseId::DEFAULT, TenantId::new(1), "c".to_string());
        let ts = (DatabaseId::DEFAULT, TenantId::new(1), "ts".to_string());
        assert!(floors.covers(&c, 9));
        assert!(!floors.covers(&c, 10));
        assert!(floors.covers(&ts, 7));
        assert!(
            !floors.covers(&ts, 8),
            "the other core's truncate is not ours"
        );
        assert!(floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "c", 1));
        assert!(!floors.covers_collection(DatabaseId::DEFAULT, TenantId::new(1), "other", 1));
    }
}
