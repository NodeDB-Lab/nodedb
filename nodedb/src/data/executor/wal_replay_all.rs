// SPDX-License-Identifier: BUSL-1.1

//! Unified WAL-replay orchestration for a data-plane core on startup.
//!
//! Both the production data-plane runtime (`crate::data::runtime`) and the
//! integration-test core-loop runner call this ONE method so the replay
//! sequence never drifts between them.
//!
//! ## Crash injection
//!
//! Recovery is only idempotent if a crash PART WAY THROUGH it is, so the
//! sequence carries fail points a crash harness can arm through
//! `NODEDB_FAILPOINTS` (feature `failpoints`; `abort` kills the process the way
//! a real crash would):
//!
//! * `replay::before_engine_passes` — nothing applied yet.
//! * `replay::between_engine_passes` — one engine's pass complete, the next
//!   not started.
//! * `replay::kv_mid_pass` — part way through a single engine's records.
//! * `replay::between_standalone_and_redo` — every engine arm done, the
//!   redo-only document / graph arms not yet run.
//! * `replay::before_sync_hwm_pass` — every engine arm is done but the sync
//!   idempotency gate has not been rebuilt.

use nodedb_wal::{TombstoneSet, WalRecord};
use tracing::{error, info};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay every WAL record class into this core's engines, in the exact
    /// order restart correctness requires. No-op when `records` is empty.
    pub fn replay_all_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &TombstoneSet,
    ) {
        if records.is_empty() {
            return;
        }
        let core_id = self.core_id;

        // Replay decides every record handed to it before this core serves a
        // request or writes a checkpoint: it applies the record, or a stamp,
        // a tombstone or an abort marker says it must not. Every one of them
        // is therefore at or below the outcome floor once replay ends. Raised
        // before the passes so the records replay applies are not collected
        // one by one into the applied set.
        if let Some(wal_end) = records.iter().map(|record| record.header.lsn).max() {
            self.floors
                .applied_prefix
                .seed_replayed_through(crate::types::Lsn::new(wal_end));
        }

        crate::fail_point!("replay::before_engine_passes");

        // Every engine-bearing record class — standalone (autocommit) records
        // and the sub-records of committed `TransactionRedo` groups alike —
        // replays here in ONE globally LSN-ordered pass. Splitting the two
        // classes into separate passes inverts LSN order for any key written
        // both inside a transaction and by a later autocommit, and because redo
        // ops are absolute overwrites the older post-image would win.
        //
        // Fatal on error: a redo group that cannot be reconstituted is a
        // committed transaction that cannot be applied, and continuing would
        // open the database with a hole in the replayed suffix.
        if let Err(e) = self.replay_engines_in_lsn_order(records, num_cores, tombstones) {
            error!(
                core_id,
                error = %e,
                "StartupError: committed-transaction redo replay failed — \
                 refusing to start with an incompletely replayed WAL"
            );
            std::process::exit(1);
        }

        crate::fail_point!("replay::before_sync_hwm_pass");

        // Reconstruct sync HWM maps from SyncSeqAdvance records so
        // post-restart deduplication is correct. Fatal on error —
        // a partially-recovered HWM is not safe to operate with.
        match crate::wal::replay::replay_sync_hwm_records(records) {
            Ok((maps, stats)) => {
                if stats.records > 0 {
                    info!(
                        core_id,
                        records = stats.records,
                        "sync HWM WAL replay complete"
                    );
                }
                self.install_sync_hwm_maps(maps);
            }
            Err(e) => {
                error!(
                    core_id,
                    error = %e,
                    "StartupError: sync HWM WAL replay failed — \
                     refusing to start with a partially-recovered idempotency gate"
                );
                std::process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use nodedb_wal::{TombstoneSet, WalRecord};

    use nodedb_types::Value;
    use nodedb_types::columnar::{
        COLUMNAR_IMAGE_KIND, ColumnDef, ColumnType, ColumnarImageWalRecord, ColumnarImageWalRow,
        ColumnarSchema,
    };

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
    use crate::types::{DatabaseId, Lsn, TenantId};
    use crate::wal::{RedoRecord, RedoSubRecord};

    const TID: u64 = 1;

    fn kv_put_record(lsn: u64, key: &[u8]) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: zerompk::to_msgpack_vec(&(
                "kv_put",
                "cache",
                key.to_vec(),
                b"v".to_vec(),
                0u64,
                None::<u64>,
                1u32,
            ))
            .expect("encode kv put"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build record")
    }

    /// Every record replay reads is decided when replay ends, so a checkpoint
    /// written afterwards names all of them through its prefix, and the
    /// applied set holds none of them.
    #[test]
    fn replay_seeds_the_applied_prefix_through_the_wal_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let records = vec![kv_put_record(40, b"a"), kv_put_record(77, b"b")];

        core.replay_all_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(core.floors.applied_prefix.outcome_floor(), Lsn::new(77));
        let stamp = core.floors.applied_prefix.stamp().expect("exact stamp");
        assert_eq!(stamp.prefix, 77);
        assert!(stamp.applied_above.is_empty());
    }

    fn redo_bytes(op: RedoSubRecord) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops: vec![op],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo")
    }

    fn redo_record(lsn: u64, redo: &[u8]) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: 0,
            payload: redo.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build redo record")
    }

    /// Install `redo` live at `lsn`, the way a committed record reaches a core.
    fn install(core: &mut CoreLoop, lsn: u64, redo: &[u8], collection: &str) {
        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(lsn));
        let response = core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo,
                collections: &[collection.to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(
            response.status,
            Status::Ok,
            "install at {lsn}: {response:?}"
        );
    }

    fn kv_put(key: &[u8], value: &[u8], surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                "kv_put",
                "cache",
                key.to_vec(),
                value.to_vec(),
                0u64,
                None::<u64>,
                surrogate,
            ))
            .expect("encode kv put"),
        }
    }

    fn columnar_row(id: i64, value: i64, surrogate: u32) -> RedoSubRecord {
        let mut image = std::collections::HashMap::new();
        image.insert("id".to_string(), Value::Integer(id));
        image.insert("v".to_string(), Value::Integer(value));
        let record = ColumnarImageWalRecord {
            kind: COLUMNAR_IMAGE_KIND.to_string(),
            collection: "m".to_string(),
            schema_bytes: Vec::new(),
            rows: vec![ColumnarImageWalRow {
                surrogate,
                prior_pk_msgpack: Vec::new(),
                image_msgpack: nodedb_types::value_to_msgpack(&Value::Object(image))
                    .expect("encode image"),
            }],
        };
        RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload: zerompk::to_msgpack_vec(&record).expect("encode image record"),
        }
    }

    fn columnar_ids(core: &CoreLoop) -> Vec<i64> {
        let key = (DatabaseId::DEFAULT, TenantId::new(TID), "m".to_string());
        let mut ids: Vec<i64> = core
            .columnar_engines
            .get(&key)
            .expect("engine")
            .scan_memtable_rows()
            .filter_map(|row| match row.first() {
                Some(Value::Integer(id)) => Some(*id),
                _ => None,
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Record B, at LSN 30, applies and a checkpoint is written while record A,
    /// at LSN 20, is still on its way. A applies after the checkpoint and the
    /// core stops before another one. Restart replay must apply A and skip B.
    #[test]
    fn a_kv_record_in_flight_at_a_checkpoint_replays_after_a_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = redo_bytes(kv_put(b"a", b"held", 1));
        let b = redo_bytes(kv_put(b"b", b"applied", 2));
        {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            core.floors
                .applied_prefix
                .observe_outcome_floor(Lsn::new(10));
            install(&mut core, 30, &b, "cache");
            core.checkpoint_kv_engines().expect("checkpoint");
            install(&mut core, 20, &a, "cache");
        }

        let (mut restored, _req, _resp) = make_core_with_dir(dir.path());
        restored.load_kv_checkpoints().expect("load");
        restored.replay_all_wal(
            &[redo_record(20, &a), redo_record(30, &b)],
            1,
            &TombstoneSet::new(),
        );
        let now = crate::engine::kv::current_ms();
        let expected: [(&[u8], &[u8]); 2] = [(b"a", b"held"), (b"b", b"applied")];
        for (key, value) in expected {
            assert_eq!(
                restored.kv_engine.get(0, TID, "cache", key, now).as_deref(),
                Some(value),
                "the replayed state equals the live one"
            );
        }
    }

    #[test]
    fn a_columnar_record_in_flight_at_a_checkpoint_replays_after_a_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = redo_bytes(columnar_row(1, 10, 1));
        let b = redo_bytes(columnar_row(2, 20, 2));
        let live = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            let schema = ColumnarSchema {
                columns: vec![
                    ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                    ColumnDef::required("v", ColumnType::Int64),
                ],
                version: 1,
            };
            core.columnar_engines.insert(
                (DatabaseId::DEFAULT, TenantId::new(TID), "m".to_string()),
                nodedb_columnar::MutationEngine::new("m".to_string(), schema),
            );
            core.floors
                .applied_prefix
                .observe_outcome_floor(Lsn::new(10));
            install(&mut core, 30, &b, "m");
            core.checkpoint_columnar_engines().expect("checkpoint");
            install(&mut core, 20, &a, "m");
            columnar_ids(&core)
        };
        assert_eq!(live, vec![1, 2]);

        let (mut restored, _req, _resp) = make_core_with_dir(dir.path());
        restored.load_columnar_checkpoints().expect("load");
        assert_eq!(
            columnar_ids(&restored),
            vec![2],
            "the checkpoint holds B and not A"
        );
        restored.replay_all_wal(
            &[redo_record(20, &a), redo_record(30, &b)],
            1,
            &TombstoneSet::new(),
        );
        assert_eq!(
            columnar_ids(&restored),
            live,
            "replay applies A once and never applies B again"
        );
    }
}
