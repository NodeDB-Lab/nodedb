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
//! * `replay::between_standalone_and_redo` — every engine arm done but the
//!   graph edge arm.
//! * `replay::before_sync_hwm_pass` — every engine arm is done but the sync
//!   idempotency gate has not been rebuilt.

use nodedb_wal::{TombstoneSet, WalRecord};
use tracing::{error, info};

use super::core_loop::CoreLoop;
use super::core_loop::fail_stop::FailStopCause;

impl CoreLoop {
    /// Bound the versions restart replay could not rebuild, then forget the
    /// record stamps.
    ///
    /// A record an engine checkpoint already held applies no write, so its
    /// keys keep no version. Each vShard this core owns therefore holds every
    /// write through the newest of its WAL records, and a read older than
    /// that no longer validates against a row with no recorded version.
    fn settle_replayed_versions(&mut self, num_cores: usize) {
        let owned: Vec<_> = self
            .write_index
            .record_stamps
            .latest_by_vshard()
            .filter(|(vshard, _)| crate::types::core_for_vshard(*vshard, num_cores) == self.core_id)
            .collect();
        for (vshard, version) in owned {
            self.write_index.raise_installed(vshard, version);
        }
        self.write_index.record_stamps.clear();
    }

    /// Replay every WAL record class into this core's engines, in the exact
    /// order restart correctness requires. No-op when `records` is empty.
    ///
    /// `Err` when replay cannot bring the core to the state the WAL holds: a
    /// redo group that does not reconstitute, a committed record an arm
    /// cannot apply, or a sync HWM stream that does not decode. The core
    /// applied nothing past the failure, and the caller must not serve it.
    pub fn replay_all_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &TombstoneSet,
    ) -> crate::Result<()> {
        if records.is_empty() {
            return Ok(());
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

        // Every write a replay arm applies takes its record's vShard and the
        // log position of the data-group entry the record applies, so a
        // recovered write records the version its live apply did.
        self.write_index.record_stamps =
            super::core_loop::write_index::RecordStamps::from_wal(records);

        crate::fail_point!(self.fail_scope, "replay::before_engine_passes");

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
        let replayed = self.replay_engines_in_lsn_order(records, num_cores, tombstones);
        self.settle_replayed_versions(num_cores);
        if let Err(e) = replayed {
            error!(
                core_id,
                error = %e,
                "StartupError: committed-transaction redo replay failed — \
                 refusing to start with an incompletely replayed WAL"
            );
            self.fail_stop_core(FailStopCause::ReplayRecordUnapplied, &e.to_string());
            return Err(e);
        }
        // A committed record an arm cannot apply halts every arm there.
        if let Some(e) = self.replay_halt_error() {
            return Err(e);
        }

        crate::fail_point!(self.fail_scope, "replay::before_sync_hwm_pass");

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
                Ok(())
            }
            Err(e) => {
                error!(
                    core_id,
                    error = %e,
                    "StartupError: sync HWM WAL replay failed — \
                     refusing to start with a partially-recovered idempotency gate"
                );
                self.fail_stop_core(FailStopCause::ReplayRecordUnapplied, &e.to_string());
                Err(e)
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

    /// A KV put record the apply of the proposal keyed `apply_key` appended.
    fn keyed_kv_put_record(lsn: u64, key: &[u8], apply_key: u64) -> WalRecord {
        WalRecord::new_stamped(
            WalRecordArgs {
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
            },
            nodedb_wal::record::RecordStamp {
                apply_key,
                event_source: nodedb_wal::NO_EVENT_SOURCE,
                commit_hlc: 0,
            },
        )
        .expect("build record")
    }

    /// The `ChangePosition` marker of the entry at `(epoch, log_index)` whose
    /// proposal key is `apply_key`.
    fn position_marker(lsn: u64, apply_key: u64, epoch: u64, log_index: u64) -> WalRecord {
        let marker = crate::event::cdc::position::ChangePositionMarker {
            apply_key,
            position: crate::event::cdc::position::ReplicatedPosition {
                epoch,
                group_id: 9,
                log_index,
            },
        };
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::ChangePosition as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: marker.to_bytes().to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build marker")
    }

    fn kv_version(core: &CoreLoop, key: &[u8]) -> Option<nodedb_types::WriteVersion> {
        core.write_index
            .key_version(&crate::data::executor::core_loop::write_index::WriteKey {
                vshard: crate::types::VShardId::new(0),
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(TID),
                collection: Box::from("cache"),
                key: crate::types::KeyRepr::KvKey(Box::from(key)),
            })
    }

    /// Restart replay rebuilds each write's version from the data-group
    /// entry its record applied, the version every replica recorded live.
    /// A record no entry applied takes its WAL LSN, as it did live.
    #[test]
    fn replay_records_the_entry_position_each_record_applied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let records = vec![
            position_marker(10, 0xA1, 2, 70),
            keyed_kv_put_record(11, b"replicated", 0xA1),
            keyed_kv_put_record(12, b"local", 0),
        ];

        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .expect("replay");

        let entry = nodedb_types::WriteVersion::logged(2, 70);
        assert_eq!(kv_version(&core, b"replicated"), Some(entry));
        assert_eq!(
            kv_version(&core, b"local"),
            Some(nodedb_types::WriteVersion::local_after(entry, 12))
        );
        assert_eq!(
            core.write_index.latest(crate::types::VShardId::new(0)),
            nodedb_types::WriteVersion::local_after(entry, 12)
        );
        assert!(
            core.write_index.record_stamps.get(11).is_none(),
            "replay forgets the record stamps once it ends"
        );
    }

    /// A key whose record an engine checkpoint already held keeps no version
    /// after replay. A read older than the WAL's newest record then no longer
    /// validates against it.
    #[test]
    fn replay_bounds_the_versions_a_checkpoint_hid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let records = vec![
            position_marker(20, 0xB2, 0, 40),
            keyed_kv_put_record(21, b"k", 0xB2),
        ];
        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .expect("replay");

        let never_replayed = nodedb_types::calvin::ReadKeyIdent::Point(
            crate::types::KeyRepr::KvKey(Box::from(b"absent".as_slice())),
        );
        let valid = |read| {
            core.write_index.read_is_valid(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                crate::types::VShardId::new(0),
                "cache",
                &never_replayed,
                nodedb_types::WriteVersion::logged(0, read),
            )
        };
        assert!(!valid(39), "a read older than the WAL's newest record");
        assert!(valid(40), "a read at the WAL's newest record");
    }

    /// Every record replay reads is decided when replay ends, so a checkpoint
    /// written afterwards names all of them through its prefix, and the
    /// applied set holds none of them.
    #[test]
    fn replay_seeds_the_applied_prefix_through_the_wal_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let records = vec![kv_put_record(40, b"a"), kv_put_record(77, b"b")];

        core.replay_all_wal(&records, 1, &TombstoneSet::new())
            .expect("replay");

        assert_eq!(core.floors.applied_prefix.outcome_floor(), Lsn::new(77));
        let stamp = core.floors.applied_prefix.stamp().expect("exact stamp");
        assert_eq!(stamp.prefix, 77);
        assert!(stamp.applied_above.is_empty());
    }

    fn timeseries_record(lsn: u64, payload: Vec<u8>) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn,
            tenant_id: TID,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build record")
    }

    /// A committed record replay cannot apply halts replay without exiting
    /// the process: the core fail-stops, no later record of the pass
    /// applies, and the caller gets the error.
    #[test]
    fn an_unapplyable_record_halts_replay_and_returns_its_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let undecodable = zerompk::to_msgpack_vec(&(
            "timeseries".to_string(),
            "metrics".to_string(),
            b"metrics value=1i".to_vec(),
            None::<nodedb_types::sync::wire::SyncProvenance>,
            "ilp".to_string(),
        ))
        .expect("encode five-element tuple");
        let later = crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
            crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                collection: "later",
                payload: b"later value=2i",
                provenance: None,
                format: "ilp",
                default_timestamp_ms: 1_700_000_000_000,
            },
        )
        .expect("encode ingest record");
        let records = vec![
            timeseries_record(10, undecodable),
            timeseries_record(20, later),
        ];

        let error = core
            .replay_all_wal(&records, 1, &TombstoneSet::new())
            .expect_err("an unapplyable committed record fails replay");
        assert!(error.to_string().contains("lsn 10"), "{error}");
        assert!(core.is_fail_stopped(), "the core serves no holed state");
        assert!(
            !core
                .columnar_memtables
                .keys()
                .any(|(_, _, collection)| collection == "later"),
            "no record after the halt applies"
        );
    }

    fn redo_bytes(op: RedoSubRecord) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops: vec![op],
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
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
            .map(|row| row.expect("read"))
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
        restored
            .replay_all_wal(
                &[redo_record(20, &a), redo_record(30, &b)],
                1,
                &TombstoneSet::new(),
            )
            .expect("replay");
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
        restored
            .replay_all_wal(
                &[redo_record(20, &a), redo_record(30, &b)],
                1,
                &TombstoneSet::new(),
            )
            .expect("replay");
        assert_eq!(
            columnar_ids(&restored),
            live,
            "replay applies A once and never applies B again"
        );
    }
}
