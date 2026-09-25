// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for columnar predicate DML (`ColumnarOp::Update` /
//! `ColumnarOp::Delete`, autocommit path).
//!
//! `wal_append_if_write` appends a [`nodedb_types::columnar::ColumnarDmlWalRecord`]
//! BEFORE dispatch (see `control::server::wal_dispatch::core`), carrying the
//! predicate — collection, filters, and (for `Update`) field assignments —
//! rather than a row post-image, because the matching set is only known once
//! the Data Plane scans current state. Replay re-executes the exact predicate
//! through the exact same live handler (`execute_columnar_update` /
//! `execute_columnar_delete`) that ran on first application, so replay and
//! live semantics cannot diverge.
//!
//! ## Idempotence constraint, and the floor that satisfies it
//!
//! `Delete` (tombstone bit + PK-index removal) is idempotent. `Update` is
//! **not**: it is implemented as delete-old-PK + insert-new-row, so a second
//! application appends a duplicate row. Correctness rests entirely on this
//! function being invoked exactly once per record, in ascending LSN order,
//! against engine state that does not already contain it.
//!
//! Without `columnar_checkpoint`, that guarantee would hold trivially: state is
//! rebuilt from scratch on every restart. `ColumnarOp::Update` / `ColumnarOp::Delete`
//! target only the plain-columnar and spatial profiles
//! (`sql_plan_convert/dml/update_delete` routes
//! `EngineType::Columnar | EngineType::Spatial` here and nothing else), and
//! those profiles live in `CoreLoop::columnar_engines` /
//! `columnar_flushed_segments` — an in-memory `MutationEngine` plus in-memory
//! flushed segment bytes with no store behind them. Replay from empty state has
//! no durable partial state to double-apply against.
//!
//! `columnar_checkpoint` changes that. Restoring a generation means replay
//! starts from state that already contains every record at or below the
//! manifest's LSN, which is exactly the precondition this function's
//! non-idempotence depends on. [`ReplayFloors::columnar`] carries that LSN and
//! gates those records out; everything above it is absent from the restored
//! state and must still replay. Without the gate the checkpoint would duplicate
//! rows; without the replay above it the checkpoint would lose them.
//!
//! Note the gate is NOT the timeseries collection stamp
//! (`timeseries_checkpoint::stamp`): that stamp covers the separate
//! `ts_registries` / bucketed-partition machinery, which this op pair never
//! targets.

use super::core_loop::CoreLoop;
use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::ColumnarOp;
use nodedb_types::RlsWriteCheck;
use nodedb_types::Value;
use nodedb_types::columnar::ColumnarDmlWalRecord;

impl CoreLoop {
    /// Try to decode `payload` as a `columnar_dml` predicate-DML record and,
    /// if it is one, replay it. Returns `None` when the payload does not
    /// decode as this shape (caller falls back to `decode_batch_record`'s
    /// row-payload / legacy-tuple attempts), `Some(0)` when it decoded but was
    /// tombstoned, already covered by the restored columnar checkpoint, or the
    /// live re-execution reported a typed error (logged, never panics),
    /// `Some(1)` on successful replay.
    pub(in crate::data::executor) fn try_replay_columnar_predicate_dml(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
        truncate_floors: &super::wal_replay_columnar_truncate::TruncateFloors,
    ) -> Option<usize> {
        let record: ColumnarDmlWalRecord = zerompk::from_msgpack(payload).ok()?;
        if record.kind != "columnar_dml" {
            return None;
        }

        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &record.collection,
            record_lsn,
        ) {
            return Some(0);
        }
        // A later truncate removed every row this predicate could touch.
        if truncate_floors.covers_collection(
            database_id,
            TenantId::new(tenant_id),
            &record.collection,
            record_lsn,
        ) {
            return Some(0);
        }

        // Already folded into the restored checkpoint. Re-executing an `Update`
        // here would append a duplicate row (delete-old-PK + insert-new-row is
        // not idempotent), so the gate precedes the re-execution rather than
        // trying to detect the duplicate afterwards. Returns `Some(0)` and not
        // `None`: the record decoded as this shape, so the caller must not fall
        // through to its row-payload decoders and mis-classify it.
        if self.replay_watermark_skips(self.floors.replay_floors.columnar.covers(record_lsn)) {
            return Some(0);
        }

        let tid = TenantId::new(tenant_id);
        let vshard_id = VShardId::from_collection_in_database(database_id, &record.collection);

        // The task carries the real predicate even though today's handlers read
        // only `task.request.{database_id, tenant_id}`. A placeholder plan would
        // silently degrade to a no-op against an empty collection name the day a
        // handler starts reading the plan; on a once-per-record startup path the
        // clone costs nothing worth that risk.
        // No predicate here: this is crash-recovery replay of an
        // already-committed WAL record. The identity that wrote it is not
        // present at boot to resolve `$auth.*` against. A refused write never
        // reaches replay at all — its record is cancelled by a
        // `WriteAborted` marker before the refusal is acknowledged.
        let plan = if record.is_update {
            PhysicalPlan::Columnar(ColumnarOp::Update {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                filters: record.filters.clone(),
                updates: record.updates.clone(),
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            })
        } else {
            PhysicalPlan::Columnar(ColumnarOp::Delete {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                filters: record.filters.clone(),
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            })
        };
        let task = Self::replay_task(
            tid,
            database_id,
            vshard_id,
            plan,
            Some(Lsn::new(record_lsn)),
        );

        // Re-execute via the same live handlers the autocommit dispatch used.
        // Inside a committed-redo install the handlers capture the pre-image
        // of every row they change, so a later sub-record's failure rolls the
        // mutation back with the rest of the record.
        //
        // Replay carries no predicate. The policy decided these rows when the
        // record was written, and the identity that wrote it is not present at
        // boot to resolve `$auth.*` against. A refused write never reaches
        // replay: its record is cancelled by a `WriteAborted` marker before
        // the refusal is acknowledged.
        let replay_check = nodedb_types::RlsWriteCheck::already_decided_elsewhere();
        let mut undo = Vec::new();
        let recording = self.recording_redo_undo();
        let response = if record.is_update {
            self.execute_columnar_update(
                &task,
                &record.collection,
                &record.filters,
                &record.updates,
                &replay_check,
                recording.then_some(&mut undo),
            )
        } else {
            self.execute_columnar_delete(
                &task,
                &record.collection,
                &record.filters,
                &replay_check,
                recording.then_some(&mut undo),
            )
        };
        self.record_redo_undo(undo);

        if response.status != Status::Ok {
            self.replay_record_rejected(
                "columnar",
                record_lsn,
                response.error_code,
                &format!(
                    "columnar predicate DML replay on '{}' failed (update: {})",
                    record.collection, record.is_update
                ),
            );
            return Some(0);
        }
        Some(1)
    }

    /// Try to decode `payload` as a `columnar_resolved_dml` record (the
    /// resolved-row-set form of `ColumnarOp::ResolvedUpdate` /
    /// `ColumnarOp::ResolvedDelete`) and, if it is one, replay it.
    ///
    /// Returns `None` when the payload does not decode as this shape,
    /// `Some(0)` when it decoded but was tombstoned, already covered by the
    /// restored columnar checkpoint, failed to decode a row, or the live
    /// re-execution reported a typed error, `Some(1)` on successful replay.
    ///
    /// Replay carries `RlsWriteCheck::already_decided_elsewhere()`: the
    /// policy decided these rows when the record was written, and the
    /// identity that wrote it is not present at boot to resolve `$auth.*`
    /// against. A refused write never reaches replay — its record is
    /// cancelled by a `WriteAborted` marker before the refusal is
    /// acknowledged.
    ///
    /// ## The drift check at replay
    ///
    /// `execute_columnar_resolved_update` / `execute_columnar_resolved_delete`
    /// verify every shipped PK is still present before mutating anything, and
    /// answer `ErrorCode::OllpRetryRequired` on a miss. On the live write path
    /// that code asks the caller to re-resolve and retry. Replay has no
    /// caller to retry against — it runs once, against state rebuilt from the
    /// committed WAL prefix, which by construction already contains every row
    /// this record's rows were resolved against. So a miss here is never a
    /// normal drift condition; it is a genuine WAL/state inconsistency. This
    /// function does not treat that specially — it can't distinguish
    /// `OllpRetryRequired` from any other execution error at the response
    /// level — but it reports it exactly like the existing predicate-DML
    /// replay reports any failed re-execution: `warn!` with the LSN, then
    /// skip the record rather than aborting boot. Matching that established
    /// idiom (rather than inventing a second failure-reporting path in the
    /// same file) keeps one place operators check for a botched columnar WAL
    /// replay.
    pub(in crate::data::executor) fn try_replay_columnar_resolved_predicate_dml(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
        truncate_floors: &super::wal_replay_columnar_truncate::TruncateFloors,
    ) -> Option<usize> {
        let record: nodedb_types::columnar::ColumnarResolvedDmlWalRecord =
            zerompk::from_msgpack(payload).ok()?;
        if record.kind != "columnar_resolved_dml" {
            return None;
        }

        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &record.collection,
            record_lsn,
        ) {
            return Some(0);
        }
        // A later truncate removed every row this record names.
        if truncate_floors.covers_collection(
            database_id,
            TenantId::new(tenant_id),
            &record.collection,
            record_lsn,
        ) {
            return Some(0);
        }

        if self.replay_watermark_skips(self.floors.replay_floors.columnar.covers(record_lsn)) {
            return Some(0);
        }

        let tid = TenantId::new(tenant_id);
        let vshard_id = VShardId::from_collection_in_database(database_id, &record.collection);
        let replay_check = RlsWriteCheck::already_decided_elsewhere();

        let response = if record.is_update {
            let mut rows: Vec<(Value, Vec<Value>)> = Vec::with_capacity(record.rows.len());
            for wal_row in &record.rows {
                let pk = match nodedb_types::value_from_msgpack(&wal_row.pk_msgpack) {
                    Ok(v) => v,
                    Err(e) => {
                        self.replay_record_rejected(
                            "columnar",
                            record_lsn,
                            None,
                            &format!(
                                "columnar resolved-row-set DML on '{}': malformed PK: {e}",
                                record.collection
                            ),
                        );
                        return Some(0);
                    }
                };
                let new_row = match nodedb_types::value_from_msgpack(&wal_row.new_row_msgpack) {
                    Ok(Value::Array(arr)) => arr,
                    Ok(_) | Err(_) => {
                        self.replay_record_rejected(
                            "columnar",
                            record_lsn,
                            None,
                            &format!(
                                "columnar resolved-row-set DML on '{}': malformed post-image row",
                                record.collection
                            ),
                        );
                        return Some(0);
                    }
                };
                rows.push((pk, new_row));
            }
            let plan = PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                rows: rows.clone(),
                rls_write_check: replay_check.clone(),
            });
            let task = Self::replay_task(
                tid,
                database_id,
                vshard_id,
                plan,
                Some(Lsn::new(record_lsn)),
            );
            self.execute_columnar_resolved_update(
                &task,
                &record.collection,
                &rows,
                &replay_check,
                None,
            )
        } else {
            let mut pks: Vec<Value> = Vec::with_capacity(record.rows.len());
            for wal_row in &record.rows {
                let pk = match nodedb_types::value_from_msgpack(&wal_row.pk_msgpack) {
                    Ok(v) => v,
                    Err(e) => {
                        self.replay_record_rejected(
                            "columnar",
                            record_lsn,
                            None,
                            &format!(
                                "columnar resolved-row-set DML on '{}': malformed PK: {e}",
                                record.collection
                            ),
                        );
                        return Some(0);
                    }
                };
                pks.push(pk);
            }
            let plan = PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    record.collection.clone(),
                ),
                pks: pks.clone(),
                rls_write_check: replay_check.clone(),
            });
            let task = Self::replay_task(
                tid,
                database_id,
                vshard_id,
                plan,
                Some(Lsn::new(record_lsn)),
            );
            self.execute_columnar_resolved_delete(
                &task,
                &record.collection,
                &pks,
                &replay_check,
                None,
            )
        };

        if response.status != Status::Ok {
            self.replay_record_rejected(
                "columnar",
                record_lsn,
                response.error_code,
                &format!(
                    "columnar resolved-row-set DML replay on '{}' failed (update: {})",
                    record.collection, record.is_update
                ),
            );
            return Some(0);
        }
        Some(1)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Value};
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;

    const TID: u64 = 1;
    const COLLECTION: &str = "m";

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn insert_plan(rows: Vec<Value>) -> PhysicalPlan {
        // Must match what the planner emits (`rows_to_msgpack_array`): a plain
        // msgpack array of row maps. `zerompk::to_msgpack_vec` on a `Value`
        // would emit the tagged `[variant, payload]` enum encoding instead,
        // which the replay path decodes as zero rows.
        let payload = nodedb_types::value_to_msgpack(&Value::Array(rows))
            .expect("encode columnar insert payload");
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn row(id: i64, v: i64) -> Value {
        Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::Integer(id)),
            ("v".to_string(), Value::Integer(v)),
        ]))
    }

    fn eq_filter_bytes(field: &str, value: Value) -> Vec<u8> {
        let filters = vec![ScanFilter {
            field: field.to_string(),
            op: FilterOp::Eq,
            value,
            clauses: Vec::new(),
            expr: None,
        }];
        zerompk::to_msgpack_vec(&filters).expect("encode filters")
    }

    fn append_via_autocommit(plans: &[PhysicalPlan]) -> Vec<nodedb_wal::WalRecord> {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        for plan in plans {
            let outcome = wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "columnar predicate DML autocommit writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn scan_ids(core: &mut CoreLoop) -> Vec<(i64, i64)> {
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        );
        let engine = core
            .columnar_engines
            .get(&key)
            .expect("columnar engine present after replay");
        let schema = engine.schema();
        // Schema-inferred column order comes from `HashMap` iteration and is
        // not guaranteed stable, so look up each field by name rather than by
        // position.
        let id_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "id")
            .expect("schema has 'id' column");
        let v_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "v")
            .expect("schema has 'v' column");
        engine
            .scan_memtable_rows()
            .map(|row| {
                let id = match &row[id_idx] {
                    Value::Integer(n) => *n,
                    other => panic!("expected integer id, got {other:?}"),
                };
                let v = match &row[v_idx] {
                    Value::Integer(n) => *n,
                    other => panic!("expected integer v, got {other:?}"),
                };
                (id, v)
            })
            .collect()
    }

    #[test]
    fn autocommit_delete_produces_durable_lsn() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            filters: eq_filter_bytes("id", Value::Integer(2)),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &delete,
        )
        .expect("wal append delete");
        assert!(
            outcome.lsn.is_some(),
            "autocommit columnar predicate DELETE must be durably WAL-appended \
             (pre-fix: ColumnarOp::Delete fell through the catch-all and was never logged)"
        );
    }

    #[test]
    fn autocommit_update_produces_durable_lsn() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            filters: eq_filter_bytes("id", Value::Integer(1)),
            updates: vec![(
                "v".to_string(),
                // `execute_columnar_update` decodes each update value with the
                // plain reader (`value_from_msgpack`), matching the planner's
                // `sql_value_to_msgpack` output — not the tagged `Value` enum
                // encoding `zerompk::to_msgpack_vec(&Value)` would emit.
                nodedb_types::value_to_msgpack(&Value::Integer(999)).expect("encode"),
            )],
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &update,
        )
        .expect("wal append update");
        assert!(
            outcome.lsn.is_some(),
            "autocommit columnar predicate UPDATE must be durably WAL-appended \
             (pre-fix: ColumnarOp::Update fell through the catch-all and was never logged)"
        );
    }

    #[test]
    fn deleted_rows_do_not_reappear_after_replay_from_empty() {
        let insert = insert_plan(vec![row(1, 10), row(2, 20), row(3, 30)]);
        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            filters: eq_filter_bytes("id", Value::Integer(2)),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        });

        let records = append_via_autocommit(&[insert, delete]);

        let mut h = make_core();
        h.core
            .replay_timeseries_wal(&records, 1, &TombstoneSet::new());

        let mut rows = scan_ids(&mut h.core);
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, 10), (3, 30)],
            "deleted row (id=2) must NOT reappear after replay from empty \
             (pre-fix: the Delete was never WAL-logged, so replay only re-ran \
             the Insert and the deleted row resurrected)"
        );
    }

    #[test]
    fn updated_row_present_exactly_once_after_replay_from_empty() {
        let insert = insert_plan(vec![row(1, 10), row(2, 20)]);
        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            filters: eq_filter_bytes("id", Value::Integer(1)),
            // The planner emits raw msgpack primitives here
            // (`sql_value_to_msgpack`), not the tagged `Value` enum encoding.
            updates: vec![(
                "v".to_string(),
                nodedb_types::value_to_msgpack(&Value::Integer(999)).expect("encode"),
            )],
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        });

        let records = append_via_autocommit(&[insert, update]);

        let mut h = make_core();
        h.core
            .replay_timeseries_wal(&records, 1, &TombstoneSet::new());

        let mut rows = scan_ids(&mut h.core);
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, 999), (2, 20)],
            "updated row must carry the new value exactly once after replay \
             (pre-fix: the Update was never WAL-logged, so the value reverted; \
             a non-idempotent double-apply would instead duplicate the row)"
        );
    }
}
