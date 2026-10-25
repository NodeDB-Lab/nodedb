// SPDX-License-Identifier: BUSL-1.1

//! The `_ts_system` ordinal each row of a bitemporal columnar write stamps,
//! by the rule every bitemporal write stamps by (`KeyStamps`).
//!
//! An insert row starts from one ordinal of the insert's run: the Calvin
//! transaction's ordinal, else `system_from + row`, else a fresh ordinal.
//! An UPDATE's new versions and a DELETE's tombstones start from one ordinal
//! for the statement. Each row then rises above its keys' newest versions.
//! For an UPDATE those keys are the key it supersedes and, for a
//! key-changing update, the new key. A `_ts_system` the client sends or the
//! SET list assigns never decides the stamp.
//!
//! A RESTORE re-issue (`ColumnarInsertIntent::Restore`) installs the stamp
//! each row carries, the stamp its original write took.
//!
//! A key's newest stored version is its bound row or, for a key a delete
//! closed, the tombstone, whichever is newer.

use std::collections::HashMap;

use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::bitemporal_time::KeyStamps;

/// What one row's stamp is decided from.
pub(super) struct RowStampInput<'a> {
    pub engine_key: &'a (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub schema: &'a ColumnarSchema,
    pub intent: ColumnarInsertIntent,
    /// The first ordinal of the insert's carried run.
    pub system_from: Option<i64>,
    pub row_idx: usize,
    /// The row as the client or the restore sent it.
    pub incoming: &'a HashMap<String, Value>,
    /// The row in schema order, its `_ts_system` cell not yet final.
    pub values: &'a [Value],
}

impl CoreLoop {
    /// The `_ts_system` ordinal row `input.row_idx` of a bitemporal columnar
    /// insert stamps. The stamp folds into the shared clock.
    ///
    /// `Err` when a restored row carries no integer `_ts_system`, the
    /// collection has no engine, or the key's stored rows do not read.
    pub(super) fn columnar_row_stamp(
        &self,
        input: RowStampInput<'_>,
        batch: &mut KeyStamps,
    ) -> crate::Result<i64> {
        let RowStampInput {
            engine_key,
            schema,
            intent,
            system_from,
            row_idx,
            incoming,
            values,
        } = input;
        match intent {
            ColumnarInsertIntent::Restore => {
                let Some(Value::Integer(stamp)) = incoming.get(TS_SYSTEM) else {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "restore into bitemporal columnar '{}': row {row_idx} carries no \
                             integer {TS_SYSTEM}; a restored version keeps the system time its \
                             write stamped",
                            engine_key.2
                        ),
                    });
                };
                self.observe_bitemporal_ordinal(*stamp);
                Ok(*stamp)
            }
            ColumnarInsertIntent::Insert
            | ColumnarInsertIntent::InsertIfAbsent
            | ColumnarInsertIntent::Put
            | ColumnarInsertIntent::InsertUnique => {
                let engine = self
                    .columnar_engines
                    .get(engine_key)
                    .ok_or_else(|| missing_engine(engine_key))?;
                let pk_bytes = engine.encode_pk_from_row(values)?;
                let base = self.columnar_system_ordinal(system_from, row_idx);
                let stamp = batch.stamp_row(base, vec![pk_bytes], |pk| {
                    self.newest_stored_stamp(engine_key, schema, pk)
                })?;
                self.observe_bitemporal_ordinal(stamp);
                Ok(stamp)
            }
        }
    }
}

impl CoreLoop {
    /// Stamp the new version of every row a bitemporal columnar UPDATE
    /// writes: `(old primary key, post-image)` pairs in apply order. A
    /// collection that is not bitemporal stamps nothing.
    ///
    /// The statement's base is the Calvin transaction's ordinal, else the
    /// carried `system_from` (folded into the shared clock), else a fresh
    /// ordinal. Each row rises above its old and new keys' newest versions
    /// ([`KeyStamps`]). Every stamp folds.
    ///
    /// `Err` when the collection has no engine, a stored row does not read,
    /// or a post-image has no key.
    pub(in crate::data::executor) fn stamp_columnar_update_rows(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        schema: &ColumnarSchema,
        rows: &mut [(Value, Vec<Value>)],
        system_from: Option<i64>,
    ) -> crate::Result<()> {
        let Some(ts_idx) = schema.ts_system_idx() else {
            return Ok(());
        };
        let engine = self
            .columnar_engines
            .get(engine_key)
            .ok_or_else(|| missing_engine(engine_key))?;
        let base = self.columnar_system_ordinal(system_from, 0);
        let mut batch = KeyStamps::default();
        for (old_pk, new_row) in rows.iter_mut() {
            let old_bytes = nodedb_columnar::pk_index::encode_pk(old_pk);
            let new_bytes = engine.encode_pk_from_row(new_row)?;
            let stamp = batch.stamp_row(base, vec![old_bytes, new_bytes], |pk| {
                self.newest_stored_stamp(engine_key, schema, pk)
            })?;
            self.observe_bitemporal_ordinal(stamp);
            if let Some(cell) = new_row.get_mut(ts_idx) {
                *cell = Value::Integer(stamp);
            }
        }
        Ok(())
    }
}

/// The error of a bitemporal columnar write whose collection has no engine
/// on this core.
fn missing_engine(
    engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "bitemporal columnar '{}': no engine on this core to stamp _ts_system against",
            engine_key.2
        ),
    }
}

impl CoreLoop {
    /// The stamp of the newest stored version of `pk_bytes`: its bound row,
    /// or the tombstone that closed it, whichever is newer. `None` for a key
    /// with neither.
    ///
    /// `Err` when either row does not read.
    pub(in crate::data::executor) fn newest_stored_stamp(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        schema: &ColumnarSchema,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<i64>> {
        let bound = self
            .read_columnar_row_by_pk(engine_key, pk_bytes)?
            .and_then(|row| stored_stamp(schema, &row));
        let closed = self
            .read_columnar_closed_row(engine_key, pk_bytes)?
            .and_then(|row| stored_stamp(schema, &row));
        Ok(bound.max(closed))
    }
}

/// The `_ts_system` stamp a stored row carries.
pub(in crate::data::executor) fn stored_stamp(
    schema: &ColumnarSchema,
    row: &[Value],
) -> Option<i64> {
    match row.get(schema.ts_system_idx()?) {
        Some(Value::Integer(stamp)) => Some(*stamp),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
    use nodedb_types::{RlsWriteCheck, Value};

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::columnar_mutation::ColumnarDml;
    use crate::data::executor::handlers::columnar_write::ColumnarInsertParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, TenantId, VShardId};

    const TID: u64 = 1;
    const COLLECTION: &str = "stamped";

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        )
    }

    /// A core whose `COLLECTION` is bitemporal. The bridge ends stay alive
    /// beside it.
    fn bitemporal_core(
        dir: &std::path::Path,
    ) -> (CoreLoop, impl Sized + use<>, impl Sized + use<>) {
        let (mut core, tx, rx) = make_core_with_dir(dir);
        let mut config = crate::engine::document::store::CollectionConfig::new(COLLECTION);
        config.bitemporal = true;
        core.doc_configs.insert(key(), config);
        (core, tx, rx)
    }

    fn task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            crate::bridge::envelope::PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
                system_from: None,
                erase_history: false,
            }),
            None,
        )
    }

    fn row(id: i64, v: i64, ts_system: Option<i64>) -> Value {
        let mut fields = HashMap::from([
            ("id".to_string(), Value::Integer(id)),
            ("v".to_string(), Value::Integer(v)),
        ]);
        if let Some(stamp) = ts_system {
            fields.insert("_ts_system".to_string(), Value::Integer(stamp));
        }
        Value::Object(fields)
    }

    fn insert(
        core: &mut CoreLoop,
        intent: ColumnarInsertIntent,
        system_from: Option<i64>,
        rows: Vec<Value>,
    ) {
        let payload = nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("encode rows");
        let response = core.execute_columnar_insert(
            &task(),
            ColumnarInsertParams {
                collection: COLLECTION,
                payload: &payload,
                format: "msgpack",
                intent,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
                provenance: None,
                system_from,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    /// `(v, _ts_system)` of every stored row, in append order.
    fn stamps(core: &CoreLoop) -> Vec<(i64, i64)> {
        let engine = core.columnar_engines.get(&key()).expect("engine");
        let schema = engine.schema();
        let ts_idx = schema.ts_system_idx().expect("bitemporal schema");
        let v_idx = schema
            .columns
            .iter()
            .position(|c| c.name == "v")
            .expect("v column");
        engine
            .scan_memtable_rows()
            .map(|row| {
                let row = row.expect("read");
                match (row.get(v_idx), row.get(ts_idx)) {
                    (Some(Value::Integer(v)), Some(Value::Integer(ts))) => (*v, *ts),
                    other => panic!("unexpected row cells: {other:?}"),
                }
            })
            .collect()
    }

    /// A restore installs the stamp each row carries and folds it into the
    /// clock.
    #[test]
    fn a_restore_keeps_each_rows_system_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let first = core.hlc.peek().saturating_add(5_000_000_000_000);
        insert(
            &mut core,
            ColumnarInsertIntent::Restore,
            Some(1),
            vec![row(1, 10, Some(first)), row(1, 20, Some(first + 7))],
        );
        assert_eq!(stamps(&core), vec![(10, first), (20, first + 7)]);
        assert!(core.hlc.peek() >= first + 7, "the restored stamps fold");
    }

    /// A restored bitemporal row with no stamp refuses the restore.
    #[test]
    fn a_restored_row_without_a_stamp_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let payload =
            nodedb_types::value_to_msgpack(&Value::Array(vec![row(1, 10, None)])).expect("rows");
        let response = core.execute_columnar_insert(
            &task(),
            ColumnarInsertParams {
                collection: COLLECTION,
                payload: &payload,
                format: "msgpack",
                intent: ColumnarInsertIntent::Restore,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
                provenance: None,
                system_from: None,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        assert_eq!(response.status, Status::Error);
    }

    fn update(core: &mut CoreLoop, v: i64, system_from: Option<i64>) {
        let updates = vec![(
            "v".to_string(),
            nodedb_physical::physical_plan::UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(v)).expect("encode"),
            ),
        )];
        let response = core.execute_columnar_update(
            &task(),
            ColumnarDml {
                collection: COLLECTION,
                filter_bytes: &[],
                system_from,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
            },
            &updates,
            None,
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    /// Two updates of one key land two new versions, each with its own
    /// system time above the version it supersedes. A carried stamp below
    /// the bound row rises above it. The superseded versions stay.
    #[test]
    fn each_update_stamps_a_new_version_above_the_last() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let first = core.hlc.peek().saturating_add(1_000_000_000);
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            Some(first),
            vec![row(1, 10, None)],
        );
        update(&mut core, 20, Some(first + 50));
        // A stamp the update carries below the bound version.
        update(&mut core, 30, Some(first + 10));
        assert_eq!(
            stamps(&core),
            vec![(10, first), (20, first + 50), (30, first + 51)]
        );
        // An update with no carried stamp takes a fresh one past the clock.
        update(&mut core, 40, None);
        let all = stamps(&core);
        assert_eq!(all.len(), 4);
        assert!(all[3].1 > first + 51, "{all:?}");
    }

    /// A carried stamp at or below its key's newest stored version rises one
    /// above it. A key with no newer version keeps its run's stamp, and a
    /// client-sent `_ts_system` never decides the stamp.
    #[test]
    fn a_carried_stamp_rises_above_its_keys_newest_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            Some(ahead),
            vec![row(1, 10, None)],
        );
        let behind = ahead - 1_000;
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            Some(behind),
            vec![row(1, 20, Some(1)), row(2, 30, None), row(1, 40, None)],
        );
        assert_eq!(
            stamps(&core),
            vec![
                (10, ahead),
                (20, ahead + 1),
                (30, behind + 1),
                (40, ahead + 2)
            ]
        );
        assert!(core.hlc.peek() >= ahead + 2, "every stamp folds");
    }

    /// A Calvin insert starts every row at the transaction's ordinal and
    /// raises per key: two rows of one key take rising stamps, and a key
    /// whose stored version sits above the ordinal rises above it.
    #[test]
    fn a_calvin_insert_raises_per_key_above_the_txn_ordinal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            Some(ahead),
            vec![row(1, 10, None)],
        );
        let txn = ahead - 1_000;
        core.apply_scope.calvin_txn_ordinal = Some(txn);
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            None,
            vec![row(2, 20, None), row(2, 21, None), row(1, 30, None)],
        );
        core.apply_scope.calvin_txn_ordinal = None;
        assert_eq!(
            stamps(&core),
            vec![(10, ahead), (20, txn), (21, txn + 1), (30, ahead + 1)]
        );
        assert!(core.hlc.peek() > ahead, "every stamp folds");
    }

    fn delete(core: &mut CoreLoop, system_from: Option<i64>) {
        let response = core.execute_columnar_delete(
            &task(),
            ColumnarDml {
                collection: COLLECTION,
                filter_bytes: &[],
                system_from,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
            },
            None,
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    /// A delete closes its key with a tombstone version raised above the
    /// version it closes. The closed version stays: a current read finds no
    /// row, and a read at the closed version's stamp finds it. A later
    /// insert of the key rises above the tombstone.
    #[test]
    fn a_delete_closes_its_key_above_the_version_it_closes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = bitemporal_core(dir.path());
        let first = core.hlc.peek().saturating_add(1_000_000_000);
        insert(
            &mut core,
            ColumnarInsertIntent::Insert,
            Some(first),
            vec![row(1, 10, None)],
        );
        // A stamp the delete carries below the version it closes.
        delete(&mut core, Some(first - 5));
        assert_eq!(stamps(&core), vec![(10, first), (10, first + 1)]);

        let engine = core.columnar_engines.get(&key()).expect("engine");
        let pk = nodedb_columnar::pk_index::encode_pk(&Value::Integer(1));
        assert!(
            !engine.pk_index().contains(&pk),
            "the closed key is unbound"
        );
        let stored: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .map(|row| row.expect("read"))
            .collect();
        let gate = |cut: Option<i64>| {
            core.columnar_version_gate(&key(), cut, false)
                .expect("gate")
                .expect("bitemporal")
        };
        let current = gate(None);
        assert!(
            stored.iter().all(|row| !current.admits(row)),
            "no current row"
        );
        let before_close = gate(Some(first));
        assert_eq!(
            stored.iter().filter(|row| before_close.admits(row)).count(),
            1,
            "the closed version answers a read before the close"
        );

        // A closed key takes a fresh unique insert, which rises above the
        // tombstone.
        insert(
            &mut core,
            ColumnarInsertIntent::InsertUnique,
            Some(first - 100),
            vec![row(1, 20, None)],
        );
        assert_eq!(stamps(&core).last(), Some(&(20, first + 2)));
    }
}
