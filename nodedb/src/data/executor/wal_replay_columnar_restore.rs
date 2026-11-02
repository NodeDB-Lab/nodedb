// SPDX-License-Identifier: BUSL-1.1

//! Install of one restored columnar key (`crate::wal::ColumnarRestoreRedo`),
//! for restart replay and the committed-redo apply alike.
//!
//! The record carries the exact versions its resolve chose, each with its
//! interval. The install appends each at its `sys_from`. Each ends the row
//! its key binds at that `sys_from`, as an overwrite ends it, and an ended
//! version records its `sys_to`. A version `[at, at)` ends the key's current
//! row and is visible at no cut. The install reads nothing to choose, so
//! every replica and every replay writes the same versions.
//!
//! A bitemporal version takes its `sys_from` as its `_ts_system`. The stamps
//! fold into the shared clock, so a later write stamps above every restored
//! version.

use std::collections::HashMap;

use nodedb_columnar::InstallRow;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::{Surrogate, Value};

use super::core_loop::CoreLoop;
use super::wal_replay_columnar_truncate::TruncateFloors;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::data::executor::handlers::columnar_write::restore_ingest::schema_row_values;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn, TenantId};
use crate::wal::ColumnarRestoreRedo;

/// The cells of the restored row `row`, a native-`Value` msgpack object, in
/// the column order of `schema`. A bitemporal `_ts_system` cell is `0` until
/// the caller stamps it.
pub(in crate::data::executor) fn restored_row_values(
    schema: &ColumnarSchema,
    row: &[u8],
    collection: &str,
) -> crate::Result<Vec<Value>> {
    let fields = restored_row_fields(row, collection)?;
    schema_row_values(schema, schema.is_bitemporal(), &fields)
}

/// The restored row `row` as its field map.
fn restored_row_fields(row: &[u8], collection: &str) -> crate::Result<HashMap<String, Value>> {
    match nodedb_types::value_from_msgpack(row) {
        Ok(Value::Object(fields)) => Ok(fields),
        Ok(_) => Err(crate::Error::Internal {
            detail: format!("a restored row of columnar '{collection}' is not an object"),
        }),
        Err(e) => Err(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("a restored row of columnar '{collection}' does not decode: {e}"),
        }),
    }
}

impl CoreLoop {
    /// Install a restored columnar key. `None` when the payload is not one.
    /// `Some(n)` with the number of versions written otherwise, `Some(0)`
    /// when a gate skipped the record or the install failed (reported
    /// through `replay_policy`).
    pub(in crate::data::executor) fn try_replay_columnar_restore(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
        truncate_floors: &TruncateFloors,
    ) -> Option<usize> {
        let redo = ColumnarRestoreRedo::decode(payload)?;
        let collection = redo.collection.clone();
        if tombstones.is_tombstoned(database_id.as_u64(), tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        let tid = TenantId::new(tenant_id);
        if truncate_floors.covers_collection(database_id, tid, &collection, record_lsn) {
            return Some(0);
        }
        // Already folded into the restored checkpoint.
        if self.replay_watermark_skips(self.floors.replay_floors.columnar.covers(record_lsn)) {
            return Some(0);
        }
        // A restored key names a bound row. One under `ZERO` is refused.
        if redo.surrogate == 0 {
            self.replay_record_unapplied(
                "columnar",
                "restore_identity",
                record_lsn,
                &format!("restored key of '{collection}' carries no surrogate"),
            );
            return Some(0);
        }
        let mut objects = Vec::with_capacity(redo.versions.len());
        for version in &redo.versions {
            match restored_row_fields(&version.row, &collection) {
                Ok(fields) => objects.push(fields),
                Err(error) => {
                    self.replay_record_unapplied(
                        "columnar",
                        "restore_decode",
                        record_lsn,
                        &error.to_string(),
                    );
                    return Some(0);
                }
            }
        }
        if self.claim_for_validation() {
            return Some(0);
        }
        let Some(vshard) = self.replay_vshard("columnar", record_lsn, database_id, &collection)
        else {
            return Some(0);
        };
        let task = Self::replay_task(
            tid,
            database_id,
            vshard,
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                payload: Vec::new(),
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Restore,
                on_conflict_updates: Vec::new(),
                surrogates: vec![Surrogate::new(redo.surrogate)],
                schema_bytes: redo.schema_bytes.clone(),
                provenance: None,
                wal_lsn: Some(record_lsn),
                system_from: None,
                restored_versions: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            Some(Lsn::new(record_lsn)),
        );
        match self.install_restored_columnar_key(&task, &redo, objects) {
            Ok(written) => Some(written),
            Err(error) => {
                self.replay_record_rejected(
                    "columnar",
                    record_lsn,
                    Some(Box::new(error)),
                    &format!("restore of a columnar key into '{collection}' failed"),
                );
                Some(0)
            }
        }
    }

    /// Install every version of `redo`, whose rows are `objects`, or none.
    fn install_restored_columnar_key(
        &mut self,
        task: &ExecutionTask,
        redo: &ColumnarRestoreRedo,
        objects: Vec<HashMap<String, Value>>,
    ) -> Result<usize, ErrorCode> {
        let collection = redo.collection.as_str();
        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let recording = self.recording_redo_undo();
        let Some(first) = objects.first() else {
            return Ok(0);
        };
        if !self.columnar_engines.contains_key(&key) {
            if recording {
                self.record_redo_undo([UndoEntry::ColumnarEngineCreated {
                    collection_key: key.clone(),
                }]);
            }
            let bitemporal =
                self.is_bitemporal(key.0.as_u64(), task.request.tenant_id.as_u64(), collection);
            self.ensure_columnar_engine_schema(
                &key,
                collection,
                bitemporal,
                &Value::Object(first.clone()),
                &redo.schema_bytes,
            )
            .map_err(ErrorCode::from)?;
        }
        let schema = match self.columnar_engines.get(&key) {
            Some(engine) => engine.schema().clone(),
            None => {
                return Err(ErrorCode::Internal {
                    detail: format!("columnar engine of '{collection}' vanished during restore"),
                });
            }
        };
        let bitemporal = schema.is_bitemporal();
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(objects.len());
        for (fields, version) in objects.iter().zip(&redo.versions) {
            let mut values =
                schema_row_values(&schema, bitemporal, fields).map_err(ErrorCode::from)?;
            if bitemporal
                && let Some(slot) = schema.ts_system_idx().and_then(|idx| values.get_mut(idx))
            {
                *slot = Value::Integer(version.sys_from);
            }
            self.observe_bitemporal_ordinal(version.sys_to.unwrap_or(version.sys_from));
            rows.push(values);
        }
        if recording {
            let row_count_before = self
                .columnar_engines
                .get(&key)
                .map_or(0, |engine| engine.memtable().row_count());
            let (inserted_pks, displaced) =
                self.columnar_insert_undo_state(&key, &rows, ColumnarInsertIntent::Restore);
            self.record_redo_undo([UndoEntry::ColumnarInsert {
                collection_key: key.clone(),
                row_count_before,
                inserted_pks,
                displaced,
            }]);
        }
        let surrogate = Surrogate::new(redo.surrogate);
        let Some(engine) = self.columnar_engines.get_mut(&key) else {
            return Err(ErrorCode::Internal {
                detail: format!("columnar engine of '{collection}' vanished during restore"),
            });
        };
        let install = rows
            .iter()
            .zip(&redo.versions)
            .map(|(values, version)| InstallRow {
                values,
                surrogate: Some(surrogate),
                sys_from: version.sys_from,
                sys_to: version.sys_to,
                clone_copy: false,
            });
        engine
            .install_versions(install)
            .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;

        // A flush drains the memtable rows the undo truncates, so the
        // install flushes once the whole record landed.
        if recording {
            self.note_redo_columnar_written(key.clone());
        } else {
            self.flush_columnar_memtable_if_needed(task, &key, collection)
                .map_err(|response| {
                    response.error_code.map_or_else(
                        || ErrorCode::Internal {
                            detail: format!("columnar flush of '{collection}' failed"),
                        },
                        |error| *error,
                    )
                })?;
        }
        // Only the open version is current: it alone enters the geometry
        // index.
        let current: Vec<Value> = objects
            .into_iter()
            .zip(&redo.versions)
            .filter(|(_, version)| version.sys_to.is_none())
            .map(|(fields, _)| Value::Object(fields))
            .collect();
        let delta = self
            .index_columnar_geometry_columns(task, &schema, collection, &current)
            .map_err(ErrorCode::from)?;
        if recording {
            let mut undo = Vec::new();
            Self::push_geometry_index_undo(&mut undo, delta);
            self.record_redo_undo(undo);
        }
        let written = rows.len();
        if written > 0 {
            self.invalidate_aggregate_cache_for_collection(
                key.0.as_u64(),
                task.request.tenant_id.as_u64(),
                collection,
            );
            self.checkpoint_coordinator.mark_dirty("columnar", written);
            self.note_collection_write(task, collection);
        }
        Ok(written)
    }
}
