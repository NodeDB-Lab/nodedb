// SPDX-License-Identifier: BUSL-1.1

//! The RESTORE re-issue ingest (`ColumnarInsertIntent::Restore`): every row
//! installs at the `sys_from` and `sys_to` its `restored_versions` entry
//! carries, so the restored history equals the backed-up one. A clone copy
//! carries the same intent, with each entry marked `clone_copy`: its row
//! installs only where its key holds no version.
//!
//! The rows arrive ascending by `sys_from`. The engine installs them in that
//! order, which rebuilds each key's current row, its chain of ended versions,
//! and its closed-key binding. The stamps fold into the shared clock, so a
//! write after the restore stamps above every restored version.

use std::collections::HashMap;

use nodedb_columnar::InstallRow;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::row_ingest::{RowIngestOutcome, RowIngestParams};
use super::schema::ndb_field_to_value;

impl CoreLoop {
    /// Install every row of a RESTORE re-issue at its carried stamps, or
    /// none of them.
    ///
    /// `Err(Response)` when the stamps do not align with the rows, a row is
    /// not an object, a cell does not coerce to its column, or the engine
    /// refuses a version.
    pub(super) fn install_restored_columnar_rows(
        &mut self,
        task: &ExecutionTask,
        params: &RowIngestParams<'_>,
    ) -> Result<RowIngestOutcome, Response> {
        let RowIngestParams {
            engine_key,
            schema,
            bitemporal,
            surrogates,
            ndb_rows,
            restored_versions,
            collect_stored_rows,
            ..
        } = *params;
        if restored_versions.len() != ndb_rows.len() {
            return Err(self.response_error(
                task,
                crate::Error::BadRequest {
                    detail: format!(
                        "restore into columnar '{}': {} rows carry {} version stamps; every \
                         restored row carries the stamps its original writes took",
                        engine_key.2,
                        ndb_rows.len(),
                        restored_versions.len()
                    ),
                },
            ));
        }

        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(ndb_rows.len());
        for (row_idx, (row, version)) in ndb_rows.iter().zip(restored_versions).enumerate() {
            let Value::Object(obj) = row else {
                return Err(self.response_error(
                    task,
                    crate::Error::BadRequest {
                        detail: format!(
                            "restore into columnar '{}': row {row_idx} is not an object",
                            engine_key.2
                        ),
                    },
                ));
            };
            let mut values = schema_row_values(schema, bitemporal, obj)
                .map_err(|e| self.response_error(task, ErrorCode::from(e)))?;
            // A bitemporal version's `_ts_system` is its `sys_from`.
            if bitemporal
                && let Some(slot) = schema.ts_system_idx().and_then(|idx| values.get_mut(idx))
            {
                *slot = Value::Integer(version.sys_from);
            }
            self.observe_bitemporal_ordinal(version.sys_to.unwrap_or(version.sys_from));
            rows.push(values);
        }

        let Some(engine) = self.columnar_engines.get_mut(engine_key) else {
            return Err(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar engine vanished during restore".into(),
                },
            ));
        };
        let install =
            rows.iter()
                .zip(restored_versions)
                .enumerate()
                .map(|(row_idx, (values, version))| InstallRow {
                    values,
                    surrogate: surrogates.get(row_idx).copied(),
                    sys_from: version.sys_from,
                    sys_to: version.sys_to,
                    clone_copy: version.clone_copy,
                });
        if let Err(e) = engine.install_versions(install) {
            return Err(self.response_error(task, ErrorCode::from(crate::Error::from(e))));
        }

        Ok(RowIngestOutcome {
            accepted: rows.len() as u64,
            stored_rows: if collect_stored_rows {
                rows
            } else {
                Vec::new()
            },
        })
    }
}

/// The cells of `obj` in schema column order, each coerced to its column.
///
/// On a bitemporal collection the three reserved columns fill in when
/// absent: `_ts_system` takes `0` until the caller stamps it, and
/// `_ts_valid_from` / `_ts_valid_until` default to the open interval
/// `[i64::MIN, i64::MAX)`. A client never sets its own `_ts_system`.
pub(in crate::data::executor) fn schema_row_values(
    schema: &ColumnarSchema,
    bitemporal: bool,
    obj: &HashMap<String, Value>,
) -> crate::Result<Vec<Value>> {
    schema
        .columns
        .iter()
        .map(|col| match col.name.as_str() {
            TS_SYSTEM if bitemporal => Ok(Value::Integer(0)),
            TS_VALID_FROM if bitemporal => Ok(match obj.get(TS_VALID_FROM) {
                Some(Value::Integer(i)) => Value::Integer(*i),
                _ => Value::Integer(i64::MIN),
            }),
            TS_VALID_UNTIL if bitemporal => Ok(match obj.get(TS_VALID_UNTIL) {
                Some(Value::Integer(i)) => Value::Integer(*i),
                _ => Value::Integer(i64::MAX),
            }),
            _ => ndb_field_to_value(obj.get(&col.name), col),
        })
        .collect()
}
