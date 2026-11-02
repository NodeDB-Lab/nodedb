// SPDX-License-Identifier: BUSL-1.1

//! The restored timeseries rows a destination already holds.
//!
//! A restored row keeps its backed-up `_ts_system` ordinal, so it has an
//! identity: its series tags, timestamp, `_ts_system` and values
//! (`row_identity`). The resolve reads the destination's current rows with
//! those ordinals, the memtable and every partition no TRUNCATE ended, and
//! drops each restored row the destination already holds. The resolve runs
//! at the transaction's turn on every replica, against the state every
//! replica holds there, so every replica drops the same rows. A second
//! RESTORE of the same backup then installs nothing.
//!
//! Identities count as a multiset: a backup holding one row twice installs
//! it twice into an empty destination, and never a third time.

use std::collections::{HashMap, HashSet};

use nodedb_columnar::VersionCut;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::partition_read::{
    TsPartitionColumns, read_ts_partition,
};
use crate::engine::timeseries::columnar_memtable::ColumnData;
use crate::engine::timeseries::row_cells::{
    insert_non_null, memtable_cell, partition_cell, row_identity,
};
use crate::types::TenantId;

use super::restore_reassert::RestoreScope;

/// Identity counts of stored rows.
pub(super) type HeldRows = HashMap<Vec<u8>, usize>;

fn identity_error(collection: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore resolve: the identity of a row of '{collection}': {e}"),
    }
}

fn cell_error(collection: &str, column: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("restore resolve: column '{column}' of '{collection}': {e}"),
    }
}

/// The identity of the restored row `row`, and its `_ts_system` ordinal.
pub(super) fn restored_identity(
    collection: &str,
    row: &Value,
) -> crate::Result<(Vec<u8>, Option<i64>)> {
    let Value::Object(fields) = row else {
        return Err(crate::Error::Internal {
            detail: format!("restore resolve: a restored row of '{collection}' is not an object"),
        });
    };
    let system = match fields.get(TS_SYSTEM) {
        Some(Value::Integer(ordinal)) => Some(*ordinal),
        _ => None,
    };
    Ok((
        row_identity(fields).map_err(|e| identity_error(collection, e))?,
        system,
    ))
}

/// Drop from `rows` every row `held` names, once per held copy. Returns the
/// rows to install, in order.
pub(super) fn drop_held(
    collection: &str,
    rows: Vec<Value>,
    mut held: HeldRows,
) -> crate::Result<Vec<Value>> {
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let (identity, _) = restored_identity(collection, &row)?;
        match held.get_mut(&identity) {
            Some(count) if *count > 0 => *count -= 1,
            _ => kept.push(row),
        }
    }
    Ok(kept)
}

impl CoreLoop {
    /// The identity counts of the destination's current rows of
    /// `collection` whose `_ts_system` is one of `ordinals`.
    ///
    /// `Err` when a partition does not read or a cell does not decode.
    pub(super) fn held_ts_rows(
        &self,
        scope: RestoreScope,
        collection: &str,
        ordinals: &HashSet<i64>,
    ) -> crate::Result<HeldRows> {
        let mut held = HeldRows::new();
        let Some(lowest) = ordinals.iter().min().copied() else {
            return Ok(held);
        };
        let key = (
            scope.database_id,
            TenantId::new(scope.tid),
            collection.to_string(),
        );
        if let Some(mt) = self.columnar_memtables.get(&key) {
            let schema = mt.schema();
            let systems: &[i64] = match schema.ts_system_idx().map(|idx| mt.column(idx)) {
                Some(ColumnData::Int64(systems)) => systems,
                _ => &[],
            };
            for (idx, system) in systems.iter().enumerate() {
                if !ordinals.contains(system) {
                    continue;
                }
                let mut fields = HashMap::with_capacity(schema.columns.len());
                for (col_idx, (name, ty)) in schema.columns.iter().enumerate() {
                    let cell = memtable_cell(mt, col_idx, *ty, idx)
                        .map_err(|e| cell_error(collection, name, e))?;
                    insert_non_null(&mut fields, name, cell);
                }
                let identity = row_identity(&fields).map_err(|e| identity_error(collection, e))?;
                *held.entry(identity).or_default() += 1;
            }
        }
        let Some(registry) = self.ts_registries.get(&key) else {
            return Ok(held);
        };
        let everything = nodedb_types::timeseries::TimeRange::new(i64::MIN, i64::MAX);
        let base = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            scope.database_id.as_u64(),
            scope.tid,
            collection,
        );
        for entry in registry.query_partitions(&everything, VersionCut::Current) {
            // A partition whose newest ordinal is below every restored one
            // holds none of them. A partition with no ordinals gives its
            // rows its `max_system_ts`, as the backup decode does.
            if entry.meta.max_system_ts > 0 && entry.meta.max_system_ts < lowest {
                continue;
            }
            let TsPartitionColumns {
                schema,
                columns,
                sym_dicts,
            } = read_ts_partition(
                &base.join(&entry.dir_name),
                "restore_resolve",
                self.segment_keks.ts_segment_kek.as_ref(),
            )?;
            let row_count = columns
                .get(schema.timestamp_idx)
                .and_then(Option::as_ref)
                .map_or(0, ColumnData::len);
            let system_column = schema
                .ts_system_idx()
                .and_then(|idx| columns.get(idx))
                .and_then(Option::as_ref);
            for idx in 0..row_count {
                let system = match system_column {
                    Some(ColumnData::Int64(systems)) => systems.get(idx).copied(),
                    _ => Some(entry.meta.max_system_ts),
                };
                if !system.is_some_and(|system| ordinals.contains(&system)) {
                    continue;
                }
                let mut fields = HashMap::with_capacity(schema.columns.len());
                for (col_idx, (name, ty)) in schema.columns.iter().enumerate() {
                    let Some(data) = columns.get(col_idx).and_then(Option::as_ref) else {
                        continue;
                    };
                    let cell = partition_cell(data, *ty, col_idx, &sym_dicts, idx)
                        .map_err(|e| cell_error(collection, name, e))?;
                    insert_non_null(&mut fields, name, cell);
                }
                if let Some(system) = system {
                    fields
                        .entry(TS_SYSTEM.to_string())
                        .or_insert(Value::Integer(system));
                }
                let identity = row_identity(&fields).map_err(|e| identity_error(collection, e))?;
                *held.entry(identity).or_default() += 1;
            }
        }
        Ok(held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(host: &str, system: i64) -> Value {
        Value::Object(
            [
                ("host".to_string(), Value::String(host.into())),
                (TS_SYSTEM.to_string(), Value::Integer(system)),
            ]
            .into_iter()
            .collect(),
        )
    }

    /// A row the destination holds is dropped once per held copy: a second
    /// RESTORE of the same rows installs nothing, and a backup holding a row
    /// twice keeps its second copy against one held copy.
    #[test]
    fn held_rows_are_dropped_once_per_copy() {
        let rows = vec![row("a", 7), row("a", 7), row("b", 8)];
        let mut held = HeldRows::new();
        let (a, system) = restored_identity("cpu", &row("a", 7)).expect("identity");
        assert_eq!(system, Some(7));
        held.insert(a, 1);
        let kept = drop_held("cpu", rows.clone(), held).expect("drop");
        assert_eq!(kept, vec![row("a", 7), row("b", 8)]);

        let mut all = HeldRows::new();
        for r in &rows {
            let (identity, _) = restored_identity("cpu", r).expect("identity");
            *all.entry(identity).or_default() += 1;
        }
        assert!(drop_held("cpu", rows, all).expect("drop").is_empty());
    }
}
