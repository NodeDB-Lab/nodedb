// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated materialize scan for timeseries collections.
//!
//! Timeseries data lives in two structures the plain-columnar scan does not
//! touch:
//!
//!   1. `self.columnar_memtables` — active in-memory rows.
//!   2. On-disk partitions listed via `self.ts_registries`.
//!
//! ## Cursor format (8 bytes, big-endian)
//!
//! ```text
//! [ segment_id: u32 BE | row_index: u32 BE ]
//! ```
//!
//! * `segment_id == 0` → **memtable phase**.  `row_index` = next memtable row
//!   to emit (0-based).  Memtable is scanned **first**.
//! * `segment_id >= 1` → **partition phase**.  `segment_id` is the 1-based
//!   index into the registry's partition list, ordered by start-timestamp
//!   ascending (BTreeMap natural order).  `row_index` = next row within that
//!   partition.
//!
//! Ordering: memtable first, then partitions ascending by start-timestamp.
//! This mirrors `raw_scan.rs` and gives stable resume semantics.
//!
//! ## Surrogate encoding
//!
//! The surrogate is used only for the Control Plane's tombstone/copyup probe
//! (`catalog.get_clone_copyup(…)`) which must be unique within the collection:
//!
//! * Memtable rows:   `surrogate = 0x8000_0000 | (row_idx & 0x7FFF_FFFF)`
//! * Partition rows:  `surrogate = ((partition_id_1based & 0xFFFF) << 16) | (row_idx & 0xFFFF)`
//!
//! These ranges are disjoint (bit 31 distinguishes memtable from partition
//! rows) so no collisions can occur within a single collection.
//!
//! ## `system_as_of_ms` handling
//!
//! Timeseries collections do not normally have a `_ts_system` column (their
//! time axis is the user-supplied TIME_KEY, not a bitemporal system column).
//! If a `_ts_system` column is present and `system_as_of_ms` is `Some(cutoff)`,
//! rows where `_ts_system > cutoff` are skipped.  For normal timeseries (no
//! `_ts_system`), the cutoff has no effect.

use std::collections::HashMap;
use std::path::PathBuf;

use nodedb_types::value::Value;

use super::materialize_scan::{build_response, encode_cursor};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::ColumnarSegmentReader;

impl CoreLoop {
    /// Execute a cursor-paginated materialize scan for a timeseries collection.
    ///
    /// Called from `execute_columnar_materialize_scan` when the collection is
    /// absent from `columnar_engines` (i.e. it is a timeseries collection).
    pub(in crate::data::executor) fn execute_ts_materialize_scan(
        &self,
        task: &ExecutionTask,
        collection: &str,
        cursor: &[u8],
        count: usize,
        system_as_of_ms: Option<i64>,
    ) -> crate::bridge::envelope::Response {
        let tid = task.request.tenant_id;
        let engine_key = (task.request.database_id, tid, collection.to_string());

        // Cursor: (segment_id, row_index).  segment_id == 0 → memtable phase.
        let (start_segment, start_row) = parse_cursor_ts(cursor);

        let mut entries: Vec<(u32, Vec<u8>)> = Vec::with_capacity(count.min(256));
        let mut last_segment: u32 = start_segment;
        let mut last_row: u32 = start_row;

        // ── Phase 1: memtable ────────────────────────────────────────────────
        // Always emit memtable rows before partition rows for stable ordering.
        if start_segment == 0
            && let Some(mt) = self.columnar_memtables.get(&engine_key)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let ts_system_idx = schema.ts_system_idx();
            let col_count = schema.columns.len();
            let row_count = mt.row_count() as usize;
            let first_row = start_row as usize;

            for row_idx in first_row..row_count {
                // `system_as_of_ms` filter: skip rows newer than the cutoff.
                if let (Some(sys_idx), Some(cutoff)) = (ts_system_idx, system_as_of_ms) {
                    let ts_val = ts_memtable_value(mt.column(sys_idx), row_idx);
                    if ts_val > cutoff {
                        continue;
                    }
                }

                let value_bytes = match encode_ts_memtable_row(mt, col_count, row_idx, collection) {
                    Some(b) => b,
                    None => continue,
                };

                // Surrogate: bit 31 set (memtable) | lower 31 bits = row_idx.
                let surrogate: u32 = 0x8000_0000 | (row_idx as u32 & 0x7FFF_FFFF);

                entries.push((surrogate, value_bytes));
                last_segment = 0;
                last_row = (row_idx + 1) as u32;

                if entries.len() >= count {
                    break;
                }
            }
        }

        // ── Phase 2: on-disk partitions ──────────────────────────────────────
        // Only enter if memtable phase is done (segment_id > 0 from cursor, OR
        // we just finished the memtable phase above).
        let enter_partitions = start_segment >= 1 || (entries.len() < count && start_segment == 0);

        if enter_partitions
            && entries.len() < count
            && let Some(registry) = self.ts_registries.get(&engine_key)
        {
            // Collect partitions sorted by start-timestamp (BTreeMap order).
            let partition_dirs: Vec<(usize, PathBuf)> = registry
                .iter()
                .enumerate()
                .map(|(i, (_start_ts, entry))| {
                    let part_id = i + 1; // 1-based (usize)
                    let dir =
                        crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
                            &self.data_dir,
                            task.request.database_id.as_u64(),
                            tid.as_u64(),
                            collection,
                        )
                        .join(&entry.dir_name);
                    (part_id, dir)
                })
                .collect();

            // First partition to visit: determined by cursor.
            let first_part_id = if start_segment >= 1 {
                start_segment as usize
            } else {
                // Just finished memtable; start from partition 1.
                1
            };

            'part_loop: for (part_id, part_dir) in &partition_dirs {
                if *part_id < first_part_id {
                    continue;
                }
                if !part_dir.exists() {
                    continue;
                }

                let schema = match ColumnarSegmentReader::read_schema(part_dir, None) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(
                            collection,
                            part_id,
                            error = %e,
                            "ts_materialize_scan: failed to read schema; skipping partition"
                        );
                        continue;
                    }
                };

                let ts_system_idx = schema.ts_system_idx();

                // Read all columns.
                let col_data: Vec<Option<ColumnData>> = schema
                    .columns
                    .iter()
                    .map(|(name, ty)| {
                        ColumnarSegmentReader::read_column(part_dir, name, *ty, None).ok()
                    })
                    .collect();

                // Read symbol dictionaries.
                let sym_dicts: HashMap<usize, nodedb_types::timeseries::SymbolDictionary> = schema
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
                    .filter_map(|(i, (name, _))| {
                        ColumnarSegmentReader::read_symbol_dict(part_dir, name, None)
                            .ok()
                            .map(|dict| (i, dict))
                    })
                    .collect();

                // Determine row count from the timestamp column.
                let ts_col = col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref());
                let row_count = match ts_col {
                    Some(col) => col.len(),
                    None => continue,
                };

                let first_row_in_part = if *part_id == start_segment as usize {
                    start_row as usize
                } else {
                    0
                };

                for row_idx in first_row_in_part..row_count {
                    // `system_as_of_ms` filter.
                    if let (Some(sys_idx), Some(cutoff)) = (ts_system_idx, system_as_of_ms)
                        && let Some(sys_col) = &col_data[sys_idx]
                        && ts_partition_value(sys_col, row_idx) > cutoff
                    {
                        continue;
                    }

                    let value_bytes = match encode_ts_partition_row(
                        &schema.columns,
                        &col_data,
                        &sym_dicts,
                        row_idx,
                        collection,
                    ) {
                        Some(b) => b,
                        None => continue,
                    };

                    // Surrogate: (part_id & 0xFFFF) << 16 | (row_idx & 0xFFFF).
                    let surrogate: u32 = encode_ts_part_surrogate(*part_id as u32, row_idx as u32);

                    entries.push((surrogate, value_bytes));
                    last_segment = *part_id as u32;
                    last_row = (row_idx + 1) as u32;

                    if entries.len() >= count {
                        break 'part_loop;
                    }
                }
            }
        }

        let next_cursor = if entries.len() < count {
            Vec::new()
        } else {
            encode_cursor(last_segment, last_row)
        };

        build_response(self, task, entries, next_cursor)
    }
}

// ---------------------------------------------------------------------------
// Cursor helpers
// ---------------------------------------------------------------------------

/// Parse a timeseries materialize cursor.
///
/// Empty cursor → (0, 0): start from memtable phase, row 0.
fn parse_cursor_ts(cursor: &[u8]) -> (u32, u32) {
    if cursor.len() < 8 {
        return (0, 0); // Start: memtable phase, row 0.
    }
    let seg = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    let row = u32::from_be_bytes([cursor[4], cursor[5], cursor[6], cursor[7]]);
    (seg, row)
}

// ---------------------------------------------------------------------------
// Surrogate helpers
// ---------------------------------------------------------------------------

/// Encode a partition row surrogate.
///
/// * Partition rows: `(part_id_1based & 0xFFFF) << 16 | (row_idx & 0xFFFF)`
///
/// These values have bit 31 clear (part_id fits in 16 bits for any realistic
/// collection), making them disjoint from the memtable range (`0x8000_0000 |
/// row_idx`).  Uniqueness within the collection is guaranteed as long as
/// neither dimension overflows its 16-bit field (65 535 partitions / rows per
/// partition).  The probe is idempotent so the rare collision would at worst
/// cause one extra InsertIfAbsent no-op.
pub(super) fn encode_ts_part_surrogate(part_id_1based: u32, row_idx: u32) -> u32 {
    (part_id_1based & 0xFFFF) << 16 | (row_idx & 0xFFFF)
}

// ---------------------------------------------------------------------------
// Row encoding helpers
// ---------------------------------------------------------------------------

/// Encode a memtable row as msgpack `Value::Object` bytes.
///
/// Returns `None` if serialization fails (logged by caller).
fn encode_ts_memtable_row(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_count: usize,
    row_idx: usize,
    collection: &str,
) -> Option<Vec<u8>> {
    let mut map: HashMap<String, Value> = HashMap::with_capacity(col_count);
    let schema = mt.schema();

    for (col_idx, (col_name, col_type)) in schema.columns.iter().enumerate() {
        let col_data = mt.column(col_idx);
        let val = memtable_col_to_value(col_data, col_type, col_idx, mt, row_idx);
        map.insert(col_name.clone(), val);
    }

    let ndb_val = Value::Object(map);
    match nodedb_types::value_to_msgpack(&ndb_val) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::warn!(
                collection,
                row_idx,
                error = %e,
                "ts_materialize_scan: memtable row encode failed; skipping"
            );
            None
        }
    }
}

/// Encode a partition row as msgpack `Value::Object` bytes.
fn encode_ts_partition_row(
    schema_columns: &[(String, ColumnType)],
    col_data: &[Option<ColumnData>],
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    row_idx: usize,
    collection: &str,
) -> Option<Vec<u8>> {
    let mut map: HashMap<String, Value> = HashMap::with_capacity(schema_columns.len());

    for (col_i, (col_name, col_type)) in schema_columns.iter().enumerate() {
        let Some(data) = &col_data[col_i] else {
            continue;
        };
        let val = partition_col_to_value(data, col_type, col_i, sym_dicts, row_idx);
        map.insert(col_name.clone(), val);
    }

    let ndb_val = Value::Object(map);
    match nodedb_types::value_to_msgpack(&ndb_val) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::warn!(
                collection,
                row_idx,
                error = %e,
                "ts_materialize_scan: partition row encode failed; skipping"
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Column-to-Value converters
// ---------------------------------------------------------------------------

/// Convert a memtable column entry to `nodedb_types::Value`.
fn memtable_col_to_value(
    col_data: &ColumnData,
    col_type: &ColumnType,
    col_idx: usize,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    row_idx: usize,
) -> Value {
    match col_type {
        ColumnType::Timestamp(_) => Value::Integer(col_data.as_timestamps()[row_idx]),
        ColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Int64 => Value::Integer(col_data.as_i64()[row_idx]),
        ColumnType::Symbol => {
            let sym_id = col_data.as_symbols()[row_idx];
            mt.symbol_dict(col_idx)
                .and_then(|d| d.get(sym_id))
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null)
        }
    }
}

/// Convert a partition column entry to `nodedb_types::Value`.
fn partition_col_to_value(
    data: &ColumnData,
    col_type: &ColumnType,
    col_i: usize,
    sym_dicts: &HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    row_idx: usize,
) -> Value {
    match col_type {
        ColumnType::Timestamp(_) => Value::Integer(data.as_timestamps()[row_idx]),
        ColumnType::Float64 => {
            let v = data.as_f64()[row_idx];
            if v.is_nan() {
                Value::Null
            } else {
                Value::Float(v)
            }
        }
        ColumnType::Int64 => {
            if let ColumnData::Int64(vals) = data {
                Value::Integer(vals[row_idx])
            } else {
                Value::Null
            }
        }
        ColumnType::Symbol => {
            if let ColumnData::Symbol(ids) = data {
                sym_dicts
                    .get(&col_i)
                    .and_then(|dict| dict.get(ids[row_idx]))
                    .map(|s| Value::String(s.to_string()))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
    }
}

/// Extract a timestamp value from a column (for `_ts_system` filtering).
fn ts_memtable_value(col_data: &ColumnData, row_idx: usize) -> i64 {
    match col_data {
        ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.get(row_idx).copied().unwrap_or(0),
        _ => 0,
    }
}

fn ts_partition_value(col_data: &ColumnData, row_idx: usize) -> i64 {
    match col_data {
        ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.get(row_idx).copied().unwrap_or(0),
        _ => 0,
    }
}
