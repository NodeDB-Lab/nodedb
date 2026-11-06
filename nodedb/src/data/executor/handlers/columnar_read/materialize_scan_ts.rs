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
//! ## System time
//!
//! Every timeseries row stores `_ts_system`: the system-time ordinal of the
//! batch that wrote it. `system_as_of_ms` is a millisecond cut. The scan
//! reads at its last ordinal (`ms_to_ordinal_upper`), so a row whose stored
//! ordinal is above `ms_to_ordinal_upper(system_as_of_ms)` is skipped.
//!
//! An emitted row renders `_ts_system` as the raw scan does: a bitemporal
//! collection (one with `_ts_valid_from`) shows the ordinal's millisecond,
//! and every other collection drops the column, since there it is the
//! engine's version stamp, not a declared column.

use std::collections::HashMap;
use std::path::PathBuf;

use nodedb_types::value::Value;

use super::materialize_scan::{build_response, encode_cursor};
use super::materialize_scan_ts_row::{RowShape, encode_row_map, system_time};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::cell_read::{TsCell, read_ts_cell};
use crate::data::executor::handlers::timeseries::partition_read::{
    TsPartitionColumns, account_partition_read, partition_corrupt, read_ts_partition,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};

/// The read path the corruption report names.
const SITE: &str = "timeseries_materialize_scan";

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
        // Stored `_ts_system` cells are ordinals: the millisecond cut reads
        // at the last ordinal of its millisecond.
        let cutoff_ordinal = system_as_of_ms.map(nodedb_types::ms_to_ordinal_upper);

        let mut entries: Vec<(u32, Vec<u8>, Option<i64>)> = Vec::with_capacity(count.min(256));
        let mut last_segment: u32 = start_segment;
        let mut last_row: u32 = start_row;

        // ── Phase 1: memtable ────────────────────────────────────────────────
        // Always emit memtable rows before partition rows for stable ordering.
        if start_segment == 0
            && let Some(mt) = self.columnar_memtables.get(&engine_key)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let shape = RowShape::of(&schema.columns, schema.ts_system_idx());
            let row_count = mt.row_count() as usize;
            let first_row = start_row as usize;

            for row_idx in first_row..row_count {
                let ordinal = match shape.system_idx {
                    Some(sys_idx) => match memtable_system_time(mt, sys_idx, row_idx) {
                        Ok(ordinal) => Some(ordinal),
                        Err(e) => return self.response_error(task, e),
                    },
                    None => None,
                };
                // System-time cut: skip rows stamped above the cut ordinal.
                if let (Some(ordinal), Some(cutoff)) = (ordinal, cutoff_ordinal)
                    && ordinal > cutoff
                {
                    continue;
                }

                let value_bytes = match encode_ts_memtable_row(mt, shape, row_idx) {
                    Ok(b) => b,
                    Err(e) => return self.response_error(task, e),
                };

                // Surrogate: bit 31 set (memtable) | lower 31 bits = row_idx.
                let surrogate: u32 = 0x8000_0000 | (row_idx as u32 & 0x7FFF_FFFF);

                entries.push((surrogate, value_bytes, ordinal));
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
            // Collect partitions sorted by start-timestamp (BTreeMap order). A
            // partition a TRUNCATE ended at or below the cut holds no row the
            // clone reads. It keeps its id, so a cursor stays stable.
            let at = cutoff_ordinal;
            let partition_dirs: Vec<(usize, PathBuf)> = registry
                .iter()
                .enumerate()
                .filter(|(_, (_start_ts, entry))| entry.meta.open_at(at))
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

                // A partition that does not read refuses the scan, a missing
                // directory included. Skipping it would hand the materializer
                // a clone without its rows.
                let TsPartitionColumns {
                    schema,
                    columns: col_data,
                    sym_dicts,
                } = match account_partition_read(
                    self.ts_partition_quarantine(task.request.database_id, tid, collection)
                        .as_ref(),
                    part_dir,
                    read_ts_partition(part_dir, SITE, self.segment_keks.ts_segment_kek.as_ref()),
                ) {
                    Ok(partition) => partition,
                    Err(e) => return self.response_error(task, e),
                };

                let shape = RowShape::of(&schema.columns, schema.ts_system_idx());

                // Determine row count from the timestamp column.
                let row_count = match col_data.get(schema.timestamp_idx).and_then(|d| d.as_ref()) {
                    Some(col) => col.len(),
                    None => {
                        return self.response_error(
                            task,
                            partition_corrupt(
                                part_dir,
                                "schema",
                                SITE,
                                format!(
                                    "time column index {} is outside its schema",
                                    schema.timestamp_idx
                                ),
                            ),
                        );
                    }
                };

                let first_row_in_part = if *part_id == start_segment as usize {
                    start_row as usize
                } else {
                    0
                };

                let partition = PartitionCells {
                    part_dir,
                    schema_columns: &schema.columns,
                    col_data: &col_data,
                    sym_dicts: &sym_dicts,
                    shape,
                };
                for row_idx in first_row_in_part..row_count {
                    let ordinal = match shape.system_idx {
                        Some(sys_idx) => match partition.system_time(sys_idx, row_idx) {
                            Ok(ordinal) => Some(ordinal),
                            Err(e) => return self.response_error(task, e),
                        },
                        None => None,
                    };
                    // System-time cut: skip rows stamped above the cut ordinal.
                    if let (Some(ordinal), Some(cutoff)) = (ordinal, cutoff_ordinal)
                        && ordinal > cutoff
                    {
                        continue;
                    }

                    let value_bytes = match partition.encode_row(row_idx) {
                        Ok(b) => b,
                        Err(e) => return self.response_error(task, e),
                    };

                    // Surrogate: (part_id & 0xFFFF) << 16 | (row_idx & 0xFFFF).
                    let surrogate: u32 = encode_ts_part_surrogate(*part_id as u32, row_idx as u32);

                    entries.push((surrogate, value_bytes, ordinal));
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
/// A cell that cannot be read as its column's type, or a row that cannot
/// be encoded, fails the scan: a skipped row is a silently short result.
fn encode_ts_memtable_row(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    shape: RowShape,
    row_idx: usize,
) -> crate::Result<Vec<u8>> {
    let schema = mt.schema();
    let mut map: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());

    for (col_idx, (col_name, _)) in schema.columns.iter().enumerate() {
        let value = shape.emitted_value(
            col_idx,
            col_name,
            || memtable_cell(mt, col_idx, row_idx),
            || memtable_system_time(mt, col_idx, row_idx),
        )?;
        if let Some(value) = value {
            map.insert(col_name.clone(), value);
        }
    }

    encode_row_map(map, row_idx)
}

/// Cell `row_idx` of memtable column `col_idx`. A cell that does not read
/// as its declared type is an error.
fn memtable_cell(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    row_idx: usize,
) -> crate::Result<TsCell<'_>> {
    let (col_name, col_type) = &mt.schema().columns[col_idx];
    read_ts_cell(
        mt.column(col_idx),
        *col_type,
        col_name,
        mt.symbol_dict(col_idx),
        row_idx,
    )
    .map_err(crate::Error::from)
}

/// The system time of memtable row `row_idx`, read from column `sys_idx`.
fn memtable_system_time(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    sys_idx: usize,
    row_idx: usize,
) -> crate::Result<i64> {
    let cell = memtable_cell(mt, sys_idx, row_idx)?;
    system_time(cell).ok_or_else(|| crate::Error::SegmentCorrupted {
        detail: format!(
            "timeseries memtable column {} row {row_idx} holds {cell:?}, not a system time",
            mt.schema().columns[sys_idx].0
        ),
    })
}

/// The decoded columns of one partition, read cell by cell.
struct PartitionCells<'a> {
    part_dir: &'a std::path::Path,
    schema_columns: &'a [(String, ColumnType)],
    col_data: &'a [Option<ColumnData>],
    sym_dicts: &'a HashMap<usize, nodedb_types::timeseries::SymbolDictionary>,
    shape: RowShape,
}

impl PartitionCells<'_> {
    /// Cell `row_idx` of column `col_i`. A column the partition read did not
    /// decode, and a cell that does not read as its declared type, are
    /// corruption: each files one report.
    fn cell(&self, col_i: usize, row_idx: usize) -> crate::Result<TsCell<'_>> {
        let (col_name, col_type) = &self.schema_columns[col_i];
        let Some(data) = self.col_data.get(col_i).and_then(Option::as_ref) else {
            return Err(partition_corrupt(
                self.part_dir,
                "column",
                SITE,
                format!("column '{col_name}' was not decoded"),
            ));
        };
        read_ts_cell(
            data,
            *col_type,
            col_name,
            self.sym_dicts.get(&col_i),
            row_idx,
        )
        .map_err(|e| partition_corrupt(self.part_dir, "cell", SITE, e))
    }

    /// The system time of row `row_idx`, read from column `sys_idx`.
    fn system_time(&self, sys_idx: usize, row_idx: usize) -> crate::Result<i64> {
        let cell = self.cell(sys_idx, row_idx)?;
        system_time(cell).ok_or_else(|| {
            partition_corrupt(
                self.part_dir,
                "cell",
                SITE,
                format!(
                    "column {} row {row_idx} holds {cell:?}, not a system time",
                    self.schema_columns[sys_idx].0
                ),
            )
        })
    }

    /// Encode row `row_idx` as msgpack `Value::Object` bytes.
    fn encode_row(&self, row_idx: usize) -> crate::Result<Vec<u8>> {
        let mut map: HashMap<String, Value> = HashMap::with_capacity(self.schema_columns.len());
        for (col_i, (col_name, _)) in self.schema_columns.iter().enumerate() {
            let value = self.shape.emitted_value(
                col_i,
                col_name,
                || self.cell(col_i, row_idx),
                || self.system_time(col_i, row_idx),
            )?;
            if let Some(value) = value {
                map.insert(col_name.clone(), value);
            }
        }
        encode_row_map(map, row_idx)
    }
}
