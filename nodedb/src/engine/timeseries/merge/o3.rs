//! Out-of-order buffer merge into an existing sealed partition.

use std::path::Path;

use nodedb_types::timeseries::PartitionMeta;

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};
use crate::engine::timeseries::columnar_segment::{
    ColumnarSegmentReader, ColumnarSegmentWriter, SegmentError,
};
use crate::engine::timeseries::o3_buffer::O3Row;

/// Incrementally merge O3 buffer rows into an existing sealed partition.
///
/// Copy-on-write: reads the existing partition, merges O3 rows in sorted order,
/// writes a new partition file, then atomically swaps via rename.
/// The old partition file is untouched until the swap succeeds — concurrent
/// queries read the old version until the swap completes.
pub fn merge_o3_into_partition(
    base_dir: &Path,
    partition_dir_name: &str,
    o3_rows: &[O3Row],
) -> Result<PartitionMeta, SegmentError> {
    if o3_rows.is_empty() {
        return Err(SegmentError::Io("no O3 rows to merge".into()));
    }

    let partition_dir = base_dir.join(partition_dir_name);
    if !partition_dir.exists() {
        return Err(SegmentError::Io(format!(
            "partition {} does not exist",
            partition_dir.display()
        )));
    }

    // Read existing partition data.
    let existing_meta = ColumnarSegmentReader::read_meta(&partition_dir)?;
    let existing_schema = ColumnarSegmentReader::read_schema(&partition_dir)?;

    let ts_col =
        ColumnarSegmentReader::read_column(&partition_dir, "timestamp", ColumnType::Timestamp)?;
    let val_col = ColumnarSegmentReader::read_column(&partition_dir, "value", ColumnType::Float64)?;

    let existing_ts = ts_col.as_timestamps();
    let existing_val = val_col.as_f64();

    // Merge: sorted merge of existing rows + O3 rows.
    let total_rows = existing_ts.len() + o3_rows.len();
    let mut merged_ts = Vec::with_capacity(total_rows);
    let mut merged_val = Vec::with_capacity(total_rows);

    let mut ei = 0usize; // existing index
    let mut oi = 0usize; // o3 index

    // O3 rows are already sorted by drain_for_partition.
    while ei < existing_ts.len() && oi < o3_rows.len() {
        if existing_ts[ei] <= o3_rows[oi].timestamp_ms {
            merged_ts.push(existing_ts[ei]);
            merged_val.push(existing_val[ei]);
            ei += 1;
        } else {
            merged_ts.push(o3_rows[oi].timestamp_ms);
            merged_val.push(o3_rows[oi].value);
            oi += 1;
        }
    }
    while ei < existing_ts.len() {
        merged_ts.push(existing_ts[ei]);
        merged_val.push(existing_val[ei]);
        ei += 1;
    }
    while oi < o3_rows.len() {
        merged_ts.push(o3_rows[oi].timestamp_ms);
        merged_val.push(o3_rows[oi].value);
        oi += 1;
    }

    // Write to a temporary partition directory.
    let tmp_name = format!("{partition_dir_name}.o3merge");
    let drain = crate::engine::timeseries::columnar_memtable::ColumnarDrainResult {
        columns: vec![
            ColumnData::Timestamp(merged_ts),
            ColumnData::Float64(merged_val),
        ],
        schema: existing_schema,
        symbol_dicts: std::collections::HashMap::new(),
        row_count: total_rows as u64,
        min_ts: existing_meta.min_ts.min(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .min()
                .unwrap_or(i64::MAX),
        ),
        max_ts: existing_meta.max_ts.max(
            o3_rows
                .iter()
                .map(|r| r.timestamp_ms)
                .max()
                .unwrap_or(i64::MIN),
        ),
        // O3 merge preserves the existing partition's system-time max;
        // O3Row does not carry `_ts_system` (O3 is a value-only buffer).
        max_system_ts: existing_meta.max_system_ts,
        series_row_counts: std::collections::HashMap::new(),
    };

    let writer = ColumnarSegmentWriter::new(base_dir);
    let new_meta = writer.write_partition(
        &tmp_name,
        &drain,
        existing_meta.interval_ms,
        existing_meta.last_flushed_wal_lsn,
    )?;

    // Atomic swap: rename tmp → original.
    let tmp_dir = base_dir.join(&tmp_name);
    let backup_name = format!("{partition_dir_name}.old");
    let backup_dir = base_dir.join(&backup_name);

    // Durable directory swap: rename original → backup, tmp → original, then
    // fsync the parent directory so both renames are metadata-durable before
    // the backup is removed.
    nodedb_wal::segment::atomic_swap_dirs_fsync(&partition_dir, &backup_dir, &tmp_dir)
        .map_err(|e| SegmentError::Io(format!("atomic dir swap: {e}")))?;
    let _ = std::fs::remove_dir_all(&backup_dir); // Best-effort cleanup.

    Ok(new_meta)
}
