//! Columnar segment writer and reader for time-partitioned storage.
//!
//! Flushes columnar memtable data to per-column files within a partition
//! directory. Each column is stored as a separate file for projection pushdown
//! (only read columns the query needs).
//!
//! Layout per partition directory:
//! ```text
//! {collection}/ts-{start}_{end}/
//! ├── timestamp.col       # Gorilla-encoded i64 timestamps
//! ├── value.col           # Gorilla-encoded f64 values (or raw for non-metric)
//! ├── {tag}.sym           # Symbol dictionary for tag columns
//! ├── {tag}.col           # u32 symbol IDs
//! └── partition.meta      # PartitionMeta (JSON)
//! ```

use std::path::{Path, PathBuf};

use nodedb_types::gorilla::GorillaEncoder;
use nodedb_types::timeseries::{PartitionMeta, PartitionState, SymbolDictionary};

use super::columnar_memtable::{ColumnData, ColumnType, ColumnarDrainResult, ColumnarSchema};

// ---------------------------------------------------------------------------
// Segment writer
// ---------------------------------------------------------------------------

/// Writes drained columnar memtable data to a partition directory.
pub struct ColumnarSegmentWriter {
    base_dir: PathBuf,
}

impl ColumnarSegmentWriter {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Write a drained memtable to a partition directory.
    ///
    /// Creates the directory and writes one file per column plus metadata.
    /// Returns the partition metadata.
    pub fn write_partition(
        &self,
        partition_name: &str,
        drain: &ColumnarDrainResult,
        interval_ms: u64,
        flush_wal_lsn: u64,
    ) -> Result<PartitionMeta, SegmentError> {
        let partition_dir = self.base_dir.join(partition_name);
        std::fs::create_dir_all(&partition_dir)
            .map_err(|e| SegmentError::Io(format!("create dir: {e}")))?;

        for (i, (col_name, col_type)) in drain.schema.columns.iter().enumerate() {
            let col_data = &drain.columns[i];

            match col_type {
                ColumnType::Timestamp => {
                    let encoded = encode_timestamps(col_data.as_timestamps());
                    let path = partition_dir.join(format!("{col_name}.col"));
                    std::fs::write(&path, &encoded)
                        .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;
                }
                ColumnType::Float64 => {
                    let encoded = encode_f64_values(col_data.as_f64());
                    let path = partition_dir.join(format!("{col_name}.col"));
                    std::fs::write(&path, &encoded)
                        .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;
                }
                ColumnType::Int64 => {
                    let encoded = encode_i64_values(col_data.as_i64());
                    let path = partition_dir.join(format!("{col_name}.col"));
                    std::fs::write(&path, &encoded)
                        .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;
                }
                ColumnType::Symbol => {
                    // Write symbol IDs as raw u32 LE bytes.
                    let ids = col_data.as_symbols();
                    let mut buf = Vec::with_capacity(ids.len() * 4);
                    for &id in ids {
                        buf.extend_from_slice(&id.to_le_bytes());
                    }
                    let path = partition_dir.join(format!("{col_name}.col"));
                    std::fs::write(&path, &buf)
                        .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;

                    // Write symbol dictionary.
                    if let Some(dict) = drain.symbol_dicts.get(&i) {
                        let dict_json = serde_json::to_vec(dict)
                            .map_err(|e| SegmentError::Io(format!("serialize dict: {e}")))?;
                        let sym_path = partition_dir.join(format!("{col_name}.sym"));
                        std::fs::write(&sym_path, &dict_json).map_err(|e| {
                            SegmentError::Io(format!("write {}: {e}", sym_path.display()))
                        })?;
                    }
                }
            }
        }

        // Write schema.
        let schema_json = serde_json::to_vec(&schema_to_json(&drain.schema))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        std::fs::write(partition_dir.join("schema.json"), &schema_json)
            .map_err(|e| SegmentError::Io(format!("write schema: {e}")))?;

        let size_bytes = dir_size(&partition_dir)?;

        let meta = PartitionMeta {
            min_ts: drain.min_ts,
            max_ts: drain.max_ts,
            row_count: drain.row_count,
            size_bytes,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms,
            last_flushed_wal_lsn: flush_wal_lsn,
        };

        // Write metadata.
        let meta_json = serde_json::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        std::fs::write(partition_dir.join("partition.meta"), &meta_json)
            .map_err(|e| SegmentError::Io(format!("write meta: {e}")))?;

        Ok(meta)
    }
}

// ---------------------------------------------------------------------------
// Segment reader
// ---------------------------------------------------------------------------

/// Reads columnar data from a partition directory.
pub struct ColumnarSegmentReader;

impl ColumnarSegmentReader {
    /// Read a partition's metadata.
    pub fn read_meta(partition_dir: &Path) -> Result<PartitionMeta, SegmentError> {
        let meta_path = partition_dir.join("partition.meta");
        let data = std::fs::read(&meta_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", meta_path.display())))?;
        serde_json::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse meta: {e}")))
    }

    /// Read the schema from a partition directory.
    pub fn read_schema(partition_dir: &Path) -> Result<ColumnarSchema, SegmentError> {
        let schema_path = partition_dir.join("schema.json");
        let data = std::fs::read(&schema_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", schema_path.display())))?;
        let json: Vec<(String, String)> = serde_json::from_slice(&data)
            .map_err(|e| SegmentError::Io(format!("parse schema: {e}")))?;
        schema_from_json(&json)
    }

    /// Read a single column from a partition directory.
    pub fn read_column(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
    ) -> Result<ColumnData, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let data = std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;

        match col_type {
            ColumnType::Timestamp => {
                let timestamps = decode_timestamps(&data)?;
                Ok(ColumnData::Timestamp(timestamps))
            }
            ColumnType::Float64 => {
                let values = decode_f64_values(&data)?;
                Ok(ColumnData::Float64(values))
            }
            ColumnType::Int64 => {
                let values = decode_i64_values(&data)?;
                Ok(ColumnData::Int64(values))
            }
            ColumnType::Symbol => {
                if data.len() % 4 != 0 {
                    return Err(SegmentError::Corrupt(
                        "symbol column not aligned to 4 bytes".into(),
                    ));
                }
                let ids: Vec<u32> = data
                    .chunks_exact(4)
                    .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
                    .collect();
                Ok(ColumnData::Symbol(ids))
            }
        }
    }

    /// Read the symbol dictionary for a tag column.
    pub fn read_symbol_dict(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<SymbolDictionary, SegmentError> {
        let sym_path = partition_dir.join(format!("{col_name}.sym"));
        let data = std::fs::read(&sym_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", sym_path.display())))?;
        serde_json::from_slice(&data)
            .map_err(|e| SegmentError::Io(format!("parse symbol dict: {e}")))
    }

    /// Read specific columns by name (projection pushdown).
    pub fn read_columns(
        partition_dir: &Path,
        requested: &[(String, ColumnType)],
    ) -> Result<Vec<ColumnData>, SegmentError> {
        let mut result = Vec::with_capacity(requested.len());
        for (name, ty) in requested {
            result.push(Self::read_column(partition_dir, name, *ty)?);
        }
        Ok(result)
    }

    /// Memory-map a column file for zero-copy SIMD access.
    ///
    /// Returns the raw mmap'd bytes. The caller can cast the pointer
    /// to `&[f64]` or `&[i64]` for direct SIMD aggregation without
    /// any deserialization or copy.
    ///
    /// For Gorilla-encoded columns (timestamp, f64), the caller must
    /// still decode. For raw LE columns (i64, symbol u32), the mmap'd
    /// bytes can be reinterpreted directly.
    ///
    /// # Safety
    /// The returned `Mmap` must outlive any references to its data.
    /// The underlying file must not be modified while mapped.
    pub fn mmap_column(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<memmap2::Mmap, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let file = std::fs::File::open(&col_path)
            .map_err(|e| SegmentError::Io(format!("open {}: {e}", col_path.display())))?;
        // SAFETY: We require the file not be modified while mapped.
        // Data Plane partitions are immutable once sealed.
        unsafe {
            memmap2::MmapOptions::new()
                .map(&file)
                .map_err(|e| SegmentError::Io(format!("mmap {}: {e}", col_path.display())))
        }
    }

    /// Interpret mmap'd raw LE bytes as an i64 slice (zero-copy).
    ///
    /// For columns stored as raw little-endian i64 (Int64 type).
    /// The caller must ensure the mmap'd data was written as LE i64.
    pub fn mmap_as_i64(mmap: &memmap2::Mmap) -> Result<&[i64], SegmentError> {
        if !mmap.len().is_multiple_of(8) {
            return Err(SegmentError::Corrupt(
                "i64 mmap not aligned to 8 bytes".into(),
            ));
        }
        // SAFETY: mmap is aligned by the OS, and we verified length alignment.
        // The data was written as LE i64 by encode_i64_values.
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const i64, mmap.len() / 8) })
    }

    /// Interpret mmap'd raw LE bytes as a u32 slice (zero-copy).
    ///
    /// For symbol columns stored as raw little-endian u32.
    pub fn mmap_as_u32(mmap: &memmap2::Mmap) -> Result<&[u32], SegmentError> {
        if !mmap.len().is_multiple_of(4) {
            return Err(SegmentError::Corrupt(
                "u32 mmap not aligned to 4 bytes".into(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const u32, mmap.len() / 4) })
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode timestamps using Gorilla delta-of-delta compression.
fn encode_timestamps(timestamps: &[i64]) -> Vec<u8> {
    let mut enc = GorillaEncoder::new();
    for &ts in timestamps {
        // Use timestamp as both ts and value (value channel unused for pure ts encoding).
        // Actually, encode as (ts, 0.0) to reuse Gorilla — the value channel compresses to ~0 bits.
        enc.encode(ts, 0.0);
    }
    enc.finish()
}

/// Decode Gorilla-encoded timestamps.
fn decode_timestamps(data: &[u8]) -> Result<Vec<i64>, SegmentError> {
    let mut dec = nodedb_types::gorilla::GorillaDecoder::new(data);
    let samples = dec.decode_all();
    Ok(samples.into_iter().map(|(ts, _)| ts).collect())
}

/// Encode f64 values using Gorilla XOR compression.
fn encode_f64_values(values: &[f64]) -> Vec<u8> {
    let mut enc = GorillaEncoder::new();
    for (i, &v) in values.iter().enumerate() {
        // Use index as synthetic timestamp for delta encoding.
        enc.encode(i as i64, v);
    }
    enc.finish()
}

/// Decode Gorilla-encoded f64 values.
fn decode_f64_values(data: &[u8]) -> Result<Vec<f64>, SegmentError> {
    let mut dec = nodedb_types::gorilla::GorillaDecoder::new(data);
    let samples = dec.decode_all();
    Ok(samples.into_iter().map(|(_, v)| v).collect())
}

/// Encode i64 values as raw little-endian bytes (Gorilla is for floats).
fn encode_i64_values(values: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    for &v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf
}

/// Decode raw LE i64 values.
fn decode_i64_values(data: &[u8]) -> Result<Vec<i64>, SegmentError> {
    if !data.len().is_multiple_of(8) {
        return Err(SegmentError::Corrupt(
            "i64 column not aligned to 8 bytes".into(),
        ));
    }
    Ok(data
        .chunks_exact(8)
        .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

// ---------------------------------------------------------------------------
// Schema serialization
// ---------------------------------------------------------------------------

fn schema_to_json(schema: &ColumnarSchema) -> Vec<(String, String)> {
    schema
        .columns
        .iter()
        .map(|(name, ty)| {
            let ty_str = match ty {
                ColumnType::Timestamp => "timestamp",
                ColumnType::Float64 => "float64",
                ColumnType::Int64 => "int64",
                ColumnType::Symbol => "symbol",
            };
            (name.clone(), ty_str.to_string())
        })
        .collect()
}

fn schema_from_json(json: &[(String, String)]) -> Result<ColumnarSchema, SegmentError> {
    let mut columns = Vec::with_capacity(json.len());
    let mut timestamp_idx = 0;
    for (i, (name, ty_str)) in json.iter().enumerate() {
        let ty = match ty_str.as_str() {
            "timestamp" => {
                timestamp_idx = i;
                ColumnType::Timestamp
            }
            "float64" => ColumnType::Float64,
            "int64" => ColumnType::Int64,
            "symbol" => ColumnType::Symbol,
            other => {
                return Err(SegmentError::Corrupt(format!(
                    "unknown column type: {other}"
                )));
            }
        };
        columns.push((name.clone(), ty));
    }
    Ok(ColumnarSchema {
        columns,
        timestamp_idx,
    })
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn dir_size(dir: &Path) -> Result<u64, SegmentError> {
    let mut size = 0u64;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| SegmentError::Io(format!("read dir {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::Io(format!("dir entry: {e}")))?;
        let meta = entry
            .metadata()
            .map_err(|e| SegmentError::Io(format!("metadata {}: {e}", entry.path().display())))?;
        size += meta.len();
    }
    Ok(size)
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum SegmentError {
    Io(String),
    Corrupt(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "segment I/O error: {msg}"),
            Self::Corrupt(msg) => write!(f, "segment corrupt: {msg}"),
        }
    }
}

impl std::error::Error for SegmentError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig,
    };
    use nodedb_types::timeseries::MetricSample;
    use tempfile::TempDir;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn write_and_read_simple_partition() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        // Populate memtable.
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64 * 0.5,
                },
            );
        }
        let drain = mt.drain();

        // Write partition.
        let meta = writer
            .write_partition("ts-20240101-000000_20240102-000000", &drain, 86_400_000, 42)
            .unwrap();
        assert_eq!(meta.row_count, 100);
        assert_eq!(meta.min_ts, 1000);
        assert_eq!(meta.max_ts, 1099);
        assert_eq!(meta.last_flushed_wal_lsn, 42);
        assert!(meta.size_bytes > 0);

        // Read back.
        let part_dir = tmp.path().join("ts-20240101-000000_20240102-000000");
        let read_meta = ColumnarSegmentReader::read_meta(&part_dir).unwrap();
        assert_eq!(read_meta.row_count, 100);

        let schema = ColumnarSegmentReader::read_schema(&part_dir).unwrap();
        assert_eq!(schema.columns.len(), 2);

        let ts_col =
            ColumnarSegmentReader::read_column(&part_dir, "timestamp", ColumnType::Timestamp)
                .unwrap();
        let timestamps = ts_col.as_timestamps();
        assert_eq!(timestamps.len(), 100);
        assert_eq!(timestamps[0], 1000);
        assert_eq!(timestamps[99], 1099);

        let val_col =
            ColumnarSegmentReader::read_column(&part_dir, "value", ColumnType::Float64).unwrap();
        let values = val_col.as_f64();
        assert_eq!(values.len(), 100);
        assert!((values[0] - 0.0).abs() < f64::EPSILON);
        assert!((values[99] - 49.5).abs() < f64::EPSILON);
    }

    #[test]
    fn write_and_read_with_tags() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("cpu".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());

        for i in 0..50 {
            let host = if i % 2 == 0 { "prod-1" } else { "prod-2" };
            mt.ingest_row(
                i % 2,
                &[
                    ColumnValue::Timestamp(5000 + i as i64),
                    ColumnValue::Float64(50.0 + i as f64),
                    ColumnValue::Symbol(host),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();

        let meta = writer
            .write_partition("ts-20240115-000000_20240116-000000", &drain, 86_400_000, 99)
            .unwrap();
        assert_eq!(meta.row_count, 50);

        // Read back tag column.
        let part_dir = tmp.path().join("ts-20240115-000000_20240116-000000");
        let host_col =
            ColumnarSegmentReader::read_column(&part_dir, "host", ColumnType::Symbol).unwrap();
        assert_eq!(host_col.as_symbols().len(), 50);

        let host_dict = ColumnarSegmentReader::read_symbol_dict(&part_dir, "host").unwrap();
        assert_eq!(host_dict.len(), 2);
        assert!(host_dict.get_id("prod-1").is_some());
        assert!(host_dict.get_id("prod-2").is_some());
    }

    #[test]
    fn column_projection() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..20 {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(i * 100),
                    ColumnValue::Float64(i as f64),
                    ColumnValue::Int64(i * 10),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-proj", &drain, 86_400_000, 0)
            .unwrap();

        // Read only timestamp + value (skip extra).
        let part_dir = tmp.path().join("ts-proj");
        let projected = ColumnarSegmentReader::read_columns(
            &part_dir,
            &[
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
        )
        .unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].len(), 20);
        assert_eq!(projected[1].len(), 20);
    }

    #[test]
    fn gorilla_timestamp_roundtrip() {
        let timestamps: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 10_000).collect();
        let encoded = encode_timestamps(&timestamps);
        let decoded = decode_timestamps(&encoded).unwrap();
        assert_eq!(timestamps, decoded);
    }

    #[test]
    fn gorilla_f64_roundtrip() {
        let values: Vec<f64> = (0..500).map(|i| 42.0 + i as f64 * 0.1).collect();
        let encoded = encode_f64_values(&values);
        let decoded = decode_f64_values(&encoded).unwrap();
        assert_eq!(values.len(), decoded.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}
