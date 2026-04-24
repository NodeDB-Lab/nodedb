//! Single-file packed partition format with footer byte ranges.
//!
//! Combines all column files, schema, sparse index, and metadata into a
//! single immutable file for cold/S3 storage. The footer at the end of
//! the file contains exact byte ranges per column, enabling:
//!
//! - HTTP range requests for individual columns (no full-file download)
//! - Projection pushdown at the storage layer
//! - Zero-copy mmap of specific columns
//!
//! File layout:
//! ```text
//! ┌───────────────────────────────────────┐
//! │  Column 0 data (compressed bytes)     │
//! │  Column 1 data (compressed bytes)     │
//! │  Column 2 data (compressed bytes)     │
//! │  ...                                  │
//! ├───────────────────────────────────────┤
//! │  Sparse index data                    │
//! ├───────────────────────────────────────┤
//! │  Footer (JSON)                        │
//! │  - column_ranges: [(offset, length)]  │
//! │  - sparse_index_range: (offset, len)  │
//! │  - schema                             │
//! │  - partition_meta                     │
//! ├───────────────────────────────────────┤
//! │  Footer length (4 bytes, LE u32)      │
//! │  Magic bytes "NDPK" (4 bytes)         │
//! └───────────────────────────────────────┘
//! ```
//!
//! Reading: read last 8 bytes → footer length → seek back → parse footer →
//! use byte ranges to read only needed columns.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use nodedb_codec::ColumnCodec;
use nodedb_types::timeseries::PartitionMeta;
use sonic_rs;

/// Magic bytes at the end of a packed partition file.
const MAGIC: &[u8; 4] = b"NDPK";

/// Footer of a packed partition file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackedFooter {
    /// Per-column byte ranges: column_name → (offset, length).
    pub column_ranges: HashMap<String, (u64, u64)>,
    /// Sparse index byte range (offset, length).
    pub sparse_index_range: Option<(u64, u64)>,
    /// Schema: column names, types, codecs.
    pub schema: Vec<PackedColumnSchema>,
    /// Partition metadata.
    pub meta: PartitionMeta,
}

/// Schema entry for a column in the packed format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackedColumnSchema {
    pub name: String,
    pub col_type: String,
    pub codec: ColumnCodec,
}

/// Write a packed partition file from individual column files.
pub fn write_packed(
    output_path: &Path,
    columns: &[(String, Vec<u8>)], // (column_name, compressed_bytes)
    sparse_index: Option<&[u8]>,
    schema: &[PackedColumnSchema],
    meta: &PartitionMeta,
) -> Result<u64, PackedError> {
    let mut buf = Vec::new();
    let mut column_ranges = HashMap::new();

    // Write column data.
    for (name, data) in columns {
        let offset = buf.len() as u64;
        buf.extend_from_slice(data);
        column_ranges.insert(name.clone(), (offset, data.len() as u64));
    }

    // Write sparse index.
    let sparse_index_range = sparse_index.map(|idx_data| {
        let offset = buf.len() as u64;
        buf.extend_from_slice(idx_data);
        (offset, idx_data.len() as u64)
    });

    // Build footer.
    let footer = PackedFooter {
        column_ranges,
        sparse_index_range,
        schema: schema.to_vec(),
        meta: meta.clone(),
    };
    let footer_json =
        sonic_rs::to_vec(&footer).map_err(|e| PackedError::Io(format!("footer json: {e}")))?;

    let footer_offset = buf.len();
    buf.extend_from_slice(&footer_json);

    // Write footer length + magic.
    let footer_len = (buf.len() - footer_offset) as u32;
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf.extend_from_slice(MAGIC);

    let total_size = buf.len() as u64;
    std::fs::write(output_path, &buf)
        .map_err(|e| PackedError::Io(format!("write {}: {e}", output_path.display())))?;

    Ok(total_size)
}

/// Read the footer from a packed partition file.
pub fn read_footer(file_path: &Path) -> Result<PackedFooter, PackedError> {
    let data = std::fs::read(file_path)
        .map_err(|e| PackedError::Io(format!("read {}: {e}", file_path.display())))?;
    read_footer_from_bytes(&data)
}

/// Read the footer from in-memory bytes (for testing or mmap'd files).
pub fn read_footer_from_bytes(data: &[u8]) -> Result<PackedFooter, PackedError> {
    if data.len() < 8 {
        return Err(PackedError::Corrupt("file too small for footer".into()));
    }

    // Check magic.
    let magic = &data[data.len() - 4..];
    if magic != MAGIC {
        return Err(PackedError::Corrupt(format!(
            "invalid magic: expected NDPK, got {:?}",
            magic
        )));
    }

    // Read footer length.
    let footer_len = u32::from_le_bytes([
        data[data.len() - 8],
        data[data.len() - 7],
        data[data.len() - 6],
        data[data.len() - 5],
    ]) as usize;

    let footer_start = data.len() - 8 - footer_len;
    if footer_start > data.len() {
        return Err(PackedError::Corrupt(
            "footer length exceeds file size".into(),
        ));
    }

    let footer_bytes = &data[footer_start..footer_start + footer_len];
    sonic_rs::from_slice(footer_bytes)
        .map_err(|e| PackedError::Corrupt(format!("footer json: {e}")))
}

/// Read a single column's data from a packed file using byte range.
pub fn read_column(
    file_path: &Path,
    footer: &PackedFooter,
    column_name: &str,
) -> Result<Vec<u8>, PackedError> {
    let (offset, length) = footer
        .column_ranges
        .get(column_name)
        .ok_or_else(|| PackedError::Corrupt(format!("column '{column_name}' not in footer")))?;

    let data = std::fs::read(file_path).map_err(|e| PackedError::Io(format!("read: {e}")))?;

    let start = *offset as usize;
    let end = start + *length as usize;
    if end > data.len() {
        return Err(PackedError::Corrupt(
            "column range exceeds file size".into(),
        ));
    }

    Ok(data[start..end].to_vec())
}

/// Generate HTTP Range header values for fetching specific columns.
///
/// Returns `(column_name, "bytes=offset-end")` pairs for use with
/// S3 GetObject or HTTP range requests.
pub fn http_range_headers(footer: &PackedFooter, columns: &[&str]) -> Vec<(String, String)> {
    columns
        .iter()
        .filter_map(|&name| {
            footer.column_ranges.get(name).map(|&(offset, length)| {
                let end = offset + length - 1; // HTTP ranges are inclusive.
                (name.to_string(), format!("bytes={offset}-{end}"))
            })
        })
        .collect()
}

/// Error type for packed partition operations.
#[derive(thiserror::Error, Debug)]
pub enum PackedError {
    #[error("packed partition I/O: {0}")]
    Io(String),
    #[error("packed partition corrupt: {0}")]
    Corrupt(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::PartitionState;
    use tempfile::TempDir;

    fn test_meta() -> PartitionMeta {
        PartitionMeta {
            min_ts: 1000,
            max_ts: 2000,
            row_count: 100,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 0,
        }
    }

    #[test]
    fn write_and_read_footer() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let columns = vec![
            ("timestamp".to_string(), vec![1u8, 2, 3, 4, 5]),
            ("value".to_string(), vec![10, 20, 30]),
        ];
        let schema = vec![
            PackedColumnSchema {
                name: "timestamp".into(),
                col_type: "timestamp".into(),
                codec: ColumnCodec::DeltaFastLanesLz4,
            },
            PackedColumnSchema {
                name: "value".into(),
                col_type: "float64".into(),
                codec: ColumnCodec::AlpFastLanesLz4,
            },
        ];

        let size = write_packed(&path, &columns, None, &schema, &test_meta()).unwrap();
        assert!(size > 0);

        let footer = read_footer(&path).unwrap();
        assert_eq!(footer.column_ranges.len(), 2);
        assert_eq!(footer.column_ranges["timestamp"], (0, 5));
        assert_eq!(footer.column_ranges["value"], (5, 3));
        assert_eq!(footer.meta.row_count, 100);
    }

    #[test]
    fn read_single_column() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let ts_data = vec![1u8, 2, 3, 4, 5];
        let val_data = vec![10u8, 20, 30];
        let columns = vec![
            ("timestamp".to_string(), ts_data.clone()),
            ("value".to_string(), val_data.clone()),
        ];

        write_packed(&path, &columns, None, &[], &test_meta()).unwrap();

        let footer = read_footer(&path).unwrap();
        let ts_bytes = read_column(&path, &footer, "timestamp").unwrap();
        assert_eq!(ts_bytes, ts_data);

        let val_bytes = read_column(&path, &footer, "value").unwrap();
        assert_eq!(val_bytes, val_data);
    }

    #[test]
    fn http_range_headers_for_projection() {
        let mut column_ranges = HashMap::new();
        column_ranges.insert("timestamp".to_string(), (0u64, 1000));
        column_ranges.insert("cpu".to_string(), (1000, 500));
        column_ranges.insert("host".to_string(), (1500, 200));

        let footer = PackedFooter {
            column_ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
        };

        // Only request cpu column — skip timestamp and host.
        let ranges = http_range_headers(&footer, &["cpu"]);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, "cpu");
        assert_eq!(ranges[0].1, "bytes=1000-1499");
    }

    #[test]
    fn with_sparse_index() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let sparse_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let columns = vec![("ts".to_string(), vec![1, 2, 3])];

        write_packed(&path, &columns, Some(&sparse_data), &[], &test_meta()).unwrap();

        let footer = read_footer(&path).unwrap();
        assert!(footer.sparse_index_range.is_some());
        let (offset, length) = footer.sparse_index_range.unwrap();
        assert_eq!(offset, 3); // After the 3-byte ts column.
        assert_eq!(length, 4);
    }

    #[test]
    fn invalid_magic() {
        let data = vec![0u8; 100]; // No magic.
        assert!(read_footer_from_bytes(&data).is_err());
    }

    #[test]
    fn projection_pushdown_skips_columns() {
        let mut ranges = HashMap::new();
        ranges.insert("ts".to_string(), (0u64, 10000));
        ranges.insert("cpu".to_string(), (10000, 5000));
        ranges.insert("mem".to_string(), (15000, 5000));
        ranges.insert("host".to_string(), (20000, 2000));

        let footer = PackedFooter {
            column_ranges: ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
        };

        // Query only needs ts + cpu → skip mem and host entirely.
        let headers = http_range_headers(&footer, &["ts", "cpu"]);
        assert_eq!(headers.len(), 2);

        // Total bytes fetched: 10000 + 5000 = 15000 out of 22000 total.
        let fetched: u64 = headers
            .iter()
            .map(|(name, _)| footer.column_ranges[name].1)
            .sum();
        assert_eq!(fetched, 15000);
    }
}
