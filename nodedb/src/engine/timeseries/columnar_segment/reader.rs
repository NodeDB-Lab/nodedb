//! Columnar segment reader.

use std::path::Path;

use nodedb_codec::{ColumnCodec, ColumnStatistics};
use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};

use super::super::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};
use super::codec::{decode_column, legacy_default_codec};
use super::error::SegmentError;
use super::mmap::{ColumnMmap, advise_sequential};
use super::schema::{SchemaJson, schema_from_parsed};

/// Reads columnar data from a partition directory.
pub struct ColumnarSegmentReader;

impl ColumnarSegmentReader {
    /// Read a partition's metadata.
    pub fn read_meta(partition_dir: &Path) -> Result<PartitionMeta, SegmentError> {
        let meta_path = partition_dir.join("partition.meta");
        let data = std::fs::read(&meta_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", meta_path.display())))?;
        sonic_rs::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse meta: {e}")))
    }

    /// Read the schema from a partition directory.
    pub fn read_schema(partition_dir: &Path) -> Result<ColumnarSchema, SegmentError> {
        let schema_path = partition_dir.join("schema.json");
        let data = std::fs::read(&schema_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", schema_path.display())))?;
        let json: SchemaJson = sonic_rs::from_slice(&data)
            .map_err(|e| SegmentError::Io(format!("parse schema: {e}")))?;
        schema_from_parsed(&json)
    }

    /// Read a single column from a partition directory, using the codec
    /// stored in schema metadata.
    pub fn read_column(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
    ) -> Result<ColumnData, SegmentError> {
        Self::read_column_with_codec(partition_dir, col_name, col_type, None)
    }

    /// Read a single column with an explicit codec override.
    pub fn read_column_with_codec(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
    ) -> Result<ColumnData, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let data = std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;

        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        decode_column(&data, col_type, codec)
    }

    /// Decode a column from pre-read raw bytes.
    pub fn decode_column_from_bytes(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
        raw_bytes: &[u8],
    ) -> Result<ColumnData, SegmentError> {
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });
        decode_column(raw_bytes, col_type, codec)
    }

    /// Read the symbol dictionary for a tag column.
    pub fn read_symbol_dict(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<SymbolDictionary, SegmentError> {
        let sym_path = partition_dir.join(format!("{col_name}.sym"));
        let data = std::fs::read(&sym_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", sym_path.display())))?;
        sonic_rs::from_slice(&data).map_err(|e| SegmentError::Io(format!("parse symbol dict: {e}")))
    }

    /// Read specific columns by name (projection pushdown).
    pub fn read_columns(
        partition_dir: &Path,
        requested: &[(String, ColumnType)],
    ) -> Result<Vec<ColumnData>, SegmentError> {
        let meta = Self::read_meta(partition_dir).ok();
        let mut result = Vec::with_capacity(requested.len());
        for (name, ty) in requested {
            let codec = meta
                .as_ref()
                .and_then(|m| m.column_stats.get(name).map(|s| s.codec));
            result.push(Self::read_column_with_codec(
                partition_dir,
                name,
                *ty,
                codec,
            )?);
        }
        Ok(result)
    }

    /// Read raw compressed bytes of a column without decoding.
    pub fn read_column_raw(partition_dir: &Path, col_name: &str) -> Result<Vec<u8>, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))
    }

    /// Decode specific FastLanes blocks from a column.
    pub fn read_column_blocks(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ColumnCodec>,
        block_indices: &[usize],
    ) -> Result<(ColumnData, Vec<(usize, usize)>), SegmentError> {
        let raw = Self::read_column_raw(partition_dir, col_name)?;
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        let is_fastlanes = matches!(codec, ColumnCodec::FastLanesLz4);

        if !is_fastlanes || block_indices.is_empty() {
            let data = decode_column(&raw, col_type, codec)?;
            let total = match &data {
                ColumnData::Timestamp(v) => v.len(),
                ColumnData::Float64(v) => v.len(),
                ColumnData::Int64(v) => v.len(),
                ColumnData::Symbol(v) => v.len(),
                ColumnData::DictEncoded { ids, .. } => ids.len(),
            };
            return Ok((data, vec![(0, total)]));
        }

        let fastlanes_bytes = if codec == ColumnCodec::FastLanesLz4 {
            nodedb_codec::decode_bytes_pipeline(&raw, ColumnCodec::Lz4)
                .map_err(|e| SegmentError::Io(format!("lz4 decode: {e}")))?
        } else {
            raw
        };

        let mut all_values = Vec::new();
        let mut ranges = Vec::new();
        let mut iter = nodedb_codec::fastlanes::BlockIterator::new(&fastlanes_bytes)
            .map_err(|e| SegmentError::Io(format!("block iter: {e}")))?;

        let mut current_block = 0;
        let mut bi_pos = 0;

        while bi_pos < block_indices.len() {
            let target = block_indices[bi_pos];

            while current_block < target {
                if iter.skip_block().is_err() {
                    break;
                }
                current_block += 1;
            }

            if current_block != target {
                break;
            }

            let start = all_values.len();
            match iter.next() {
                Some(Ok(block_vals)) => {
                    all_values.extend(block_vals);
                }
                Some(Err(e)) => {
                    return Err(SegmentError::Io(format!("block decode: {e}")));
                }
                None => break,
            }
            ranges.push((start, all_values.len()));
            current_block += 1;
            bi_pos += 1;
        }

        let data = match col_type {
            ColumnType::Timestamp => ColumnData::Timestamp(all_values),
            ColumnType::Int64 => ColumnData::Int64(all_values),
            ColumnType::Float64 => {
                let f64_vals: Vec<f64> = all_values
                    .iter()
                    .map(|&v| f64::from_bits(v as u64))
                    .collect();
                ColumnData::Float64(f64_vals)
            }
            ColumnType::Symbol => {
                let u32_vals: Vec<u32> = all_values.iter().map(|&v| v as u32).collect();
                ColumnData::Symbol(u32_vals)
            }
        };

        Ok((data, ranges))
    }

    /// Memory-map a column file for zero-copy SIMD access.
    ///
    /// The returned [`ColumnMmap`] advises `MADV_SEQUENTIAL` on the mapped
    /// region and emits `POSIX_FADV_DONTNEED` when dropped — column scans
    /// are forward-sequential and their pages should not pin page cache
    /// across engine boundaries.
    pub fn mmap_column(partition_dir: &Path, col_name: &str) -> Result<ColumnMmap, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let file = std::fs::File::open(&col_path)
            .map_err(|e| SegmentError::Io(format!("open {}: {e}", col_path.display())))?;
        // SAFETY: We require the file not be modified while mapped.
        // Data Plane partitions are immutable once sealed.
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .map(&file)
                .map_err(|e| SegmentError::Io(format!("mmap {}: {e}", col_path.display())))?
        };

        advise_sequential(&mmap, &col_path);

        Ok(ColumnMmap {
            mmap,
            file,
            path: col_path,
        })
    }

    /// Interpret mmap'd raw LE bytes as an i64 slice (zero-copy).
    pub fn mmap_as_i64(mmap: &memmap2::Mmap) -> Result<&[i64], SegmentError> {
        if !mmap.len().is_multiple_of(8) {
            return Err(SegmentError::Corrupt(
                "i64 mmap not aligned to 8 bytes".into(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const i64, mmap.len() / 8) })
    }

    /// Interpret mmap'd raw LE bytes as a u32 slice (zero-copy).
    pub fn mmap_as_u32(mmap: &memmap2::Mmap) -> Result<&[u32], SegmentError> {
        if !mmap.len().is_multiple_of(4) {
            return Err(SegmentError::Corrupt(
                "u32 mmap not aligned to 4 bytes".into(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const u32, mmap.len() / 4) })
    }

    /// Read the sparse primary index for a partition.
    pub fn read_sparse_index(
        partition_dir: &Path,
    ) -> Result<Option<super::super::sparse_index::SparseIndex>, SegmentError> {
        let idx_path = partition_dir.join("sparse_index.bin");
        match std::fs::read(&idx_path) {
            Ok(data) => {
                let idx = super::super::sparse_index::SparseIndex::from_bytes(&data)
                    .map_err(|e| SegmentError::Corrupt(format!("sparse index: {e}")))?;
                Ok(Some(idx))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SegmentError::Io(format!(
                "read {}: {e}",
                idx_path.display()
            ))),
        }
    }

    /// Get the row count from partition metadata without reading any column data.
    pub fn metadata_row_count(partition_dir: &Path) -> Result<u64, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok(meta.row_count)
    }

    /// Get the timestamp range from partition metadata.
    pub fn metadata_ts_range(partition_dir: &Path) -> Result<(i64, i64), SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok((meta.min_ts, meta.max_ts))
    }

    /// Get per-column statistics from partition metadata.
    pub fn metadata_column_stats(
        partition_dir: &Path,
        col_name: &str,
    ) -> Result<Option<ColumnStatistics>, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        Ok(meta.column_stats.get(col_name).cloned())
    }

    /// Check if a partition could contain rows matching a predicate.
    pub fn metadata_might_match(
        partition_dir: &Path,
        col_name: &str,
        predicate: &super::super::sparse_index::BlockPredicate,
    ) -> Result<bool, SegmentError> {
        let meta = Self::read_meta(partition_dir)?;
        match meta.column_stats.get(col_name) {
            Some(stats) => {
                let block_stats = super::super::sparse_index::BlockColumnStats {
                    min: stats.min.unwrap_or(f64::NEG_INFINITY),
                    max: stats.max.unwrap_or(f64::INFINITY),
                };
                Ok(predicate.might_match(&block_stats))
            }
            None => Ok(true),
        }
    }
}
