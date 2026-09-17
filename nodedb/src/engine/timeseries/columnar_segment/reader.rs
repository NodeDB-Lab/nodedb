// SPDX-License-Identifier: BUSL-1.1

//! Columnar segment reader.

use std::path::Path;

use nodedb_codec::{ColumnCodec, ColumnStatistics, ResolvedColumnCodec};
use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};
use nodedb_wal::crypto::WalEncryptionKey;

use super::super::columnar_memtable::{ColumnData, ColumnType, ColumnarSchema};
use super::codec::{decode_column, legacy_default_codec};
use super::encrypt::{decrypt_file, is_encrypted};
use super::error::SegmentError;
use super::mmap::{BackingStore, ColumnMmap, advise_sequential};
use super::schema::{SchemaJson, schema_from_parsed};

/// Reads columnar data from a partition directory.
pub struct ColumnarSegmentReader;

impl ColumnarSegmentReader {
    /// Read a partition's metadata.
    ///
    /// When `kek` is `Some` the file is expected to be encrypted (`SEGT`);
    /// a plaintext file with a KEK present returns `UnexpectedPlaintext`.
    /// An encrypted file without a KEK returns `MissingKek`.
    pub fn read_meta(
        partition_dir: &Path,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<PartitionMeta, SegmentError> {
        let meta_path = partition_dir.join("partition.meta");
        let raw = std::fs::read(&meta_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", meta_path.display())))?;
        let plaintext = decrypt_segment_file(kek, &raw)?;
        sonic_rs::from_slice(&plaintext).map_err(|e| SegmentError::Io(format!("parse meta: {e}")))
    }

    /// Read the schema from a partition directory.
    pub fn read_schema(
        partition_dir: &Path,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<ColumnarSchema, SegmentError> {
        let schema_path = partition_dir.join("schema.json");
        let raw = std::fs::read(&schema_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", schema_path.display())))?;
        let plaintext = decrypt_segment_file(kek, &raw)?;
        let json: SchemaJson = sonic_rs::from_slice(&plaintext)
            .map_err(|e| SegmentError::Io(format!("parse schema: {e}")))?;
        schema_from_parsed(&json)
    }

    /// Read a single column from a partition directory, using the codec
    /// stored in schema metadata.
    pub fn read_column(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<ColumnData, SegmentError> {
        Self::read_column_with_codec(partition_dir, col_name, col_type, None, kek)
    }

    /// Read a single column with an explicit codec override.
    pub fn read_column_with_codec(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ResolvedColumnCodec>,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<ColumnData, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let raw = std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;
        let data = decrypt_segment_file(kek, &raw)?;

        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir, kek)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        decode_column(&data, col_type, codec)
    }

    /// Decode a column from pre-read raw bytes (already decrypted by caller).
    pub fn decode_column_from_bytes(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ResolvedColumnCodec>,
        raw_bytes: &[u8],
        kek: Option<&WalEncryptionKey>,
    ) -> Result<ColumnData, SegmentError> {
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir, kek)
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
        kek: Option<&WalEncryptionKey>,
    ) -> Result<SymbolDictionary, SegmentError> {
        let sym_path = partition_dir.join(format!("{col_name}.sym"));
        let raw = std::fs::read(&sym_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", sym_path.display())))?;
        let plaintext = decrypt_segment_file(kek, &raw)?;
        sonic_rs::from_slice(&plaintext)
            .map_err(|e| SegmentError::Io(format!("parse symbol dict: {e}")))
    }

    /// Read specific columns by name (projection pushdown).
    pub fn read_columns(
        partition_dir: &Path,
        requested: &[(String, ColumnType)],
        kek: Option<&WalEncryptionKey>,
    ) -> Result<Vec<ColumnData>, SegmentError> {
        let meta = Self::read_meta(partition_dir, kek).ok();
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
                kek,
            )?);
        }
        Ok(result)
    }

    /// Read raw compressed bytes of a column without decoding.
    ///
    /// When the file is encrypted the returned bytes are the plaintext
    /// compressed data (not the on-disk ciphertext).
    pub fn read_column_raw(
        partition_dir: &Path,
        col_name: &str,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<Vec<u8>, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        let raw = std::fs::read(&col_path)
            .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;
        decrypt_segment_file(kek, &raw)
    }

    /// Decode specific FastLanes blocks from a column.
    pub fn read_column_blocks(
        partition_dir: &Path,
        col_name: &str,
        col_type: ColumnType,
        codec: Option<ResolvedColumnCodec>,
        block_indices: &[usize],
        kek: Option<&WalEncryptionKey>,
    ) -> Result<(ColumnData, Vec<(usize, usize)>), SegmentError> {
        let raw = Self::read_column_raw(partition_dir, col_name, kek)?;
        let codec = codec.unwrap_or_else(|| {
            Self::read_meta(partition_dir, kek)
                .ok()
                .and_then(|meta| meta.column_stats.get(col_name).map(|s| s.codec))
                .unwrap_or_else(|| legacy_default_codec(col_type))
        });

        let is_fastlanes = matches!(codec, ResolvedColumnCodec::FastLanesLz4);

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

        let fastlanes_bytes = if codec == ResolvedColumnCodec::FastLanesLz4 {
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
            ColumnType::Timestamp(_) => ColumnData::Timestamp(all_values),
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
    /// For plaintext partitions, the file is mmap'd directly (zero-copy).
    /// For encrypted partitions (`kek` is `Some`), the file is read into an
    /// owned buffer and decrypted — mmap zero-copy is not possible for
    /// encrypted on-disk blobs. The returned [`ColumnMmap`] is transparent to
    /// callers; both backing stores implement `Deref<Target = [u8]>`.
    pub fn mmap_column(
        partition_dir: &Path,
        col_name: &str,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<ColumnMmap, SegmentError> {
        let col_path = partition_dir.join(format!("{col_name}.col"));
        match kek {
            None => {
                let file = std::fs::File::open(&col_path)
                    .map_err(|e| SegmentError::Io(format!("open {}: {e}", col_path.display())))?;
                // SAFETY: Sealed partitions are immutable once written.
                let mmap = unsafe {
                    memmap2::MmapOptions::new().map(&file).map_err(|e| {
                        SegmentError::Io(format!("mmap {}: {e}", col_path.display()))
                    })?
                };
                advise_sequential(&mmap, &col_path);
                Ok(ColumnMmap {
                    backing: BackingStore::Mmap { mmap, file },
                    path: col_path,
                })
            }
            Some(key) => {
                let raw = std::fs::read(&col_path)
                    .map_err(|e| SegmentError::Io(format!("read {}: {e}", col_path.display())))?;
                let plaintext = decrypt_segment_file(Some(key), &raw)?;
                Ok(ColumnMmap {
                    backing: BackingStore::Decrypted(plaintext),
                    path: col_path,
                })
            }
        }
    }

    /// Parse raw little-endian bytes as owned i64 values.
    ///
    /// Returning owned values is portable across mmap and decrypted `Vec<u8>`
    /// backing stores, neither of which guarantees i64 alignment.
    pub fn mmap_as_i64(bytes: &[u8]) -> Result<Vec<i64>, SegmentError> {
        let (chunks, remainder) = bytes.as_chunks::<8>();
        if !remainder.is_empty() {
            return Err(SegmentError::Corrupt(
                "i64 byte length is not a multiple of 8".into(),
            ));
        }
        Ok(chunks
            .iter()
            .map(|chunk| i64::from_le_bytes(*chunk))
            .collect())
    }

    /// Parse raw little-endian bytes as owned u32 values.
    ///
    /// Returning owned values is portable across mmap and decrypted `Vec<u8>`
    /// backing stores, neither of which guarantees u32 alignment.
    pub fn mmap_as_u32(bytes: &[u8]) -> Result<Vec<u32>, SegmentError> {
        let (chunks, remainder) = bytes.as_chunks::<4>();
        if !remainder.is_empty() {
            return Err(SegmentError::Corrupt(
                "u32 byte length is not a multiple of 4".into(),
            ));
        }
        Ok(chunks
            .iter()
            .map(|chunk| u32::from_le_bytes(*chunk))
            .collect())
    }

    /// Read the sparse primary index for a partition.
    pub fn read_sparse_index(
        partition_dir: &Path,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<Option<super::super::sparse_index::SparseIndex>, SegmentError> {
        let idx_path = partition_dir.join("sparse_index.bin");
        match std::fs::read(&idx_path) {
            Ok(raw) => {
                let data = decrypt_segment_file(kek, &raw)?;
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
    pub fn metadata_row_count(
        partition_dir: &Path,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<u64, SegmentError> {
        let meta = Self::read_meta(partition_dir, kek)?;
        Ok(meta.row_count)
    }

    /// Get the timestamp range from partition metadata.
    pub fn metadata_ts_range(
        partition_dir: &Path,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<(i64, i64), SegmentError> {
        let meta = Self::read_meta(partition_dir, kek)?;
        Ok((meta.min_ts, meta.max_ts))
    }

    /// Get per-column statistics from partition metadata.
    pub fn metadata_column_stats(
        partition_dir: &Path,
        col_name: &str,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<Option<ColumnStatistics>, SegmentError> {
        let meta = Self::read_meta(partition_dir, kek)?;
        Ok(meta.column_stats.get(col_name).cloned())
    }

    /// Check if a partition could contain rows matching a predicate.
    pub fn metadata_might_match(
        partition_dir: &Path,
        col_name: &str,
        predicate: &super::super::sparse_index::BlockPredicate,
        kek: Option<&WalEncryptionKey>,
    ) -> Result<bool, SegmentError> {
        let meta = Self::read_meta(partition_dir, kek)?;
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

/// Sniff + conditionally decrypt a raw file buffer.
///
/// - `kek = None`, file is plaintext → return as-is.
/// - `kek = None`, file is encrypted (`SEGT`) → `MissingKek`.
/// - `kek = Some`, file is encrypted → decrypt and return.
/// - `kek = Some`, file is plaintext → `UnexpectedPlaintext`.
pub(super) fn decrypt_segment_file(
    kek: Option<&WalEncryptionKey>,
    raw: &[u8],
) -> Result<Vec<u8>, SegmentError> {
    let encrypted = is_encrypted(raw)?;
    match (kek, encrypted) {
        (None, false) => Ok(raw.to_vec()),
        (None, true) => Err(SegmentError::MissingKek),
        (Some(key), true) => decrypt_file(key, raw),
        (Some(_), false) => Err(SegmentError::UnexpectedPlaintext),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{MetricSample, PartitionMeta, PartitionState};
    use tempfile::TempDir;

    use super::super::super::columnar_memtable::{
        ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, TimeKind,
    };
    use super::super::writer::ColumnarSegmentWriter;
    use super::*;

    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn test_kek() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    fn build_simple_drain() -> (
        TempDir,
        crate::engine::timeseries::columnar_memtable::ColumnarDrainResult,
    ) {
        let tmp = TempDir::new().unwrap();
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_000_000 + i * 1000,
                    value: i as f64 * 2.0,
                },
            );
        }
        (tmp, mt.drain())
    }

    #[test]
    fn column_projection() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), MILLIS),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Int64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 3],
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
        let meta = writer
            .write_partition("ts-proj", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        assert!(matches!(
            meta.column_stats["extra"].codec,
            ResolvedColumnCodec::Delta | ResolvedColumnCodec::DoubleDelta
        ));

        let part_dir = tmp.path().join("ts-proj");
        let projected = ColumnarSegmentReader::read_columns(
            &part_dir,
            &[
                ("timestamp".into(), MILLIS),
                ("value".into(), ColumnType::Float64),
            ],
            None,
        )
        .unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].len(), 20);
        assert_eq!(projected[1].len(), 20);
    }

    #[test]
    fn sparse_index_time_range_query() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..5000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 1000,
                    value: i as f64,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-range", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let part_dir = tmp.path().join("ts-range");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir, None)
            .unwrap()
            .unwrap();

        assert_eq!(sparse.block_count(), 5);

        let start = 1_700_000_000_000 + 2000 * 1000;
        let end = 1_700_000_000_000 + 3000 * 1000;
        let matching = sparse.blocks_in_time_range(start, end);
        assert!(matching.len() < 5, "should skip at least 1 block");
        assert!(!matching.is_empty());
    }

    #[test]
    fn sparse_index_predicate_pushdown() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), MILLIS),
                ("cpu".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, test_config());
        for i in 0..2048 {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(1_700_000_000_000 + i as i64 * 1000),
                    ColumnValue::Float64(if i < 1024 {
                        (i % 50) as f64
                    } else {
                        50.0 + (i % 50) as f64
                    }),
                ],
            )
            .unwrap();
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-pred", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let part_dir = tmp.path().join("ts-pred");
        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir, None)
            .unwrap()
            .unwrap();

        use crate::engine::timeseries::sparse_index::BlockPredicate;
        let preds = vec![BlockPredicate::GreaterThan {
            column_idx: 1,
            threshold: 60.0,
        }];
        let matching = sparse.filter_blocks(i64::MIN, i64::MAX, &preds);
        assert_eq!(matching, vec![1]);
    }

    #[test]
    fn metadata_only_queries() {
        let tmp = TempDir::new().unwrap();
        let writer = ColumnarSegmentWriter::new(tmp.path());

        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..1000 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_700_000_000_000 + i * 10_000,
                    value: 42.0 + i as f64 * 0.1,
                },
            );
        }
        let drain = mt.drain();
        writer
            .write_partition("ts-meta", &drain.view(), 86_400_000, 0, None)
            .unwrap();

        let part_dir = tmp.path().join("ts-meta");

        let count = ColumnarSegmentReader::metadata_row_count(&part_dir, None).unwrap();
        assert_eq!(count, 1000);

        let (min_ts, max_ts) = ColumnarSegmentReader::metadata_ts_range(&part_dir, None).unwrap();
        assert_eq!(min_ts, 1_700_000_000_000);
        assert_eq!(max_ts, 1_700_000_000_000 + 999 * 10_000);

        let stats = ColumnarSegmentReader::metadata_column_stats(&part_dir, "value", None)
            .unwrap()
            .unwrap();
        assert_eq!(stats.count, 1000);
        assert!(stats.min.unwrap() < 43.0);
        assert!(stats.max.unwrap() > 140.0);

        use crate::engine::timeseries::sparse_index::BlockPredicate;
        let pred = BlockPredicate::GreaterThan {
            column_idx: 0,
            threshold: 200.0,
        };
        let might =
            ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred, None).unwrap();
        assert!(!might);

        let pred2 = BlockPredicate::GreaterThan {
            column_idx: 0,
            threshold: 50.0,
        };
        let might2 =
            ColumnarSegmentReader::metadata_might_match(&part_dir, "value", &pred2, None).unwrap();
        assert!(might2);
    }

    #[test]
    fn legacy_partition_no_sparse_index() {
        let tmp = TempDir::new().unwrap();
        let part_dir = tmp.path().join("ts-legacy");
        std::fs::create_dir_all(&part_dir).unwrap();
        std::fs::write(
            part_dir.join("partition.meta"),
            sonic_rs::to_vec(&PartitionMeta {
                min_ts: 0,
                max_ts: 100,
                row_count: 10,
                size_bytes: 100,
                schema_version: 1,
                state: PartitionState::Sealed,
                interval_ms: 86_400_000,
                last_flushed_wal_lsn: 0,
                column_stats: std::collections::HashMap::new(),
                max_system_ts: 0,
            })
            .unwrap(),
        )
        .unwrap();

        let sparse = ColumnarSegmentReader::read_sparse_index(&part_dir, None).unwrap();
        assert!(sparse.is_none());
    }

    #[test]
    fn columnar_segment_refuses_plaintext_with_kek() {
        let (tmp, drain) = build_simple_drain();
        let writer = ColumnarSegmentWriter::new(tmp.path());
        // Write WITHOUT encryption.
        writer
            .write_partition("plain-part", &drain.view(), 86_400_000, 1, None)
            .unwrap();

        let part_dir = tmp.path().join("plain-part");
        let kek = test_kek();

        // Reading with a KEK must fail with UnexpectedPlaintext.
        let err = ColumnarSegmentReader::read_meta(&part_dir, Some(&kek)).unwrap_err();
        assert!(
            matches!(err, SegmentError::UnexpectedPlaintext),
            "expected UnexpectedPlaintext, got {err:?}"
        );
    }

    #[test]
    fn columnar_segment_refuses_encrypted_without_kek() {
        let kek = test_kek();
        let (tmp, drain) = build_simple_drain();
        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("enc-part2", &drain.view(), 86_400_000, 1, Some(&kek))
            .unwrap();

        let part_dir = tmp.path().join("enc-part2");

        // Reading WITHOUT a KEK must fail with MissingKek.
        let err = ColumnarSegmentReader::read_meta(&part_dir, None).unwrap_err();
        assert!(
            matches!(err, SegmentError::MissingKek),
            "expected MissingKek, got {err:?}"
        );
    }

    #[test]
    fn columnar_segment_tampered_ciphertext_rejected() {
        let kek = test_kek();
        let (tmp, drain) = build_simple_drain();
        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("tamper-part", &drain.view(), 86_400_000, 1, Some(&kek))
            .unwrap();

        let part_dir = tmp.path().join("tamper-part");
        let col_path = part_dir.join("timestamp.col");
        let mut bytes = std::fs::read(&col_path).unwrap();
        // Flip a byte inside ciphertext after the current envelope preamble.
        bytes[nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE + 2] ^= 0xFF;
        std::fs::write(&col_path, &bytes).unwrap();

        let err = ColumnarSegmentReader::read_column(&part_dir, "timestamp", MILLIS, Some(&kek))
            .unwrap_err();
        assert!(
            matches!(err, SegmentError::DecryptionFailed(_)),
            "expected DecryptionFailed, got {err:?}"
        );
    }

    #[test]
    fn mmap_integer_parsers_accept_unaligned_little_endian_subslices() {
        let mut i64_fixture = vec![0xFF];
        i64_fixture.extend_from_slice(&(-7i64).to_le_bytes());
        i64_fixture.extend_from_slice(&(i64::MAX).to_le_bytes());
        assert_eq!(
            ColumnarSegmentReader::mmap_as_i64(&i64_fixture[1..]).unwrap(),
            vec![-7, i64::MAX]
        );

        let mut u32_fixture = vec![0xFF];
        u32_fixture.extend_from_slice(&0x0102_0304u32.to_le_bytes());
        u32_fixture.extend_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            ColumnarSegmentReader::mmap_as_u32(&u32_fixture[1..]).unwrap(),
            vec![0x0102_0304, u32::MAX]
        );
    }

    #[test]
    fn mmap_integer_parsers_reject_partial_little_endian_words() {
        assert!(ColumnarSegmentReader::mmap_as_i64(&[0; 7]).is_err());
        assert!(ColumnarSegmentReader::mmap_as_u32(&[0; 3]).is_err());
    }
}
