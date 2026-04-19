//! Columnar segment writer.

use std::collections::HashMap;
use std::path::PathBuf;

use nodedb_types::timeseries::{PartitionMeta, PartitionState};

use super::super::columnar_memtable::{ColumnType, ColumnarDrainResult, ColumnarSchema};
use super::codec::encode_column;
use super::error::SegmentError;
use super::schema::schema_to_json;
use super::util::dir_size;

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

        let mut column_stats = HashMap::new();
        let mut resolved_codecs = Vec::with_capacity(drain.schema.columns.len());

        for (i, (col_name, col_type)) in drain.schema.columns.iter().enumerate() {
            let col_data = &drain.columns[i];
            let requested_codec = drain.schema.codec(i);

            let (encoded, resolved_codec, stats) =
                encode_column(col_data, *col_type, requested_codec)?;

            let path = partition_dir.join(format!("{col_name}.col"));
            std::fs::write(&path, &encoded)
                .map_err(|e| SegmentError::Io(format!("write {}: {e}", path.display())))?;

            // Write symbol dictionary for tag columns.
            if *col_type == ColumnType::Symbol
                && let Some(dict) = drain.symbol_dicts.get(&i)
            {
                let dict_json = sonic_rs::to_vec(dict)
                    .map_err(|e| SegmentError::Io(format!("serialize dict: {e}")))?;
                let sym_path = partition_dir.join(format!("{col_name}.sym"));
                std::fs::write(&sym_path, &dict_json)
                    .map_err(|e| SegmentError::Io(format!("write {}: {e}", sym_path.display())))?;
            }

            column_stats.insert(col_name.clone(), stats);
            resolved_codecs.push(resolved_codec);
        }

        // Write schema with resolved codecs.
        let schema_with_codecs = ColumnarSchema {
            columns: drain.schema.columns.clone(),
            timestamp_idx: drain.schema.timestamp_idx,
            codecs: resolved_codecs,
        };
        let schema_json = sonic_rs::to_vec(&schema_to_json(&schema_with_codecs))
            .map_err(|e| SegmentError::Io(format!("serialize schema: {e}")))?;
        std::fs::write(partition_dir.join("schema.json"), &schema_json)
            .map_err(|e| SegmentError::Io(format!("write schema: {e}")))?;

        // Build and write sparse index from raw (pre-compression) column data.
        let sparse_idx = super::super::sparse_index::SparseIndex::build(
            &drain.columns,
            &drain.schema,
            drain.row_count,
            super::super::sparse_index::DEFAULT_BLOCK_SIZE,
        );
        let sparse_bytes = sparse_idx.to_bytes();
        std::fs::write(partition_dir.join("sparse_index.bin"), &sparse_bytes)
            .map_err(|e| SegmentError::Io(format!("write sparse index: {e}")))?;

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
            column_stats,
        };

        let meta_json = sonic_rs::to_vec(&meta)
            .map_err(|e| SegmentError::Io(format!("serialize meta: {e}")))?;
        std::fs::write(partition_dir.join("partition.meta"), &meta_json)
            .map_err(|e| SegmentError::Io(format!("write meta: {e}")))?;

        Ok(meta)
    }
}
