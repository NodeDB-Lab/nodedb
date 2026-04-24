//! Plain columnar profile temporal-purge.
//!
//! Row-level audit purge on bitemporal plain-columnar collections.
//! Walks sealed segments (via [`nodedb_columnar::SegmentReader`]) and the
//! live memtable, groups rows by primary key, and marks every
//! *superseded* row whose `_ts_system` is below the cutoff in the
//! engine's per-segment delete bitmap.
//!
//! The single latest version per PK is always preserved — even if it is
//! itself below the cutoff — so "AS OF" reads beyond the cutoff can still
//! resolve each logical row's terminal state.

use std::collections::HashMap;

use nodedb_types::TenantId;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::decoded_col_to_value;

pub(super) struct RowVersion {
    pub seg_id: u32,
    pub row_idx: u32,
    pub pk_bytes: Vec<u8>,
    pub sys_ts: i64,
}

impl CoreLoop {
    /// See module docs. Returns the number of rows tombstoned in
    /// per-segment delete bitmaps. No WAL records are appended here; the
    /// caller is responsible for the `RecordType::TemporalPurge` audit
    /// record that covers the batch.
    pub(super) fn plain_columnar_purge(
        &mut self,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        let key = (tid, collection.to_string());
        let engine = match self.columnar_engines.get(&key) {
            Some(e) => e,
            None => return Ok(0),
        };
        let schema = engine.schema();
        if !schema.is_bitemporal() {
            return Ok(0);
        }
        let ts_idx = schema
            .ts_system_idx()
            .ok_or_else(|| crate::Error::Storage {
                engine: "columnar".into(),
                detail: "bitemporal schema missing _ts_system column".into(),
            })?;
        let pk_indices: Vec<usize> = engine.pk_col_indices().to_vec();
        if pk_indices.is_empty() {
            return Err(crate::Error::Storage {
                engine: "columnar".into(),
                detail: "bitemporal collection without primary key columns".into(),
            });
        }

        let mut versions: Vec<RowVersion> = Vec::new();

        // Flushed segments: segment_id starts at 1 (memtable is id 0).
        if let Some(segments) = self.columnar_flushed_segments.get(&key) {
            for (seg_idx, seg_bytes) in segments.iter().enumerate() {
                let seg_id = seg_idx as u32 + 1;
                let reader = nodedb_columnar::SegmentReader::open(seg_bytes).map_err(|e| {
                    crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!("open segment for purge: {e}"),
                    }
                })?;
                let ts_col = reader
                    .read_column(ts_idx)
                    .map_err(|e| crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!("read _ts_system: {e}"),
                    })?;
                let pk_cols: Vec<nodedb_columnar::reader::DecodedColumn> = pk_indices
                    .iter()
                    .map(|&i| reader.read_column(i))
                    .collect::<Result<_, _>>()
                    .map_err(|e| crate::Error::Storage {
                        engine: "columnar".into(),
                        detail: format!("read pk column: {e}"),
                    })?;
                let row_count = reader.row_count() as usize;
                for row_idx in 0..row_count {
                    let Some(sys_ts) = int64_from_decoded(&ts_col, row_idx) else {
                        continue;
                    };
                    let pk_bytes = encode_pk_from_decoded_cols(&pk_cols, row_idx);
                    versions.push(RowVersion {
                        seg_id,
                        row_idx: row_idx as u32,
                        pk_bytes,
                        sys_ts,
                    });
                }
            }
        }

        // Memtable rows.
        let memtable_seg_id = engine.memtable_segment_id();
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();
        for (row_idx, row) in rows.iter().enumerate() {
            let sys_ts = match row.get(ts_idx) {
                Some(nodedb_types::value::Value::Integer(n)) => *n,
                _ => continue,
            };
            let pk_values: Vec<&nodedb_types::value::Value> =
                pk_indices.iter().filter_map(|&i| row.get(i)).collect();
            if pk_values.len() != pk_indices.len() {
                continue;
            }
            let pk_bytes = if pk_values.len() == 1 {
                nodedb_columnar::pk_index::encode_pk(pk_values[0])
            } else {
                nodedb_columnar::pk_index::encode_composite_pk(&pk_values)
            };
            versions.push(RowVersion {
                seg_id: memtable_seg_id,
                row_idx: row_idx as u32,
                pk_bytes,
                sys_ts,
            });
        }

        // Find latest system_ts per PK.
        let mut latest: HashMap<Vec<u8>, i64> = HashMap::new();
        for v in &versions {
            latest
                .entry(v.pk_bytes.clone())
                .and_modify(|cur| {
                    if v.sys_ts > *cur {
                        *cur = v.sys_ts;
                    }
                })
                .or_insert(v.sys_ts);
        }

        // Victims: superseded AND below cutoff AND not already tombstoned.
        let mut victims_per_seg: HashMap<u32, Vec<u32>> = HashMap::new();
        for v in versions {
            let lat = latest.get(&v.pk_bytes).copied().unwrap_or(v.sys_ts);
            if v.sys_ts < cutoff_system_ms && v.sys_ts < lat {
                let already = engine
                    .delete_bitmap(v.seg_id)
                    .is_some_and(|bm| bm.is_deleted(v.row_idx));
                if !already {
                    victims_per_seg.entry(v.seg_id).or_default().push(v.row_idx);
                }
            }
        }

        if victims_per_seg.is_empty() {
            return Ok(0);
        }

        let engine_mut = self
            .columnar_engines
            .get_mut(&key)
            .expect("engine vanished mid-purge");
        let mut total = 0usize;
        for (seg_id, mut row_indices) in victims_per_seg {
            row_indices.sort_unstable();
            row_indices.dedup();
            total += row_indices.len();
            engine_mut
                .delete_bitmap_mut(seg_id)
                .mark_deleted_batch(&row_indices);
        }
        Ok(total)
    }
}

fn int64_from_decoded(col: &nodedb_columnar::reader::DecodedColumn, row_idx: usize) -> Option<i64> {
    use nodedb_columnar::reader::DecodedColumn;
    match col {
        DecodedColumn::Int64 { values, valid } | DecodedColumn::Timestamp { values, valid } => {
            (row_idx < valid.len() && valid[row_idx]).then(|| values[row_idx])
        }
        _ => None,
    }
}

fn encode_pk_from_decoded_cols(
    cols: &[nodedb_columnar::reader::DecodedColumn],
    row_idx: usize,
) -> Vec<u8> {
    let values: Vec<nodedb_types::value::Value> = cols
        .iter()
        .map(|c| decoded_col_to_value(c, row_idx))
        .collect();
    if values.len() == 1 {
        nodedb_columnar::pk_index::encode_pk(&values[0])
    } else {
        let refs: Vec<&nodedb_types::value::Value> = values.iter().collect();
        nodedb_columnar::pk_index::encode_composite_pk(&refs)
    }
}
