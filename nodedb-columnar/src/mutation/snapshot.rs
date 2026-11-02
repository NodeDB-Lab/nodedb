// SPDX-License-Identifier: Apache-2.0

//! Lossless backup/restore of the columnar `MutationEngine`.
//!
//! `export_snapshot` captures the full in-memory state (memtable columns,
//! PK index, delete bitmaps, version stamps and chains, counters, schema) plus caller-supplied flushed-
//! segment blobs into a single `ColumnarEngineSnapshot` that can be round-
//! tripped through MessagePack and passed to `from_snapshot` to reconstruct
//! an identical engine.
//!
//! The `DictEncoded.reverse` map is NOT serialised — it is deterministically
//! rebuilt from `dictionary` on restore.

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::surrogate::Surrogate;
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::delete_bitmap::DeleteBitmap;
use crate::error::ColumnarError;
use crate::memtable::ColumnData;
use crate::pk_index::{PkIndex, RowLocation};
use crate::versions::{SegmentVersions, SegmentVersionsSnapshot, VersionChains};

use super::engine::MutationEngine;

/// Per-row cross-engine surrogates for flushed segments, parallel to the
/// flushed-segment blob Vec: outer index == segment index (segment_id ==
/// index + 1), inner Vec is per-row.
pub type FlushedSurrogateTable = Vec<Vec<Option<Surrogate>>>;

// ── Wire types ───────────────────────────────────────────────────────────────

/// A lossless projection of one `ColumnData` variant that survives a
/// MessagePack round-trip.
///
/// Every variant mirrors the corresponding `ColumnData` variant exactly,
/// except `DictEncoded` which drops the `reverse` map (rebuilt on import).
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub enum ColumnDataSnapshot {
    Int64 {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Option<Vec<bool>>,
    },
    Bool {
        values: Vec<bool>,
        valid: Option<Vec<bool>>,
    },
    Timestamp {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Decimal {
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    Uuid {
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    String {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Json {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Geometry {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Vector {
        data: Vec<f32>,
        dim: u32,
        valid: Option<Vec<bool>>,
    },
    /// Dictionary-encoded string column.
    ///
    /// `reverse` is NOT stored — it is rebuilt from `dictionary` on
    /// `from_snapshot`. The rebuild is O(n) in the dictionary size.
    DictEncoded {
        ids: Vec<u32>,
        dictionary: Vec<String>,
        valid: Option<Vec<bool>>,
    },
}

/// Complete serialisable snapshot of one `MutationEngine` instance.
///
/// Map-encoded (`#[msgpack(map)]`). The version stamps and chains are
/// required: a snapshot without them does not decode.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
#[msgpack(map)]
pub struct ColumnarEngineSnapshot {
    /// Collection name.
    pub collection: String,
    /// Schema at export time.
    pub schema: ColumnarSchema,
    /// Projected memtable columns (parallel to `schema.columns`).
    pub memtable_columns: Vec<ColumnDataSnapshot>,
    /// Per-row surrogates (parallel to memtable rows).
    pub memtable_surrogates: Vec<Option<Surrogate>>,
    /// Serialised `PkIndex` bytes (via `PkIndex::to_bytes()`).
    pub pk_index_bytes: Vec<u8>,
    /// Serialised closed-key index bytes (via `PkIndex::to_bytes()`): each
    /// key a bitemporal close unbound, to its tombstone row. Empty when no
    /// key was ever closed.
    pub closed_index_bytes: Vec<u8>,
    /// Serialised delete bitmaps for FLUSHED segments only.
    ///
    /// Excludes the entry keyed by `memtable_segment_id`; that is
    /// stored separately in `memtable_delete_bitmap_bytes`.
    pub delete_bitmaps: Vec<(u64, Vec<u8>)>,
    /// Serialised delete bitmap for the memtable's virtual segment,
    /// or empty bytes if no rows have been deleted from the memtable.
    pub memtable_delete_bitmap_bytes: Vec<u8>,
    /// Raw NDBS segment blobs for every flushed segment, in segment-ID
    /// order (index 0 == segment_id 1 by convention).
    pub flushed_segments: Vec<Vec<u8>>,
    /// Per-row cross-engine surrogates for each flushed segment, parallel to
    /// `flushed_segments` (outer index == segment index, segment_id == index+1;
    /// inner Vec is per-row). Its length equals `flushed_segments.len()`.
    pub flushed_surrogates: FlushedSurrogateTable,
    /// Next segment ID to be assigned on flush.
    pub next_segment_id: u64,
    /// Virtual segment ID currently used for memtable rows.
    pub memtable_segment_id: u64,
    /// Row counter within the current memtable (resets on flush).
    pub memtable_row_counter: u32,
    /// Version stamps of every segment, the memtable's virtual segment
    /// included, ascending by segment id.
    pub versions: Vec<SegmentVersionsSnapshot>,
    /// Per-key chains of ended versions as `(pk bytes, locations oldest
    /// first)`, ascending by key.
    pub chains: Vec<(Vec<u8>, Vec<RowLocation>)>,
}

// ── Export ────────────────────────────────────────────────────────────────────

impl MutationEngine {
    /// Export the full engine state as a lossless snapshot.
    ///
    /// `flushed_segments` must contain the raw NDBS blobs for every flushed
    /// segment, ordered by segment ID (index 0 == segment_id 1). The caller
    /// is responsible for reading the blobs from disk.
    ///
    /// `flushed_surrogates` carries the per-row cross-engine surrogates for
    /// each flushed segment, parallel to `flushed_segments` (outer index ==
    /// segment index). Both slices are empty for a collection that never
    /// flushed.
    pub fn export_snapshot(
        &self,
        flushed_segments: &[Vec<u8>],
        flushed_surrogates: &[Vec<Option<Surrogate>>],
    ) -> Result<ColumnarEngineSnapshot, ColumnarError> {
        // Project each memtable column to its snapshot form.
        let memtable_columns = self
            .memtable
            .columns()
            .iter()
            .map(column_to_snapshot)
            .collect::<Vec<_>>();

        // Serialise the PK index.
        let pk_index_bytes = self.pk_index.to_bytes()?;
        let closed_index_bytes = if self.closed_index.is_empty() {
            Vec::new()
        } else {
            self.closed_index.to_bytes()?
        };

        // Partition delete bitmaps: separate the memtable's virtual segment
        // from flushed-segment bitmaps.
        let mut delete_bitmaps: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut memtable_delete_bitmap_bytes = Vec::new();

        for (&seg_id, bitmap) in &self.delete_bitmaps {
            let bytes = bitmap.to_bytes()?;
            if seg_id == self.memtable_segment_id {
                memtable_delete_bitmap_bytes = bytes;
            } else {
                delete_bitmaps.push((seg_id, bytes));
            }
        }

        let mut versions: Vec<SegmentVersionsSnapshot> = self
            .versions
            .iter()
            .map(|(&seg_id, stamps)| stamps.to_snapshot(seg_id))
            .collect();
        versions.sort_unstable_by_key(|v| v.segment_id);

        Ok(ColumnarEngineSnapshot {
            collection: self.collection.clone(),
            schema: self.schema.clone(),
            memtable_columns,
            memtable_surrogates: self.memtable_surrogates.clone(),
            pk_index_bytes,
            closed_index_bytes,
            delete_bitmaps,
            memtable_delete_bitmap_bytes,
            flushed_segments: flushed_segments.to_vec(),
            flushed_surrogates: flushed_surrogates.to_vec(),
            next_segment_id: self.next_segment_id,
            memtable_segment_id: self.memtable_segment_id,
            memtable_row_counter: self.memtable_row_counter,
            versions,
            chains: self.chains.to_entries(),
        })
    }

    /// Reconstruct a `MutationEngine` from a previously exported snapshot.
    ///
    /// Returns `(engine, flushed_segment_blobs, flushed_surrogates)`. The
    /// caller is responsible for writing the blobs back to the appropriate
    /// on-disk locations and for re-attaching the surrogate sidecar. The
    /// surrogate Vec is parallel to the blob Vec (outer index == segment
    /// index).
    ///
    /// # Errors
    ///
    /// Returns `ColumnarError::SchemaMismatch` if the number of memtable
    /// columns in the snapshot does not match the schema column count.
    /// Returns `ColumnarError::Corruption` if the surrogate table length
    /// differs from the flushed segment count.
    /// Returns `ColumnarError::Serialization` if the PK index or any delete
    /// bitmap bytes are corrupt.
    /// Returns `ColumnarError::Corruption` if a column snapshot variant does
    /// not match the expected variant shape (mismatched field counts etc.).
    pub fn from_snapshot(
        snap: ColumnarEngineSnapshot,
    ) -> Result<(MutationEngine, Vec<Vec<u8>>, FlushedSurrogateTable), ColumnarError> {
        let col_count = snap.schema.columns.len();
        if snap.memtable_columns.len() != col_count {
            return Err(ColumnarError::SchemaMismatch {
                expected: col_count,
                got: snap.memtable_columns.len(),
            });
        }
        if snap.flushed_surrogates.len() != snap.flushed_segments.len() {
            return Err(ColumnarError::Corruption {
                segment_id: None,
                reason: format!(
                    "snapshot of '{}' carries {} surrogate tables for {} flushed segments",
                    snap.collection,
                    snap.flushed_surrogates.len(),
                    snap.flushed_segments.len()
                ),
                offset: None,
            });
        }

        // Rebuild PK index.
        let pk_index = PkIndex::from_bytes(&snap.pk_index_bytes)?;
        let closed_index = if snap.closed_index_bytes.is_empty() {
            PkIndex::new()
        } else {
            PkIndex::from_bytes(&snap.closed_index_bytes)?
        };

        // Rebuild flushed-segment delete bitmaps.
        let mut delete_bitmaps: HashMap<u64, DeleteBitmap> = HashMap::new();
        for (seg_id, bytes) in snap.delete_bitmaps {
            let bm = DeleteBitmap::from_bytes(&bytes)?;
            delete_bitmaps.insert(seg_id, bm);
        }

        // Restore memtable delete bitmap under the virtual segment ID.
        if !snap.memtable_delete_bitmap_bytes.is_empty() {
            let bm = DeleteBitmap::from_bytes(&snap.memtable_delete_bitmap_bytes)?;
            delete_bitmaps.insert(snap.memtable_segment_id, bm);
        }

        // Rebuild column data from snapshots.
        let columns: Vec<ColumnData> = snap
            .memtable_columns
            .into_iter()
            .map(snapshot_to_column)
            .collect();

        // Rebuild pk_col_indices from schema (mirrors MutationEngine::new).
        let pk_col_indices: Vec<usize> = snap
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        // Reconstruct the memtable from raw columns.
        let memtable = crate::memtable::ColumnarMemtable::from_raw_columns(
            &snap.schema,
            columns,
            snap.memtable_row_counter as usize,
        );

        let engine = MutationEngine {
            collection: snap.collection,
            schema: snap.schema,
            memtable,
            pk_index,
            closed_index,
            delete_bitmaps,
            pk_col_indices,
            next_segment_id: snap.next_segment_id,
            memtable_segment_id: snap.memtable_segment_id,
            memtable_row_counter: snap.memtable_row_counter,
            memtable_surrogates: snap.memtable_surrogates,
            versions: snap
                .versions
                .into_iter()
                .map(|v| (v.segment_id, SegmentVersions::from_snapshot(v)))
                .collect(),
            chains: VersionChains::from_entries(snap.chains),
        };

        Ok((engine, snap.flushed_segments, snap.flushed_surrogates))
    }
}

// ── Projection helpers ────────────────────────────────────────────────────────

/// Project a `ColumnData` into its serialisable `ColumnDataSnapshot` form.
fn column_to_snapshot(col: &ColumnData) -> ColumnDataSnapshot {
    match col {
        ColumnData::Int64 { values, valid } => ColumnDataSnapshot::Int64 {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::Float64 { values, valid } => ColumnDataSnapshot::Float64 {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::Bool { values, valid } => ColumnDataSnapshot::Bool {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::Timestamp { values, valid } => ColumnDataSnapshot::Timestamp {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::Decimal { values, valid } => ColumnDataSnapshot::Decimal {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::Uuid { values, valid } => ColumnDataSnapshot::Uuid {
            values: values.clone(),
            valid: valid.clone(),
        },
        ColumnData::String {
            data,
            offsets,
            valid,
        } => ColumnDataSnapshot::String {
            data: data.clone(),
            offsets: offsets.clone(),
            valid: valid.clone(),
        },
        ColumnData::Bytes {
            data,
            offsets,
            valid,
        } => ColumnDataSnapshot::Bytes {
            data: data.clone(),
            offsets: offsets.clone(),
            valid: valid.clone(),
        },
        ColumnData::Json {
            data,
            offsets,
            valid,
        } => ColumnDataSnapshot::Json {
            data: data.clone(),
            offsets: offsets.clone(),
            valid: valid.clone(),
        },
        ColumnData::Geometry {
            data,
            offsets,
            valid,
        } => ColumnDataSnapshot::Geometry {
            data: data.clone(),
            offsets: offsets.clone(),
            valid: valid.clone(),
        },
        ColumnData::Vector { data, dim, valid } => ColumnDataSnapshot::Vector {
            data: data.clone(),
            dim: *dim,
            valid: valid.clone(),
        },
        ColumnData::DictEncoded {
            ids,
            dictionary,
            valid,
            // `reverse` is intentionally dropped — rebuilt on import.
            ..
        } => ColumnDataSnapshot::DictEncoded {
            ids: ids.clone(),
            dictionary: dictionary.clone(),
            valid: valid.clone(),
        },
    }
}

/// Reconstruct a `ColumnData` from its `ColumnDataSnapshot`.
///
/// `DictEncoded.reverse` is rebuilt from `dictionary` (id == index).
fn snapshot_to_column(snap: ColumnDataSnapshot) -> ColumnData {
    match snap {
        ColumnDataSnapshot::Int64 { values, valid } => ColumnData::Int64 { values, valid },
        ColumnDataSnapshot::Float64 { values, valid } => ColumnData::Float64 { values, valid },
        ColumnDataSnapshot::Bool { values, valid } => ColumnData::Bool { values, valid },
        ColumnDataSnapshot::Timestamp { values, valid } => ColumnData::Timestamp { values, valid },
        ColumnDataSnapshot::Decimal { values, valid } => ColumnData::Decimal { values, valid },
        ColumnDataSnapshot::Uuid { values, valid } => ColumnData::Uuid { values, valid },
        ColumnDataSnapshot::String {
            data,
            offsets,
            valid,
        } => ColumnData::String {
            data,
            offsets,
            valid,
        },
        ColumnDataSnapshot::Bytes {
            data,
            offsets,
            valid,
        } => ColumnData::Bytes {
            data,
            offsets,
            valid,
        },
        ColumnDataSnapshot::Json {
            data,
            offsets,
            valid,
        } => ColumnData::Json {
            data,
            offsets,
            valid,
        },
        ColumnDataSnapshot::Geometry {
            data,
            offsets,
            valid,
        } => ColumnData::Geometry {
            data,
            offsets,
            valid,
        },
        ColumnDataSnapshot::Vector { data, dim, valid } => ColumnData::Vector { data, dim, valid },
        ColumnDataSnapshot::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            // Rebuild reverse map: string → id (id == index in dictionary).
            let reverse: HashMap<String, u32> = dictionary
                .iter()
                .enumerate()
                .map(|(i, s)| (s.clone(), i as u32))
                .collect();
            ColumnData::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            }
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;

    fn simple_schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("score", ColumnType::Float64),
            ],
            version: 1,
        }
    }

    fn insert_row(engine: &mut MutationEngine, id: i64, name: &str, score: Option<f64>) {
        let score_val = score.map(Value::Float).unwrap_or(Value::Null);
        engine
            .insert(
                &[Value::Integer(id), Value::String(name.into()), score_val],
                id * 10,
            )
            .expect("insert");
    }

    #[test]
    fn round_trip_memtable_rows() {
        let schema = simple_schema();
        let mut engine = MutationEngine::new("test_col".to_string(), schema);

        insert_row(&mut engine, 1, "Alice", Some(0.9));
        insert_row(&mut engine, 2, "Bob", None);
        insert_row(&mut engine, 3, "Carol", Some(0.5));

        // Export snapshot with no flushed segments.
        let snap = engine.export_snapshot(&[], &[]).expect("export");

        // Verify basic fields.
        assert_eq!(snap.collection, "test_col");
        assert_eq!(snap.memtable_row_counter, 3);
        assert_eq!(snap.next_segment_id, 1);
        assert_eq!(snap.memtable_segment_id, 0);
        assert_eq!(snap.flushed_segments.len(), 0);

        // Round-trip through MessagePack.
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");

        // Restore engine.
        let (restored, flushed, _) = MutationEngine::from_snapshot(snap2).expect("from_snapshot");
        assert!(flushed.is_empty());
        assert_eq!(restored.next_segment_id(), 1);
        assert_eq!(restored.memtable_segment_id(), 0);
        assert_eq!(restored.memtable_row_counter, 3);

        // PK index should have 3 entries.
        assert_eq!(restored.pk_index().len(), 3);

        // Scan rows — all 3 should be present.
        let rows: Vec<Vec<Value>> = restored
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Integer(1));
        assert_eq!(rows[1][1], Value::String("Bob".into()));
        assert_eq!(rows[2][2], Value::Float(0.5));
    }

    #[test]
    fn round_trip_with_tombstone() {
        let schema = simple_schema();
        let mut engine = MutationEngine::new("tombstone_test".to_string(), schema);

        insert_row(&mut engine, 10, "Xena", Some(1.0));
        insert_row(&mut engine, 20, "Yara", Some(2.0));

        // End row 1 (id=20) at ordinal 300.
        engine.delete(&Value::Integer(20), 300).expect("delete");

        let snap = engine.export_snapshot(&[], &[]).expect("export");

        // The memtable bitmap bytes must be non-empty.
        assert!(!snap.memtable_delete_bitmap_bytes.is_empty());

        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let (restored, _, _) = MutationEngine::from_snapshot(snap2).expect("from_snapshot");

        // scan_memtable_rows skips deleted row 1 (id=20).
        let rows: Vec<Vec<Value>> = restored
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Integer(10));

        // Delete bitmap must still be present.
        assert!(
            restored
                .delete_bitmap(restored.memtable_segment_id())
                .is_some_and(|bm| bm.is_deleted(1))
        );

        // The history round-trips: a read before the delete sees id=20.
        let at = |cut| -> Vec<i64> {
            restored
                .scan_memtable_versions(cut)
                .map(|r| match r.expect("read").2[0] {
                    Value::Integer(id) => id,
                    _ => -1,
                })
                .collect()
        };
        assert_eq!(at(crate::versions::VersionCut::At(250)), vec![10, 20]);
        assert_eq!(at(crate::versions::VersionCut::At(300)), vec![10]);
        assert_eq!(at(crate::versions::VersionCut::At(150)), vec![10]);
        assert_eq!(
            restored
                .version_chains()
                .get(&crate::pk_index::encode_pk(&Value::Integer(20)))
                .len(),
            1
        );
    }

    #[test]
    fn round_trip_flushed_segment_blob() {
        let schema = simple_schema();
        let engine = MutationEngine::new("flushed_test".to_string(), schema);

        // Simulate a pre-existing flushed segment as an opaque blob.
        let fake_blob: Vec<u8> = vec![0x4E, 0x44, 0x42, 0x53, 0x01, 0x02, 0x03]; // "NDBS" + junk

        let snap = engine
            .export_snapshot(std::slice::from_ref(&fake_blob), &[vec![None]])
            .expect("export");
        assert_eq!(snap.flushed_segments.len(), 1);

        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let (_, flushed, _) = MutationEngine::from_snapshot(snap2).expect("from_snapshot");

        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0], fake_blob);
    }

    #[test]
    fn schema_mismatch_rejected() {
        let schema = simple_schema();
        let engine = MutationEngine::new("mismatch".to_string(), schema);
        let mut snap = engine.export_snapshot(&[], &[]).expect("export");

        // Corrupt the snapshot: add an extra spurious column snapshot.
        snap.memtable_columns.push(ColumnDataSnapshot::Int64 {
            values: vec![],
            valid: None,
        });

        let result = MutationEngine::from_snapshot(snap);
        assert!(
            matches!(
                result,
                Err(ColumnarError::SchemaMismatch {
                    expected: 3,
                    got: 4
                })
            ),
            "expected SchemaMismatch error on extra column",
        );
    }

    #[test]
    fn pk_index_survives_round_trip() {
        let schema = simple_schema();
        let mut engine = MutationEngine::new("pk_test".to_string(), schema);

        for i in 0..5i64 {
            insert_row(&mut engine, i, &format!("u{i}"), None);
        }

        let snap = engine.export_snapshot(&[], &[]).expect("export");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let (restored, _, _) = MutationEngine::from_snapshot(snap2).expect("from_snapshot");

        assert_eq!(restored.pk_index().len(), 5);
        for i in 0..5i64 {
            let pk = crate::pk_index::encode_pk(&Value::Integer(i));
            assert!(restored.pk_index().contains(&pk), "missing pk {i}");
        }
    }

    #[test]
    fn counters_preserved() {
        let schema = simple_schema();
        let mut engine = MutationEngine::new("counters".to_string(), schema);

        insert_row(&mut engine, 99, "Z", Some(2.5));

        // Simulate having allocated several segment IDs already.
        engine.next_segment_id = 7;
        engine.memtable_segment_id = 6;

        let snap = engine.export_snapshot(&[], &[]).expect("export");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let (restored, _, _) = MutationEngine::from_snapshot(snap2).expect("from_snapshot");

        assert_eq!(restored.next_segment_id, 7);
        assert_eq!(restored.memtable_segment_id, 6);
        assert_eq!(restored.memtable_row_counter, 1);
    }

    #[test]
    fn flushed_surrogates_survive_round_trip() {
        let schema = simple_schema();
        let engine = MutationEngine::new("surr_test".to_string(), schema);

        // Two flushed segments, each with a per-row surrogate sidecar.
        let blob0: Vec<u8> = vec![0x4E, 0x44, 0x42, 0x53, 0xAA];
        let blob1: Vec<u8> = vec![0x4E, 0x44, 0x42, 0x53, 0xBB];
        let surrogates: FlushedSurrogateTable = vec![
            vec![Some(Surrogate::new(10)), None, Some(Surrogate::new(12))],
            vec![Some(Surrogate::new(20))],
        ];

        let snap = engine
            .export_snapshot(&[blob0.clone(), blob1.clone()], &surrogates)
            .expect("export");
        assert_eq!(snap.flushed_segments.len(), 2);
        assert_eq!(snap.flushed_surrogates.len(), 2);

        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: ColumnarEngineSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let (_, flushed, flushed_surrogates) =
            MutationEngine::from_snapshot(snap2).expect("from_snapshot");

        assert_eq!(flushed, vec![blob0, blob1]);
        assert_eq!(flushed_surrogates, surrogates);
    }

    #[test]
    fn missing_flushed_surrogates_is_refused() {
        // Encode a snapshot, then drop the `flushed_surrogates` key from the
        // map. The field is required, so the decode fails.
        let schema = simple_schema();
        let engine = MutationEngine::new("strict_test".to_string(), schema);
        let snap = engine.export_snapshot(&[], &[]).expect("export");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");

        let value: zerompk::Value<'_> = zerompk::from_msgpack(&bytes).expect("decode as value");
        let zerompk::Value::Map(entries) = value else {
            panic!("snapshot must encode as a map");
        };
        let before = entries.len();
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|(k, _)| {
                !matches!(k, zerompk::Value::String(name) if &**name == "flushed_surrogates")
            })
            .collect();
        assert_eq!(entries.len(), before - 1, "the field must be present");
        let stripped = zerompk::to_msgpack_vec(&zerompk::Value::Map(entries)).expect("re-encode");

        assert!(zerompk::from_msgpack::<ColumnarEngineSnapshot>(&stripped).is_err());
    }

    #[test]
    fn surrogate_table_length_mismatch_is_refused() {
        let schema = simple_schema();
        let engine = MutationEngine::new("mismatch_test".to_string(), schema);
        let blob: Vec<u8> = vec![0x4E, 0x44, 0x42, 0x53, 0x01];
        let snap = engine
            .export_snapshot(std::slice::from_ref(&blob), &[])
            .expect("export");

        assert!(matches!(
            MutationEngine::from_snapshot(snap),
            Err(ColumnarError::Corruption { .. })
        ));
    }

    /// A memtable JSON cell restored with bytes that are not MessagePack is
    /// refused on every read path, never read as NULL or as an absent row.
    #[test]
    fn a_corrupt_json_cell_is_refused_not_read_as_null() {
        let schema = ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::nullable("doc", ColumnType::Json),
            ],
            version: 1,
        };
        let mut engine = MutationEngine::new("json_col".to_string(), schema);
        engine
            .insert(&[Value::Integer(1), Value::Integer(5)], 1)
            .expect("insert");
        let mut snap = engine.export_snapshot(&[], &[]).expect("export");
        let Some(ColumnDataSnapshot::Json { data, offsets, .. }) = snap.memtable_columns.get_mut(1)
        else {
            panic!("doc column snapshots as JSON");
        };
        // 0xC1 is the one MessagePack marker that is never valid.
        *data = vec![0xC1];
        *offsets = vec![0, 1];
        let (restored, _, _) = MutationEngine::from_snapshot(snap).expect("from_snapshot");

        let is_corrupt = |e: &ColumnarError| matches!(e, ColumnarError::MemtableCellCorrupt { column, row: 0, .. } if column == "doc");
        let scanned: Vec<_> = restored.scan_memtable_rows().collect();
        assert_eq!(scanned.len(), 1);
        assert!(scanned[0].as_ref().is_err_and(is_corrupt));
        let with_surrogates: Vec<_> = restored.scan_memtable_rows_with_surrogates().collect();
        assert!(with_surrogates[0].as_ref().is_err_and(is_corrupt));
        assert!(restored.get_memtable_row(0).as_ref().is_err_and(is_corrupt));
        let pk = crate::pk_index::encode_pk(&Value::Integer(1));
        assert!(
            restored
                .lookup_memtable_row_by_pk(&pk)
                .as_ref()
                .is_err_and(is_corrupt)
        );
    }
}
