// SPDX-License-Identifier: BUSL-1.1

//! Rebuilding the geometry R-tree entries of a restored columnar collection from
//! its restored rows.
//!
//! ## Why the checkpoint cannot leave this to the spatial checkpoint
//!
//! A `Geometry` column on a columnar collection is indexed as a live side-effect
//! of the insert: `execute_columnar_insert` calls
//! `index_columnar_geometry_columns`, which populates `CoreLoop::spatial_indexes`
//! and `spatial_doc_map`. Those two maps have their own checkpoint
//! (`spatial_checkpoint/`), and before the columnar replay floor existed they
//! also had a second, unconditional source: full WAL replay re-ran every insert
//! and therefore re-ran the indexing.
//!
//! Installing a columnar floor removes that second source. The floor suppresses
//! exactly the insert records whose geometry entries the R-tree would otherwise
//! be rebuilt from, which would leave the R-tree dependent on the spatial
//! checkpoint alone — and that checkpoint reports no LSN and logs its write
//! failures rather than propagating them. A spatial flush that failed in the
//! same cycle this one succeeded would then silently lose the R-tree entries for
//! every gated row, with the rows themselves still present: spatial predicates
//! would stop matching rows that a full scan still returns.
//!
//! Rebuilding here removes the cross-engine dependency instead of ranking the
//! two checkpoints against each other. The R-tree is a derived index over rows
//! this generation already restored, so it is reconstructible from them and does
//! not belong in the checkpoint file. It is idempotent: the shared indexing
//! helper removes a document's existing entries before inserting — the same
//! property that lets WAL redo of a document collection's `Put`s (via
//! `apply_point_put_spatial`) safely re-index document geometry over whatever a
//! restored spatial checkpoint already holds.
//!
//! ## What this rebuild can and cannot see
//!
//! The live insert path indexes each row under the `id` field of the PAYLOAD it
//! was handed. This rebuild reads the row back from the engine, so it can only
//! offer the columns the SCHEMA declares — the entries agree exactly when `id`
//! is a declared string column, which is the case for a collection whose DDL
//! declares it.
//!
//! A row whose `id` reached the insert path as an undeclared payload field is
//! not indexed here — and cannot be: an undeclared field is never written into
//! the segment or the memtable, so no restore path could recover it. Such a row
//! is equally invisible to any read of the restored rows, not just to this one;
//! the identity was already lost at write time, and this rebuild neither creates
//! nor widens that gap.

use super::super::core_loop::CoreLoop;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
use nodedb_types::RlsWriteCheck;
use nodedb_types::columnar::ColumnType;

impl CoreLoop {
    /// Re-index every `Geometry` column of a restored collection into the
    /// R-tree, from both its restored memtable rows and its restored flushed
    /// segments. Returns the number of rows fed to the indexer.
    ///
    /// A no-op — without decoding anything — for the overwhelmingly common case
    /// of a collection with no geometry column.
    ///
    /// `Err` when a restored row does not read: a segment that does not open
    /// or decode, a corrupt cell, or a corrupt memtable cell. The rebuilt
    /// R-tree would miss that row, so the restore is refused and the core
    /// does not come up.
    pub(super) fn restore_columnar_geometry_indexes(
        &mut self,
        key: &(DatabaseId, TenantId, String),
        engine: &nodedb_columnar::MutationEngine,
        segments: &[Vec<u8>],
    ) -> crate::Result<usize> {
        let (db_id, tenant_id, collection) = key;
        let schema = engine.schema().clone();
        if !schema
            .columns
            .iter()
            .any(|c| c.column_type == ColumnType::Geometry)
        {
            return Ok(0);
        }

        // Every fallible read runs before the spatial maps change, so a
        // refused restore leaves them as the spatial checkpoint loaded them.
        let mut rows = self.restored_flushed_rows(engine, segments, &schema, collection)?;
        let memtable_segment_id = engine.memtable_segment_id();
        for row_idx in 0..engine.memtable().row_count() {
            let Some(row) = engine.get_memtable_row(row_idx)? else {
                continue;
            };
            if is_current(engine, &row, memtable_segment_id, row_idx)? {
                rows.push(row);
            }
        }
        // The checkpoint key carries the stored, database-qualified name.
        let vshard = nodedb_types::CollectionKey::from_qualified_str(*db_id, collection)?.vshard();

        // The R-tree is derived from the restored rows and nothing else. An
        // entry a restored spatial checkpoint holds for a row this generation
        // no longer has (deleted or truncated after that checkpoint) must not
        // survive, so the collection's entries are dropped before the rebuild.
        self.spatial_indexes
            .retain(|(d, t, c, _), _| !(d == db_id && t == tenant_id && c == collection));
        self.spatial_doc_map
            .retain(|(d, t, c, _, _), _| !(d == db_id && t == tenant_id && c == collection));

        // The indexer takes documents, not positional rows: rebuild each row as
        // the `Value::Object` shape the live insert path hands it, so the two
        // agree by construction rather than by a second implementation of the
        // geometry extraction that could drift from it.
        let docs: Vec<nodedb_types::Value> = rows
            .iter()
            .map(|row| {
                let mut obj = std::collections::HashMap::with_capacity(schema.columns.len());
                for (i, col) in schema.columns.iter().enumerate() {
                    if let Some(v) = row.get(i) {
                        obj.insert(col.name.clone(), v.clone());
                    }
                }
                nodedb_types::Value::Object(obj)
            })
            .collect();

        // `wal_lsn: None` is load-bearing: this restore re-derives an index from
        // rows that are already durable, and is not itself a write. Noting an
        // LSN here would raise the core watermark during boot from a path that
        // applied no record.
        let task = Self::replay_task(
            *tenant_id,
            *db_id,
            vshard,
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                payload: Vec::new(),
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
                system_from: None,
                // No predicate here: this restore re-derives an index from
                // rows that are already durable and admits no new write. The
                // writing identity that admitted those rows is gone.
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            None,
        );

        let indexed = docs.len();
        // Boot-time rebuild: nothing to roll back, so the delta is dropped. A
        // restored row set in mixed SRIDs refuses the restore, as an
        // unreadable row does.
        self.index_columnar_geometry_columns(&task, &schema, collection, &docs)?;
        Ok(indexed)
    }

    /// Decode the live (non-tombstoned) rows of every restored flushed segment.
    ///
    /// Mirrors `scan_flushed.rs`: segment ids are 1-based because id 0 is the
    /// memtable's virtual segment, so `segments[i]` is `segment_id i + 1`, and a
    /// row whose delete-bitmap bit is set is not a row any more.
    ///
    /// `Err` on the first segment that does not open or decode, or the first
    /// corrupt cell of a live row. The shared segment reader files the
    /// corruption report.
    fn restored_flushed_rows(
        &self,
        engine: &nodedb_columnar::MutationEngine,
        segments: &[Vec<u8>],
        schema: &nodedb_types::columnar::ColumnarSchema,
        collection: &str,
    ) -> crate::Result<Vec<Vec<nodedb_types::value::Value>>> {
        let mut out = Vec::new();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            let seg_id = seg_idx as u64 + 1;
            let segment = self.decode_flushed_segment(
                collection,
                seg_id,
                seg_bytes,
                schema.columns.len(),
                "columnar_checkpoint_geometry_restore",
            )?;
            let delete_bm = engine.delete_bitmap(seg_id);
            for row_idx in 0..segment.row_count() {
                if delete_bm.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                    continue;
                }
                let row = segment.row(schema, row_idx)?;
                if is_current(engine, &row, seg_id, row_idx)? {
                    out.push(row);
                }
            }
        }
        Ok(out)
    }
}

/// Whether the live row at `(segment_id, row_idx)` is its key's current
/// version, the one the R-tree indexes. Every live row of a collection that
/// is not bitemporal, or has no primary key, is. In a bitemporal collection
/// only the row its key is bound to is: a superseded version and a tombstone
/// are history.
///
/// `Err` when the row's primary key does not encode.
fn is_current(
    engine: &nodedb_columnar::MutationEngine,
    row: &[nodedb_types::value::Value],
    segment_id: u64,
    row_idx: usize,
) -> crate::Result<bool> {
    if !engine.schema().is_bitemporal() || engine.pk_col_indices().is_empty() {
        return Ok(true);
    }
    let pk_bytes = engine.encode_pk_from_row(row)?;
    let bound = engine.pk_index().get(&pk_bytes).copied();
    Ok(bound.is_some_and(|loc| {
        loc.segment_id == segment_id && u32::try_from(row_idx).is_ok_and(|r| r == loc.row_index)
    }))
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, TenantId};

    type EngineKey = (DatabaseId, TenantId, String);

    fn key() -> EngineKey {
        (DatabaseId::DEFAULT, TenantId::new(1), "geo".to_string())
    }

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("loc", ColumnType::Geometry),
        ])
        .expect("valid")
    }

    /// An engine whose one row lives only in a flushed segment, encoded as
    /// the flush path encodes it. Returns the engine and the segment bytes.
    fn flushed_engine(core: &CoreLoop) -> (MutationEngine, Vec<u8>) {
        let key = key();
        let mut engine = MutationEngine::new(key.2.clone(), schema());
        engine
            .insert(&[
                Value::String("a".into()),
                Value::String(r#"{"type":"Point","coordinates":[1.0,2.0]}"#.into()),
            ])
            .expect("insert");
        let segment_id = engine.next_segment_id();
        let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            key.0,
            key.1,
            nodedb_mem::EngineId::Columnar,
        );
        let blob =
            nodedb_columnar::SegmentWriter::new(nodedb_columnar::writer::PROFILE_PLAIN, memory)
                .write_segment(&seg_schema, &columns, row_count, None)
                .expect("write_segment");
        engine
            .on_memtable_flushed(segment_id)
            .expect("on_memtable_flushed");
        (engine, blob)
    }

    #[test]
    fn a_readable_segment_rebuilds_its_geometry_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let (engine, blob) = flushed_engine(&core);
        let indexed = core
            .restore_columnar_geometry_indexes(&key(), &engine, &[blob])
            .expect("restore");
        assert_eq!(indexed, 1);
    }

    #[test]
    fn an_unopenable_segment_refuses_the_restore() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let (engine, _blob) = flushed_engine(&core);
        let result = core.restore_columnar_geometry_indexes(&key(), &engine, &[Vec::new()]);
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 1 of 'geo'")),
            "{result:?}"
        );
    }

    /// A segment that opens and decodes, but whose geometry cell is not
    /// UTF-8 text. Skipping it would leave the row out of the R-tree while
    /// a full scan still sees the segment, so the restore is refused.
    #[test]
    fn a_corrupt_segment_cell_refuses_the_restore() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = key();
        let columns = vec![
            nodedb_columnar::memtable::ColumnData::String {
                data: b"a".to_vec(),
                offsets: vec![0, 1],
                valid: None,
            },
            nodedb_columnar::memtable::ColumnData::Geometry {
                data: vec![0xFF, 0xFE],
                offsets: vec![0, 2],
                valid: Some(vec![true]),
            },
        ];
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            key.0,
            key.1,
            nodedb_mem::EngineId::Columnar,
        );
        let blob =
            nodedb_columnar::SegmentWriter::new(nodedb_columnar::writer::PROFILE_PLAIN, memory)
                .write_segment(&schema(), &columns, 1, None)
                .expect("write_segment");
        let engine = MutationEngine::new(key.2.clone(), schema());

        let result = core.restore_columnar_geometry_indexes(&key, &engine, &[blob]);
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 1 of 'geo'")),
            "{result:?}"
        );
    }
}
