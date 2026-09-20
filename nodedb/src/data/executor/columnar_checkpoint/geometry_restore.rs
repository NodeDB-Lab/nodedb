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

use tracing::warn;

use super::super::core_loop::CoreLoop;
use super::super::scan_normalize::decoded_col_to_value;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};
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
    pub(super) fn restore_columnar_geometry_indexes(
        &mut self,
        key: &(DatabaseId, TenantId, String),
        engine: &nodedb_columnar::MutationEngine,
        segments: &[Vec<u8>],
    ) -> usize {
        let (db_id, tenant_id, collection) = key;
        let schema = engine.schema().clone();
        if !schema
            .columns
            .iter()
            .any(|c| c.column_type == ColumnType::Geometry)
        {
            return 0;
        }

        // The R-tree is derived from the restored rows and nothing else. An
        // entry a restored spatial checkpoint holds for a row this generation
        // no longer has (deleted or truncated after that checkpoint) must not
        // survive, so the collection's entries are dropped before the rebuild.
        self.spatial_indexes
            .retain(|(d, t, c, _), _| !(d == db_id && t == tenant_id && c == collection));
        self.spatial_doc_map
            .retain(|(d, t, c, _, _), _| !(d == db_id && t == tenant_id && c == collection));

        let mut rows: Vec<Vec<nodedb_types::value::Value>> = Vec::new();
        rows.extend(Self::restored_flushed_rows(
            engine, segments, &schema, collection,
        ));
        rows.extend(engine.scan_memtable_rows());

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
            VShardId::from_collection_in_database(*db_id, collection),
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
        // Boot-time rebuild: nothing to roll back, so the delta is dropped.
        let _ = self.index_columnar_geometry_columns(&task, &schema, collection, &docs);
        indexed
    }

    /// Decode the live (non-tombstoned) rows of every restored flushed segment.
    ///
    /// Mirrors `scan_flushed.rs`: segment ids are 1-based because id 0 is the
    /// memtable's virtual segment, so `segments[i]` is `segment_id i + 1`, and a
    /// row whose delete-bitmap bit is set is not a row any more.
    ///
    /// A segment that fails to open or decode is warned about and skipped rather
    /// than aborting the restore: this rebuilds a derived index, and skipping
    /// costs the same geometry entries a `scan_flushed` over the identical
    /// unreadable bytes would also fail to produce.
    fn restored_flushed_rows(
        engine: &nodedb_columnar::MutationEngine,
        segments: &[Vec<u8>],
        schema: &nodedb_types::columnar::ColumnarSchema,
        collection: &str,
    ) -> Vec<Vec<nodedb_types::value::Value>> {
        let mut out = Vec::new();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            let seg_id = seg_idx as u64 + 1;
            let reader = match nodedb_columnar::SegmentReader::open(seg_bytes) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        %collection,
                        seg_id,
                        error = %e,
                        "columnar checkpoint restore: flushed segment unreadable; its \
                         geometry rows are absent from the rebuilt R-tree"
                    );
                    continue;
                }
            };

            let mut decoded_cols = Vec::with_capacity(schema.columns.len());
            let mut decode_ok = true;
            for col_idx in 0..schema.columns.len() {
                match reader.read_column(col_idx) {
                    Ok(dc) => decoded_cols.push(dc),
                    Err(e) => {
                        warn!(
                            %collection,
                            seg_id,
                            col_idx,
                            error = %e,
                            "columnar checkpoint restore: column decode failed; the \
                             segment's geometry rows are absent from the rebuilt R-tree"
                        );
                        decode_ok = false;
                        break;
                    }
                }
            }
            if !decode_ok {
                continue;
            }

            let delete_bm = engine.delete_bitmap(seg_id);
            for row_idx in 0..reader.row_count() as usize {
                if delete_bm.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                    continue;
                }
                out.push(
                    decoded_cols
                        .iter()
                        .zip(&schema.columns)
                        .map(|(dc, col_def)| {
                            decoded_col_to_value(dc, row_idx, &col_def.column_type)
                        })
                        .collect(),
                );
            }
        }
        out
    }
}
