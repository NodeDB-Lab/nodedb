// SPDX-License-Identifier: BUSL-1.1

//! The one reader every flushed columnar segment read goes through.
//!
//! A segment that does not open, a column that does not decode, and a cell
//! that does not hold its declared type are all corruption. Each one returns
//! a typed error and files one corruption report here, where it is detected.
//! No caller skips a segment or a row it cannot read: a skipped row reads as
//! an absent row, which is a wrong answer, not a degraded one.
//!
//! A read decodes only the 1024-row blocks that can hold a row it returns
//! ([`SegmentBlocks`]). A block outside the selection is never decompressed,
//! and its rows are not read.

use nodedb_columnar::predicate::ScanPredicate;
use nodedb_columnar::reader::DecodedColumn;
use nodedb_columnar::{BlockSelection, MutationEngine, VersionCut};
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_mutation_apply::ColumnarEngineKey;
use crate::data::executor::scan_normalize::decoded_col_to_value;
use crate::storage::quarantine::CollectionScope;

/// Which blocks of a flushed segment a read decodes.
pub(in crate::data::executor) enum SegmentBlocks<'a> {
    /// The blocks that hold a row a read at `cut` sees (`visible_blocks`)
    /// and whose statistics can match every one of `predicates`.
    Visible {
        engine: &'a MutationEngine,
        cut: VersionCut,
        predicates: &'a [ScanPredicate],
    },
    /// The one block that holds row `row`.
    Row(usize),
}

/// One flushed segment with every schema column decoded in the selected
/// blocks.
pub(in crate::data::executor) struct DecodedFlushedSegment<'a> {
    collection: &'a str,
    segment_id: u64,
    site: &'static str,
    row_count: usize,
    selection: BlockSelection,
    columns: Vec<DecodedColumn>,
}

impl CoreLoop {
    /// Open flushed segment `segment_id` of collection `key` and decode its
    /// first `column_count` columns in the blocks `blocks` selects.
    ///
    /// The open goes through the quarantine registry when one is wired, so a
    /// repeated CRC failure quarantines the segment. The quarantine record is
    /// scoped to the database and tenant in `key`. `site` names the read path
    /// in the corruption report.
    pub(in crate::data::executor) fn decode_flushed_segment<'a>(
        &self,
        key: &'a ColumnarEngineKey,
        segment_id: u64,
        seg_bytes: &[u8],
        column_count: usize,
        blocks: SegmentBlocks<'_>,
        site: &'static str,
    ) -> crate::Result<DecodedFlushedSegment<'a>> {
        let collection = key.2.as_str();
        let report = |err: crate::Error, stage: &'static str| {
            crate::diag::columnar_segment_corrupt(&err, collection, segment_id, stage, site);
            err
        };
        let opened = match &self.quarantine_registry {
            Some(reg) => crate::storage::quarantine::engines::open_segment_with_quarantine(
                reg,
                seg_bytes,
                &CollectionScope::new(key.0.as_u64(), key.1.as_u64(), collection),
                segment_id,
            )
            .map_err(crate::Error::from),
            None => nodedb_columnar::SegmentReader::open(seg_bytes).map_err(crate::Error::from),
        };
        let reader =
            opened.map_err(|e| report(segment_error(collection, segment_id, e), "open"))?;
        let row_count = usize::try_from(reader.row_count()).map_err(|_| {
            report(
                crate::Error::SegmentCorrupted {
                    detail: format!(
                        "columnar segment {segment_id} of '{collection}' reports {} rows, \
                         more than this machine can address",
                        reader.row_count()
                    ),
                },
                "open",
            )
        })?;
        let block_count = reader.block_count();
        let selection = match blocks {
            SegmentBlocks::Visible {
                engine,
                cut,
                predicates,
            } => engine
                .visible_blocks(segment_id, row_count, cut)
                .intersect(&reader.select_blocks(predicates)),
            SegmentBlocks::Row(row) => BlockSelection::only_row(block_count, row),
        };
        let col_indices: Vec<usize> = (0..column_count).collect();
        let columns = reader
            .read_columns_selected(&col_indices, &selection)
            .map_err(|e| {
                report(
                    segment_error(collection, segment_id, crate::Error::from(e)),
                    "column",
                )
            })?;
        Ok(DecodedFlushedSegment {
            collection,
            segment_id,
            site,
            row_count,
            selection,
            columns,
        })
    }
}

impl DecodedFlushedSegment<'_> {
    /// Rows the segment holds, tombstoned ones included.
    pub(in crate::data::executor) fn row_count(&self) -> usize {
        self.row_count
    }

    /// Whether the read decoded the block that holds row `row_idx`. A row
    /// outside the decoded blocks is one the read never returns.
    pub(in crate::data::executor) fn holds_row(&self, row_idx: usize) -> bool {
        self.selection.holds_row(row_idx)
    }

    /// Row `row_idx`, each cell typed by its declared column. `Internal`
    /// when the read did not decode the row's block.
    pub(in crate::data::executor) fn row(
        &self,
        schema: &ColumnarSchema,
        row_idx: usize,
    ) -> crate::Result<Vec<Value>> {
        if !self.holds_row(row_idx) {
            return Err(crate::Error::Internal {
                detail: format!(
                    "columnar segment {} of '{}': row {row_idx} is read outside the \
                     decoded blocks ({})",
                    self.segment_id, self.collection, self.site
                ),
            });
        }
        self.columns
            .iter()
            .zip(&schema.columns)
            .map(|(col, def)| self.decode_cell(col, row_idx, &def.column_type))
            .collect()
    }

    fn decode_cell(
        &self,
        col: &DecodedColumn,
        row_idx: usize,
        declared: &ColumnType,
    ) -> crate::Result<Value> {
        decoded_col_to_value(col, row_idx, declared).map_err(|e| {
            let err = segment_error(self.collection, self.segment_id, e);
            crate::diag::columnar_segment_corrupt(
                &err,
                self.collection,
                self.segment_id,
                "cell",
                self.site,
            );
            err
        })
    }
}

/// `err` with the segment and collection named in its detail when it is a
/// corruption error, so the refusal says which segment is damaged.
fn segment_error(collection: &str, segment_id: u64, err: crate::Error) -> crate::Error {
    match err {
        crate::Error::SegmentCorrupted { detail } => crate::Error::SegmentCorrupted {
            detail: format!("columnar segment {segment_id} of '{collection}': {detail}"),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("doc", ColumnType::Json),
        ])
        .expect("valid")
    }

    fn engine_key(collection: &str) -> ColumnarEngineKey {
        (
            crate::types::DatabaseId::DEFAULT,
            crate::types::TenantId::new(1),
            collection.to_string(),
        )
    }

    /// 2100 rows of `(id, v = id)` flushed into one segment: block 0
    /// stamped 10, blocks 1 and 2 stamped 20. Returns the engine and the
    /// segment bytes.
    fn flushed_three_blocks(core: &CoreLoop) -> (MutationEngine, Vec<u8>) {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("blocks".into(), schema);
        for i in 0..2100i64 {
            let stamp = if i < 1024 { 10 } else { 20 };
            engine
                .insert(&[Value::Integer(i), Value::Integer(i)], stamp)
                .expect("insert");
        }
        let segment_id = engine.next_segment_id();
        let (seg_schema, columns, row_count) = engine.memtable_mut().drain_optimized();
        let memory = nodedb_mem::ScopedMemory::new(
            core.governor.clone(),
            crate::types::DatabaseId::DEFAULT,
            crate::types::TenantId::new(1),
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

    /// A read decodes only the blocks its cut and its predicates can match,
    /// and a decoded row reads as a whole-segment read reads it.
    #[test]
    fn a_read_decodes_only_the_blocks_it_can_return() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let (engine, blob) = flushed_three_blocks(&core);
        let schema = engine.schema().clone();
        let key = engine_key("blocks");
        let decode = |cut, predicates: &[ScanPredicate]| {
            core.decode_flushed_segment(
                &key,
                1,
                &blob,
                schema.columns.len(),
                SegmentBlocks::Visible {
                    engine: &engine,
                    cut,
                    predicates,
                },
                "test",
            )
            .expect("decode")
        };

        let whole = decode(VersionCut::Current, &[]);
        assert_eq!(whole.selection.selected_count(), 3);

        let early = decode(VersionCut::At(15), &[]);
        assert_eq!(
            early.selection.selected_count(),
            1,
            "blocks 1 and 2 start at 20"
        );
        assert!(early.holds_row(0) && !early.holds_row(1500));
        assert!(
            early.row(&schema, 1500).is_err(),
            "row outside the decoded blocks"
        );

        let filtered = decode(VersionCut::Current, &[ScanPredicate::gte_i64(1, 2050)]);
        assert_eq!(filtered.selection.selected_count(), 1);
        for row in [2048, 2060, 2099] {
            assert_eq!(
                filtered.row(&schema, row).expect("row"),
                whole.row(&schema, row).expect("row"),
                "row {row}"
            );
        }
    }

    #[test]
    fn unopenable_segment_bytes_are_refused_as_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = engine_key("m");
        let result = core.decode_flushed_segment(
            &key,
            1,
            &[],
            schema().columns.len(),
            SegmentBlocks::Row(0),
            "test",
        );
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 1 of 'm'"))
        );
    }

    #[test]
    fn a_cell_that_does_not_fit_its_type_is_refused_as_corruption() {
        let segment = DecodedFlushedSegment {
            collection: "m",
            segment_id: 2,
            site: "test",
            row_count: 1,
            selection: BlockSelection::all(1),
            columns: vec![
                DecodedColumn::Int64 {
                    values: vec![1],
                    valid: vec![true],
                },
                DecodedColumn::Binary {
                    data: vec![0xC1],
                    offsets: vec![0, 1],
                    valid: vec![true],
                },
            ],
        };
        let result = segment.row(&schema(), 0);
        assert!(
            matches!(result, Err(crate::Error::SegmentCorrupted { ref detail }) if detail.contains("segment 2 of 'm'"))
        );
    }
}
