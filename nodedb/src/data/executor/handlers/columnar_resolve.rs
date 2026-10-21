// SPDX-License-Identifier: BUSL-1.1

//! Shared row selection for a columnar UPDATE/DELETE: which current rows
//! match the WHERE filters, and for an UPDATE, what each match becomes once
//! the assignments apply.
//!
//! A current row is a row the delete bitmaps do not tombstone and the
//! primary-key index binds. It lives in a flushed segment or in the
//! memtable. Flushed rows read through the shared flushed-segment reader.
//!
//! `execute_columnar_update` / `execute_columnar_delete`
//! (`columnar_mutation.rs`) apply the selection.
//! `execute_columnar_resolve_dml` (`columnar_resolve_dml.rs`) reports the
//! same selection to the Control Plane without applying it. The transaction
//! staging path (`stage_columnar_dml.rs`) reads the same current rows. One
//! selection decides what a predicate DML writes and what it reports.

use nodedb_columnar::MutationEngine;
use nodedb_columnar::pk_index::RowLocation;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::{Surrogate, Value};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_assignments::ColumnarAssignments;
use crate::data::executor::handlers::columnar_mutation_apply::{
    ColumnarEngineKey, flushed_row_surrogate,
};
use crate::data::executor::handlers::columnar_read::filter::row_matches_filters;
use crate::data::executor::handlers::rls_write_gate::admit_columnar_row;

/// Column index of the schema's primary-key column, or the standard
/// "columnar UPDATE/DELETE requires a PRIMARY KEY column" error every
/// columnar mutation path returns for a PK-less schema.
pub(in crate::data::executor) fn require_pk_column_index(
    schema: &ColumnarSchema,
    op_name: &str,
) -> crate::Result<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.primary_key)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("columnar {op_name} requires a PRIMARY KEY column"),
        })
}

/// One current row of a columnar collection.
pub(in crate::data::executor) struct CurrentColumnarRow {
    /// The cross-engine surrogate the row carries, when one was recorded.
    pub surrogate: Option<Surrogate>,
    /// The row's cells in schema order.
    pub values: Vec<Value>,
}

/// Bundled arguments for [`CoreLoop::resolve_columnar_update_rows`].
pub(in crate::data::executor) struct ResolveUpdateRowsParams<'a> {
    pub key: &'a ColumnarEngineKey,
    pub schema: &'a ColumnarSchema,
    pub pk_col_idx: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub updates: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub tid: u64,
    pub collection: &'a str,
}

/// Bundled arguments for [`CoreLoop::resolve_columnar_delete_rows`].
pub(in crate::data::executor) struct ResolveDeleteRowsParams<'a> {
    pub key: &'a ColumnarEngineKey,
    pub schema: &'a ColumnarSchema,
    pub pk_col_idx: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    pub tid: u64,
    pub collection: &'a str,
}

impl CoreLoop {
    /// Visit every current row of the collection at `key`: each flushed
    /// segment in segment order, then the memtable.
    ///
    /// A row its segment's delete bitmap marks is not visited. A live row the
    /// primary-key index does not bind is a superseded version, which only a
    /// bitemporal collection keeps. It is not visited. In any other collection
    /// an unbound live row is an `Internal` error.
    ///
    /// `Err` when a segment or a memtable cell does not read, or when `visit`
    /// fails. The shared flushed reader files the corruption report, with
    /// `site` naming the read path.
    pub(in crate::data::executor) fn for_each_current_columnar_row(
        &self,
        key: &ColumnarEngineKey,
        site: &'static str,
        mut visit: impl FnMut(CurrentColumnarRow) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let Some(engine) = self.columnar_engines.get(key) else {
            return Ok(());
        };
        let schema = engine.schema();
        let collection = key.2.as_str();

        let segments = self
            .columnar_flushed_segments
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            // Segment ids are 1-based. Id 0 names the memtable.
            let segment_id = seg_idx as u64 + 1;
            let segment = self.decode_flushed_segment(
                collection,
                segment_id,
                seg_bytes,
                schema.columns.len(),
                site,
            )?;
            let deletes = engine.delete_bitmap(segment_id);
            for row_idx in 0..segment.row_count() {
                let location = RowLocation {
                    segment_id,
                    row_index: row_index_u32(collection, segment_id, row_idx)?,
                };
                if deletes.is_some_and(|bm| bm.is_deleted(location.row_index)) {
                    continue;
                }
                let values = segment.row(schema, row_idx)?;
                if !row_is_bound(engine, &values, location, collection)? {
                    continue;
                }
                let surrogate =
                    flushed_row_surrogate(&self.columnar_flushed_surrogates, key, location);
                visit(CurrentColumnarRow { surrogate, values })?;
            }
        }

        let memtable_segment = engine.memtable_segment_id();
        let surrogates = engine.memtable_surrogates();
        for row_idx in 0..engine.memtable().row_count() {
            // `None` is a row the memtable delete bitmap marks.
            let Some(values) = engine.get_memtable_row(row_idx)? else {
                continue;
            };
            let location = RowLocation {
                segment_id: memtable_segment,
                row_index: row_index_u32(collection, memtable_segment, row_idx)?,
            };
            if !row_is_bound(engine, &values, location, collection)? {
                continue;
            }
            let surrogate = surrogates.get(row_idx).copied().flatten();
            visit(CurrentColumnarRow { surrogate, values })?;
        }
        Ok(())
    }

    /// Match current rows against `filter_predicates`, apply `updates` to
    /// build each match's post-image, and decide every post-image against
    /// `rls_write_check`. An expression assignment evaluates against the
    /// matched row's pre-image. Each post-image meets the declared column
    /// rule. The first row that fails any step is the statement's error.
    ///
    /// Mutates nothing. Returns `(old_primary_key, post_image)` pairs in match
    /// order. `old_primary_key` is the row's PK value before `updates`
    /// applies. It identifies the row to remove when the update assigns the
    /// PK column a new value.
    pub(in crate::data::executor) fn resolve_columnar_update_rows(
        &self,
        params: ResolveUpdateRowsParams<'_>,
    ) -> crate::Result<Vec<(Value, Vec<Value>)>> {
        let ResolveUpdateRowsParams {
            key,
            schema,
            pk_col_idx,
            filter_predicates,
            updates,
            rls_write_check,
            tid,
            collection,
        } = params;
        let assignments = ColumnarAssignments::bind(schema, updates)?;
        let crs = self.crs_context();
        let mut resolved = Vec::new();
        self.for_each_current_columnar_row(key, "columnar_dml_resolve", |row| {
            let row = row.values;
            if !row_matches(&row, schema, filter_predicates, &crs)? {
                return Ok(());
            }
            let old_pk = pk_value(&row, pk_col_idx, collection)?;
            let new_row = assignments.apply(schema, row, &crs)?;
            admit_columnar_row(rls_write_check, &new_row, schema, tid, collection, &crs)?;
            resolved.push((old_pk, new_row));
            Ok(())
        })?;
        Ok(resolved)
    }

    /// Match current rows against `filter_predicates` and decide each matched
    /// row's pre-image, the image a DELETE removes, against
    /// `rls_write_check`. Mutates nothing. Returns matched primary-key values
    /// in match order.
    pub(in crate::data::executor) fn resolve_columnar_delete_rows(
        &self,
        params: ResolveDeleteRowsParams<'_>,
    ) -> crate::Result<Vec<Value>> {
        let ResolveDeleteRowsParams {
            key,
            schema,
            pk_col_idx,
            filter_predicates,
            rls_write_check,
            tid,
            collection,
        } = params;
        let mut pks = Vec::new();
        let crs = self.crs_context();
        self.for_each_current_columnar_row(key, "columnar_dml_resolve", |row| {
            let row = row.values;
            if !row_matches(&row, schema, filter_predicates, &crs)? {
                return Ok(());
            }
            admit_columnar_row(rls_write_check, &row, schema, tid, collection, &crs)?;
            pks.push(pk_value(&row, pk_col_idx, collection)?);
            Ok(())
        })?;
        Ok(pks)
    }
}

/// Whether `row` passes every WHERE predicate. No predicate passes.
fn row_matches(
    row: &[Value],
    schema: &ColumnarSchema,
    filter_predicates: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<bool> {
    if filter_predicates.is_empty() {
        return Ok(true);
    }
    row_matches_filters(row, schema, filter_predicates, crs).map_err(crate::Error::from)
}

/// The primary-key cell of `row`.
fn pk_value(row: &[Value], pk_col_idx: usize, collection: &str) -> crate::Result<Value> {
    row.get(pk_col_idx)
        .cloned()
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "columnar '{collection}': row holds {} cells, no primary-key cell {pk_col_idx}",
                row.len()
            ),
        })
}

/// Whether the primary-key index binds `values` to `location`.
///
/// `Ok(false)` for a superseded bitemporal version. `Err` for an unbound live
/// row of a collection that is not bitemporal: the index lost a binding.
fn row_is_bound(
    engine: &MutationEngine,
    values: &[Value],
    location: RowLocation,
    collection: &str,
) -> crate::Result<bool> {
    let pk_bytes = engine.encode_pk_from_row(values)?;
    if engine.pk_index().get(&pk_bytes) == Some(&location) {
        return Ok(true);
    }
    if engine.schema().is_bitemporal() {
        return Ok(false);
    }
    Err(crate::Error::Internal {
        detail: format!(
            "columnar '{collection}': live row {} of segment {} is not bound by the \
             primary-key index",
            location.row_index, location.segment_id
        ),
    })
}

/// `row_idx` as the `u32` row index a `RowLocation` carries.
fn row_index_u32(collection: &str, segment_id: u64, row_idx: usize) -> crate::Result<u32> {
    u32::try_from(row_idx).map_err(|_| crate::Error::Internal {
        detail: format!(
            "columnar '{collection}': row {row_idx} of segment {segment_id} is past the \
             u32 row index range"
        ),
    })
}
