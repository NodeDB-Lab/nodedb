// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for LATERAL join handlers.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::handlers::join::{binary_row_project, merge_join_docs_binary};
use nodedb_physical::physical_plan::{DocumentOp, JoinProjection};
use nodedb_query::msgpack_scan;

pub(super) const MAX_RESULT_ROWS: usize = 100_000;

/// Extract a field value from an outer scan row.
///
/// Scan rows are `{id: "<doc_id>", data: <payload>}` wrappers. The primary key
/// (`id`) lives at the top level; all other fields live inside `data`. This
/// function checks both levels so that correlation predicates referencing the
/// primary key (e.g. `WHERE e.user_id = u.id`) work correctly.
///
/// `Ok(None)` when neither level has the field. A field whose bytes do not
/// decode is an error, never a missing field: a missing correlation value
/// drops the predicate it binds.
pub(super) fn extract_outer_field(
    outer_bytes: &[u8],
    field: &str,
) -> crate::Result<Option<nodedb_types::Value>> {
    let decode = |bytes: &[u8]| {
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Codec {
            detail: format!("LATERAL outer field '{field}': {e}"),
        })
    };
    // Prefer the `data` sub-map (user-visible document fields, including the
    // user-provided primary key). The wrapper-level `id` is the internal hex
    // surrogate and must not shadow the user-visible `id` stored in `data`.
    if let Some((s, e)) = msgpack_scan::extract_field(outer_bytes, 0, "data") {
        let data_bytes = &outer_bytes[s..e];
        if let Some((start, end)) = msgpack_scan::extract_field(data_bytes, 0, field) {
            return decode(&data_bytes[start..end]).map(Some);
        }
    }
    // Fall back to the top-level wrapper (covers non-`data` wrapper fields
    // that have no user-data counterpart).
    if let Some((start, end)) = msgpack_scan::extract_field(outer_bytes, 0, field) {
        return decode(&outer_bytes[start..end]).map(Some);
    }
    Ok(None)
}

/// The correlation value `outer_col` supplies for an equality with an inner
/// column. `Ok(None)` when the outer value is missing or NULL: `inner = NULL`
/// is never true, so no inner row matches.
pub(super) fn correlation_value(
    outer_bytes: &[u8],
    outer_col: &str,
) -> crate::Result<Option<nodedb_types::Value>> {
    Ok(extract_outer_field(outer_bytes, outer_col)?
        .filter(|v| !matches!(v, nodedb_types::Value::Null)))
}

/// Walk `filters` and substitute any `*Column` filter whose column reference
/// names an outer-row field with a literal value from that row.
///
/// `*Column` ops (`EqColumn`, `GtColumn`, etc.) store the name of the column
/// to compare against in their `value` field as a `Value::String`. When the
/// referenced column belongs to the outer alias (e.g. `"u.created_at"` or
/// `"created_at"`) we replace the filter with the equivalent literal op so the
/// inner scan can evaluate it without knowledge of the outer table.
/// Strip table-alias qualifiers from all filter field names and bind any
/// `*Column` ops that reference the outer row.
pub(super) fn bind_outer_values(
    filters: Vec<ScanFilter>,
    outer_bytes: &[u8],
    outer_alias: &str,
) -> crate::Result<Vec<ScanFilter>> {
    filters
        .into_iter()
        .map(|f| bind_filter_outer(f, outer_bytes, outer_alias))
        .collect()
}

/// Strip table-alias qualifiers from filter field names without binding
/// outer-row values. Used for base inner filters in LateralTopK where no
/// outer-correlated `*Column` ops are present but field names may still be
/// qualified (e.g. `"e.score"` → `"score"`).
pub(super) fn strip_filter_qualifiers(filters: Vec<ScanFilter>) -> Vec<ScanFilter> {
    filters
        .into_iter()
        .map(|f| {
            let unqualified = f
                .field
                .find('.')
                .map_or(f.field.clone(), |dot| f.field[dot + 1..].to_string());
            ScanFilter {
                field: unqualified,
                ..f
            }
        })
        .collect()
}

pub(super) fn bind_filter_outer(
    f: ScanFilter,
    outer_bytes: &[u8],
    outer_alias: &str,
) -> crate::Result<ScanFilter> {
    let literal_op = match f.op {
        FilterOp::EqColumn => Some(FilterOp::Eq),
        FilterOp::GtColumn => Some(FilterOp::Gt),
        FilterOp::GteColumn => Some(FilterOp::Gte),
        FilterOp::LtColumn => Some(FilterOp::Lt),
        FilterOp::LteColumn => Some(FilterOp::Lte),
        FilterOp::NeColumn => Some(FilterOp::Ne),
        _ => None,
    };
    let Some(lit_op) = literal_op else {
        // Not a column-comparison op.
        // Strip any table-alias qualifier from the field name so the inner
        // scan can resolve it (e.g. "i.ref_val" → "ref_val").
        let unqualified = f
            .field
            .find('.')
            .map_or(f.field.clone(), |dot| f.field[dot + 1..].to_string());
        // Recurse into OR clauses if present.
        if !f.clauses.is_empty() {
            let bound_clauses = f
                .clauses
                .into_iter()
                .map(|clause| {
                    clause
                        .into_iter()
                        .map(|sf| bind_filter_outer(sf, outer_bytes, outer_alias))
                        .collect::<crate::Result<Vec<_>>>()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return Ok(ScanFilter {
                field: unqualified,
                clauses: bound_clauses,
                ..f
            });
        }
        return Ok(ScanFilter {
            field: unqualified,
            ..f
        });
    };

    // The column reference is stored as a string in `value`.
    let col_ref = match &f.value {
        nodedb_types::Value::String(s) => s.clone(),
        _ => return Ok(f),
    };

    // Resolve: strip the outer alias prefix if present (e.g. "u.created_at" → "created_at").
    let bare = if let Some(rest) = col_ref
        .strip_prefix(outer_alias)
        .and_then(|s| s.strip_prefix('.'))
    {
        rest.to_string()
    } else {
        col_ref.clone()
    };

    if let Some(val) = extract_outer_field(outer_bytes, &bare)? {
        // Also strip any table-alias qualifier from the left-side field name
        // (e.g. "e.log_time" → "log_time") so the inner scan can find it.
        let unqualified_field = f
            .field
            .find('.')
            .map_or(f.field.as_str(), |dot| &f.field[dot + 1..])
            .to_string();
        Ok(ScanFilter {
            field: unqualified_field,
            op: lit_op,
            value: val,
            clauses: Vec::new(),
            expr: None,
        })
    } else {
        // Column not found in outer row — leave filter unchanged (will fail at scan time).
        Ok(f)
    }
}

/// Build a `DocumentOp::Scan` physical plan for the inner collection, read
/// at `system_time`: the LATERAL join's inner scope. The scan runs under the
/// join's task, so a transaction snapshot merges the task's staged writes.
pub(super) fn build_scan_plan(
    collection: &str,
    filter_bytes: Vec<u8>,
    order_by: &[nodedb_physical::physical_plan::SortKeySpec],
    limit: usize,
    system_time: nodedb_types::SystemTimeScope,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        // One row past the result cap, so an inner result the cap cuts
        // overflows it and errors instead of being cut silently.
        limit: limit.min(MAX_RESULT_ROWS + 1),
        offset: 0,
        sort_keys: order_by.to_vec(),
        filters: filter_bytes,
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time,
        valid_at_ms: None,
        prefilter: None,
    })
}

/// Build and optionally project a merged row.
pub(super) fn build_row(
    outer: &[u8],
    inner: Option<&[u8]>,
    outer_alias: &str,
    lateral_alias: &str,
    projection: &[JoinProjection],
) -> Vec<u8> {
    let merged = merge_join_docs_binary(outer, inner, outer_alias, lateral_alias);
    if projection.is_empty() {
        merged
    } else {
        binary_row_project(&merged, projection)
    }
}

/// If `bytes` is a scan-row wrapper `{id: ..., data: ...}`, return the `data`
/// field's bytes. Otherwise return `bytes` as-is (already a plain map).
pub(super) fn unwrap_data_field(bytes: &[u8]) -> &[u8] {
    if let Some((start, end)) = msgpack_scan::extract_field(bytes, 0, "data") {
        &bytes[start..end]
    } else {
        bytes
    }
}

/// Produce a flat msgpack map containing the user-visible fields of an outer
/// scan row.
///
/// Scan rows arrive as `{id: "<hex_surrogate>", data: {field1: v1, ...}}`.
/// The wrapper `id` is an internal surrogate; user queries always refer to
/// the fields inside `data` (including the user-provided primary key). This
/// function returns the `data` sub-map bytes directly so that `merge_join_docs_binary`
/// can prefix those fields with the outer alias for the projection step.
///
/// If the input is not a `{id, data}` wrapper (e.g. already a flat map from a
/// prior join), it is returned as-is.
pub(super) fn flatten_outer_row(bytes: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = msgpack_scan::extract_field(bytes, 0, "data") {
        bytes[s..e].to_vec()
    } else {
        bytes.to_vec()
    }
}
