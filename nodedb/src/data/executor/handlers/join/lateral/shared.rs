// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for LATERAL join handlers.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, describe_row_context};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::handlers::join::{binary_row_project, merge_join_docs_binary};
use crate::data::executor::response_codec::{RowContext, scan_rows_in_context};
use nodedb_physical::physical_plan::{DocumentOp, JoinProjection};
use nodedb_types::object_msgpack::ObjectView;

pub(super) const MAX_RESULT_ROWS: usize = 100_000;

/// One outer row of a LATERAL join, split by the row shape its plan
/// produces.
pub(super) struct OuterRow {
    /// The user-visible fields as one flat body.
    pub body: Vec<u8>,
    /// The storage key of a scan envelope. `None` for a flat row.
    pub envelope_id: Option<String>,
}

/// The rows of the outer plan's `payload`, split under the row shape
/// `outer_plan` produces: a document scan answers `{id, data}` envelopes, and
/// a materialized `ProviderScan` answers flat rows. A flat row whose columns
/// are `id` and `data` stays whole.
pub(super) fn outer_rows(
    payload: &[u8],
    outer_plan: &PhysicalPlan,
) -> crate::Result<Vec<OuterRow>> {
    let context = describe_row_context(outer_plan);
    Ok(scan_rows_in_context(payload, context)?
        .into_iter()
        .map(|(id, body)| OuterRow {
            body,
            envelope_id: match context {
                RowContext::ScanEnvelope => Some(id),
                RowContext::Flat => None,
            },
        })
        .collect())
}

/// The bodies of the inner scan's `payload`, split under the row shape
/// `inner_plan` produces.
pub(super) fn inner_rows(payload: &[u8], inner_plan: &PhysicalPlan) -> crate::Result<Vec<Vec<u8>>> {
    Ok(
        scan_rows_in_context(payload, describe_row_context(inner_plan))?
            .into_iter()
            .map(|(_, body)| body)
            .collect(),
    )
}

/// Extract a field value from an outer row.
///
/// The field is read from the row's body. A scan envelope's storage key
/// answers `id` when the body has no `id` field, so a correlation on the
/// primary key (e.g. `WHERE e.user_id = u.id`) binds.
///
/// `Ok(None)` when the row has no such field. A field whose bytes do not
/// decode is an error, never a missing field: a missing correlation value
/// drops the predicate it binds.
pub(super) fn extract_outer_field(
    outer: &OuterRow,
    field: &str,
) -> crate::Result<Option<nodedb_types::Value>> {
    let view = ObjectView::new(&outer.body).map_err(|error| crate::Error::Codec {
        detail: format!("LATERAL outer object: {error}"),
    })?;
    let value = view.get(field).map_err(|error| crate::Error::Codec {
        detail: format!("LATERAL outer field '{field}': {error}"),
    })?;
    Ok(match (value, &outer.envelope_id) {
        (Some(value), _) => Some(value),
        (None, Some(id)) if field == "id" => Some(nodedb_types::Value::String(id.clone())),
        (None, _) => None,
    })
}

/// The correlation value `outer_col` supplies for an equality with an inner
/// column. `Ok(None)` when the outer value is missing or NULL: `inner = NULL`
/// is never true, so no inner row matches.
pub(super) fn correlation_value(
    outer: &OuterRow,
    outer_col: &str,
) -> crate::Result<Option<nodedb_types::Value>> {
    Ok(extract_outer_field(outer, outer_col)?.filter(|v| !matches!(v, nodedb_types::Value::Null)))
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
    outer: &OuterRow,
    outer_alias: &str,
) -> crate::Result<Vec<ScanFilter>> {
    filters
        .into_iter()
        .map(|f| bind_filter_outer(f, outer, outer_alias))
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
    outer: &OuterRow,
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
                        .map(|sf| bind_filter_outer(sf, outer, outer_alias))
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

    if let Some(val) = extract_outer_field(outer, &bare)? {
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
    database_id: nodedb_types::DatabaseId,
    collection: &str,
    filter_bytes: Vec<u8>,
    order_by: &[nodedb_physical::physical_plan::SortKeySpec],
    limit: usize,
    system_time: nodedb_types::SystemTimeScope,
) -> Result<PhysicalPlan, ErrorCode> {
    let collection =
        nodedb_types::QualifiedCollection::from_stored(database_id, collection.to_string())
            .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
    Ok(PhysicalPlan::Document(DocumentOp::Scan {
        collection,
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
    }))
}

/// Build and optionally project a merged row.
pub(super) fn build_row(
    outer: &[u8],
    inner: Option<&[u8]>,
    outer_alias: &str,
    lateral_alias: &str,
    projection: &[JoinProjection],
) -> crate::Result<Vec<u8>> {
    let merged = merge_join_docs_binary(outer, inner, outer_alias, lateral_alias)?;
    if projection.is_empty() {
        Ok(merged)
    } else {
        Ok(binary_row_project(&merged, projection)?)
    }
}

#[cfg(test)]
mod native_tests {
    use super::{OuterRow, correlation_value, extract_outer_field};
    use nodedb_types::Value;

    fn flat(body: Vec<u8>) -> OuterRow {
        OuterRow {
            body,
            envelope_id: None,
        }
    }

    #[test]
    fn correlation_reads_whole_native_objects_and_preserves_plain_arrays() {
        let decimal = Value::Decimal("123456789.123456789".parse().expect("decimal"));
        let native = zerompk::to_msgpack_vec(&Value::Object(
            [("key".into(), decimal.clone())].into_iter().collect(),
        ))
        .expect("native object");
        assert_eq!(
            extract_outer_field(&flat(native), "key").expect("lookup"),
            Some(decimal)
        );
        let plain = nodedb_types::json_to_msgpack(&serde_json::json!({"key": [4, "text"]}))
            .expect("plain object");
        assert_eq!(
            extract_outer_field(&flat(plain), "key").expect("lookup"),
            Some(Value::Array(vec![
                Value::Integer(4),
                Value::String("text".into())
            ]))
        );
        assert!(extract_outer_field(&flat(vec![0x92, 7, 0x81]), "key").is_err());
    }

    /// A flat row with a column named `data` is never read as an envelope:
    /// `data` is a value, and the row's other columns stay reachable.
    #[test]
    fn a_flat_row_with_a_data_column_stays_whole() {
        let row = Value::Object(
            [
                ("id".to_owned(), Value::String("u1".into())),
                (
                    "data".to_owned(),
                    Value::Object(
                        [("key".to_owned(), Value::Integer(5))]
                            .into_iter()
                            .collect(),
                    ),
                ),
                ("key".to_owned(), Value::Integer(9)),
            ]
            .into_iter()
            .collect(),
        );
        for body in [
            zerompk::to_msgpack_vec(&row).expect("native row"),
            nodedb_types::value_to_msgpack(&row).expect("plain row"),
        ] {
            let outer = flat(body);
            assert_eq!(
                correlation_value(&outer, "key").expect("key"),
                Some(Value::Integer(9))
            );
            assert_eq!(
                extract_outer_field(&outer, "id").expect("id"),
                Some(Value::String("u1".into()))
            );
            assert!(matches!(
                extract_outer_field(&outer, "data").expect("data"),
                Some(Value::Object(_))
            ));
        }
    }

    /// An envelope body answers its fields, and the storage key answers
    /// `id` only when the body has none.
    #[test]
    fn an_envelope_row_reads_its_body_and_falls_back_to_its_key_for_id() {
        let body = Value::Object(
            [("key".to_owned(), Value::Integer(9_007_199_254_740_993))]
                .into_iter()
                .collect(),
        );
        let outer = OuterRow {
            body: zerompk::to_msgpack_vec(&body).expect("native body"),
            envelope_id: Some("k1".into()),
        };
        assert_eq!(
            correlation_value(&outer, "key").expect("key"),
            Some(Value::Integer(9_007_199_254_740_993))
        );
        assert_eq!(
            extract_outer_field(&outer, "id").expect("id"),
            Some(Value::String("k1".into()))
        );
        assert_eq!(correlation_value(&outer, "absent").expect("absent"), None);
    }
}
