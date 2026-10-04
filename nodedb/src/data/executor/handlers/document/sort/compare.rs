// SPDX-License-Identifier: BUSL-1.1

//! Sort key evaluation and row comparators.
//!
//! Two comparison strategies, chosen per query by the shape of the ORDER BY:
//!
//! - **Every key is a stored column** — compare the raw msgpack bytes field by
//!   field, with no decode at all. This is the common `ORDER BY col` case and
//!   the reason the binary comparator exists.
//! - **Any key is computed** (`ORDER BY 100 / weight`, `ORDER BY UPPER(name)`)
//!   — the value being sorted on exists in no field, so each row's keys are
//!   evaluated once up front and the rows are ordered by those values.
//!
//! Both produce the same ordering for the same query; the second simply
//! handles keys the first cannot name. Evaluating is fallible — a zero divisor
//! in a sort key fails the statement with SQLSTATE `22012` rather than
//! returning the rows in storage order under a sort the client asked for.

use std::str::FromStr;

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_query::{compare_json, eval_expr_on_json, msgpack_scan};
use nodedb_types::columnar::{ColumnType, StrictSchema};
use rust_decimal::Decimal;

/// One row's evaluated sort keys, positionally aligned with the ORDER BY list.
pub(in crate::data::executor) type SortValues = Vec<serde_json::Value>;

/// Per ORDER BY key, whether it is a bare column `schema` declares `DECIMAL`.
///
/// A `DECIMAL` cell is text, or a `uint64` past `i64::MAX`, so the generic
/// comparators order such a column by type rank and by bytes. A flagged key
/// compares by value instead. A collection with no strict schema flags none.
pub(in crate::data::executor) fn decimal_sort_keys(
    sort_keys: &[SortKeySpec],
    schema: Option<&StrictSchema>,
) -> Vec<bool> {
    let Some(schema) = schema else {
        return Vec::new();
    };
    sort_keys
        .iter()
        .map(|key| {
            key.as_column().is_some_and(|field| {
                schema.columns.iter().any(|column| {
                    column.name == field && matches!(column.column_type, ColumnType::Decimal { .. })
                })
            })
        })
        .collect()
}

/// Whether key `idx` is flagged `DECIMAL` in `decimal_keys`.
pub(in crate::data::executor) fn is_decimal_key(decimal_keys: &[bool], idx: usize) -> bool {
    decimal_keys.get(idx).copied().unwrap_or(false)
}

/// The decimal an evaluated sort value stands for: a decimal string or a
/// number. `None` for any other value.
fn json_decimal(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::String(text) => Decimal::from_str(text.trim())
            .or_else(|_| Decimal::from_scientific(text.trim()))
            .ok(),
        serde_json::Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                Some(Decimal::from(i))
            } else if let Some(u) = number.as_u64() {
                Some(Decimal::from(u))
            } else {
                number.as_f64().and_then(|f| Decimal::try_from(f).ok())
            }
        }
        _ => None,
    }
}

/// Order two evaluated sort values, by value when the key is `DECIMAL`.
fn compare_key_values(
    a: &serde_json::Value,
    b: &serde_json::Value,
    decimal: bool,
) -> std::cmp::Ordering {
    if decimal && let (Some(da), Some(db)) = (json_decimal(a), json_decimal(b)) {
        return da.cmp(&db);
    }
    compare_json(a, b)
}

/// Order two stored cells, by value when the key is `DECIMAL`.
pub(in crate::data::executor) fn compare_cells(
    a_bytes: &[u8],
    a_range: (usize, usize),
    b_bytes: &[u8],
    b_range: (usize, usize),
    decimal: bool,
) -> std::cmp::Ordering {
    if decimal {
        msgpack_scan::compare_decimal_field_bytes(a_bytes, a_range, b_bytes, b_range)
    } else {
        msgpack_scan::compare_field_bytes(a_bytes, a_range, b_bytes, b_range)
    }
}

/// True when every key is a bare stored column, so the zero-decode binary
/// comparator can be used.
pub(in crate::data::executor) fn all_column_keys(sort_keys: &[SortKeySpec]) -> bool {
    sort_keys.iter().all(|k| k.as_column().is_some())
}

/// Evaluate every sort key against one raw msgpack document.
///
/// A document that cannot be decoded contributes NULL keys and sorts to the
/// front rather than failing the scan — matching the binary comparator, which
/// treats an absent field as ordering-neutral. An expression *evaluation*
/// failure is a different thing entirely and propagates.
pub(in crate::data::executor) fn eval_sort_values(
    doc_bytes: &[u8],
    sort_keys: &[SortKeySpec],
) -> crate::Result<SortValues> {
    let doc = match nodedb_types::json_from_msgpack(doc_bytes) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(err = %e, "msgpack decode failed during sort; treating row as null");
            serde_json::Value::Null
        }
    };

    sort_keys
        .iter()
        .map(|k| eval_expr_on_json(&k.expr, &doc).map_err(crate::Error::from))
        .collect()
}

/// Compare two rows by their pre-evaluated sort values. `decimal_keys` flags
/// the keys [`decimal_sort_keys`] found `DECIMAL`.
pub(in crate::data::executor) fn compare_sort_values(
    a: &[serde_json::Value],
    b: &[serde_json::Value],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        // NULL placement is decided by the key's NULLS FIRST/LAST setting and
        // is never flipped by the sort direction.
        let ord = match key.order_nulls(va.is_null(), vb.is_null()) {
            Some(ord) => ord,
            None => key.direct(compare_key_values(
                va,
                vb,
                is_decimal_key(decimal_keys, idx),
            )),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// Compare two raw msgpack documents by column-only sort keys.
///
/// Uses binary field extraction — no decode. Shared by the in-memory sort and
/// the external merge so both order rows identically. `decimal_keys` flags
/// the keys [`decimal_sort_keys`] found `DECIMAL`.
pub(in crate::data::executor) fn compare_docs_by_keys_binary(
    a_bytes: &[u8],
    b_bytes: &[u8],
    sort_keys: &[SortKeySpec],
    decimal_keys: &[bool],
) -> std::cmp::Ordering {
    for (idx, key) in sort_keys.iter().enumerate() {
        // Column-only callers guarantee `as_column`; a computed key has no
        // field to extract and compares equal here, which is why the caller
        // routes those rows through `compare_sort_values` instead.
        let Some(field) = key.as_column() else {
            continue;
        };
        let a_range = msgpack_scan::extract_field(a_bytes, 0, field);
        let b_range = msgpack_scan::extract_field(b_bytes, 0, field);

        // An absent field and a stored NULL are both NULL for ordering, and
        // their placement follows the key's NULLS FIRST/LAST setting.
        let ordered = match key.order_nulls(
            is_null_range(a_bytes, a_range),
            is_null_range(b_bytes, b_range),
        ) {
            Some(ord) => ord,
            None => match (a_range, b_range) {
                (Some(ar), Some(br)) => key.direct(compare_cells(
                    a_bytes,
                    ar,
                    b_bytes,
                    br,
                    is_decimal_key(decimal_keys, idx),
                )),
                _ => std::cmp::Ordering::Equal,
            },
        };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

/// Encode evaluated sort values for a spill run record.
pub(in crate::data::executor) fn encode_sort_values(values: &SortValues) -> crate::Result<Vec<u8>> {
    nodedb_types::json_to_msgpack(&serde_json::Value::Array(values.clone())).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("sort key values: {e}"),
        }
    })
}

/// Decode evaluated sort values read back from a spill run record.
pub(in crate::data::executor) fn decode_sort_values(bytes: &[u8]) -> crate::Result<SortValues> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    match nodedb_types::json_from_msgpack(bytes) {
        Ok(serde_json::Value::Array(values)) => Ok(values),
        Ok(other) => Err(crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("sort run corrupt: key values are not an array ({other})"),
        }),
        Err(e) => Err(crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("sort run corrupt: key values undecodable: {e}"),
        }),
    }
}

/// Whether an extracted field range is SQL NULL: the field is absent, or the
/// bytes it points at are a msgpack nil.
pub(in crate::data::executor) fn is_null_range(doc: &[u8], range: Option<(usize, usize)>) -> bool {
    const MSGPACK_NIL: u8 = 0xc0;
    match range {
        None => true,
        Some((start, _)) => doc.get(start) == Some(&MSGPACK_NIL),
    }
}
