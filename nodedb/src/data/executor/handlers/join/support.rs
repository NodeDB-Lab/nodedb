// SPDX-License-Identifier: BUSL-1.1

//! Shared binary-msgpack helpers used by the join execution handlers:
//! row merging, map-header writing, field filtering, and projection.

use nodedb_query::EvalError;
use nodedb_types::object_msgpack::{ObjectFormat, ObjectView};

use crate::data::executor::msgpack_utils::write_str;

/// Merge a left and optional right document into a single msgpack map,
/// prefixing each key with its source collection name.
///
/// Returns raw msgpack bytes — no JSON decode, no serde_json::Value.
/// Uses binary scan to iterate source map entries and writes directly
/// to the output buffer.
pub fn merge_join_docs_binary(
    left_bytes: &[u8],
    right_bytes: Option<&[u8]>,
    left_collection: &str,
    right_collection: &str,
) -> Result<Vec<u8>, EvalError> {
    let left = (!left_bytes.is_empty())
        .then(|| ObjectView::new(left_bytes))
        .transpose()
        .map_err(object_error)?;
    let right = right_bytes
        .map(ObjectView::new)
        .transpose()
        .map_err(object_error)?;
    let native = left
        .as_ref()
        .into_iter()
        .chain(right.as_ref())
        .any(|view| view.format() == ObjectFormat::Native);
    let mut entries = Vec::new();
    for (view, prefix) in [
        (left.as_ref(), left_collection),
        (right.as_ref(), right_collection),
    ] {
        let Some(view) = view else {
            continue;
        };
        for field in view.fields() {
            let (key, value) = field.map_err(object_error)?;
            let key = if prefix.is_empty() || key.contains('.') {
                key.to_owned()
            } else {
                format!("{prefix}.{key}")
            };
            let value = if native && view.format() == ObjectFormat::Plain {
                zerompk::to_msgpack_vec(&view.decode_value(value).map_err(object_error)?)
                    .map_err(object_error)?
            } else {
                view.decode_value(value).map_err(object_error)?;
                value.to_vec()
            };
            entries.push((key, value));
        }
    }
    Ok(encode_entries(&entries, native))
}

fn object_error(error: impl std::fmt::Display) -> EvalError {
    EvalError::ArgumentValue {
        function: "join",
        position: 1,
        reason: format!("invalid object row: {error}"),
    }
}

fn encode_entries(entries: &[(String, Vec<u8>)], native: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    if native {
        bytes.extend_from_slice(&[0x92, 7]);
    }
    write_map_header(&mut bytes, entries.len());
    for (key, value) in entries {
        write_str(&mut bytes, key);
        bytes.extend_from_slice(value);
    }
    bytes
}

/// The qualifier a side scanned by name gives its columns in the merged row:
/// the bare collection name, the name SQL qualifies the columns with.
///
/// `collection` is the stored, database-qualified name. Outside the default
/// database it carries a `<database_id>/` prefix, which this strips.
pub(super) fn row_qualifier(collection: &str, database_id: crate::types::DatabaseId) -> &str {
    if database_id == crate::types::DatabaseId::DEFAULT {
        return collection;
    }
    let prefix = format!("{}/", database_id.as_u64());
    collection
        .strip_prefix(prefix.as_str())
        .unwrap_or(collection)
}

/// Write a msgpack map header.
pub(super) fn write_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Order two rows' typed join key values, key by key, with the stored-column
/// sort order. Plain and native rows compare alike, since both read through
/// their object format into the same values.
pub(super) fn compare_join_keys(
    a: &[nodedb_types::Value],
    b: &[nodedb_types::Value],
) -> std::cmp::Ordering {
    use crate::data::executor::handlers::document::sort::compare::compare_column_values;
    a.iter()
        .zip(b)
        .map(|(a, b)| compare_column_values(a, b))
        .find(|order| order.is_ne())
        .unwrap_or(std::cmp::Ordering::Equal)
}

/// Filter a binary msgpack row against ScanFilter predicates.
///
/// The merged join row keys every field `qualifier.column`. The planner
/// qualifies every column reference, but a filter built elsewhere can name a
/// bare column ("amount"). Such a filter reads a view of the row where each
/// bare name also resolves to its qualified field ("orders.amount"), so the
/// field is found before it is treated as absent. An absent field reads as
/// NULL only when no qualified field carries the name.
///
/// Returns `Err(EvalError::DivisionByZero)` when a `FilterOp::Expr`
/// predicate divides or takes a modulus by zero. Every caller propagates
/// this as a genuine statement error (SQLSTATE 22012): the
/// WHERE-shaped post-filter path (`join::params::filter_and_project`) and the
/// hash-join probe path (`join::hash`'s `probe_hash_index` / `probe_rows_into`,
/// including the grace-hash spill/streaming family) both surface it rather than
/// folding a residual ON-predicate error to "no match".
pub(super) fn binary_row_matches_filters(
    row: &[u8],
    filters: &[crate::bridge::scan_filter::ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<bool, EvalError> {
    use crate::bridge::scan_filter::FilterOp;

    // Built once per row, and only when a filter reads a bare name.
    let mut bare_view: Option<Vec<u8>> = None;
    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        let doc: &[u8] = if filter_reads_bare_name(f) {
            if bare_view.is_none() {
                bare_view = Some(with_bare_names(row)?);
            }
            bare_view.as_deref().unwrap_or(row)
        } else {
            row
        };
        if !f.matches_binary(doc, crs)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether `f` names a column without a qualifier: its field, the right
/// column of a column comparison, an `OR` clause's filter, or a column an
/// expression reads.
fn filter_reads_bare_name(f: &crate::bridge::scan_filter::ScanFilter) -> bool {
    use crate::bridge::scan_filter::FilterOp;

    match f.op {
        FilterOp::MatchAll | FilterOp::Exists | FilterOp::NotExists => false,
        FilterOp::Or => f.clauses.iter().flatten().any(filter_reads_bare_name),
        FilterOp::Expr => f.expr.as_ref().is_some_and(expr_reads_bare_name),
        FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => {
            is_bare(&f.field)
                || matches!(&f.value, nodedb_types::Value::String(right) if is_bare(right))
        }
        FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte
        | FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::Ilike
        | FilterOp::NotIlike
        | FilterOp::In
        | FilterOp::NotIn
        | FilterOp::IsNull
        | FilterOp::IsNotNull
        | FilterOp::ArrayContains
        | FilterOp::ArrayContainsAll
        | FilterOp::ArrayOverlap => is_bare(&f.field),
    }
}

/// Whether an expression reads a column without a qualifier.
fn expr_reads_bare_name(expr: &crate::bridge::expr_eval::SqlExpr) -> bool {
    use crate::bridge::expr_eval::SqlExpr;

    match expr {
        SqlExpr::Column(name) => is_bare(name),
        SqlExpr::Literal(_) | SqlExpr::OldColumn(_) | SqlExpr::ExcludedColumn(_) => false,
        SqlExpr::BinaryOp { left, right, .. } | SqlExpr::NullIf(left, right) => {
            expr_reads_bare_name(left) || expr_reads_bare_name(right)
        }
        SqlExpr::Negate(inner)
        | SqlExpr::Cast { expr: inner, .. }
        | SqlExpr::IsNull { expr: inner, .. } => expr_reads_bare_name(inner),
        SqlExpr::Function { args, .. } | SqlExpr::Coalesce(args) => {
            args.iter().any(expr_reads_bare_name)
        }
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            operand.as_deref().is_some_and(expr_reads_bare_name)
                || when_thens
                    .iter()
                    .any(|(when, then)| expr_reads_bare_name(when) || expr_reads_bare_name(then))
                || else_expr.as_deref().is_some_and(expr_reads_bare_name)
        }
    }
}

/// Whether a field name carries no `qualifier.` prefix.
fn is_bare(name: &str) -> bool {
    !name.is_empty() && !name.contains('.')
}

/// `row` with each qualified field also reachable by its bare column name.
///
/// Every entry is kept. For each `qualifier.column` key whose `column` is not
/// already a key, an entry `column` with the same value is added. When two
/// sides carry the same column, the first in row order wins, as a suffix
/// lookup does.
fn with_bare_names(row: &[u8]) -> Result<Vec<u8>, EvalError> {
    let view = ObjectView::new(row).map_err(object_error)?;
    let mut entries = view
        .fields()
        .map(|field| {
            let (key, value) = field.map_err(object_error)?;
            view.decode_value(value).map_err(object_error)?;
            Ok((key.to_owned(), value.to_vec()))
        })
        .collect::<Result<Vec<_>, EvalError>>()?;
    let mut names: std::collections::HashSet<String> =
        entries.iter().map(|(key, _)| key.clone()).collect();
    let mut added = Vec::new();
    for (key, value) in &entries {
        if let Some((_, column)) = key.rsplit_once('.')
            && !column.is_empty()
            && names.insert(column.to_owned())
        {
            added.push((column.to_owned(), value.clone()));
        }
    }
    entries.extend(added);
    Ok(encode_entries(
        &entries,
        view.format() == ObjectFormat::Native,
    ))
}

/// Apply projection to a binary msgpack row, keeping only requested columns.
///
/// Projection names may be unqualified ("name") while keys are qualified
/// ("users.name"). Returns a new msgpack map with only matching fields,
/// using the unqualified name as the output key.
pub(super) fn binary_row_project(
    row: &[u8],
    projection: &[nodedb_physical::physical_plan::JoinProjection],
) -> Result<Vec<u8>, EvalError> {
    let view = ObjectView::new(row).map_err(object_error)?;
    let mut entries = Vec::with_capacity(projection.len());
    for field in view.fields() {
        let (key, value) = field.map_err(object_error)?;
        view.decode_value(value).map_err(object_error)?;
        let short = key.rsplit('.').next().unwrap_or(key);
        if let Some(projected) = projection
            .iter()
            .find(|p| p.source == short || p.source == key)
        {
            entries.push((projected.output.clone(), value.to_vec()));
        }
    }
    Ok(encode_entries(
        &entries,
        view.format() == ObjectFormat::Native,
    ))
}

#[cfg(test)]
mod tests {
    use super::{binary_row_matches_filters, binary_row_project, merge_join_docs_binary};
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};

    fn msgpack_row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
    }

    #[test]
    fn self_join_aliases_survive_column_filter_comparison() {
        let left = msgpack_row(&[
            ("id", serde_json::json!(1)),
            ("name", serde_json::json!("Alice")),
            ("dept", serde_json::json!("eng")),
        ]);
        let right = msgpack_row(&[
            ("id", serde_json::json!(2)),
            ("name", serde_json::json!("Bob")),
            ("dept", serde_json::json!("eng")),
        ]);

        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b").unwrap();
        let filters = vec![ScanFilter {
            field: "a.id".into(),
            op: FilterOp::LtColumn,
            value: nodedb_types::Value::String("b.id".into()),
            clauses: Vec::new(),
            expr: None,
        }];

        assert!(
            binary_row_matches_filters(&merged, &filters, &nodedb_crs::CrsContext::builtin_only())
                .unwrap()
        );
    }

    #[test]
    fn qualified_projection_keeps_distinct_join_columns() {
        let left = msgpack_row(&[("name", serde_json::json!("Alice"))]);
        let right = msgpack_row(&[("name", serde_json::json!("Bob"))]);
        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b").unwrap();

        let projected = binary_row_project(
            &merged,
            &[
                nodedb_physical::physical_plan::JoinProjection {
                    source: "a.name".into(),
                    output: "a.name".into(),
                },
                nodedb_physical::physical_plan::JoinProjection {
                    source: "b.name".into(),
                    output: "b.name".into(),
                },
            ],
        )
        .unwrap();
        let json = nodedb_types::json_from_msgpack(&projected).unwrap();
        let obj = json.as_object().unwrap();

        assert_eq!(obj.get("a.name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(obj.get("b.name").and_then(|v| v.as_str()), Some("Bob"));
    }

    #[test]
    fn aliased_projection_renames_join_columns() {
        let left = msgpack_row(&[
            ("name", serde_json::json!("Alice")),
            ("dept", serde_json::json!("eng")),
        ]);
        let right = msgpack_row(&[("name", serde_json::json!("Bob"))]);
        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b").unwrap();

        let projected = binary_row_project(
            &merged,
            &[
                nodedb_physical::physical_plan::JoinProjection {
                    source: "a.name".into(),
                    output: "emp1".into(),
                },
                nodedb_physical::physical_plan::JoinProjection {
                    source: "b.name".into(),
                    output: "emp2".into(),
                },
                nodedb_physical::physical_plan::JoinProjection {
                    source: "a.dept".into(),
                    output: "a.dept".into(),
                },
            ],
        )
        .unwrap();

        let json = nodedb_types::json_from_msgpack(&projected).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("emp1").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(obj.get("emp2").and_then(|v| v.as_str()), Some("Bob"));
        assert_eq!(obj.get("a.dept").and_then(|v| v.as_str()), Some("eng"));
    }
}

#[cfg(test)]
#[path = "support_native_tests.rs"]
mod native_tests;
