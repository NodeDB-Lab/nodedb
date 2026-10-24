// SPDX-License-Identifier: BUSL-1.1

//! Shared binary-msgpack helpers used by the join execution handlers:
//! row merging, map-header writing, field filtering, and projection.

use nodedb_query::EvalError;
use nodedb_query::msgpack_scan;

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
) -> Vec<u8> {
    let left_count = count_map_entries(left_bytes);
    let right_count = right_bytes.map_or(0, count_map_entries);
    let total = left_count + right_count;

    // Estimate capacity: original data + prefixed keys overhead.
    let cap = left_bytes.len()
        + right_bytes.map_or(0, |b| b.len())
        + total * (left_collection.len().max(right_collection.len()) + 8);
    let mut buf = Vec::with_capacity(cap);

    write_map_header(&mut buf, total);
    write_prefixed_entries(&mut buf, left_bytes, left_collection);
    if let Some(rb) = right_bytes {
        write_prefixed_entries(&mut buf, rb, right_collection);
    }
    buf
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

/// Count entries in a msgpack map.
fn count_map_entries(bytes: &[u8]) -> usize {
    msgpack_scan::map_header(bytes, 0).map_or(0, |(count, _)| count)
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

/// Iterate msgpack map entries and write each key prefixed with `prefix.`
/// and its value bytes verbatim.
fn write_prefixed_entries(buf: &mut Vec<u8>, bytes: &[u8], prefix: &str) {
    let Some((count, mut pos)) = msgpack_scan::map_header(bytes, 0) else {
        return;
    };
    for _ in 0..count {
        // Read key string.
        let key = msgpack_scan::read_str(bytes, pos);
        pos = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };
        // Copy value bytes verbatim.
        let value_start = pos;
        let value_end = match msgpack_scan::skip_value(bytes, pos) {
            Some(p) => p,
            None => return,
        };

        // Write prefixed key.
        if let Some(k) = key {
            // A merged join row's keys are alias-qualified exactly once. When
            // the source is itself a join result (multi-way join), its keys are
            // already qualified (`c.relname`, `a.attname`); re-prefixing would
            // produce `c.c.relname`, which no projection or filter can resolve.
            // SQL column identifiers never contain '.', so a '.' in the key
            // reliably means "already qualified" — pass it through verbatim.
            if prefix.is_empty() || k.contains('.') {
                write_str(buf, k);
            } else {
                // Avoid allocation: write prefix.key directly.
                let prefixed_len = prefix.len() + 1 + k.len();
                if prefixed_len < 32 {
                    buf.push(0xA0 | prefixed_len as u8);
                } else if prefixed_len <= u8::MAX as usize {
                    buf.push(0xD9);
                    buf.push(prefixed_len as u8);
                } else if prefixed_len <= u16::MAX as usize {
                    buf.push(0xDA);
                    buf.extend_from_slice(&(prefixed_len as u16).to_be_bytes());
                } else {
                    buf.push(0xDB);
                    buf.extend_from_slice(&(prefixed_len as u32).to_be_bytes());
                }
                buf.extend_from_slice(prefix.as_bytes());
                buf.push(b'.');
                buf.extend_from_slice(k.as_bytes());
            }
        }
        // Write value bytes verbatim — zero decode.
        buf.extend_from_slice(&bytes[value_start..value_end]);
        pos = value_end;
    }
}

/// Compare two documents using pre-extracted key byte ranges.
/// `a_ranges`/`b_ranges` are `(start, end)` byte slices into the respective docs.
pub(super) fn compare_preextracted(
    a_doc: &[u8],
    a_ranges: &[(usize, usize)],
    b_doc: &[u8],
    b_ranges: &[(usize, usize)],
) -> std::cmp::Ordering {
    use nodedb_query::msgpack_scan::compare_field_bytes;
    for (a_range, b_range) in a_ranges.iter().zip(b_ranges.iter()) {
        let ord = compare_field_bytes(a_doc, *a_range, b_doc, *b_range);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
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
            bare_view.get_or_insert_with(|| with_bare_names(row))
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
fn with_bare_names(row: &[u8]) -> Vec<u8> {
    let Some((count, mut pos)) = msgpack_scan::map_header(row, 0) else {
        return row.to_vec();
    };
    let body_start = pos;
    let mut keys: Vec<&str> = Vec::with_capacity(count);
    let mut values: Vec<(usize, usize)> = Vec::with_capacity(count);
    for _ in 0..count {
        let key = msgpack_scan::read_str(row, pos);
        let Some(val_start) = msgpack_scan::skip_value(row, pos) else {
            return row.to_vec();
        };
        let Some(val_end) = msgpack_scan::skip_value(row, val_start) else {
            return row.to_vec();
        };
        keys.push(key.unwrap_or(""));
        values.push((val_start, val_end));
        pos = val_end;
    }
    let body_end = pos;

    let mut added: Vec<(&str, (usize, usize))> = Vec::new();
    for (key, value) in keys.iter().zip(&values) {
        let Some((_, column)) = key.rsplit_once('.') else {
            continue;
        };
        if column.is_empty()
            || keys.contains(&column)
            || added.iter().any(|(name, _)| *name == column)
        {
            continue;
        }
        added.push((column, *value));
    }

    let mut buf = Vec::with_capacity(row.len() * 2);
    write_map_header(&mut buf, count + added.len());
    buf.extend_from_slice(&row[body_start..body_end]);
    for (name, (start, end)) in &added {
        write_str(&mut buf, name);
        buf.extend_from_slice(&row[*start..*end]);
    }
    buf
}

/// Apply projection to a binary msgpack row, keeping only requested columns.
///
/// Projection names may be unqualified ("name") while keys are qualified
/// ("users.name"). Returns a new msgpack map with only matching fields,
/// using the unqualified name as the output key.
pub(super) fn binary_row_project(
    row: &[u8],
    projection: &[nodedb_physical::physical_plan::JoinProjection],
) -> Vec<u8> {
    let Some((count, pos)) = msgpack_scan::map_header(row, 0) else {
        return row.to_vec();
    };

    // First pass: find matching entries.
    struct Entry {
        output_key: String,
        val_start: usize,
        val_end: usize,
    }
    let mut entries = Vec::with_capacity(projection.len());
    let mut scan_pos = pos;
    for _ in 0..count {
        let key = msgpack_scan::read_str(row, scan_pos);
        scan_pos = match msgpack_scan::skip_value(row, scan_pos) {
            Some(p) => p,
            None => break,
        };
        let val_start = scan_pos;
        scan_pos = match msgpack_scan::skip_value(row, scan_pos) {
            Some(p) => p,
            None => break,
        };
        if let Some(k) = &key {
            let short = k.rsplit('.').next().unwrap_or(k);
            if let Some(projected) = projection
                .iter()
                .find(|p| p.source == short || p.source == *k)
            {
                entries.push(Entry {
                    output_key: projected.output.clone(),
                    val_start,
                    val_end: scan_pos,
                });
            }
        }
    }

    // Build output map.
    let mut buf = Vec::with_capacity(row.len());
    write_map_header(&mut buf, entries.len());
    for e in &entries {
        write_str(&mut buf, &e.output_key);
        buf.extend_from_slice(&row[e.val_start..e.val_end]);
    }
    buf
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

        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b");
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
        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b");

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
        );
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
        let merged = merge_join_docs_binary(&left, Some(&right), "a", "b");

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
        );

        let json = nodedb_types::json_from_msgpack(&projected).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.get("emp1").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(obj.get("emp2").and_then(|v| v.as_str()), Some("Bob"));
        assert_eq!(obj.get("a.dept").and_then(|v| v.as_str()), Some("eng"));
    }
}
