//! Rewrite `INSERT/UPSERT INTO coll { ... }` (and `[{ ... }, ...]`) into
//! standard `INSERT INTO coll (cols) VALUES (row), ...`.

use super::literal::value_to_sql_literal;
use crate::parser::object_literal::{parse_object_literal, parse_object_literal_array};

/// Try to rewrite `INSERT INTO coll { ... }` or `INSERT INTO coll [{ ... }, { ... }]`
/// into standard `INSERT INTO coll (cols) VALUES (row1), (row2)`.
///
/// Returns `None` if the statement doesn't use object literal syntax.
pub(super) fn try_rewrite_object_literal(sql: &str) -> Option<String> {
    let after_into = sql["INSERT INTO ".len()..].trim_start();
    let coll_end = after_into.find(|c: char| c.is_whitespace())?;
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();

    if obj_str.starts_with('[') {
        return rewrite_array_form(coll_name, obj_str);
    }

    if !obj_str.starts_with('{') {
        return None;
    }

    let fields = parse_object_literal(obj_str)?.ok()?;
    if fields.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &[fields]))
}

/// Rewrite `[{ ... }, { ... }]` → multi-row VALUES.
fn rewrite_array_form(coll_name: &str, obj_str: &str) -> Option<String> {
    let objects = parse_object_literal_array(obj_str)?.ok()?;
    if objects.is_empty() {
        return None;
    }
    Some(fields_to_values_sql(coll_name, &objects))
}

/// Build `INSERT INTO coll (col_union) VALUES (row1), (row2), ...`
///
/// Collects the union of all keys across all rows. Missing keys get NULL.
fn fields_to_values_sql(
    coll_name: &str,
    rows: &[std::collections::HashMap<String, nodedb_types::Value>],
) -> String {
    let mut all_keys: Vec<String> = rows
        .iter()
        .flat_map(|r| r.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    all_keys.sort();

    let col_list = all_keys.join(", ");

    let row_strs: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = all_keys
                .iter()
                .map(|k| match row.get(k) {
                    Some(v) => value_to_sql_literal(v),
                    None => "NULL".to_string(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        coll_name,
        col_list,
        row_strs.join(", ")
    )
}
