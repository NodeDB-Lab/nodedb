// SPDX-License-Identifier: BUSL-1.1

//! Control Plane CHECK constraint enforcement for the standard SQL path.
//!
//! Intercepts INSERT and UPDATE statements before planning to evaluate
//! general CHECK constraints. For UPDATE, fetches the current document
//! and merges SET values for cross-field CHECK evaluation.

use nodedb_types::DatabaseId;
use std::collections::HashMap;

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::types::TenantId;

use super::super::core::NodeDbPgHandler;

/// Extract the collection name and operation type from an INSERT or UPDATE SQL
/// statement. Returns `None` for any other statement kind.
fn extract_collection_from_sql(sql: &str) -> Option<(String, bool)> {
    let upper = sql.to_uppercase();
    if upper.starts_with("INSERT INTO ") {
        let after = sql["INSERT INTO ".len()..].trim_start();
        let end = after
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after.len());
        Some((after[..end].to_lowercase(), true))
    } else if upper.starts_with("UPDATE ") {
        let after = sql["UPDATE ".len()..].trim_start();
        let end = after
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after.len());
        Some((after[..end].to_lowercase(), false))
    } else {
        None
    }
}

impl NodeDbPgHandler {
    /// Enforce general CHECK constraints before planning INSERT or UPDATE SQL.
    ///
    /// Extracts the collection name from the SQL text, looks up CHECK constraints
    /// from the catalog, and if any exist, parses column/value pairs from the SQL
    /// to evaluate each CHECK expression.
    pub(super) async fn enforce_check_constraints_if_needed(
        &self,
        sql: &str,
        tenant_id: TenantId,
        database_id: DatabaseId,
    ) -> PgWireResult<()> {
        let Some((coll_name, is_insert)) = extract_collection_from_sql(sql) else {
            return Ok(());
        };

        // Look up collection and its CHECK constraints.
        let catalog = self.state.credentials.catalog();
        let coll = match catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
            Ok(Some(c)) => c,
            _ => return Ok(()),
        };
        if coll.check_constraints.is_empty() {
            return Ok(());
        }

        // Extract column/value pairs from the SQL text.
        let mut fields = if is_insert {
            extract_insert_fields(sql).map_err(|e| pgwire_err("42601", &e))?
        } else {
            extract_update_fields(sql).map_err(|e| pgwire_err("42601", &e))?
        };

        if fields.is_empty() {
            return Ok(());
        }

        // For UPDATE: merge SET values with current document for cross-field CHECK.
        if !is_insert && let Some(doc_id) = extract_where_id(sql) {
            let old = crate::control::trigger::dml_hook::fetch_old_row(
                &self.state,
                database_id,
                tenant_id,
                &coll_name,
                &doc_id,
            )
            .await;
            let mut merged = old;
            for (k, v) in &fields {
                merged.insert(k.clone(), v.clone());
            }
            fields = merged;
        }

        crate::control::server::shared::check_constraint::enforce_check_constraints(
            &self.state,
            tenant_id,
            &coll.check_constraints,
            &fields,
        )
        .await
        .map_err(|e| pgwire_err(&e.sqlstate, &e.message))
    }

    /// Validate enum-typed column values against the custom type registry.
    ///
    /// Intercepts INSERT and UPDATE for any collection whose `fields` list
    /// contains a user-defined enum type. No-op if the collection has no
    /// enum-typed columns or if the SQL is not an INSERT/UPDATE.
    pub(super) async fn enforce_enum_labels_if_needed(
        &self,
        sql: &str,
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
    ) -> PgWireResult<()> {
        let Some((coll_name, is_insert)) = extract_collection_from_sql(sql) else {
            return Ok(());
        };

        let catalog = self.state.credentials.catalog();
        let coll = match catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
            Ok(Some(c)) => c,
            _ => return Ok(()),
        };

        // Quick path: no user-defined types means nothing to validate.
        if coll.fields.is_empty() {
            return Ok(());
        }

        let fields = if is_insert {
            match extract_insert_fields(sql) {
                Ok(f) => f,
                Err(_) => return Ok(()), // Unparseable; let the planner handle errors.
            }
        } else {
            match extract_update_fields(sql) {
                Ok(f) => f,
                Err(_) => return Ok(()),
            }
        };

        for (field_name, type_name) in &coll.fields {
            let Some(value) = fields.get(field_name.as_str()) else {
                continue;
            };
            let label = match value {
                nodedb_types::Value::String(s) => s.as_str(),
                _ => continue,
            };
            if let Err(msg) = self.state.custom_type_registry.validate_enum_label(
                tenant_id.as_u64(),
                type_name,
                label,
            ) {
                return Err(pgwire_err("22P02", &msg));
            }
        }

        Ok(())
    }
}

/// Extract column/value pairs from `INSERT INTO x (col1, col2) VALUES (val1, val2)`.
fn extract_insert_fields(sql: &str) -> Result<HashMap<String, nodedb_types::Value>, String> {
    let cols_start = sql.find('(').ok_or_else(|| {
        let preview: String = sql.chars().take(60).collect();
        format!("missing '(' in INSERT: {preview}")
    })?;
    let cols_end = sql[cols_start + 1..]
        .find(')')
        .map(|p| cols_start + 1 + p)
        .ok_or_else(|| "missing ')' after column list in INSERT".to_string())?;
    let cols: Vec<&str> = sql[cols_start + 1..cols_end]
        .split(',')
        .map(|s| s.trim())
        .collect();

    let values_pos = find_ascii_case_insensitive(sql, "VALUES")
        .ok_or_else(|| "missing VALUES keyword in INSERT".to_string())?
        + 6;
    let vals_start = sql[values_pos..]
        .find('(')
        .map(|p| values_pos + p + 1)
        .ok_or_else(|| "missing '(' after VALUES in INSERT".to_string())?;

    let mut depth = 1i32;
    let mut vals_end = vals_start;
    for (i, ch) in sql[vals_start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    vals_end = vals_start + i;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("unmatched parentheses in VALUES clause".to_string());
    }

    let vals = split_top_level_commas(&sql[vals_start..vals_end]);
    let mut fields = HashMap::new();
    for (i, col) in cols.iter().enumerate() {
        if let Some(val_str) = vals.get(i) {
            let col_name = col.trim_matches('"').trim_matches('`').to_lowercase();
            let val = parse_sql_literal(val_str.trim());
            fields.insert(col_name, val);
        }
    }

    Ok(fields)
}

/// Extract column/value pairs from `UPDATE x SET col1 = val1, col2 = val2 WHERE ...`.
fn extract_update_fields(sql: &str) -> Result<HashMap<String, nodedb_types::Value>, String> {
    let set_pos = find_ascii_case_insensitive(sql, " SET ")
        .ok_or_else(|| "missing SET keyword in UPDATE".to_string())?
        + 5;

    let where_pos = find_ascii_case_insensitive_from(sql, " WHERE ", set_pos).unwrap_or(sql.len());
    let assignments_str = &sql[set_pos..where_pos];

    let mut fields = HashMap::new();
    for assignment in split_top_level_commas(assignments_str) {
        let assignment = assignment.trim();
        if let Some(eq_pos) = assignment.find('=') {
            let col = assignment[..eq_pos]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_lowercase();
            let val_str = assignment[eq_pos + 1..].trim();
            let val = parse_sql_literal(val_str);
            fields.insert(col, val);
        }
    }

    Ok(fields)
}

/// Extract document ID from a `WHERE id = 'value'` clause.
///
/// Only matches standalone `id` with word boundaries — `userid`, `order_id` etc. won't match.
fn extract_where_id(sql: &str) -> Option<String> {
    let where_pos = find_ascii_case_insensitive(sql, " WHERE ")?;
    let after = &sql[where_pos + 7..];
    // Find standalone "ID" with word boundary checks.
    let mut search_start = 0;
    loop {
        let abs_pos = find_ascii_case_insensitive_from(after, "ID", search_start)?;

        // Check word boundary before: must be start or non-alphanumeric/underscore.
        if abs_pos > 0 {
            let prev = after.as_bytes()[abs_pos - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                search_start = abs_pos + 2;
                continue;
            }
        }
        // Check word boundary after: must be end or non-alphanumeric/underscore.
        let end_pos = abs_pos + 2;
        if end_pos < after.len() {
            let next = after.as_bytes()[end_pos];
            if next.is_ascii_alphanumeric() || next == b'_' {
                search_start = end_pos;
                continue;
            }
        }

        let after_id = after[end_pos..].trim_start();
        if !after_id.starts_with('=') {
            search_start = end_pos;
            continue;
        }
        let val_str = after_id[1..].trim_start();

        if let Some(inner) = val_str.strip_prefix('\'') {
            let end = inner.find('\'')?;
            return Some(inner[..end].to_string());
        }
        if let Some(inner) = val_str.strip_prefix('"') {
            let end = inner.find('"')?;
            return Some(inner[..end].to_string());
        }
        let end = val_str
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(val_str.len());
        return Some(val_str[..end].to_string());
    }
}

/// Split a string on commas, respecting parentheses and string quotes.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => depth -= 1,
            ',' if depth == 0 && !in_single_quote && !in_double_quote => {
                parts.push(&s[last..i]);
                last = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[last..]);
    parts
}

/// Parse a SQL literal string into a Value (best-effort).
fn parse_sql_literal(s: &str) -> nodedb_types::Value {
    let s = s.trim();
    let upper = s.to_uppercase();

    if upper == "NULL" {
        return nodedb_types::Value::Null;
    }
    if upper == "TRUE" {
        return nodedb_types::Value::Bool(true);
    }
    if upper == "FALSE" {
        return nodedb_types::Value::Bool(false);
    }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return nodedb_types::Value::String(inner.replace("''", "'"));
    }
    if let Ok(i) = s.parse::<i64>() {
        return nodedb_types::Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return nodedb_types::Value::Float(f);
    }
    nodedb_types::Value::String(s.to_string())
}

fn pgwire_err(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_where_id_basic() {
        let sql = "UPDATE orders SET amount = 5 WHERE id = 'o1'";
        assert_eq!(extract_where_id(sql), Some("o1".to_string()));
    }

    #[test]
    fn extract_where_id_no_match_userid() {
        // "userid" should NOT match — only standalone "id".
        let sql = "UPDATE orders SET amount = 5 WHERE userid = 'u1'";
        assert_eq!(extract_where_id(sql), None);
    }

    #[test]
    fn extract_where_id_no_match_order_id() {
        let sql = "UPDATE orders SET amount = 5 WHERE order_id = 'x'";
        assert_eq!(extract_where_id(sql), None);
    }

    #[test]
    fn extract_where_id_after_unicode_value_preserves_original_offsets() {
        let sql = "UPDATE orders SET note = 'ǰ' WHERE id = 'o1'";
        assert_eq!(extract_where_id(sql), Some("o1".to_string()));
    }

    #[test]
    fn extract_insert_fields_basic() {
        let fields = extract_insert_fields("INSERT INTO t (a, b) VALUES ('hello', 42)").unwrap();
        assert_eq!(
            fields.get("a"),
            Some(&nodedb_types::Value::String("hello".into()))
        );
        assert_eq!(fields.get("b"), Some(&nodedb_types::Value::Integer(42)));
    }

    #[test]
    fn extract_insert_fields_error_on_bad_sql() {
        let result = extract_insert_fields("INSERT INTO t no_parens");
        assert!(result.is_err());
    }

    #[test]
    fn extract_insert_fields_with_unicode_before_values_preserves_original_offsets() {
        let fields = extract_insert_fields("INSERT INTO tﬀﬀ (a) VALUES (42)").unwrap();
        assert_eq!(fields.get("a"), Some(&nodedb_types::Value::Integer(42)));
    }

    #[test]
    fn malformed_insert_preview_respects_utf8_boundaries() {
        let sql = format!("INSERT INTO {}é no_parens", "a".repeat(47));
        assert_eq!(sql.find('é'), Some(59));
        assert!(extract_insert_fields(&sql).is_err());
    }

    #[test]
    fn extract_update_fields_basic() {
        let fields = extract_update_fields("UPDATE t SET x = 10, y = 'hi' WHERE id = '1'").unwrap();
        assert_eq!(fields.get("x"), Some(&nodedb_types::Value::Integer(10)));
        assert_eq!(
            fields.get("y"),
            Some(&nodedb_types::Value::String("hi".into()))
        );
    }

    #[test]
    fn extract_update_fields_with_unicode_before_set_preserves_original_offsets() {
        let fields = extract_update_fields("UPDATE tǰ SET x = 10 WHERE id = '1'").unwrap();
        assert_eq!(fields.get("x"), Some(&nodedb_types::Value::Integer(10)));
    }
}
