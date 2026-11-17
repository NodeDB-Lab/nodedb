// SPDX-License-Identifier: BUSL-1.1

//! Statement-level CHECK and enum-label enforcement for the SQL write path,
//! shared by every protocol.
//!
//! Runs on an INSERT or UPDATE statement before planning: the collection and
//! the column/value pairs come from the SQL text. An UPDATE's current row is
//! merged under its SET values, so a cross-field CHECK sees the whole row. A
//! statement whose collection fires a BEFORE, INSTEAD OF or SYNC AFTER body
//! is checked by the transaction route instead, against the row its BEFORE
//! bodies left.

use std::collections::HashMap;

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};
use nodedb_types::{DatabaseId, strip_prefix_ascii_case_insensitive};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;
use crate::control::trigger::statement_txn::fires_joined_body;
use crate::control::trigger::{DmlEvent, TriggerScope};
use crate::types::{TenantId, TxnId};

use super::enforce::enforce_check_constraints;

/// Enforce the general CHECK constraints of the INSERT or UPDATE in `sql`.
///
/// `txn_id` names the session's open transaction: an UPDATE's current row is
/// read as it left it. Any other statement passes.
pub async fn enforce_statement_checks(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    auth: &AuthContext,
    txn_id: Option<TxnId>,
    sql: &str,
) -> Result<(), DdlError> {
    let Some((coll_name, is_insert)) = extract_collection_from_sql(sql) else {
        return Ok(());
    };

    // CHECK evaluation is on the write path. Authorize its target before
    // catalog lookup or an OLD-row read so unauthorized SQL cannot probe
    // collection metadata or row existence.
    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        &coll_name,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| DdlError::new("42501", error.resource().to_owned()))?;

    // A write that fires a BEFORE, INSTEAD OF or SYNC AFTER body runs
    // through the transaction route, which checks the NEW row as the BEFORE
    // bodies left it.
    let event = if is_insert {
        DmlEvent::Insert
    } else {
        DmlEvent::Update
    };
    if fires_joined_body(
        state,
        TriggerScope {
            database_id,
            tenant_id,
        },
        nodedb_types::QualifiedCollection::new(database_id, &coll_name).as_str(),
        event,
    ) {
        return Ok(());
    }

    let catalog = state.credentials.catalog();
    let coll = match catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
        Ok(Some(c)) => c,
        _ => return Ok(()),
    };
    if coll.check_constraints.is_empty() {
        return Ok(());
    }

    let mut fields = if is_insert {
        extract_insert_fields(sql).map_err(|e| DdlError::new("42601", e))?
    } else {
        extract_update_fields(sql).map_err(|e| DdlError::new("42601", e))?
    };
    if fields.is_empty() {
        return Ok(());
    }

    // For UPDATE: merge SET values over the current row for cross-field CHECK.
    if !is_insert && let Some(doc_id) = extract_where_id(sql) {
        let old = crate::control::trigger::dml_hook::fetch_old_row(
            state,
            identity,
            database_id,
            auth,
            &nodedb_types::QualifiedCollection::new(database_id, &coll_name),
            &doc_id,
            txn_id,
        )
        .await
        .map_err(|error| {
            let (_, sqlstate, message) =
                crate::control::server::pgwire::types::error_to_sqlstate(&error);
            DdlError::new(sqlstate, message)
        })?;
        let mut merged = old;
        for (k, v) in &fields {
            merged.insert(k.clone(), v.clone());
        }
        fields = merged;
    }

    enforce_check_constraints(
        state,
        identity,
        database_id,
        &coll.check_constraints,
        &fields,
    )
    .await
}

/// Validate the enum-typed column values of the INSERT or UPDATE in `sql`
/// against the custom type registry. Any other statement, and a collection
/// with no enum-typed column, passes.
pub fn enforce_statement_enum_labels(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    sql: &str,
) -> Result<(), DdlError> {
    let Some((coll_name, is_insert)) = extract_collection_from_sql(sql) else {
        return Ok(());
    };

    let catalog = state.credentials.catalog();
    let coll = match catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
        Ok(Some(c)) => c,
        _ => return Ok(()),
    };
    // Quick path: no user-defined types means nothing to validate.
    if coll.fields.is_empty() {
        return Ok(());
    }

    let fields = if is_insert {
        extract_insert_fields(sql)
    } else {
        extract_update_fields(sql)
    };
    // Unparseable text is the planner's to refuse.
    let Ok(fields) = fields else {
        return Ok(());
    };

    for (field_name, type_name) in &coll.fields {
        let Some(nodedb_types::Value::String(label)) = fields.get(field_name.as_str()) else {
            continue;
        };
        if let Err(msg) = state.custom_type_registry.validate_enum_label(
            database_id.as_u64(),
            tenant_id.as_u64(),
            type_name,
            label,
        ) {
            return Err(DdlError::new("22P02", msg));
        }
    }
    Ok(())
}

/// Extract the collection name and operation type from an INSERT or UPDATE SQL
/// statement. Returns `None` for any other statement kind.
pub(super) fn extract_collection_from_sql(sql: &str) -> Option<(String, bool)> {
    if let Some(after) = strip_prefix_ascii_case_insensitive(sql, "INSERT INTO ") {
        let after = after.trim_start();
        let end = after
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(after.len());
        Some((after[..end].to_lowercase(), true))
    } else if let Some(after) = strip_prefix_ascii_case_insensitive(sql, "UPDATE ") {
        let after = after.trim_start();
        let end = after
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after.len());
        Some((after[..end].to_lowercase(), false))
    } else {
        None
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
        let Some(val_str) = after_id.strip_prefix('=') else {
            search_start = end_pos;
            continue;
        };
        let val_str = val_str.trim_start();

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

    if s.eq_ignore_ascii_case("NULL") {
        return nodedb_types::Value::Null;
    }
    if s.eq_ignore_ascii_case("TRUE") {
        return nodedb_types::Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("FALSE") {
        return nodedb_types::Value::Bool(false);
    }
    if let Some(inner) = s
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    {
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
        // "userid" must NOT match — only standalone "id".
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
