// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::server::shared::ddl::sql_parse::{parse_sql_value, split_values};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::types::{ParsedInsert, ddl_err};

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let the SQL path handle it).
pub(in crate::control::server::shared::ddl::neutral::collection) fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    keyword: &str,
) -> Option<Result<ParsedInsert, DdlError>> {
    let kw_pos = find_ascii_case_insensitive(sql, keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let the SQL path handle typed INSERT
    // with VALUES syntax, but always handle here for pre-write concerns:
    // - UPSERT (triggers + nodedb-sql handles the routing)
    // - { } object literal syntax (triggers + nodedb-sql handles the routing)
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    let after_coll_trimmed = after_into[coll_name_str.len()..].trim_start();
    let is_object_literal =
        after_coll_trimmed.starts_with('{') || after_coll_trimmed.starts_with('[');
    let mut coll_type: Option<nodedb_types::CollectionType> = None;
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), &coll_name) {
        // Skip non-schemaless collections for standard VALUES INSERT (let SQL path handle).
        // But always handle here for: UPSERT, { } object literal (any collection type).
        if !is_upsert && !is_object_literal && !coll.collection_type.is_schemaless() {
            return None;
        }
        coll_type = Some(coll.collection_type.clone());
    }

    // Determine which form this statement uses: { } object literal or (cols) VALUES (vals).
    // If { }, rewrite to VALUES SQL via nodedb-sql's preprocess, then parse that.
    let after_coll_name = after_into[coll_name_str.len()..].trim_start();
    if after_coll_name.starts_with('{') || after_coll_name.starts_with('[') {
        if let Ok(Some(preprocessed)) = nodedb_sql::parser::preprocess::preprocess(sql) {
            let rewritten = preprocessed.sql;
            // The preprocessed SQL is always INSERT INTO regardless of original keyword.
            return parse_values_form(&rewritten, "INSERT INTO ", &coll_name, coll_type);
        }
        return Some(Err(ddl_err(
            "42601",
            "failed to parse object literal in INSERT/UPSERT statement",
        )));
    }

    parse_values_form(sql, keyword, &coll_name, coll_type)
}

/// Parse the `(cols) VALUES (vals)` form.
fn parse_values_form(
    sql: &str,
    keyword: &str,
    coll_name: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<Result<ParsedInsert, DdlError>> {
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err(
                "42601",
                format!("missing column list in {}", keyword.trim()),
            )));
        }
    };
    let values_kw = match find_ascii_case_insensitive(sql, "VALUES") {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing VALUES clause"))),
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(ddl_err("42601", "missing closing ) for column list")));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing VALUES (...)"))),
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => return Some(Err(ddl_err("42601", "missing closing ) for VALUES"))),
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(ddl_err(
            "42601",
            format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    let mut doc_id = String::new();
    let mut fields = HashMap::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        has_returning: find_ascii_case_insensitive(sql, "RETURNING").is_some(),
        collection_type: coll_type,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_keyword_after_unicode_identifier_preserves_original_offsets() {
        let sql = "INSERT INTO tﬀﬀ (a) VALUES (42)";
        let parsed = parse_values_form(sql, "INSERT INTO ", "tﬀﬀ", None)
            .expect("statement should be recognized")
            .expect("statement should parse");
        assert_eq!(
            parsed.fields.get("a"),
            Some(&nodedb_types::Value::Integer(42))
        );
    }
}
