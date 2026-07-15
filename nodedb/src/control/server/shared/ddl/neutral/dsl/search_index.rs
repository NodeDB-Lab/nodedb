// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SEARCH INDEX` DSL handler (higher-level alias for fulltext).

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};
use nodedb_physical::physical_plan::TextOp;
use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CREATE SEARCH INDEX ON <collection> FIELDS <field1>[, <field2>...] [ANALYZER '<name>'] [FUZZY true|false]
///
/// `ANALYZER '<name>'`, when present, binds the collection's per-collection
/// FTS analyzer (`InvertedIndex::set_collection_analyzer`) — the SAME
/// analyzer-registry lookup forward indexing (`index_document_in_txn`), the
/// staged-write overlay (`fts_merge`/`fts_score`), and the base search path
/// all resolve through via `InvertedIndex::analyze_for_collection`. Binding
/// is dispatched to the Data Plane exactly like `VectorOp::SetParams` (the
/// same one-shot, non-WAL-durable config-write pattern `CREATE VECTOR INDEX`
/// uses).
pub async fn create_search_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let on_pos = find_ascii_case_insensitive(sql, " ON ").ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;
    let after_on = sql[on_pos + 4..].trim_start();
    let fields_pos = find_ascii_case_insensitive(sql, " FIELDS ").ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;

    let collection = after_on[..fields_pos - on_pos - 4].trim().to_lowercase();
    if collection.is_empty() {
        return Err(ddl_err("42601", "missing collection name"));
    }

    let after_fields = &sql[fields_pos + 8..];
    let analyzer_pos = find_ascii_case_insensitive(after_fields, " ANALYZER ");
    let fields_end = analyzer_pos
        .or_else(|| find_ascii_case_insensitive(after_fields, " FUZZY "))
        .unwrap_or(after_fields.len());
    let fields_str = after_fields[..fields_end].trim();
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();

    if fields.is_empty() || fields[0].is_empty() {
        return Err(ddl_err("42601", "missing field list"));
    }

    let analyzer_name = analyzer_pos
        .map(|rel_pos| {
            let after_analyzer = &after_fields[rel_pos + 10..];
            parse_analyzer_name(after_analyzer)
        })
        .transpose()?;

    let tenant_id = identity.tenant_id;

    for field in &fields {
        let index_name = format!("fts_{}_{}", collection, field);

        crate::control::server::shared::ddl::owner::propose_owner(
            state,
            "fulltext_index",
            tenant_id,
            &index_name,
            &identity.username,
        )?;

        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("created search index '{index_name}' on '{collection}' ({field})"),
        );
    }

    if let Some(analyzer_name) = analyzer_name {
        let vshard =
            crate::types::VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
        let set_analyzer_plan = PhysicalPlan::Text(TextOp::SetAnalyzer {
            collection: collection.clone(),
            analyzer_name: analyzer_name.clone(),
        });
        crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            DatabaseId::DEFAULT,
            vshard,
            set_analyzer_plan,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| ddl_err("58000", format!("failed to bind analyzer: {e}")))?;

        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("bound analyzer '{analyzer_name}' to collection '{collection}'"),
        );
    }

    Ok(vec![DdlResult::Status {
        command: "CREATE SEARCH INDEX".to_string(),
        rows_affected: None,
    }])
}

/// Extract the quoted analyzer name immediately following ` ANALYZER `.
/// Accepts `'name'` or `"name"`; rejects a missing/unterminated literal.
fn parse_analyzer_name(after_analyzer: &str) -> Result<String, DdlError> {
    let trimmed = after_analyzer.trim_start();
    let quote = trimmed.chars().next().filter(|c| *c == '\'' || *c == '"');
    let Some(quote) = quote else {
        return Err(ddl_err(
            "42601",
            "syntax: ANALYZER requires a quoted name, e.g. ANALYZER 'english'",
        ));
    };
    let rest = &trimmed[1..];
    let end = rest.find(quote).ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: unterminated ANALYZER name literal (missing closing quote)",
        )
    })?;
    let name = rest[..end].trim().to_string();
    if name.is_empty() {
        return Err(ddl_err("42601", "ANALYZER name must not be empty"));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::parse_analyzer_name;

    #[test]
    fn parses_single_quoted_name() {
        assert_eq!(
            parse_analyzer_name("'english' FUZZY true").unwrap(),
            "english"
        );
    }

    #[test]
    fn parses_double_quoted_name() {
        assert_eq!(parse_analyzer_name("\"simple\"").unwrap(), "simple");
    }

    #[test]
    fn rejects_unquoted_name() {
        assert!(parse_analyzer_name("english").is_err());
    }

    #[test]
    fn rejects_unterminated_literal() {
        assert!(parse_analyzer_name("'english").is_err());
    }

    #[test]
    fn rejects_empty_name() {
        assert!(parse_analyzer_name("''").is_err());
    }
}
