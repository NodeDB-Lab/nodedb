// SPDX-License-Identifier: BUSL-1.1

//! SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::catalog::types::CheckpointDoc;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;

/// Names the checkpoint listing in the refusal a read policy raises: each row
/// is metadata *about* one document — that it exists, when it was checkpointed
/// and by whom — and the listing carries no document row a filter could be
/// evaluated against.
const SHOW_VERSIONS_WHAT: &str =
    "a checkpoint listing, which returns version metadata about a document";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]
pub fn show_versions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, doc_id, limit) = parse_show_versions(sql)?;
    let tenant_id = identity.tenant_id;

    // The listing is read straight from the catalog, so nothing downstream
    // authorizes it: a checkpoint name, its creator, and its timestamp all
    // describe a document in `collection`, and disclosing them requires the
    // same read grant the document itself does.
    RefusingReadGate::open(
        state,
        identity,
        database_id,
        &collection,
        SHOW_VERSIONS_WHAT,
    )?;

    let catalog = state.credentials.catalog();

    let records = catalog
        .list_checkpoints(
            CheckpointDoc::new(
                database_id.as_u64(),
                tenant_id.as_u64(),
                &collection,
                &doc_id,
            ),
            if limit > 0 { limit } else { 1000 },
        )
        .map_err(|e| err("XX000", e.to_string()))?;

    let columns = vec![
        "checkpoint_name".to_string(),
        "version_vector".to_string(),
        "created_by".to_string(),
        "created_at".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ];

    let mut rows = Vec::with_capacity(records.len());
    for record in &records {
        let mut row = Map::new();
        row.insert(
            "checkpoint_name".to_string(),
            JsonValue::String(record.checkpoint_name.clone()),
        );
        row.insert(
            "version_vector".to_string(),
            JsonValue::String(record.version_vector_json.clone()),
        );
        row.insert(
            "created_by".to_string(),
            JsonValue::String(record.created_by.clone()),
        );
        row.insert(
            "created_at".to_string(),
            JsonValue::String((record.created_at as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// Parse: SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]
fn parse_show_versions(sql: &str) -> Result<(String, String, usize), DdlError> {
    let rest = sql["SHOW VERSIONS OF ".len()..].trim();

    let where_pos = find_ascii_case_insensitive(rest, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE id = '<doc_id>'".to_string()))?;
    let collection = parse_ident_token(rest[..where_pos].trim())?;
    let after_where = rest[where_pos + 5..].trim();

    // Parse "id = 'doc-id'" potentially followed by "LIMIT N"
    let limit_pos = find_ascii_case_insensitive(after_where, "LIMIT");
    let (id_clause, limit) = if let Some(lp) = limit_pos {
        let id_part = &after_where[..lp];
        let limit_part = after_where[lp + 5..].trim().trim_end_matches(';').trim();
        let limit = limit_part.parse::<usize>().unwrap_or(20);
        (id_part, limit)
    } else {
        (after_where, 20)
    };

    let eq_pos = id_clause
        .find('=')
        .ok_or_else(|| err("42601", "expected 'id = <value>'".to_string()))?;
    let value_part = id_clause[eq_pos + 1..].trim().trim_end_matches(';').trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();
    if doc_id.is_empty() {
        return Err(err("42601", "document ID is empty".to_string()));
    }

    Ok((collection, doc_id, limit))
}
