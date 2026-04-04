//! SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]
pub fn show_versions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, doc_id, limit) = parse_show_versions(sql)?;
    let tenant_id = identity.tenant_id;

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };

    let records = catalog
        .list_checkpoints(
            tenant_id.as_u32(),
            &collection,
            &doc_id,
            if limit > 0 { limit } else { 1000 },
        )
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![
        text_field("checkpoint_name"),
        text_field("version_vector"),
        text_field("created_by"),
        int8_field("created_at"),
    ]);

    let mut rows = Vec::with_capacity(records.len());
    for record in &records {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&record.checkpoint_name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&record.version_vector_json)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&record.created_by)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(record.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Parse: SHOW VERSIONS OF collection WHERE id = 'doc-id' [LIMIT N]
fn parse_show_versions(sql: &str) -> PgWireResult<(String, String, usize)> {
    let rest = sql["SHOW VERSIONS OF ".len()..].trim();

    let where_pos = rest
        .to_uppercase()
        .find("WHERE")
        .ok_or_else(|| sqlstate_error("42601", "expected WHERE id = '<doc_id>'"))?;
    let collection = rest[..where_pos].trim().to_lowercase();
    let after_where = rest[where_pos + 5..].trim();

    // Parse "id = 'doc-id'" potentially followed by "LIMIT N"
    let limit_pos = after_where.to_uppercase().find("LIMIT");
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
        .ok_or_else(|| sqlstate_error("42601", "expected 'id = <value>'"))?;
    let value_part = id_clause[eq_pos + 1..].trim().trim_end_matches(';').trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();
    if doc_id.is_empty() {
        return Err(sqlstate_error("42601", "document ID is empty"));
    }

    Ok((collection, doc_id, limit))
}
