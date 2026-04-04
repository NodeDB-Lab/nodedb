//! SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};

/// SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'
pub async fn select_at_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, checkpoint_name, doc_id) = parse_at_version(sql)?;
    let tenant_id = identity.tenant_id;

    // Resolve checkpoint name to version vector.
    let vv_json = resolve_checkpoint_vv(
        state,
        tenant_id.as_u32(),
        &collection,
        &doc_id,
        &checkpoint_name,
    )?;

    // Dispatch to Data Plane.
    let plan = PhysicalPlan::Crdt(CrdtOp::ReadAtVersion {
        collection: collection.clone(),
        document_id: doc_id.clone(),
        version_vector_json: vv_json,
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let payload =
        super::super::sync_dispatch::dispatch_async(state, tenant_id, &collection, plan, timeout)
            .await
            .map_err(|e| sqlstate_error("XX000", &format!("dispatch: {e}")))?;

    let text = String::from_utf8_lossy(&payload).into_owned();

    let schema = Arc::new(vec![text_field("document")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}

/// Resolve a checkpoint name to its version vector JSON.
/// If the name looks like a raw JSON object (`{...}`), use it directly.
pub(super) fn resolve_checkpoint_vv(
    state: &SharedState,
    tenant_id: u32,
    collection: &str,
    doc_id: &str,
    checkpoint_or_vv: &str,
) -> PgWireResult<String> {
    // If it looks like raw JSON VV, pass through.
    let trimmed = checkpoint_or_vv.trim();
    if trimmed.starts_with('{') {
        return Ok(trimmed.to_owned());
    }

    // Otherwise, look up checkpoint name in catalog.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "catalog unavailable"));
    };
    let record = catalog
        .get_checkpoint(tenant_id, collection, doc_id, checkpoint_or_vv)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| {
            sqlstate_error(
                "42704",
                &format!("checkpoint '{checkpoint_or_vv}' not found for {collection}/{doc_id}"),
            )
        })?;
    Ok(record.version_vector_json)
}

/// Parse: SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'
fn parse_at_version(sql: &str) -> PgWireResult<(String, String, String)> {
    let upper = sql.to_uppercase();

    // Find "AT VERSION"
    let at_pos = upper
        .find("AT VERSION")
        .ok_or_else(|| sqlstate_error("42601", "expected AT VERSION"))?;

    // Collection: between "FROM " and " AT VERSION"
    let from_pos = upper
        .find("FROM ")
        .ok_or_else(|| sqlstate_error("42601", "expected FROM <collection>"))?;
    let collection = sql[from_pos + 5..at_pos].trim().to_lowercase();

    // Checkpoint name: after "AT VERSION " until "WHERE"
    let after_at = sql[at_pos + 10..].trim();
    let where_pos = after_at
        .to_uppercase()
        .find("WHERE")
        .ok_or_else(|| sqlstate_error("42601", "expected WHERE id = '<doc_id>'"))?;
    let checkpoint_part = after_at[..where_pos].trim();
    let checkpoint = checkpoint_part
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned();

    // Doc ID from WHERE clause.
    let where_clause = after_at[where_pos + 5..].trim();
    let eq_pos = where_clause
        .find('=')
        .ok_or_else(|| sqlstate_error("42601", "expected 'id = <value>'"))?;
    let value_part = where_clause[eq_pos + 1..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();

    Ok((collection, checkpoint, doc_id))
}
