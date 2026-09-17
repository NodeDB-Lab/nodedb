// SPDX-License-Identifier: BUSL-1.1

//! SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::types::CheckpointDoc;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::dispatch::dispatch_authorized_read;

/// Names the historical read in the refusal a read policy raises: the payload
/// is the document's merged state at a version, which carries no row for a
/// filter to apply to — exactly what `rls_injection::crdt` concludes for
/// `CrdtOp::ReadAtVersion`.
const AT_VERSION_WHAT: &str =
    "a historical document read, which returns merged CRDT state at a version";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'
pub async fn select_at_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, checkpoint_name, doc_id) = parse_at_version(sql)?;
    let tenant_id = identity.tenant_id;

    // The read returns the document's stored content, so it carries the same
    // read grant a `SELECT` against the collection does — and a read policy
    // refuses it, because the merged state comes back as one opaque payload
    // with no row a filter could be evaluated against. The checkpoint lookup
    // below already discloses that a named checkpoint exists for this document,
    // so the gate runs before it.
    RefusingReadGate::open(state, identity, database_id, &collection, AT_VERSION_WHAT)?;

    // Resolve checkpoint name to version vector.
    let vv_json = resolve_checkpoint_vv(
        state,
        CheckpointDoc::new(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &collection,
            &doc_id,
        ),
        &checkpoint_name,
    )?;

    // Dispatch to the Data Plane through the authorized door — this is user
    // SQL, so the plan that reaches storage is the one authorization approved.
    let plan = PhysicalPlan::Crdt(CrdtOp::ReadAtVersion {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        document_id: doc_id.clone(),
        version_vector_json: vv_json,
    });
    let payload = dispatch_authorized_read(state, identity, database_id, &collection, plan).await?;

    let text = String::from_utf8_lossy(&payload).into_owned();

    let columns = vec!["document".to_string()];
    let mut row = Map::new();
    row.insert("document".to_string(), JsonValue::String(text));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}

/// Resolve a checkpoint name to its version vector JSON.
/// If the name looks like a raw JSON object (`{...}`), use it directly.
pub(super) fn resolve_checkpoint_vv(
    state: &SharedState,
    doc: CheckpointDoc<'_>,
    checkpoint_or_vv: &str,
) -> Result<String, DdlError> {
    // If it looks like raw JSON VV, pass through.
    let trimmed = checkpoint_or_vv.trim();
    if trimmed.starts_with('{') {
        return Ok(trimmed.to_owned());
    }

    // Otherwise, look up checkpoint name in catalog.
    let catalog = state.credentials.catalog();
    let record = catalog
        .get_checkpoint(doc, checkpoint_or_vv)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| {
            err(
                "42704",
                format!(
                    "checkpoint '{checkpoint_or_vv}' not found for {}/{}",
                    doc.collection, doc.doc_id
                ),
            )
        })?;
    Ok(record.version_vector_json)
}

/// Parse: SELECT * FROM collection AT VERSION 'checkpoint' WHERE id = 'doc-id'
fn parse_at_version(sql: &str) -> Result<(String, String, String), DdlError> {
    // Find "AT VERSION"
    let at_pos = find_ascii_case_insensitive(sql, "AT VERSION")
        .ok_or_else(|| err("42601", "expected AT VERSION".to_string()))?;

    // Collection: between "FROM " and " AT VERSION"
    let from_pos = find_ascii_case_insensitive(sql, "FROM ")
        .ok_or_else(|| err("42601", "expected FROM <collection>".to_string()))?;
    let collection = parse_ident_token(sql[from_pos + 5..at_pos].trim())?;

    // Checkpoint name: after "AT VERSION " until "WHERE"
    let after_at = sql[at_pos + 10..].trim();
    let where_pos = find_ascii_case_insensitive(after_at, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE id = '<doc_id>'".to_string()))?;
    let checkpoint_part = after_at[..where_pos].trim();
    let checkpoint = checkpoint_part
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned();

    // Doc ID from WHERE clause.
    let where_clause = after_at[where_pos + 5..].trim();
    let eq_pos = where_clause
        .find('=')
        .ok_or_else(|| err("42601", "expected 'id = <value>'".to_string()))?;
    let value_part = where_clause[eq_pos + 1..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();

    Ok((collection, checkpoint, doc_id))
}
