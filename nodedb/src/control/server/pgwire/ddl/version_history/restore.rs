//! RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'

use std::time::Duration;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;

/// RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'
///
/// Restores a document to a historical version by creating a forward delta.
/// History is preserved — this is a new mutation, not a rollback.
pub async fn restore_version(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (collection, checkpoint_name, doc_id) = parse_restore(sql)?;
    let tenant_id = identity.tenant_id;

    let vv_json = super::at_version::resolve_checkpoint_vv(
        state,
        tenant_id.as_u32(),
        &collection,
        &doc_id,
        &checkpoint_name,
    )?;

    let surrogate = state
        .surrogate_assigner
        .assign(&collection, doc_id.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &format!("surrogate assign: {e}")))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
        collection: collection.clone(),
        document_id: doc_id.clone(),
        target_version_json: vv_json,
        surrogate,
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    super::super::sync_dispatch::dispatch_async(state, tenant_id, &collection, plan, timeout)
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("restore dispatch: {e}")))?;

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("RESTORE {collection}/{doc_id} to version '{checkpoint_name}'"),
        );

    Ok(vec![Response::Execution(Tag::new("RESTORE"))])
}

/// Parse: RESTORE collection SET VERSION = 'checkpoint' WHERE id = 'doc-id'
fn parse_restore(sql: &str) -> PgWireResult<(String, String, String)> {
    let rest = sql["RESTORE ".len()..].trim();

    // Collection: before "SET VERSION"
    let set_pos = rest
        .to_uppercase()
        .find("SET VERSION")
        .ok_or_else(|| sqlstate_error("42601", "expected SET VERSION"))?;
    let collection = rest[..set_pos].trim().to_lowercase();

    // Checkpoint: between "=" and "WHERE"
    let after_set = rest[set_pos + 11..].trim(); // After "SET VERSION"
    let eq_pos = after_set
        .find('=')
        .ok_or_else(|| sqlstate_error("42601", "expected '=' after SET VERSION"))?;
    let after_eq = after_set[eq_pos + 1..].trim();

    let where_pos = after_eq
        .to_uppercase()
        .find("WHERE")
        .ok_or_else(|| sqlstate_error("42601", "expected WHERE id = '<doc_id>'"))?;
    let checkpoint = after_eq[..where_pos]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned();

    // Doc ID from WHERE clause.
    let where_clause = after_eq[where_pos + 5..].trim();
    let id_eq = where_clause
        .find('=')
        .ok_or_else(|| sqlstate_error("42601", "expected 'id = <value>'"))?;
    let value_part = where_clause[id_eq + 1..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();

    Ok((collection, checkpoint, doc_id))
}
