// SPDX-License-Identifier: BUSL-1.1

//! CREATE CHECKPOINT and DROP CHECKPOINT DDL handlers.
//!
//! ```sql
//! CREATE CHECKPOINT 'launch-ready' ON documents WHERE id = 'doc-123';
//! DROP CHECKPOINT 'launch-ready' ON documents WHERE id = 'doc-123';
//! ```

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::types::{CheckpointDoc, CheckpointRecord};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system_by,
};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// CREATE CHECKPOINT 'name' ON collection WHERE id = 'doc-id'
pub async fn create_checkpoint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (checkpoint_name, collection, doc_id) = parse_checkpoint_sql(sql, "CREATE CHECKPOINT")?;
    let tenant_id = identity.tenant_id;

    // Dispatch to Data Plane to get current version vector.
    let plan = PhysicalPlan::Crdt(CrdtOp::GetVersionVector {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
    });
    let vv_bytes = dispatch_system_by(
        state,
        SystemTask::new(
            SystemReason::CatalogMaintenance,
            tenant_id,
            nodedb_types::CollectionKey::from_bare(database_id, &collection),
            plan,
        ),
        StatementDeadline::of_statement(state),
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("dispatch", &e))?;

    let vv_json = String::from_utf8(vv_bytes)
        .map_err(|e| DdlError::internal(format!("version vector decode: {e}")))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let record = CheckpointRecord {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: collection.clone(),
        doc_id: doc_id.clone(),
        checkpoint_name: checkpoint_name.clone(),
        version_vector_json: vv_json,
        created_by: identity.username.clone(),
        created_at: now,
    };

    // The duplicate is reported here, on the leader. Apply is policy-free:
    // rejecting there will leave followers without a row the leader accepted.
    let catalog = state.credentials.catalog();
    let doc = CheckpointDoc::new(
        database_id.as_u64(),
        tenant_id.as_u64(),
        &collection,
        &doc_id,
    );
    if catalog
        .get_checkpoint(doc, &checkpoint_name)
        .map_err(|e| DdlError::from_error(&e))?
        .is_some()
    {
        return Err(err(
            "42710",
            format!("checkpoint '{checkpoint_name}' already exists for {collection}/{doc_id}"),
        ));
    }
    super::replicate::propose_put(state, &record).await?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("CREATE CHECKPOINT '{checkpoint_name}' on {collection}/{doc_id}"),
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE CHECKPOINT".to_string(),
        rows_affected: None,
    }])
}

/// DROP CHECKPOINT 'name' ON collection WHERE id = 'doc-id'
pub async fn drop_checkpoint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (checkpoint_name, collection, doc_id) = parse_checkpoint_sql(sql, "DROP CHECKPOINT")?;
    let tenant_id = identity.tenant_id;

    // Existence is checked here, on the leader. The replicated delete carries
    // no return value, so the SQLSTATE cannot be derived from apply.
    let catalog = state.credentials.catalog();
    let doc = CheckpointDoc::new(
        database_id.as_u64(),
        tenant_id.as_u64(),
        &collection,
        &doc_id,
    );
    if catalog
        .get_checkpoint(doc, &checkpoint_name)
        .map_err(|e| DdlError::from_error(&e))?
        .is_none()
    {
        return Err(err(
            "42704",
            format!("checkpoint '{checkpoint_name}' not found for {collection}/{doc_id}"),
        ));
    }
    super::replicate::propose_delete(state, doc, &checkpoint_name).await?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("DROP CHECKPOINT '{checkpoint_name}' on {collection}/{doc_id}"),
    );

    Ok(vec![DdlResult::Status {
        command: "DROP CHECKPOINT".to_string(),
        rows_affected: None,
    }])
}

/// Parse: `CMD 'name' ON collection WHERE id = 'doc-id'`
fn parse_checkpoint_sql(sql: &str, prefix: &str) -> Result<(String, String, String), DdlError> {
    let rest = sql
        .get(..prefix.len())
        .filter(|actual| actual.eq_ignore_ascii_case(prefix))
        .and_then(|_| sql.get(prefix.len()..))
        .ok_or_else(|| err("42601", format!("expected {prefix}")))?
        .trim();

    let name = extract_quoted(rest)
        .ok_or_else(|| err("42601", "expected quoted checkpoint name".to_string()))?;
    let after_name = rest.get(name.len() + 2..).unwrap_or_default().trim();

    if !after_name
        .get(.."ON ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ON "))
    {
        return Err(err("42601", "expected ON <collection>".to_string()));
    }
    let after_on = after_name.get("ON ".len()..).unwrap_or_default().trim();

    let where_pos = find_ascii_case_insensitive(after_on, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE id = '<doc_id>'".to_string()))?;
    let collection = parse_ident_token(after_on.get(..where_pos).unwrap_or_default().trim())?;
    let where_clause = after_on
        .get(where_pos + "WHERE".len()..)
        .unwrap_or_default()
        .trim();

    let doc_id = parse_id_equals(where_clause)?;

    Ok((name, collection, doc_id))
}

fn extract_quoted(s: &str) -> Option<String> {
    if !s.starts_with('\'') {
        return None;
    }
    let quoted = s.get(1..)?;
    let end = quoted.find('\'')?;
    Some(quoted.get(..end)?.to_owned())
}

fn parse_id_equals(clause: &str) -> Result<String, DdlError> {
    let eq_pos = clause.find('=').ok_or_else(|| {
        err(
            "42601",
            "expected 'id = <value>' in WHERE clause".to_string(),
        )
    })?;
    let value_part = clause
        .get(eq_pos + 1..)
        .unwrap_or_default()
        .trim()
        .trim_end_matches(';')
        .trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();
    if doc_id.is_empty() {
        return Err(err("42601", "document ID is empty".to_string()));
    }
    Ok(doc_id)
}
