// SPDX-License-Identifier: BUSL-1.1

//! CREATE CHECKPOINT and DROP CHECKPOINT DDL handlers.
//!
//! ```sql
//! CREATE CHECKPOINT 'launch-ready' ON documents WHERE id = 'doc-123';
//! DROP CHECKPOINT 'launch-ready' ON documents WHERE id = 'doc-123';
//! ```

use std::time::Duration;

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::types::CheckpointRecord;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::dispatch_async;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
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
        collection: collection.clone(),
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let vv_bytes = dispatch_async(state, tenant_id, database_id, &collection, plan, timeout)
        .await
        .map_err(|e| err("XX000", format!("dispatch: {e}")))?;

    let vv_json = String::from_utf8(vv_bytes)
        .map_err(|e| err("XX000", format!("version vector decode: {e}")))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let record = CheckpointRecord {
        tenant_id: tenant_id.as_u64(),
        collection: collection.clone(),
        doc_id: doc_id.clone(),
        checkpoint_name: checkpoint_name.clone(),
        version_vector_json: vv_json,
        created_by: identity.username.clone(),
        created_at: now,
    };

    // Check for duplicate and persist.
    let catalog = state.credentials.catalog();
    if catalog
        .get_checkpoint(tenant_id.as_u64(), &collection, &doc_id, &checkpoint_name)
        .map_err(|e| err("XX000", e.to_string()))?
        .is_some()
    {
        return Err(err(
            "42710",
            format!("checkpoint '{checkpoint_name}' already exists for {collection}/{doc_id}"),
        ));
    }
    catalog
        .put_checkpoint(&record)
        .map_err(|e| err("XX000", e.to_string()))?;

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
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
pub fn drop_checkpoint(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (checkpoint_name, collection, doc_id) = parse_checkpoint_sql(sql, "DROP CHECKPOINT")?;
    let tenant_id = identity.tenant_id;

    let catalog = state.credentials.catalog();
    let existed = catalog
        .delete_checkpoint(tenant_id.as_u64(), &collection, &doc_id, &checkpoint_name)
        .map_err(|e| err("XX000", e.to_string()))?;
    if !existed {
        return Err(err(
            "42704",
            format!("checkpoint '{checkpoint_name}' not found for {collection}/{doc_id}"),
        ));
    }

    state
        .audit
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .record(
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
    let rest = sql[prefix.len()..].trim();

    let name = extract_quoted(rest)
        .ok_or_else(|| err("42601", "expected quoted checkpoint name".to_string()))?;
    let after_name = rest[name.len() + 2..].trim();

    let upper_rest = after_name.to_uppercase();
    if !upper_rest.starts_with("ON ") {
        return Err(err("42601", "expected ON <collection>".to_string()));
    }
    let after_on = after_name[3..].trim();

    let where_pos = find_ascii_case_insensitive(after_on, "WHERE")
        .ok_or_else(|| err("42601", "expected WHERE id = '<doc_id>'".to_string()))?;
    let collection = after_on[..where_pos].trim().to_lowercase();
    let where_clause = after_on[where_pos + 5..].trim();

    let doc_id = parse_id_equals(where_clause)?;

    Ok((name, collection, doc_id))
}

fn extract_quoted(s: &str) -> Option<String> {
    if !s.starts_with('\'') {
        return None;
    }
    let end = s[1..].find('\'')?;
    Some(s[1..=end].to_owned())
}

fn parse_id_equals(clause: &str) -> Result<String, DdlError> {
    let eq_pos = clause.find('=').ok_or_else(|| {
        err(
            "42601",
            "expected 'id = <value>' in WHERE clause".to_string(),
        )
    })?;
    let value_part = clause[eq_pos + 1..].trim().trim_end_matches(';').trim();
    let doc_id = value_part.trim_matches('\'').trim_matches('"').to_owned();
    if doc_id.is_empty() {
        return Err(err("42601", "document ID is empty".to_string()));
    }
    Ok(doc_id)
}
