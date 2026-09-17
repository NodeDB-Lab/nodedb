// SPDX-License-Identifier: BUSL-1.1

//! `DROP REDACTION POLICY` and `SHOW REDACTION POLICIES`.
//!
//! A redaction policy is identified by the `(tenant, collection, for_role)`
//! triple — the same key the store and the catalog use — so DROP names the
//! collection and the role rather than the policy label.

use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::redaction::RedactionMode;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::scope::{authorize_redaction_scope, status};

/// `DROP REDACTION POLICY [IF EXISTS] ON <collection> FOR ROLE <role>
///     [TENANT <id>]`
pub fn drop_redaction_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    for_role: &str,
    if_exists: bool,
    tenant_id_override: Option<u64>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = authorize_redaction_scope(identity, tenant_id_override)?;
    let qualified_collection = db_qualified(database_id, collection);

    if !state
        .redaction
        .policy_exists(tenant_id, &qualified_collection, for_role)
    {
        if if_exists {
            return Ok(status("DROP REDACTION POLICY"));
        }
        return Err(DdlError::new(
            "42704",
            format!("no redaction policy on '{collection}' for role '{for_role}'"),
        ));
    }

    let entry = CatalogEntry::DeleteRedactionPolicy {
        tenant_id,
        collection: qualified_collection.clone(),
        for_role: for_role.to_string(),
    };
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_redaction_policy(tenant_id, &qualified_collection, for_role)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state
            .redaction
            .install_replicated_drop_policy(tenant_id, &qualified_collection, for_role);
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(crate::types::TenantId::new(tenant_id)),
        &identity.username,
        &format!("redaction policy on '{collection}' for role '{for_role}' dropped"),
    );

    Ok(status("DROP REDACTION POLICY"))
}

/// `SHOW REDACTION POLICIES [ON <collection>] [TENANT <id>]`.
pub fn show_redaction_policies(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: Option<&str>,
    tenant_id_override: Option<u64>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = authorize_redaction_scope(identity, tenant_id_override)?;

    let policies = match collection {
        Some(coll) => state
            .redaction
            .policies_for_collection(tenant_id, &db_qualified(database_id, coll)),
        None => state.redaction.policies_for_tenant(tenant_id),
    };

    let columns = vec![
        "name".to_string(),
        "collection".to_string(),
        "for_role".to_string(),
        "fields".to_string(),
        "modes".to_string(),
    ];

    let mut rows = Vec::with_capacity(policies.len());
    for policy in &policies {
        let fields: Vec<&str> = policy
            .rules
            .iter()
            .map(|rule| rule.field.as_str())
            .collect();
        let modes: Vec<String> = policy
            .rules
            .iter()
            .map(|rule| mode_label(&rule.mode))
            .collect();

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(policy.name.clone()));
        row.insert(
            "collection".to_string(),
            JsonValue::String(policy.display_collection.clone()),
        );
        row.insert(
            "for_role".to_string(),
            JsonValue::String(policy.for_role.clone()),
        );
        row.insert("fields".to_string(), JsonValue::String(fields.join(", ")));
        row.insert("modes".to_string(), JsonValue::String(modes.join(", ")));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Render a redaction mode for `SHOW`. The mask literal is deliberately
/// included: it is the replacement text, never the protected value.
fn mode_label(mode: &RedactionMode) -> String {
    match mode {
        RedactionMode::Mask(literal) => format!("MASK '{literal}'"),
        RedactionMode::Hash => "HASH".to_string(),
        RedactionMode::Null => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_labels_round_trip_the_ddl_spelling() {
        assert_eq!(mode_label(&RedactionMode::Mask("***".into())), "MASK '***'");
        assert_eq!(mode_label(&RedactionMode::Hash), "HASH");
        assert_eq!(mode_label(&RedactionMode::Null), "NULL");
    }
}
