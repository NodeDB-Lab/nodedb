//! RLS policy management DDL commands.
//!
//! CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
//!     USING (<field> <op> <value>) [TENANT <id>]
//!
//! DROP RLS POLICY <name> ON <collection> [TENANT <id>]
//!
//! SHOW RLS POLICIES [ON <collection>] [TENANT <id>]

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::rls::{PolicyType, RlsPolicy};
use crate::control::state::SharedState;

use super::super::types::{sqlstate_error, text_field};

/// CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
///     USING (<field> <op> <value>) [TENANT <id>]
pub fn create_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    // Only superuser or tenant admin can create RLS policies.
    if !identity.is_superuser
        && !identity
            .roles
            .contains(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error(
            "42501",
            "permission denied: requires superuser or tenant_admin",
        ));
    }

    // Parse: CREATE RLS POLICY <name> ON <collection> FOR <type> USING (<predicate>)
    // Minimum: CREATE RLS POLICY name ON coll FOR write USING (field eq value)
    if parts.len() < 9 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE RLS POLICY <name> ON <collection> FOR <read|write|all> USING (<field> <op> <value>)",
        ));
    }

    let name = parts[3];
    // parts[4] should be "ON"
    let collection = parts[5];
    // parts[6] should be "FOR"
    let policy_type_str = parts[7].to_uppercase();
    let policy_type = match policy_type_str.as_str() {
        "READ" => PolicyType::Read,
        "WRITE" => PolicyType::Write,
        "ALL" => PolicyType::All,
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("invalid policy type: {policy_type_str}. Expected READ, WRITE, or ALL"),
            ));
        }
    };

    // Parse USING clause — everything after "USING" joined and stripped of parens.
    let using_idx = parts
        .iter()
        .position(|p| p.to_uppercase() == "USING")
        .ok_or_else(|| sqlstate_error("42601", "missing USING clause"))?;

    let predicate_parts: Vec<&str> = parts[using_idx + 1..].to_vec();
    let predicate_str = predicate_parts
        .join(" ")
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();

    // Parse simple predicate: "<field> <op> <value>"
    let pred_parts: Vec<&str> = predicate_str.split_whitespace().collect();
    if pred_parts.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "USING predicate must be: (<field> <op> <value>)",
        ));
    }

    let field = pred_parts[0];
    let op = pred_parts[1];
    let value_str = pred_parts[2..].join(" ").trim_matches('\'').to_string();

    let filter = crate::bridge::scan_filter::ScanFilter {
        field: field.to_string(),
        op: op.to_string(),
        value: serde_json::json!(value_str),
        clauses: Vec::new(),
    };
    let predicate = rmp_serde::to_vec_named(&vec![filter])
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Determine tenant — from TENANT clause or identity.
    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(identity.tenant_id.as_u32());

    let policy = RlsPolicy {
        name: name.to_string(),
        collection: collection.to_string(),
        tenant_id,
        policy_type,
        predicate,
        enabled: true,
        created_by: identity.username.clone(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    state
        .rls
        .create_policy(policy)
        .map_err(|e| sqlstate_error("23505", &e.to_string()))?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("RLS policy '{name}' created on '{collection}' for {policy_type_str}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE RLS POLICY"))])
}

/// DROP RLS POLICY <name> ON <collection> [TENANT <id>]
pub fn drop_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if !identity.is_superuser
        && !identity
            .roles
            .contains(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(sqlstate_error("42501", "permission denied"));
    }

    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: DROP RLS POLICY <name> ON <collection>",
        ));
    }

    let name = parts[3];
    let collection = parts[5];

    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(identity.tenant_id.as_u32());

    if !state.rls.drop_policy(tenant_id, collection, name) {
        return Err(sqlstate_error(
            "42704",
            &format!("RLS policy '{name}' not found on '{collection}'"),
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("RLS policy '{name}' dropped from '{collection}'"),
    );

    Ok(vec![Response::Execution(Tag::new("DROP RLS POLICY"))])
}

/// SHOW RLS POLICIES [ON <collection>] [TENANT <id>]
pub fn show_rls_policies(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let collection = parts
        .iter()
        .position(|p| p.to_uppercase() == "ON")
        .and_then(|i| parts.get(i + 1))
        .map(|s| s.to_string());

    let tenant_id = parts
        .iter()
        .position(|p| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(identity.tenant_id.as_u32());

    let policies = if let Some(coll) = &collection {
        state.rls.all_policies(tenant_id, coll)
    } else {
        state.rls.all_policies_for_tenant(tenant_id)
    };

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("collection"),
        text_field("type"),
        text_field("enabled"),
        text_field("created_by"),
    ]);

    let rows: Vec<_> = policies
        .iter()
        .map(|p| {
            let mut enc = DataRowEncoder::new(schema.clone());
            let _ = enc.encode_field(&p.name);
            let _ = enc.encode_field(&p.collection);
            let _ = enc.encode_field(&format!("{:?}", p.policy_type));
            let _ = enc.encode_field(&p.enabled.to_string());
            let _ = enc.encode_field(&p.created_by);
            Ok(enc.take_row())
        })
        .collect();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
