// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral RLS policy DDL — CREATE / DROP / SHOW.
//!
//! Ported from the pgwire `ddl::rls` handlers. All non-return logic
//! (permission checks, predicate compilation, duplicate pre-checks, catalog
//! proposes, in-memory `RlsPolicyStore` install/drop, `audit_record`, tenant
//! scoping, and the token-based `parts` parsing for DROP / SHOW) is preserved
//! verbatim; only the result construction changed from pgwire `Response` /
//! `PgWireError` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredRlsPolicy;
use crate::control::security::deny::{self, DenyMode};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::security::predicate::RlsPredicate;
use crate::control::security::rls::RlsPolicy;
use crate::control::security::rls::compile::{compile_policy_predicate, declared_columns};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

/// Parsed `CREATE RLS POLICY` request. Fields mirror the typed
/// `PolicyStmt::CreateRlsPolicy` AST variant.
#[derive(Clone, Copy)]
pub struct CreateRlsPolicyRequest<'a> {
    pub name: &'a str,
    pub collection: &'a str,
    pub policy_type_raw: &'a str,
    pub predicate_raw: &'a str,
    pub is_restrictive: bool,
    pub on_deny_raw: Option<&'a str>,
    pub tenant_id_override: Option<u64>,
    /// Database the statement ran against. RLS enforcement looks policies
    /// up under `db_qualified(database_id, collection)` (the same key
    /// physical plan ops carry), so the policy must be stored there too.
    pub database_id: DatabaseId,
}

/// Result of compiling an RLS predicate string.
struct CompiledPredicate {
    /// Compiled predicate AST. Substituted at query time via `AuthContext`.
    compiled_predicate: Option<RlsPredicate>,
    /// Deny-mode derived from the `ON DENY` clause.
    on_deny: DenyMode,
}

/// Compile a predicate string against the target collection's declared
/// columns, and the optional `ON DENY` raw clause, into a
/// `CompiledPredicate`. Called by [`create_rls_policy`] after the typed AST
/// fields have been validated.
///
/// The collection must exist: a literal compared against a declared
/// `TIMESTAMP` / `TIMESTAMPTZ` column is typed here by the rule the planner
/// applies to a query predicate, and a literal that type cannot read
/// refuses the statement naming the column. A policy that cannot be
/// enforced as written is never stored.
fn compile_rls_predicate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    predicate_str: &str,
    on_deny_raw: Option<&str>,
) -> Result<CompiledPredicate, DdlError> {
    let columns = declared_columns(
        state.credentials.catalog(),
        database_id,
        tenant_id,
        collection,
    )
    .map_err(|e| match e {
        crate::Error::CollectionNotFound { .. } => {
            DdlError::new("42P01", format!("collection '{collection}' does not exist"))
        }
        other => DdlError::new("XX000", format!("catalog read: {other}")),
    })?;
    let compiled = compile_policy_predicate(predicate_str, &columns)
        .map_err(|e| DdlError::new("42601", e.to_string()))?;

    let on_deny = if let Some(deny_text) = on_deny_raw {
        let deny_parts: Vec<&str> = deny_text.split_whitespace().collect();
        // Strip leading DENY token if present.
        let slice = if deny_parts
            .first()
            .map(|s| s.eq_ignore_ascii_case("DENY"))
            .unwrap_or(false)
        {
            &deny_parts[1..]
        } else {
            &deny_parts[..]
        };
        deny::parse_on_deny(slice).map_err(|e| DdlError::new("42601", e.to_string()))?
    } else {
        DenyMode::default()
    };

    Ok(CompiledPredicate {
        compiled_predicate: Some(compiled),
        on_deny,
    })
}

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Resolve and authorize the tenant scope for RLS administration.
///
/// Tenant administrators may manage only their authenticated tenant. An
/// explicit cross-tenant target is reserved for superusers.
fn authorize_rls_scope(
    identity: &AuthenticatedIdentity,
    tenant_id_override: Option<u64>,
) -> Result<u64, DdlError> {
    if !identity.is_superuser && !identity.roles.contains(&Role::TenantAdmin) {
        return Err(DdlError::new(
            "42501",
            "permission denied: requires superuser or tenant_admin",
        ));
    }

    let own_tenant_id = identity.tenant_id.as_u64();
    let tenant_id = tenant_id_override.unwrap_or(own_tenant_id);
    if tenant_id != own_tenant_id && !identity.is_superuser {
        return Err(DdlError::new(
            "42501",
            "permission denied: cross-tenant RLS administration requires superuser",
        ));
    }
    Ok(tenant_id)
}

/// `CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
///     USING (<predicate>) [RESTRICTIVE] [TENANT <id>] [ON DENY ...]`
///
/// All fields are pre-parsed by the `nodedb-sql` AST layer; this handler
/// only performs predicate compilation and catalog mutation.
pub fn create_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateRlsPolicyRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateRlsPolicyRequest {
        name,
        collection,
        policy_type_raw,
        predicate_raw,
        is_restrictive,
        on_deny_raw,
        tenant_id_override,
        database_id,
    } = *req;
    let tenant_id = authorize_rls_scope(identity, tenant_id_override)?;

    // Store and key under the same qualified name enforcement looks up
    // (`db_qualified`, applied to every op's `collection` field). Keep the
    // bare, user-typed name only for display (`SHOW RLS POLICIES`).
    let qualified_collection = db_qualified(database_id, collection);

    let policy_type_label = policy_type_raw.to_uppercase();
    let policy_type = match policy_type_label.as_str() {
        "READ" => crate::control::security::rls::PolicyType::Read,
        "WRITE" => crate::control::security::rls::PolicyType::Write,
        "ALL" => crate::control::security::rls::PolicyType::All,
        other => {
            return Err(DdlError::new(
                "42601",
                format!("invalid policy type: {other}. Expected READ, WRITE, or ALL"),
            ));
        }
    };

    let mode = if is_restrictive {
        crate::control::security::predicate::PolicyMode::Restrictive
    } else {
        crate::control::security::predicate::PolicyMode::Permissive
    };

    let compiled = compile_rls_predicate(
        state,
        database_id,
        tenant_id,
        collection,
        predicate_raw,
        on_deny_raw,
    )?;

    // Pre-check duplicate so the proposing node fails fast with a
    // clean SQLSTATE instead of going through raft only to be a
    // silent overwrite.
    if state
        .rls
        .policy_exists(tenant_id, &qualified_collection, name)
    {
        return Err(DdlError::new(
            "42710",
            format!("RLS policy '{}' already exists on '{}'", name, collection),
        ));
    }

    let policy = RlsPolicy {
        name: name.to_string(),
        collection: qualified_collection,
        display_collection: collection.to_string(),
        tenant_id,
        policy_type,
        compiled_predicate: compiled.compiled_predicate,
        mode,
        on_deny: compiled.on_deny,
        enabled: true,
        created_by: identity.username.clone(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    let stored = StoredRlsPolicy::from_runtime(&policy, database_id, predicate_raw)
        .map_err(|e| DdlError::new("XX000", format!("rls serialize: {e}")))?;

    let entry = CatalogEntry::PutRlsPolicy(Box::new(stored.clone()));
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .put_rls_policy(&stored)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state.rls.install_replicated_policy(policy);
    }

    let mode_str = if is_restrictive { " RESTRICTIVE" } else { "" };
    state.audit_record(
        AuditEvent::AdminAction,
        Some(crate::types::TenantId::new(tenant_id)),
        &identity.username,
        &format!(
            "RLS policy '{}' created on '{}' for {}{}",
            name, collection, policy_type_label, mode_str
        ),
    );

    Ok(status("CREATE RLS POLICY"))
}

/// `DROP RLS POLICY <name> ON <collection> [TENANT <id>]`.
pub fn drop_rls_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    collection: &str,
    if_exists: bool,
    tenant_id_override: Option<u64>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = authorize_rls_scope(identity, tenant_id_override)?;
    let qualified_collection = db_qualified(database_id, collection);

    if !state
        .rls
        .policy_exists(tenant_id, &qualified_collection, name)
    {
        if if_exists {
            return Ok(status("DROP RLS POLICY"));
        }
        return Err(DdlError::new(
            "42704",
            format!("RLS policy '{name}' not found on '{collection}'"),
        ));
    }

    let entry = CatalogEntry::DeleteRlsPolicy {
        tenant_id,
        collection: qualified_collection.clone(),
        name: name.to_string(),
    };
    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        {
            let catalog = state.credentials.catalog();
            catalog
                .delete_rls_policy(tenant_id, &qualified_collection, name)
                .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        }
        state
            .rls
            .install_replicated_drop_policy(tenant_id, &qualified_collection, name);
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(crate::types::TenantId::new(tenant_id)),
        &identity.username,
        &format!("RLS policy '{name}' dropped from '{collection}'"),
    );

    Ok(status("DROP RLS POLICY"))
}

/// `SHOW RLS POLICIES [ON <collection>] [TENANT <id>]`.
pub fn show_rls_policies(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: Option<&str>,
    tenant_id_override: Option<u64>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = authorize_rls_scope(identity, tenant_id_override)?;

    let policies = if let Some(coll) = collection {
        state
            .rls
            .all_policies(tenant_id, &db_qualified(database_id, coll))
    } else {
        state.rls.all_policies_for_tenant(tenant_id)
    };

    let columns = vec![
        "name".to_string(),
        "collection".to_string(),
        "type".to_string(),
        "mode".to_string(),
        "has_auth_refs".to_string(),
        "enabled".to_string(),
        "created_by".to_string(),
    ];

    let mut rows = Vec::with_capacity(policies.len());
    for p in &policies {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(p.name.clone()));
        row.insert(
            "collection".to_string(),
            JsonValue::String(p.display_collection.clone()),
        );
        row.insert(
            "type".to_string(),
            JsonValue::String(format!("{:?}", p.policy_type)),
        );
        row.insert(
            "mode".to_string(),
            JsonValue::String(format!("{:?}", p.mode)),
        );
        row.insert(
            "has_auth_refs".to_string(),
            JsonValue::String(p.compiled_predicate.is_some().to_string()),
        );
        row.insert(
            "enabled".to_string(),
            JsonValue::String(p.enabled.to_string()),
        );
        row.insert(
            "created_by".to_string(),
            JsonValue::String(p.created_by.clone()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    fn identity(tenant_id: u64, roles: Vec<Role>, is_superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "test",
            TenantId::new(tenant_id),
            roles,
            is_superuser,
            None,
            AuthenticatedIdentity::default_database_set(is_superuser),
        )
    }

    #[test]
    fn rls_scope_requires_admin_and_reserves_cross_tenant_for_superusers() {
        let ordinary = identity(7, Vec::new(), false);
        let admin = identity(7, vec![Role::TenantAdmin], false);
        let superuser = identity(7, Vec::new(), true);

        assert_eq!(
            authorize_rls_scope(&ordinary, None).unwrap_err().sqlstate,
            "42501"
        );
        assert_eq!(authorize_rls_scope(&admin, None).expect("own tenant"), 7);
        assert_eq!(authorize_rls_scope(&admin, Some(7)).expect("own tenant"), 7);
        assert_eq!(
            authorize_rls_scope(&admin, Some(8)).unwrap_err().sqlstate,
            "42501"
        );
        assert_eq!(
            authorize_rls_scope(&superuser, Some(8)).expect("superuser"),
            8
        );
    }
}
