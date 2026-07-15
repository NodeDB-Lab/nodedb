// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral OIDC provider DDL — CREATE / ALTER / DROP / SHOW.
//!
//! Ported from the pgwire `ddl::oidc` handlers. All non-return logic
//! (superuser gate + its denial audit, empty-field validation, duplicate
//! name / issuer pre-checks, `StoredClaimMappingRule` build, catalog proposes,
//! local single-node fallbacks, `audit_record`) is preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! OIDC providers are system-scoped (superuser-only) and backed by the
//! `_system.oidc_providers` catalog table.

use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredOidcProvider;
use crate::control::security::catalog::oidc_providers::StoredClaimMappingRule;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use nodedb_sql::ddl_ast::statement::OidcClaimMappingClause;

use super::super::result::{DdlError, DdlResult};

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Superuser gate, folded in verbatim from the pgwire `require_superuser`
/// helper: on denial it emits `AuditEvent::PermissionDenied` (database-less
/// scope, matching the `None` `db_id` the pgwire handlers passed) and returns
/// SQLSTATE 42501, preserving both the side effect and the wire error.
fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        Ok(())
    } else {
        state.audit_record_with_db(
            AuditEvent::PermissionDenied,
            Some(identity.tenant_id),
            None,
            &identity.username,
            action,
        );
        Err(DdlError {
            sqlstate: "42501".to_string(),
            message: format!("permission denied: {action} requires superuser"),
        })
    }
}

/// Whether two providers would make the issuer route ambiguous.
fn has_ambiguous_issuer_route(existing_audience: Option<&str>, audience: Option<&str>) -> bool {
    let existing_audience = existing_audience.filter(|value| !value.is_empty());
    let audience = audience.filter(|value| !value.is_empty());
    match (existing_audience, audience) {
        (Some(existing), Some(candidate)) => existing == candidate,
        _ => true,
    }
}

fn validate_claim_mapping_roles(claim_mappings: &[OidcClaimMappingClause]) -> Result<(), DdlError> {
    if claim_mappings
        .iter()
        .flat_map(|mapping| mapping.add_roles.iter())
        .any(|role| role == "superuser")
    {
        return Err(DdlError {
            sqlstate: "22023".to_string(),
            message: "OIDC claim mappings cannot grant the database-owned superuser role"
                .to_string(),
        });
    }
    Ok(())
}

/// Borrowed fields for a `CREATE OIDC PROVIDER` operation.
pub struct CreateOidcProviderParams<'a> {
    pub name: &'a str,
    pub issuer: &'a str,
    pub jwks_uri: &'a str,
    pub tenant_id: u64,
    pub audience: Option<&'a str>,
    pub claim_mappings: &'a [OidcClaimMappingClause],
}

/// Handle `CREATE OIDC PROVIDER <name> ISSUER '<iss>' JWKS_URI '<uri>' ...`.
pub fn create_oidc_provider(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    params: CreateOidcProviderParams<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateOidcProviderParams {
        name,
        issuer,
        jwks_uri,
        tenant_id,
        audience,
        claim_mappings,
    } = params;
    require_superuser(state, identity, "create OIDC providers")?;

    if issuer.is_empty() {
        return Err(DdlError {
            sqlstate: "22023".to_string(),
            message: "ISSUER must not be empty".to_string(),
        });
    }
    if jwks_uri.is_empty() {
        return Err(DdlError {
            sqlstate: "22023".to_string(),
            message: "JWKS_URI must not be empty".to_string(),
        });
    }
    validate_claim_mapping_roles(claim_mappings)?;

    let catalog = state.credentials.catalog();

    let tenant_exists = catalog
        .load_all_tenants()
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("tenant lookup: {e}"),
        })?
        .iter()
        .any(|tenant| tenant.tenant_id == tenant_id);
    if !tenant_exists {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("tenant '{tenant_id}' does not exist"),
        });
    }

    // Check for duplicate by provider name.
    match catalog.get_oidc_provider(name) {
        Ok(Some(_)) => {
            return Err(DdlError {
                sqlstate: "42710".to_string(),
                message: format!("OIDC provider '{name}' already exists"),
            });
        }
        Ok(None) => {}
        Err(e) => {
            return Err(DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog read: {e}"),
            });
        }
    }

    // A route is `(issuer, audience)`. An absent or empty audience makes an
    // issuer route ambiguous, while distinct non-empty audiences may share it.
    match catalog.list_oidc_providers() {
        Ok(providers) => {
            if providers.iter().any(|p| {
                p.issuer == issuer && has_ambiguous_issuer_route(p.audience.as_deref(), audience)
            }) {
                return Err(DdlError {
                    sqlstate: "42710".to_string(),
                    message: format!(
                        "OIDC provider issuer/audience route for issuer '{issuer}' is ambiguous or already exists"
                    ),
                });
            }
        }
        Err(e) => {
            return Err(DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog list: {e}"),
            });
        }
    }

    let stored_mappings: Vec<StoredClaimMappingRule> = claim_mappings
        .iter()
        .map(|cm| StoredClaimMappingRule {
            claim_name: cm.claim_name.clone(),
            claim_value: cm.claim_value.clone(),
            default_database: cm.default_database,
            add_databases: cm.add_databases.clone(),
            add_roles: cm.add_roles.clone(),
        })
        .collect();

    let provider = StoredOidcProvider {
        provider_name: name.to_string(),
        issuer: issuer.to_string(),
        jwks_uri: jwks_uri.to_string(),
        audience: audience.map(str::to_string),
        tenant_id: Some(tenant_id),
        claim_mapping: stored_mappings,
        created_at_lsn: 0,
    };

    let entry = CatalogEntry::PutOidcProvider(Box::new(provider.clone()));
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.put_oidc_provider(&provider).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog write: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE OIDC PROVIDER {name} issuer={issuer}"),
    );

    Ok(status("CREATE OIDC PROVIDER"))
}

/// Handle `ALTER OIDC PROVIDER <name> SET CLAIM MAPPING WHEN ...`.
///
/// Replaces the entire claim-mapping list for the named provider.
pub fn alter_oidc_provider_claim_mapping(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    claim_mappings: &[OidcClaimMappingClause],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "alter OIDC providers")?;

    let catalog = state.credentials.catalog();

    let mut provider = catalog
        .get_oidc_provider(name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .ok_or(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("OIDC provider '{name}' does not exist"),
        })?;
    validate_claim_mapping_roles(claim_mappings)?;

    let stored_mappings: Vec<StoredClaimMappingRule> = claim_mappings
        .iter()
        .map(|cm| StoredClaimMappingRule {
            claim_name: cm.claim_name.clone(),
            claim_value: cm.claim_value.clone(),
            default_database: cm.default_database,
            add_databases: cm.add_databases.clone(),
            add_roles: cm.add_roles.clone(),
        })
        .collect();

    provider.claim_mapping = stored_mappings;

    let entry = CatalogEntry::PutOidcProvider(Box::new(provider.clone()));
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.put_oidc_provider(&provider).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog write: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "ALTER OIDC PROVIDER {name} SET CLAIM MAPPING ({} rules)",
            provider.claim_mapping.len()
        ),
    );

    Ok(status("ALTER OIDC PROVIDER"))
}

/// Handle `DROP OIDC PROVIDER [IF EXISTS] <name>`.
pub fn drop_oidc_provider(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "drop OIDC providers")?;

    let catalog = state.credentials.catalog();

    if catalog
        .get_oidc_provider(name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .is_none()
    {
        if if_exists {
            return Ok(status("DROP OIDC PROVIDER"));
        }
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("OIDC provider '{name}' does not exist"),
        });
    }

    let entry = CatalogEntry::DeleteOidcProvider {
        name: name.to_string(),
    };
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.delete_oidc_provider(name).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP OIDC PROVIDER {name}"),
    );

    Ok(status("DROP OIDC PROVIDER"))
}

/// Handle `SHOW OIDC PROVIDERS`.
pub fn show_oidc_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "show OIDC providers")?;

    let catalog = state.credentials.catalog();

    let providers = catalog.list_oidc_providers().map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("catalog list: {e}"),
    })?;

    let columns = vec![
        "name".to_string(),
        "issuer".to_string(),
        "jwks_uri".to_string(),
        "tenant_id".to_string(),
        "audience".to_string(),
        "claim_mapping_rules".to_string(),
    ];

    let mut rows = Vec::with_capacity(providers.len());
    for p in &providers {
        let mut row = Map::new();
        row.insert(
            "name".to_string(),
            JsonValue::String(p.provider_name.clone()),
        );
        row.insert("issuer".to_string(), JsonValue::String(p.issuer.clone()));
        row.insert(
            "jwks_uri".to_string(),
            JsonValue::String(p.jwks_uri.clone()),
        );
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String(p.tenant_id.map(|id| id.to_string()).unwrap_or_default()),
        );
        let aud = p.audience.as_deref().unwrap_or("");
        row.insert("audience".to_string(), JsonValue::String(aud.to_string()));
        let rule_count = p.claim_mapping.len().to_string();
        row.insert(
            "claim_mapping_rules".to_string(),
            JsonValue::String(rule_count),
        );
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
