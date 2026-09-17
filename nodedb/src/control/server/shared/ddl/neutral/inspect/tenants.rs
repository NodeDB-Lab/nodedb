// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral tenant introspection: SHOW TENANTS, SHOW TENANT
//! <name|id>, SHOW TENANTS WITH NAME <name>.
//!
//! Ported from the pgwire `ddl::inspect` handlers. The tenant-set union
//! (catalog-registered tenants + tenants owning at least one user) and the
//! per-tenant usage reads are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW TENANTS — list all tenants with quotas.
///
/// Superuser only.
pub fn show_tenants(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can list tenants",
        ));
    }

    let (columns, column_types, rows) = tenant_rows(state, |_, _| true);
    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW TENANT <name|id> — single-tenant introspection by identifier.
///
/// Resolves `ident` first as a numeric tenant id, then as a name. The
/// row shape mirrors `SHOW TENANTS` (tenant_id, name, active_requests,
/// total_requests, rejected_requests). Returns SQLSTATE `42704`
/// (undefined_object) if no tenant matches. Superuser only.
pub fn show_tenant_by_identifier(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    ident: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can introspect tenants",
        ));
    }

    let (columns, column_types, rows) = tenant_rows(state, |t_id, t_name| {
        if let Ok(n) = ident.parse::<u64>() {
            t_id == n
        } else {
            t_name.eq_ignore_ascii_case(ident)
        }
    });

    if rows.is_empty() {
        return Err(ddl_err("42704", format!("tenant '{ident}' not found")));
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// SHOW TENANTS WITH NAME <name> — filtered list form. Returns a row
/// set with the same schema as `SHOW TENANTS`. Returns SQLSTATE `42704`
/// when no tenant matches — silently returning the unfiltered list (the
/// pre-fix behaviour) would be a data-disclosure hazard.
pub fn show_tenants_filtered_by_name(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can list tenants",
        ));
    }

    let (columns, column_types, rows) =
        tenant_rows(state, |_t_id, t_name| t_name.eq_ignore_ascii_case(name));

    if rows.is_empty() {
        return Err(ddl_err("42704", format!("tenant '{name}' not found")));
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

/// Build the `(columns, column_types, rows)` triple shared by `SHOW TENANTS`
/// and its filtered variants. The predicate decides which (id, name) pairs are
/// emitted; the tenant set is the union of catalog-registered tenants and any
/// tenant that owns at least one user (usage is tracked on first request, so a
/// tenant with no traffic still needs to be listed).
type TenantRowSet = (Vec<String>, Vec<DdlColType>, Vec<Map<String, JsonValue>>);

fn tenant_rows<F>(state: &SharedState, pred: F) -> TenantRowSet
where
    F: Fn(u64, &str) -> bool,
{
    let columns = vec![
        "tenant_id".to_string(),
        "name".to_string(),
        "active_requests".to_string(),
        "total_requests".to_string(),
        "rejected_requests".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
    ];

    let tenants = match state.tenants.lock() {
        Ok(t) => t,
        Err(p) => p.into_inner(),
    };

    let mut names: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    let catalog = state.credentials.catalog();
    if let Ok(all) = catalog.load_all_tenants() {
        for t in all {
            names.insert(t.tenant_id, t.name);
        }
    }

    let mut seen: std::collections::BTreeSet<u64> = names.keys().copied().collect();
    for user in &state.credentials.list_user_details() {
        seen.insert(user.tenant_id.as_u64());
    }

    let mut rows = Vec::new();
    for tid_u64 in seen {
        let tid_name = names.get(&tid_u64).map(String::as_str).unwrap_or("");
        if !pred(tid_u64, tid_name) {
            continue;
        }
        let tid = crate::types::TenantId::new(tid_u64);
        let usage = tenants.usage(tid);
        let mut row = Map::new();
        row.insert(
            "tenant_id".to_string(),
            JsonValue::String((tid_u64 as i64).to_string()),
        );
        row.insert("name".to_string(), JsonValue::String(tid_name.to_string()));
        row.insert(
            "active_requests".to_string(),
            JsonValue::String((usage.map_or(0, |u| u.active_requests as i64)).to_string()),
        );
        row.insert(
            "total_requests".to_string(),
            JsonValue::String((usage.map_or(0, |u| u.total_requests as i64)).to_string()),
        );
        row.insert(
            "rejected_requests".to_string(),
            JsonValue::String((usage.map_or(0, |u| u.rejected_requests as i64)).to_string()),
        );
        rows.push(row);
    }

    (columns, column_types, rows)
}
