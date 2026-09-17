// SPDX-License-Identifier: BUSL-1.1

//! Shared error/result constructors and tenant-reference resolution helpers
//! for the protocol-neutral tenant DDL handlers.
//!
//! Ported verbatim from the pgwire `ddl::tenant` module: `resolve_tenant_ref`
//! and `tenant_exists` are byte-identical except for the error type
//! (`DdlError` instead of `PgWireError`).

use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::super::result::{DdlError, DdlResult};

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
pub(super) fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Build a single-element command-tag result (`rows_affected: None`).
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Build an all-text [`ShapedRows`] result.
pub(super) fn text_rows(
    columns: Vec<String>,
    rows: Vec<serde_json::Map<String, serde_json::Value>>,
) -> Vec<DdlResult> {
    vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))]
}

/// Resolve a tenant reference token to a [`TenantId`], accepting either a
/// numeric id or a tenant name.
///
/// The numeric form is the legacy fast path and requires no catalog access;
/// any `u64`-parseable token returns `Ok(Some(TenantId::new(id)))` whether or
/// not that id currently exists. Resolving a token to an id does **not** assert
/// the tenant exists — callers must gate the operation through [`tenant_exists`]
/// so that an unknown numeric id and an unknown name behave identically (both
/// `42704`, or an `IF EXISTS` no-op). Skipping that check reintroduces the
/// id/name asymmetry where a bogus numeric id silently "succeeds".
///
/// A non-numeric token is treated as a tenant name and resolved via
/// `find_tenant_by_name`. Single-quoted names are unwrapped, mirroring the
/// AST `TenantSelector` behavior introduced for the CREATE/SHOW paths.
/// `Ok(None)` is returned if the name does not match any tenant, so the
/// caller can decide between `IF EXISTS` no-op success and an explicit
/// `42704` error.
///
/// Errors:
/// - `42601` — empty token (after quote stripping).
/// - `XX000` — catalog read failure.
///
/// Used by `DROP TENANT`, `ALTER TENANT SET QUOTA`, and `PURGE TENANT` to
/// accept names in addition to numeric ids, parallel to the existing
/// `CREATE TENANT <name>` and `SHOW TENANT <name>` support.
pub(super) fn resolve_tenant_ref(
    state: &SharedState,
    token: &str,
) -> Result<Option<TenantId>, DdlError> {
    // Numeric id fast path — legacy compatible.
    if let Ok(id) = token.parse::<u64>() {
        return Ok(Some(TenantId::new(id)));
    }
    // Name resolution via catalog.
    let name = token.trim_matches('\'');
    if name.is_empty() {
        return Err(ddl_err(
            "42601",
            "TENANT reference must be a numeric id or a tenant name",
        ));
    }
    let catalog = state.credentials.catalog();
    Ok(catalog
        .find_tenant_by_name(name)
        .map_err(|e| ddl_err("XX000", format!("catalog read: {e}")))?
        .map(|stored| TenantId::new(stored.tenant_id)))
}

/// Whether `tenant_id` currently exists, consulting the redb catalog.
///
/// Shared by `DROP`, `ALTER`, and `PURGE TENANT` so existence is enforced the
/// same way for numeric ids and resolved names — see [`resolve_tenant_ref`].
pub(super) fn tenant_exists(state: &SharedState, tenant_id: TenantId) -> Result<bool, DdlError> {
    let catalog = state.credentials.catalog();
    let present = catalog
        .load_all_tenants()
        .map_err(|e| ddl_err("XX000", format!("catalog read: {e}")))?
        .iter()
        .any(|t| t.tenant_id == tenant_id.as_u64());
    Ok(present)
}
