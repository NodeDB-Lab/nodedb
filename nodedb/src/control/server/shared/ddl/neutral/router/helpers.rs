// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the DDL router clusters.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Existence check backing the `CreateCollection` `if_not_exists: true`
/// short-circuit above.
///
/// Relocated verbatim from the pgwire `router::ast::exists::collection_exists`
/// helper (now deleted, along with the pgwire guard arms that were its only
/// callers).
pub(super) fn collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    database_id: DatabaseId,
) -> bool {
    let catalog = state.credentials.catalog();
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_collection(database_id, tid, name), Ok(Some(_)))
}

/// Extract the single-quoted collection argument from `SELECT LAST_VALUES('coll')`.
///
/// Mirrors the pgwire router's `extract_quoted_arg(sql, "LAST_VALUES(")` exactly
/// so the parse behaviour stays byte-identical.
pub(super) fn extract_last_values_arg(sql: &str) -> Option<String> {
    let prefix = "LAST_VALUES(";
    let pos = find_ascii_case_insensitive(sql, prefix)?;
    let after = &sql[pos + prefix.len()..];
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract `('collection', series_id)` from a `SELECT LAST_VALUE(...)` call.
///
/// Mirrors the pgwire router's `extract_lv_args` exactly.
pub(super) fn extract_last_value_args(sql: &str) -> Option<(String, u64)> {
    let pos = find_ascii_case_insensitive(sql, "LAST_VALUE(")?;
    let after = &sql[pos + 11..];
    let close = after.find(')')?;
    let inner = &after[..close];
    let parts: Vec<&str> = inner.splitn(2, ',').collect();
    if parts.len() != 2 {
        return None;
    }
    let collection = parts[0].trim().trim_matches('\'').to_string();
    let series_id: u64 = parts[1].trim().parse().ok()?;
    Some((collection, series_id))
}
