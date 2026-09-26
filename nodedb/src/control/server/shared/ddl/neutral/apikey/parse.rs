// SPDX-License-Identifier: BUSL-1.1

//! Shared parsing helpers for the protocol-neutral API-key DDL family.
//!
//! Ported from the pgwire `ddl::apikey` helpers. The scope / database / owner
//! resolution logic is preserved verbatim; only the error construction changed
//! from pgwire `sqlstate_error` to the protocol-neutral [`DdlError`].

use nodedb_types::id::DatabaseId;
use smallvec::SmallVec;

use crate::control::security::identity::DatabaseSet;
use crate::control::state::SharedState;

use super::super::super::result::DdlError;

/// Construct a [`DdlError`] with the given SQLSTATE and message, preserving the
/// exact codes and messages the pgwire handlers produced.
pub(super) fn err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Parse `WITH SCOPES 'scope1', 'scope2'` from DDL parts.
/// Resolves scope names via ScopeStore to (permission, collection) pairs.
/// Terminators: EXPIRES, DATABASES (or end of tokens).
pub(super) fn parse_key_scopes(
    parts: &[&str],
    state: &SharedState,
) -> Result<Vec<crate::control::security::apikey::KeyScope>, DdlError> {
    let scopes_idx = parts.iter().position(|p| p.to_uppercase() == "SCOPES");
    let Some(idx) = scopes_idx else {
        return Ok(vec![]);
    };
    // Check preceding word is WITH.
    if idx == 0 || parts[idx - 1].to_uppercase() != "WITH" {
        return Ok(vec![]);
    }

    let scope_names: Vec<&str> = parts[idx + 1..]
        .iter()
        .take_while(|p| {
            let up = p.to_uppercase();
            !up.starts_with("EXPIRES") && !up.starts_with("DATABASES") && up != "WITH"
        })
        .map(|s| s.trim_matches('\'').trim_end_matches(','))
        .collect();

    let mut key_scopes = Vec::new();
    for scope_name in &scope_names {
        let resolved = state.scope_defs.resolve(scope_name);
        if resolved.is_empty() {
            return Err(err(
                "42704",
                format!("scope '{scope_name}' not found or empty"),
            ));
        }
        for (perm, coll) in resolved {
            key_scopes.push(crate::control::security::apikey::KeyScope {
                permission: perm,
                collection: coll,
            });
        }
    }

    Ok(key_scopes)
}

/// Parse `WITH DATABASES (db1, db2)` or `WITH DATABASES db1, db2` from DDL parts.
///
/// Returns `None` if the clause is absent (signals "inherit owner").
/// Returns `Some(vec![...])` with resolved `DatabaseId`s if present.
/// Rejects unknown database names with SQLSTATE `42704`.
pub(super) fn parse_with_databases(
    parts: &[&str],
    state: &SharedState,
) -> Result<Option<Vec<DatabaseId>>, DdlError> {
    let db_idx = parts.iter().position(|p| p.to_uppercase() == "DATABASES");
    let Some(idx) = db_idx else {
        return Ok(None);
    };
    // Check preceding word is WITH.
    if idx == 0 || parts[idx - 1].to_uppercase() != "WITH" {
        return Ok(None);
    }

    // Collect comma-separated names until EXPIRES or end-of-tokens.
    // Strip surrounding parens if present.
    let raw_names: Vec<&str> = parts[idx + 1..]
        .iter()
        .take_while(|p| !p.to_uppercase().starts_with("EXPIRES"))
        .map(|s| {
            s.trim_start_matches('(')
                .trim_end_matches(')')
                .trim_end_matches(',')
        })
        .filter(|s| !s.is_empty())
        .collect();

    if raw_names.is_empty() {
        return Err(err(
            "42601",
            "WITH DATABASES requires at least one database name",
        ));
    }

    let catalog = state.credentials.catalog();
    let mut ids = Vec::with_capacity(raw_names.len());
    for name in raw_names {
        let resolved: Option<DatabaseId> = catalog
            .get_database_id_by_name(name)
            .map_err(|e| err("XX000", e.to_string()))?;
        match resolved {
            Some(id) => ids.push(id),
            None => {
                return Err(err("42704", format!("database '{name}' not found")));
            }
        }
    }

    Ok(Some(ids))
}

/// Build the owner's `DatabaseSet` for CREATE-time subset validation, from
/// the user as the statement sees it.
pub(super) fn build_owner_database_set_for_user(
    state: &SharedState,
    user: &crate::control::security::catalog::auth_types::user::StoredUser,
) -> Result<DatabaseSet, DdlError> {
    if user.is_superuser {
        return Ok(DatabaseSet::All);
    }
    if user.is_service_account && !user.accessible_databases.is_empty() {
        return Ok(DatabaseSet::Some(SmallVec::from_iter(
            user.accessible_databases
                .iter()
                .map(|&id| crate::types::DatabaseId::new(id)),
        )));
    }
    // Regular user or legacy service account: read from database_grants.
    let db_ids = state
        .credentials
        .catalog()
        .list_user_grant_databases(user.user_id)
        .map_err(|e| err("XX000", e.to_string()))?;
    Ok(DatabaseSet::Some(SmallVec::from_iter(db_ids)))
}
