// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the protocol-neutral `user` / `role` DDL families:
//! the tenant-admin gate, single-tag status construction, role-name parsing,
//! and the `IF [NOT] EXISTS` token strippers.
//!
//! Errors are the protocol-neutral [`DdlError`].

use crate::control::security::identity::{AuthenticatedIdentity, Role};

use super::super::result::{DdlError, DdlResult};

/// Build a single-tag status result.
pub(super) fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Require that the identity is superuser or tenant_admin.
///
/// It does NOT emit an audit record on denial and returns SQLSTATE 42501.
pub(super) fn require_tenant_admin(
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser || identity.has_role(&Role::TenantAdmin) {
        Ok(())
    } else {
        Err(DdlError::new(
            "42501",
            format!("permission denied: only superuser or tenant_admin can {action}"),
        ))
    }
}

/// Refuse the superuser role unless the caller is superuser. Same rule as
/// `GRANT superuser`. Returns SQLSTATE 42501.
pub(super) fn require_superuser_for_role(
    identity: &AuthenticatedIdentity,
    role: &Role,
) -> Result<(), DdlError> {
    if matches!(role, Role::Superuser) && !identity.is_superuser {
        return Err(DdlError::new(
            "42501",
            "permission denied: only superuser can assign the superuser role",
        ));
    }
    Ok(())
}

/// Parse a role name string into a `Role`.
///
/// Known roles map to their enum variants; unknown names become `Role::Custom`.
/// `Role::from_str` is `Infallible`, so the `Err` arm is unreachable.
pub(super) fn parse_role(name: &str) -> Role {
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}

/// Strip a leading `IF NOT EXISTS` clause that sits immediately after the
/// `keyword_count` leading DDL keyword tokens. Returns whether the clause was
/// present and the token slice with the clause removed.
pub(super) fn strip_if_not_exists<'a>(
    parts: &[&'a str],
    keyword_count: usize,
) -> (bool, Vec<&'a str>) {
    if parts.len() >= keyword_count + 3
        && parts[keyword_count].eq_ignore_ascii_case("IF")
        && parts[keyword_count + 1].eq_ignore_ascii_case("NOT")
        && parts[keyword_count + 2].eq_ignore_ascii_case("EXISTS")
    {
        let mut remaining: Vec<&str> = parts[..keyword_count].to_vec();
        remaining.extend_from_slice(&parts[keyword_count + 3..]);
        (true, remaining)
    } else {
        (false, parts.to_vec())
    }
}

/// Strip a leading `IF EXISTS` clause that sits immediately after the
/// `keyword_count` leading DDL keyword tokens. Counterpart of
/// [`strip_if_not_exists`] for `DROP` statements.
pub(super) fn strip_if_exists<'a>(parts: &[&'a str], keyword_count: usize) -> (bool, Vec<&'a str>) {
    if parts.len() >= keyword_count + 2
        && parts[keyword_count].eq_ignore_ascii_case("IF")
        && parts[keyword_count + 1].eq_ignore_ascii_case("EXISTS")
    {
        let mut remaining: Vec<&str> = parts[..keyword_count].to_vec();
        remaining.extend_from_slice(&parts[keyword_count + 2..]);
        (true, remaining)
    } else {
        (false, parts.to_vec())
    }
}
