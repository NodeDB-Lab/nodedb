// SPDX-License-Identifier: BUSL-1.1

//! Apply Role catalog entries to `SystemCatalog` redb.
//!
//! Both entries check the role rules against the catalog at their log
//! position. The statement checked before proposing; a change that
//! committed in between is caught here, on every node alike.

use crate::control::security::catalog::{StoredRole, SystemCatalog, catalog_err};
use crate::control::security::role_assignment::{self, RoleRefusal};

use super::outcome::ApplyOutcome;

/// Write the role, unless its inheritance parent is neither built in nor
/// defined.
pub fn put(stored: &StoredRole, catalog: &SystemCatalog) -> crate::Result<ApplyOutcome> {
    let parent = stored.parent.as_str();
    if !parent.is_empty() && !role_assignment::is_builtin_role_name(parent) {
        let roles = catalog.load_all_roles()?;
        if !roles.iter().any(|role| role.name == parent) {
            let refusal = RoleRefusal::Undefined {
                name: parent.to_string(),
            };
            tracing::warn!(
                role = %stored.name,
                %refusal,
                "catalog_entry: role entry refused; its parent is undefined"
            );
            return Ok(ApplyOutcome::Refused(refusal));
        }
    }
    catalog
        .put_role(stored)
        .map_err(|e| catalog_err(&format!("put_role '{}'", stored.name), e))?;
    Ok(ApplyOutcome::Applied)
}

/// Delete the role, unless a user holds it or a role inherits from it.
pub fn delete(name: &str, catalog: &SystemCatalog) -> crate::Result<ApplyOutcome> {
    let users = catalog.load_all_users()?;
    let roles = catalog.load_all_roles()?;
    if let Err(refusal) = role_assignment::check_stored_drop(name, &users, &roles) {
        tracing::warn!(
            role = %name,
            %refusal,
            "catalog_entry: role drop refused; users or roles still depend on it"
        );
        return Ok(ApplyOutcome::Refused(refusal));
    }
    catalog
        .delete_role(name)
        .map_err(|e| catalog_err(&format!("delete_role '{name}'"), e))?;
    Ok(ApplyOutcome::Applied)
}
