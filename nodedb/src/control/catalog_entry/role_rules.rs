// SPDX-License-Identifier: BUSL-1.1

//! Role rules over a batch of catalog entries, in order.
//!
//! A transaction buffers its DDL and applies it at COMMIT. One statement can
//! break a role rule for another in the same batch: `DROP ROLE r` after
//! `CREATE USER u ROLE r`, for example. Each statement's own check saw only
//! the state before the batch. This check replays the batch's role and user
//! entries over the committed catalog, in order, and refuses the whole batch
//! before anything is written, so a COMMIT never applies part of a batch.

use std::collections::HashMap;

use crate::control::security::catalog::{StoredRole, StoredUser, SystemCatalog};
use crate::control::security::role_assignment::{self, RoleRefusal};

use super::entry::CatalogEntry;

/// Check every role and user entry of `entries` against the committed
/// catalog and the entries before it.
pub fn check_batch<'a>(
    entries: impl IntoIterator<Item = &'a CatalogEntry>,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let mut users: HashMap<String, StoredUser> = catalog
        .load_all_users()?
        .into_iter()
        .map(|user| (user.username.clone(), user))
        .collect();
    let mut roles: HashMap<String, StoredRole> = catalog
        .load_all_roles()?
        .into_iter()
        .map(|role| (role.name.clone(), role))
        .collect();
    for entry in entries {
        match entry {
            CatalogEntry::PutUser(user) => {
                if user.is_active {
                    let defined: Vec<StoredRole> = roles.values().cloned().collect();
                    role_assignment::check_stored_user(user, &defined)?;
                }
                users.insert(user.username.clone(), (**user).clone());
            }
            CatalogEntry::PutTenantWithAdmin { admin, .. } => {
                users.insert(admin.username.clone(), (**admin).clone());
            }
            CatalogEntry::DropUser { username } => {
                users.remove(username);
            }
            CatalogEntry::PutRole(role) => {
                let parent = role.parent.as_str();
                if !parent.is_empty()
                    && !role_assignment::is_builtin_role_name(parent)
                    && !roles.contains_key(parent)
                {
                    return Err(RoleRefusal::Undefined {
                        name: parent.to_string(),
                    }
                    .into());
                }
                roles.insert(role.name.clone(), (**role).clone());
            }
            CatalogEntry::DeleteRole { name } => {
                let held: Vec<StoredUser> = users.values().cloned().collect();
                let defined: Vec<StoredRole> = roles.values().cloned().collect();
                role_assignment::check_stored_drop(name, &held, &defined)?;
                roles.remove(name);
            }
            // Every other entry leaves users and roles as they are.
            _ => {}
        }
    }
    Ok(())
}
