// SPDX-License-Identifier: BUSL-1.1

//! Role rules at the DDL entry points: a user may hold only a built-in role
//! or a custom role defined in its tenant, and a custom role may be dropped
//! only while no user holds it and no role inherits from it.
//!
//! Each statement checks before it proposes. The metadata applier runs the
//! same rules at the entry's log position and skips an entry that breaks
//! them, on every node alike. A statement whose entry was skipped, because
//! a racing change committed first, learns it here after the apply and
//! reports the refusal instead of a success.
//!
//! Inside a transaction a statement sees the committed users and roles
//! overlaid with the role and user entries its transaction buffered, in
//! order: a role created earlier in the transaction can be assigned, and a
//! role a user created earlier in it holds cannot be dropped. COMMIT checks
//! the whole batch again before it applies anything.

use std::collections::HashMap;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::identity::Role;
use crate::control::security::role_assignment::{self, RoleRefusal};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::result::DdlError;

/// The DDL error for a role refusal, with PostgreSQL's SQLSTATE.
pub(super) fn refusal(refusal: RoleRefusal) -> DdlError {
    DdlError::new(refusal.sqlstate(), refusal.to_string())
}

/// Users and roles as the running statement sees them.
struct RoleView {
    /// Active users: name to held role names.
    users: HashMap<String, Vec<String>>,
    /// Custom roles: name to (tenant, parent, empty when none).
    roles: HashMap<String, (u64, String)>,
}

impl RoleView {
    /// The committed users and roles, overlaid with the entries this
    /// connection's open transaction buffered, in statement order.
    fn current(state: &SharedState) -> Self {
        let mut view = Self {
            users: state
                .credentials
                .list_user_details()
                .into_iter()
                .map(|user| {
                    let held = user.roles.iter().map(ToString::to_string).collect();
                    (user.username, held)
                })
                .collect(),
            roles: state
                .roles
                .list_roles()
                .into_iter()
                .map(|role| {
                    let parent = role.parent.unwrap_or_default();
                    (role.name, (role.tenant_id.as_u64(), parent))
                })
                .collect(),
        };
        crate::control::server::shared::session::ddl_buffer::with_buffered(|buffered| {
            for item in buffered {
                view.overlay(&item.entry);
            }
        });
        view
    }

    fn overlay(&mut self, entry: &CatalogEntry) {
        match entry {
            CatalogEntry::PutUser(user) if user.is_active => {
                self.users.insert(user.username.clone(), user.roles.clone());
            }
            CatalogEntry::PutUser(user) => {
                self.users.remove(&user.username);
            }
            CatalogEntry::PutTenantWithAdmin { admin, .. } => {
                self.users
                    .insert(admin.username.clone(), admin.roles.clone());
            }
            CatalogEntry::DropUser { username } => {
                self.users.remove(username);
            }
            CatalogEntry::PutRole(role) => {
                self.roles
                    .insert(role.name.clone(), (role.tenant_id, role.parent.clone()));
            }
            CatalogEntry::DeleteRole { name } => {
                self.roles.remove(name);
            }
            // Every other entry leaves users and roles as they are.
            _ => {}
        }
    }
}

/// Active user `username` as the running statement sees it: the committed
/// record, overlaid with the user entries its transaction buffered, in
/// order. A user created earlier in the transaction is visible; one dropped
/// earlier in it is not.
pub(super) fn visible_user(
    state: &SharedState,
    username: &str,
) -> Option<crate::control::security::catalog::StoredUser> {
    let committed = state.credentials.stored_user(username);
    crate::control::server::shared::session::ddl_buffer::with_buffered(|buffered| {
        let mut user = committed.clone();
        for item in buffered {
            match &item.entry {
                CatalogEntry::PutUser(stored) if stored.username == username => {
                    user = stored.is_active.then(|| (**stored).clone());
                }
                CatalogEntry::PutTenantWithAdmin { admin, .. } if admin.username == username => {
                    user = Some((**admin).clone());
                }
                CatalogEntry::DropUser { username: dropped } if dropped == username => {
                    user = None;
                }
                // Every other entry leaves this user as it is.
                _ => {}
            }
        }
        user
    })
    .unwrap_or(committed)
}

/// [`visible_user`], or the 42704 refusal a statement naming a missing user
/// reports.
pub(super) fn visible_user_or_missing(
    state: &SharedState,
    username: &str,
) -> Result<crate::control::security::catalog::StoredUser, DdlError> {
    visible_user(state, username)
        .ok_or_else(|| DdlError::new("42704", format!("user '{username}' not found")))
}

/// Every custom role the running statement sees, keyed by name: the
/// committed ones overlaid with those its transaction buffered.
pub(super) fn visible_roles(
    state: &SharedState,
) -> HashMap<String, crate::control::security::role::CustomRole> {
    RoleView::current(state)
        .roles
        .into_iter()
        .map(|(name, (tenant_id, parent))| {
            let role = crate::control::security::role::CustomRole {
                name: name.clone(),
                tenant_id: TenantId::new(tenant_id),
                parent: (!parent.is_empty()).then_some(parent),
                created_at: 0,
            };
            (name, role)
        })
        .collect()
}

/// Refuse any role in `roles` a user of `tenant_id` cannot hold.
pub(super) fn check_user_roles(
    state: &SharedState,
    roles: &[Role],
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let view = RoleView::current(state);
    role_assignment::check_assignable(roles, tenant_id.as_u64(), |name| {
        view.roles.get(name).map(|(tenant, _)| *tenant)
    })
    .map_err(refusal)
}

/// After a replicated user entry applied, confirm the user of `tenant_id`
/// holds exactly `roles`. The applier skips a user entry naming a role that
/// was dropped before the entry committed; that surfaces here as the role's
/// refusal. Any other mismatch is a concurrent change or a truncated entry,
/// which the client retries.
pub(super) fn confirm_user_roles(
    state: &SharedState,
    username: &str,
    roles: &[Role],
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let held = state.credentials.get_user(username).map(|user| user.roles);
    let held_exactly = held.as_ref().is_some_and(|held| {
        roles.iter().all(|role| held.contains(role)) && held.iter().all(|role| roles.contains(role))
    });
    if held_exactly {
        return Ok(());
    }
    check_user_roles(state, roles, tenant_id)?;
    Err(DdlError::new(
        "40001",
        format!(
            "transient: the entry for user '{username}' was superseded or truncated by a \
             leader change, retry"
        ),
    ))
}

/// Refuse to drop the custom role `name` while a user holds it or a role
/// inherits from it.
pub(super) fn check_role_droppable(state: &SharedState, name: &str) -> Result<(), DdlError> {
    let view = RoleView::current(state);
    role_assignment::check_droppable(
        name,
        view.users
            .iter()
            .map(|(user, held)| (user.as_str(), held.as_slice())),
        view.roles
            .iter()
            .map(|(role, (_, parent))| (role.as_str(), parent.as_str())),
    )
    .map_err(refusal)
}

/// After a replicated role entry applied, confirm role `name` exists with
/// inheritance parent `parent`. The applier skips a role entry whose parent
/// was dropped before the entry committed; that surfaces here as the
/// parent's refusal.
pub(super) fn confirm_role(
    state: &SharedState,
    name: &str,
    parent: Option<&str>,
) -> Result<(), DdlError> {
    let installed = state
        .roles
        .get_role(name)
        .is_some_and(|role| role.parent.as_deref() == parent);
    if installed {
        return Ok(());
    }
    if let Some(parent) = parent
        && !role_assignment::is_builtin_role_name(parent)
        && state.roles.get_role(parent).is_none()
    {
        return Err(refusal(RoleRefusal::Undefined {
            name: parent.to_string(),
        }));
    }
    Err(DdlError::new(
        "40001",
        format!("transient: the entry for role '{name}' was superseded or truncated, retry"),
    ))
}
