// SPDX-License-Identifier: BUSL-1.1

//! Which role names a user can hold, and when a custom role can be dropped.
//!
//! A role name parses to a built-in [`Role`] or to [`Role::Custom`]. A custom
//! name is only a name: it grants something only once a custom role of that
//! name is defined in the user's tenant. So a user may hold a custom role only
//! while it is defined there, and a custom role may be dropped only while no
//! user holds it and no role inherits from it, as PostgreSQL refuses to drop
//! a role that objects still depend on.
//!
//! The same rules run in two places, each against its own view of the
//! committed catalog:
//!
//! - **Proposal:** the DDL handler checks the in-memory role and user stores,
//!   and refuses the statement before anything is proposed.
//! - **Apply:** the metadata applier checks the redb catalog at the entry's
//!   log position. Every node applies the same log in the same order, so a
//!   `PutUser` or `DeleteRole` that raced another change is skipped on every
//!   node alike, and a replayed log never produces a user holding an
//!   undefined role.

use crate::control::security::catalog::{StoredRole, StoredUser};

use super::identity::Role;

/// Why a role assignment or a role drop is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RoleRefusal {
    /// The role is neither built in nor defined in the user's tenant.
    #[error("role \"{name}\" does not exist")]
    Undefined { name: String },
    /// Users still hold the role.
    #[error(
        "role \"{name}\" cannot be dropped because users still hold it: {}",
        .users.join(", ")
    )]
    HeldByUsers { name: String, users: Vec<String> },
    /// Other roles inherit from the role.
    #[error(
        "role \"{name}\" cannot be dropped because other roles inherit from it: {}",
        .children.join(", ")
    )]
    InheritedBy { name: String, children: Vec<String> },
}

impl RoleRefusal {
    /// The SQLSTATE PostgreSQL gives for the same refusal.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Undefined { .. } => "42704",
            Self::HeldByUsers { .. } | Self::InheritedBy { .. } => "2BP01",
        }
    }
}

impl From<RoleRefusal> for crate::Error {
    fn from(refusal: RoleRefusal) -> Self {
        match refusal {
            RoleRefusal::Undefined { name } => crate::Error::UndefinedObject { kind: "role", name },
            other => crate::Error::BadRequest {
                detail: other.to_string(),
            },
        }
    }
}

/// Parse a role name. Unknown names become [`Role::Custom`].
pub fn parse_role_name(name: &str) -> Role {
    match name.parse() {
        Ok(role) => role,
        Err(e) => match e {},
    }
}

/// Whether `name` names a built-in role, so it can never be a custom role.
pub fn is_builtin_role_name(name: &str) -> bool {
    !matches!(parse_role_name(name), Role::Custom(_))
}

/// Check that a user of `tenant_id` can hold every role in `roles`.
///
/// `custom_tenant` returns the tenant a custom role of the given name is
/// defined in, or `None` when no such role is defined.
pub fn check_assignable<'a>(
    roles: impl IntoIterator<Item = &'a Role>,
    tenant_id: u64,
    custom_tenant: impl Fn(&str) -> Option<u64>,
) -> Result<(), RoleRefusal> {
    for role in roles {
        if let Role::Custom(name) = role
            && custom_tenant(name) != Some(tenant_id)
        {
            return Err(RoleRefusal::Undefined { name: name.clone() });
        }
    }
    Ok(())
}

/// Check that the custom role `name` can be dropped: no user in `users`
/// holds it, and no role in `roles` inherits from it.
pub fn check_droppable<'a>(
    name: &str,
    users: impl IntoIterator<Item = (&'a str, &'a [String])>,
    roles: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(), RoleRefusal> {
    let mut holders: Vec<String> = users
        .into_iter()
        .filter(|(_, held)| held.iter().any(|role| role == name))
        .map(|(user, _)| user.to_string())
        .collect();
    if !holders.is_empty() {
        holders.sort();
        return Err(RoleRefusal::HeldByUsers {
            name: name.to_string(),
            users: holders,
        });
    }
    let mut children: Vec<String> = roles
        .into_iter()
        .filter(|(_, parent)| *parent == name)
        .map(|(child, _)| child.to_string())
        .collect();
    if !children.is_empty() {
        children.sort();
        return Err(RoleRefusal::InheritedBy {
            name: name.to_string(),
            children,
        });
    }
    Ok(())
}

/// [`check_assignable`] for a stored user against the stored roles.
pub fn check_stored_user(user: &StoredUser, roles: &[StoredRole]) -> Result<(), RoleRefusal> {
    let parsed: Vec<Role> = user
        .roles
        .iter()
        .map(String::as_str)
        .map(parse_role_name)
        .collect();
    check_assignable(&parsed, user.tenant_id, |name| {
        roles
            .iter()
            .find(|role| role.name == name)
            .map(|role| role.tenant_id)
    })
}

/// [`check_droppable`] for the stored role `name` against the stored users
/// and roles. Inactive users are dropped users and hold nothing.
pub fn check_stored_drop(
    name: &str,
    users: &[StoredUser],
    roles: &[StoredRole],
) -> Result<(), RoleRefusal> {
    check_droppable(
        name,
        users
            .iter()
            .filter(|user| user.is_active)
            .map(|user| (user.username.as_str(), user.roles.as_slice())),
        roles
            .iter()
            .map(|role| (role.name.as_str(), role.parent.as_str())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom(name: &str) -> Role {
        Role::Custom(name.to_string())
    }

    #[test]
    fn built_in_roles_are_always_assignable() {
        let roles = [
            Role::Superuser,
            Role::TenantAdmin,
            Role::ReadWrite,
            Role::ReadOnly,
            Role::Monitor,
        ];
        assert_eq!(check_assignable(&roles, 1, |_| None), Ok(()));
    }

    #[test]
    fn an_undefined_custom_role_is_refused_by_name() {
        let roles = [Role::ReadWrite, custom("read_write")];
        assert_eq!(
            check_assignable(&roles, 1, |_| None),
            Err(RoleRefusal::Undefined {
                name: "read_write".into()
            })
        );
        assert_eq!(
            RoleRefusal::Undefined { name: "x".into() }.sqlstate(),
            "42704"
        );
    }

    #[test]
    fn a_custom_role_is_assignable_only_in_its_own_tenant() {
        let lookup = |name: &str| (name == "analyst").then_some(7);
        assert_eq!(check_assignable(&[custom("analyst")], 7, lookup), Ok(()));
        assert!(check_assignable(&[custom("analyst")], 8, lookup).is_err());
    }

    #[test]
    fn a_held_or_inherited_role_cannot_be_dropped() {
        let held = vec!["analyst".to_string()];
        let none: Vec<String> = Vec::new();
        assert_eq!(
            check_droppable(
                "analyst",
                [("bob", held.as_slice()), ("amy", none.as_slice())],
                []
            ),
            Err(RoleRefusal::HeldByUsers {
                name: "analyst".into(),
                users: vec!["bob".into()]
            })
        );
        assert_eq!(
            check_droppable(
                "analyst",
                [("amy", none.as_slice())],
                [("junior", "analyst")]
            ),
            Err(RoleRefusal::InheritedBy {
                name: "analyst".into(),
                children: vec!["junior".into()]
            })
        );
        assert_eq!(
            check_droppable("analyst", [("amy", none.as_slice())], [("junior", "")]),
            Ok(())
        );
    }

    #[test]
    fn every_parsed_built_in_name_is_built_in() {
        for name in [
            "superuser",
            "cluster_admin",
            "tenant_admin",
            "readwrite",
            "readonly",
            "monitor",
        ] {
            assert!(is_builtin_role_name(name), "{name}");
        }
        assert!(!is_builtin_role_name("read_write"));
    }
}
