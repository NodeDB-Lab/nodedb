// SPDX-License-Identifier: BUSL-1.1

//! Privilege ceiling for identities derived from external authentication claims.

use std::sync::Once;

use super::Role;

static EXTERNAL_SUPERUSER_WARNING: Once = Once::new();

/// Parse externally supplied role names while enforcing NodeDB's privilege ceiling.
///
/// Superuser authority is owned by NodeDB's persisted credential catalog. An external
/// JWT, static provider, or OIDC claim mapping may supply ordinary built-in and custom
/// roles, but neither the `is_superuser` claim nor the `superuser` role string may
/// create superuser authority.
pub fn roles_from_external_claims(role_names: &[String], asserted_superuser: bool) -> Vec<Role> {
    let mut stripped_superuser = asserted_superuser;
    let roles = role_names
        .iter()
        .filter_map(|name| {
            let role = parse_role_infallible(name);
            if matches!(role, Role::Superuser) {
                stripped_superuser = true;
                None
            } else {
                Some(role)
            }
        })
        .collect();

    if stripped_superuser {
        EXTERNAL_SUPERUSER_WARNING.call_once(|| {
            tracing::warn!(
                "ignored superuser authority asserted by an external identity source; grant superuser through NodeDB credential administration"
            );
        });
    }

    roles
}

fn parse_role_infallible(name: &str) -> Role {
    match name.parse::<Role>() {
        Ok(role) => role,
        Err(never) => match never {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_both_superuser_representations_and_preserves_other_roles() {
        let roles = roles_from_external_claims(
            &["superuser".into(), "readwrite".into(), "custom".into()],
            true,
        );

        assert!(!roles.contains(&Role::Superuser));
        assert!(roles.contains(&Role::ReadWrite));
        assert!(roles.contains(&Role::Custom("custom".into())));
    }
}
