// SPDX-License-Identifier: BUSL-1.1

//! Refuse an external identity that resolved to an undefined custom role.
//!
//! A JWT or OIDC login takes its roles from the identity provider: the
//! token's claims, remapped by `[auth.jwt]`, or the provider's claim-mapping
//! rules. A resolved name that is not built in parses to a custom role, and a
//! custom role grants something only while it is defined in the tenant the
//! provider is bound to. An identity holding an undefined role would hold
//! nothing, and a just-in-time provisioned record would store it. So the
//! login is refused with a typed error naming the role, and the refusal is
//! recorded in the audit log, before anything is provisioned.

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::role_assignment::{self, RoleRefusal};
use crate::control::state::SharedState;

/// Refuse `identity` when any of its roles is neither built in nor defined
/// in its tenant.
pub fn refuse_undefined_roles(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<()> {
    let tenant_id = identity.tenant_id;
    let checked = role_assignment::check_assignable(&identity.roles, tenant_id.as_u64(), |name| {
        state
            .roles
            .get_role(name)
            .map(|role| role.tenant_id.as_u64())
    });
    let Err(refusal) = checked else {
        return Ok(());
    };
    let role = match refusal {
        RoleRefusal::Undefined { name } => name,
        other => other.to_string(),
    };
    state.audit_record(
        AuditEvent::AuthFailure,
        Some(tenant_id),
        &identity.username,
        &format!("external login refused: role \"{role}\" is not defined in tenant {tenant_id}"),
    );
    Err(crate::Error::ExternalRoleUndefined {
        subject: identity.username.clone(),
        role,
        tenant_id: tenant_id.as_u64(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, Role};
    use crate::types::TenantId;
    use crate::wal::WalManager;

    fn state(dir: &tempfile::TempDir) -> Arc<SharedState> {
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("roles.wal")).expect("open WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    fn external(tenant: u64, roles: Vec<Role>) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "idp-alice",
            TenantId::new(tenant),
            AuthMethod::OidcBearer,
            roles,
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    fn auth_failures(state: &SharedState) -> Vec<String> {
        state
            .audit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .query_by_event(&AuditEvent::AuthFailure)
            .into_iter()
            .map(|entry| entry.detail.clone())
            .collect()
    }

    #[test]
    fn an_undefined_claimed_role_refuses_the_login_and_is_audited() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(&dir);
        let identity = external(5, vec![Role::ReadOnly, Role::Custom("ghost".into())]);

        match refuse_undefined_roles(&state, &identity) {
            Err(crate::Error::ExternalRoleUndefined {
                subject,
                role,
                tenant_id,
            }) => {
                assert_eq!(subject, "idp-alice");
                assert_eq!(role, "ghost");
                assert_eq!(tenant_id, 5);
            }
            other => panic!("expected ExternalRoleUndefined, got {other:?}"),
        }
        let failures = auth_failures(&state);
        assert!(
            failures.iter().any(|detail| detail.contains("\"ghost\"")),
            "the refusal must be audited: {failures:?}"
        );
    }

    #[test]
    fn a_defined_role_of_the_bound_tenant_is_accepted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(&dir);
        state
            .roles
            .create_role("analyst", TenantId::new(5), None, None)
            .expect("create role");

        refuse_undefined_roles(
            &state,
            &external(5, vec![Role::ReadWrite, Role::Custom("analyst".into())]),
        )
        .expect("a defined role is accepted");
        assert!(auth_failures(&state).is_empty());
    }

    #[test]
    fn a_role_defined_only_in_another_tenant_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(&dir);
        state
            .roles
            .create_role("analyst", TenantId::new(5), None, None)
            .expect("create role");

        assert!(matches!(
            refuse_undefined_roles(&state, &external(6, vec![Role::Custom("analyst".into())])),
            Err(crate::Error::ExternalRoleUndefined { .. })
        ));
    }
}
