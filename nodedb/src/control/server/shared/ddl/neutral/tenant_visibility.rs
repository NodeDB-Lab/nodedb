// SPDX-License-Identifier: BUSL-1.1

//! Tenant visibility rules for the auth-user, org, scope, delegation and
//! metering listings.
//!
//! A superuser sees every tenant. Any other caller sees an object only when
//! its owning auth user or org belongs to the caller's tenant. An id that
//! resolves to no record is not visible to a non-superuser.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Tenant of auth user `id`. Falls back to the credential store, because a
/// delegation names the credential user id of its delegator.
fn auth_user_tenant(state: &SharedState, id: &str) -> Option<u64> {
    if let Some(user) = state.auth_users.get(id) {
        return Some(user.tenant_id);
    }
    state
        .credentials
        .list_all_user_details()
        .into_iter()
        .find(|u| u.user_id.to_string() == id)
        .map(|u| u.tenant_id.as_u64())
}

/// True when auth user `id` belongs to the caller's tenant.
pub(super) fn auth_user_visible(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    id: &str,
) -> bool {
    identity.is_superuser || auth_user_tenant(state, id) == Some(identity.tenant_id.as_u64())
}

/// True when org `org_id` belongs to the caller's tenant.
pub(super) fn org_visible(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    org_id: &str,
) -> bool {
    identity.is_superuser
        || state
            .orgs
            .get(org_id)
            .is_some_and(|o| o.tenant_id == identity.tenant_id.as_u64())
}

/// True when the grantee of a scope grant (`org` or an auth user) belongs to
/// the caller's tenant.
pub(super) fn grantee_visible(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    grantee_type: &str,
    grantee_id: &str,
) -> bool {
    if grantee_type.eq_ignore_ascii_case("org") {
        org_visible(state, identity, grantee_id)
    } else {
        auth_user_visible(state, identity, grantee_id)
    }
}
