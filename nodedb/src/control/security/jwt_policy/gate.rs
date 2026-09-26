// SPDX-License-Identifier: BUSL-1.1

//! The single post-verification gate for `[auth.jwt]` policy that needs server
//! state, applied on every bearer route after a token has been verified and
//! bound to an identity.
//!
//! Config-only policy (claim remapping, blocked status values) runs earlier,
//! inside the JWKS registry, so it applies to every caller of the registry.
//! What lives here needs the scope definitions and auth-user store that only
//! [`SharedState`] carries, so it hangs off the two call sites that own one:
//! the HTTP bearer path and the native/OIDC bearer path.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::JwtClaims;
use crate::control::state::SharedState;

use super::{provisioning, roles, scopes};

/// Apply the state-dependent half of the JWT policy to a verified token.
///
/// `identity` is the identity the token bound to: its tenant is the one its
/// provider is bound to — never a tenant asserted by the token's claims —
/// and its roles are the resolved ones the session will hold.
///
/// An identity holding a custom role not defined in its tenant is refused
/// first, before any record is provisioned. A deployment with no JWKS
/// registry has no JWT authentication at all, so there is no other policy to
/// apply.
pub fn enforce_stateful_jwt_policy(
    state: &SharedState,
    claims: &JwtClaims,
    identity: &AuthenticatedIdentity,
) -> crate::Result<()> {
    roles::refuse_undefined_roles(state, identity)?;
    let Some(registry) = state.jwks_registry.as_ref() else {
        return Ok(());
    };
    let config = registry.jwt_config();
    let tenant_id = identity.tenant_id;

    scopes::enforce_declared_scopes(config.enforce_scopes, &state.scope_defs, claims, tenant_id)?;
    provisioning::provision_and_check_status(
        &state.auth_users,
        Some(&state.orgs),
        config,
        claims,
        tenant_id,
    )
}
