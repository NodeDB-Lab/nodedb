// SPDX-License-Identifier: BUSL-1.1

//! Bearer JWT authentication against the server-wide `[auth.jwt]` providers.

use tracing::{debug, warn};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwks::registry::VerifiedJwtClaims;
use crate::control::state::SharedState;

/// Authenticate a bearer JWT and return the identity it binds to, along with
/// the verified claims (the HTTP bearer route needs both: the identity for
/// authorization, the claims for `AuthContext::from_verified_jwt` enrichment).
///
/// Same two stages as the HTTP bearer route: the JWKS registry verifies the
/// signature, the `(iss, aud)` route, and the time claims, and applies the
/// config-only claim policy; `enforce_stateful_jwt_policy` then applies the
/// half that needs server state — declared scopes and JIT-provisioned account
/// status.
///
/// `None` means "not authenticated", for every reason, including a deployment
/// with no `[auth.jwt]` section at all. A peer that presents a credential is
/// asserting an identity: when there is nothing to verify it against the
/// assertion must be refused, never downgraded to an anonymous or trust
/// identity. The reason stays server-side so a caller cannot probe which
/// providers exist or why a token was refused.
pub async fn authenticate_bearer_jwt(
    state: &SharedState,
    token: &str,
) -> Option<(AuthenticatedIdentity, VerifiedJwtClaims)> {
    let Some(registry) = state.jwks_registry.as_ref() else {
        warn!("bearer token presented but no [auth.jwt] provider is configured; refusing");
        return None;
    };

    let (identity, verified) = match registry.validate_with_claims(token).await {
        Ok(verified) => verified,
        Err(error) => {
            debug!(%error, "bearer token rejected by the JWKS registry");
            return None;
        }
    };

    if let Err(error) = crate::control::security::jwt_policy::enforce_stateful_jwt_policy(
        state,
        verified.claims(),
        &identity,
    ) {
        debug!(%error, "bearer token refused by auth.jwt policy");
        return None;
    }

    Some((identity, verified))
}
