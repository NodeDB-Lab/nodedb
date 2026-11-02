// SPDX-License-Identifier: BUSL-1.1

//! OIDC bearer-token verification entry point.
//!
//! Decodes the token header + payload (without verifying the signature) to
//! read the `iss` and `aud` claims, resolves the matching OIDC provider from
//! the catalog, delegates signature verification to `JwksRegistry`, applies claim mapping,
//! and constructs an ephemeral `AuthenticatedIdentity`.
//!
//! pgwire does NOT support OIDC bearer tokens (SCRAM-SHA-256 only).
//! Use the native protocol or HTTP for OIDC.

use nodedb_types::id::DatabaseId;
use tracing::debug;

use crate::control::security::bearer::{BearerAttempt, BearerMethod, BearerRefusal};
use crate::control::security::catalog::StoredOidcProvider;
use crate::control::security::identity::database_set::DatabaseSet;
use crate::control::security::identity::{
    AuthenticatedIdentity, ExternalClaims, ExternalProviderBinding, identity_from_external_claims,
};
use crate::control::security::jwks::registry::VerifiedJwtClaims;
use crate::control::security::jwt::JwtError;
use crate::control::security::util::base64_url_decode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::claim_mapping::apply_claim_mapping;

/// Verify an OIDC bearer token and return an ephemeral `AuthenticatedIdentity`
/// alongside the opaque proof of claim verification.
///
/// Steps:
/// 1. Decode header + payload (no signature) to extract `iss` and `aud`.
/// 2. Route by `(iss, aud)` to one catalog provider, rejecting ambiguous routes.
/// 3. Verify signature, expiration, and not-before via `JwksRegistry`.
/// 4. Re-check the verified issuer and audience against the selected provider.
/// 5. Apply claim-mapping rules to derive `default_database`, `accessible_databases`, `roles`.
/// 6. Construct `AuthenticatedIdentity` with `auth_method = OidcBearer`.
/// 7. Apply the state-dependent `[auth.jwt]` policy: declared-scope
///    enforcement and JIT provisioning / account-status gating.
///
/// The returned [`VerifiedJwtClaims`] lets callers enrich a downstream
/// `AuthContext` (email, org, groups, permissions, metadata) via
/// `AuthContext::from_verified_jwt` / `RequestAuthScopeBuilder::with_verified_jwt`
/// — mirroring what the HTTP bearer-token path already does. Authority
/// (superuser, tenant, roles) always comes from the `AuthenticatedIdentity`
/// built here, never from the claims themselves.
///
/// Every refusal records one `AuthFailure` and every login records one
/// `AuthSuccess`, through [`BearerAttempt`]. `peer_addr` is the audit source.
/// `session_id` is the id the accepted session's `AuthContext` carries.
pub async fn verify_bearer_token(
    state: &SharedState,
    token: &str,
    peer_addr: &str,
    session_id: &str,
) -> crate::Result<(AuthenticatedIdentity, VerifiedJwtClaims)> {
    let mut attempt = BearerAttempt::new(BearerMethod::OidcBearer, peer_addr, session_id);
    match verify_catalog_token(state, token, &mut attempt).await {
        Ok((identity, verified)) => {
            attempt.accept(state, &identity, &verified.claims().sub);
            Ok((identity, verified))
        }
        Err(refusal) => {
            attempt.refuse(state, &refusal.reason);
            Err(*refusal.error)
        }
    }
}

/// The verification steps of [`verify_bearer_token`].
///
/// Records the claimed `sub` and the selected provider into `attempt` as each one is learned.
async fn verify_catalog_token(
    state: &SharedState,
    token: &str,
    attempt: &mut BearerAttempt<'_>,
) -> Result<(AuthenticatedIdentity, VerifiedJwtClaims), BearerRefusal> {
    // 1. Decode payload (no sig) to read `iss` and `aud`.
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(token_refusal(JwtError::MalformedToken));
    }
    let payload_bytes =
        base64_url_decode(parts[1]).ok_or_else(|| token_refusal(JwtError::DecodingError))?;
    let claims: crate::control::security::jwt::JwtClaims =
        crate::util::bounded_json::from_slice(&payload_bytes)
            .map_err(|_| token_refusal(JwtError::InvalidClaims))?;
    attempt.set_claimed_sub(&claims.sub);

    // 2. Route by both unverified issuer and audience. These values only select
    // the verification key; the same constraints are checked again after
    // signature and time validation.
    let catalog = state.credentials.catalog();
    let providers = catalog.list_oidc_providers().map_err(|e| {
        BearerRefusal::new(
            format!("OIDC provider catalog read failed: {e}"),
            client_refusal_error(),
        )
    })?;
    let provider =
        select_catalog_provider(providers, &claims.iss, &claims.aud).map_err(token_refusal)?;
    attempt.set_provider(
        &provider.provider_name,
        Some(TenantId::new(provider.tenant_id)),
    );

    // 3. Verify signature via JwksRegistry using the catalog-provided JWKS URI.
    let jwks = state.jwks_registry.as_ref().ok_or_else(|| {
        BearerRefusal::new(
            "no JWKS registry is configured to verify the token",
            client_refusal_error(),
        )
    })?;
    let verified = jwks
        .validate_with_catalog_provider(&provider.provider_name, &provider.jwks_uri, token)
        .await
        .map_err(token_refusal)?;
    let verified_claims = verified.claims();

    // 4. Re-check the verified claims against the provider selected above.
    validate_selected_provider_claims(&provider, verified_claims).map_err(token_refusal)?;

    let tenant_id = provider.tenant_id;

    // A provider can outlive its tenant after a tenant drop. Confirm the bound
    // tenant still exists before issuing an identity from an authenticated token.
    let tenant_exists = catalog
        .load_all_tenants()
        .map_err(|e| {
            BearerRefusal::new(
                format!("tenant catalog read failed: {e}"),
                crate::Error::OidcProviderTenantUnavailable { tenant_id },
            )
        })?
        .into_iter()
        .any(|tenant| tenant.tenant_id == tenant_id);
    if !tenant_exists {
        return Err(BearerRefusal::from_error(
            crate::Error::OidcProviderTenantUnavailable { tenant_id },
        ));
    }

    // 5. Apply claim mapping.
    let mapping = apply_claim_mapping(verified_claims, &provider.claim_mapping);

    // Build the accessible-database set. The default database MUST be set
    // by a matching claim-mapping rule — there is no silent fallback to
    // `DatabaseId::DEFAULT`. An OIDC user whose claims match no rule that
    // assigns a database is rejected here so operators see the gap instead
    // of silently routing the session to the system default.
    let default_db = mapping
        .default_database
        .map(DatabaseId::new)
        .ok_or_else(|| {
            BearerRefusal::from_error(crate::Error::OidcNoDefaultDatabase {
                sub: verified_claims.sub.clone(),
            })
        })?;

    let mut accessible: smallvec::SmallVec<[DatabaseId; 4]> = smallvec::smallvec![default_db];
    for &db_raw in &mapping.accessible_databases {
        let db = DatabaseId::new(db_raw);
        if !accessible.contains(&db) {
            accessible.push(db);
        }
    }

    debug!(
        provider = %provider.provider_name,
        sub = %verified_claims.sub,
        iss = %verified_claims.iss,
        default_db = %default_db.as_u64(),
        "OIDC token verified"
    );

    let identity = identity_from_external_claims(
        ExternalClaims {
            user_id: verified_claims.user_id,
            subject: &verified_claims.sub,
            role_names: &mapping.roles,
            asserted_superuser: false,
        },
        ExternalProviderBinding::mapped_databases(
            TenantId::new(tenant_id),
            default_db,
            DatabaseSet::Some(accessible),
        ),
    );

    // 7. Apply the state-dependent `[auth.jwt]` policy (declared scopes,
    // JIT-provisioned account status) against the provider-bound tenant. The
    // config-only half — claim remapping and blocked status values — already
    // ran inside `validate_with_catalog_provider`.
    crate::control::security::jwt_policy::enforce_stateful_jwt_policy(
        state,
        verified_claims,
        &identity,
    )
    .map_err(BearerRefusal::from_error)?;

    Ok((identity, verified))
}

/// Select one catalog provider for the token's unverified issuer and audience.
///
/// A `None` or empty audience is an issuer-only wildcard, valid only when it
/// is the sole provider for that issuer. Catalog corruption or legacy data
/// cannot silently change routing because every duplicate or wildcard-sharing
/// issuer route is rejected here.
///
/// `aud` is the token's full audience list (RFC 7519 permits several).
fn select_catalog_provider(
    providers: Vec<StoredOidcProvider>,
    iss: &str,
    aud: &[String],
) -> Result<StoredOidcProvider, JwtError> {
    if iss.is_empty() {
        return Err(JwtError::InvalidIssuer);
    }

    let issuer_providers: Vec<StoredOidcProvider> = providers
        .into_iter()
        .filter(|provider| provider.issuer == iss)
        .collect();

    if issuer_providers.is_empty() {
        return Err(JwtError::InvalidIssuer);
    }

    let wildcard_count = issuer_providers
        .iter()
        .filter(|provider| provider.audience.as_deref().is_none_or(str::is_empty))
        .count();
    if wildcard_count > 0 && issuer_providers.len() > 1 {
        return Err(JwtError::InvalidIssuer);
    }

    // Exact equality against one element of the token's audience list — never
    // a substring or prefix test, which would route a token issued for an
    // unrelated audience to this provider.
    let mut exact_matches: Vec<StoredOidcProvider> = issuer_providers
        .iter()
        .filter(|provider| {
            provider.audience.as_deref().is_some_and(|expected| {
                !expected.is_empty() && aud.iter().any(|value| value == expected)
            })
        })
        .cloned()
        .collect();
    if exact_matches.len() > 1 {
        return Err(JwtError::InvalidIssuer);
    }
    if let Some(provider) = exact_matches.pop() {
        return Ok(provider);
    }

    if wildcard_count == 1 {
        return issuer_providers
            .into_iter()
            .next()
            .ok_or(JwtError::InvalidIssuer);
    }

    Err(JwtError::InvalidAudience)
}

/// Re-check selected provider constraints after signature and time validation.
fn validate_selected_provider_claims(
    provider: &StoredOidcProvider,
    claims: &crate::control::security::jwt::JwtClaims,
) -> Result<(), JwtError> {
    if claims.iss != provider.issuer {
        return Err(JwtError::InvalidIssuer);
    }
    // Exact equality against one element of the token's audience list — never
    // a substring or prefix test: a token whose audience list carries an
    // unrelated value must not pass this provider's audience constraint.
    if provider.audience.as_deref().is_some_and(|expected| {
        !expected.is_empty() && !claims.aud.iter().any(|value| value == expected)
    }) {
        return Err(JwtError::InvalidAudience);
    }
    Ok(())
}

/// A token-stage refusal: the audit log records `error`, and the client sees one generic error.
fn token_refusal(error: JwtError) -> BearerRefusal {
    BearerRefusal::new(error.to_string(), client_refusal_error())
}

/// The one client-visible error of every token-stage refusal.
/// Route, signature, algorithm, and token-state details remain non-observable.
fn client_refusal_error() -> crate::Error {
    crate::Error::BadRequest {
        detail: "OIDC authentication failed".into(),
    }
}
