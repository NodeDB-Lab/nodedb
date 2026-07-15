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

use crate::control::security::catalog::StoredOidcProvider;
use crate::control::security::identity::database_set::DatabaseSet;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::security::jwt::JwtError;
use crate::control::security::util::base64_url_decode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::claim_mapping::apply_claim_mapping;

/// Verify an OIDC bearer token and return an ephemeral `AuthenticatedIdentity`.
///
/// Steps:
/// 1. Decode header + payload (no signature) to extract `iss` and `aud`.
/// 2. Route by `(iss, aud)` to one catalog provider, rejecting ambiguous routes.
/// 3. Verify signature, expiration, and not-before via `JwksRegistry`.
/// 4. Re-check the verified issuer and audience against the selected provider.
/// 5. Apply claim-mapping rules to derive `default_database`, `accessible_databases`, `roles`.
/// 6. Construct `AuthenticatedIdentity` with `auth_method = OidcBearer`.
pub async fn verify_bearer_token(
    state: &SharedState,
    token: &str,
) -> crate::Result<AuthenticatedIdentity> {
    // 1. Decode payload (no sig) to read `iss` and `aud`.
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(jwt_error_to_crate_error(JwtError::MalformedToken));
    }
    let payload_bytes = base64_url_decode(parts[1])
        .ok_or_else(|| jwt_error_to_crate_error(JwtError::DecodingError))?;
    let claims: crate::control::security::jwt::JwtClaims = sonic_rs::from_slice(&payload_bytes)
        .map_err(|_| jwt_error_to_crate_error(JwtError::InvalidClaims))?;

    // 2. Route by both unverified issuer and audience. These values only select
    // the verification key; the same constraints are checked again after
    // signature and time validation.
    let catalog = state.credentials.catalog();
    let provider = select_catalog_provider(
        catalog
            .list_oidc_providers()
            .map_err(|_| jwt_error_to_crate_error(JwtError::InvalidIssuer))?,
        &claims.iss,
        &claims.aud,
    )
    .map_err(jwt_error_to_crate_error)?;

    // 3. Verify signature via JwksRegistry using the catalog-provided JWKS URI.
    let jwks = state
        .jwks_registry
        .as_ref()
        .ok_or_else(|| jwt_error_to_crate_error(JwtError::UnsupportedAlgorithm))?;
    let verified_claims = jwks
        .validate_with_catalog_provider(&provider.provider_name, &provider.jwks_uri, token)
        .await
        .map_err(jwt_error_to_crate_error)?;

    // 4. Re-check the verified claims against the provider selected above.
    validate_selected_provider_claims(&provider, &verified_claims)
        .map_err(jwt_error_to_crate_error)?;

    // Legacy records without a tenant binding must not reveal catalog metadata
    // until the token has passed signature, time, issuer, and audience checks.
    let tenant_id = provider
        .tenant_id
        .ok_or(crate::Error::OidcProviderTenantUnbound)?;

    // A provider can outlive its tenant after a tenant drop. Confirm the bound
    // tenant still exists before issuing an identity from an authenticated token.
    let tenant_exists = catalog
        .load_all_tenants()
        .map_err(|_| crate::Error::OidcProviderTenantUnavailable { tenant_id })?
        .into_iter()
        .any(|tenant| tenant.tenant_id == tenant_id);
    if !tenant_exists {
        return Err(crate::Error::OidcProviderTenantUnavailable { tenant_id });
    }

    // 5. Apply claim mapping.
    let mapping = apply_claim_mapping(&verified_claims, &provider.claim_mapping);

    // Build the accessible-database set. The default database MUST be set
    // by a matching claim-mapping rule — there is no silent fallback to
    // `DatabaseId::DEFAULT`. An OIDC user whose claims match no rule that
    // assigns a database is rejected here so operators see the gap instead
    // of silently routing the session to the system default.
    let default_db = mapping
        .default_database
        .map(DatabaseId::new)
        .ok_or_else(|| crate::Error::OidcNoDefaultDatabase {
            sub: verified_claims.sub.clone(),
        })?;

    let mut accessible: smallvec::SmallVec<[DatabaseId; 4]> = smallvec::smallvec![default_db];
    for &db_raw in &mapping.accessible_databases {
        let db = DatabaseId::new(db_raw);
        if !accessible.contains(&db) {
            accessible.push(db);
        }
    }

    // Map role strings to Role enum values. `Role::from_str` is infallible
    // (unknown names land in `Role::Custom`), so destructure the Result
    // without a phantom fallback that the type system says cannot fire.
    let roles: Vec<Role> = mapping
        .roles
        .iter()
        .map(|r| match r.parse::<Role>() {
            Ok(role) => role,
            Err(never) => match never {},
        })
        .collect();

    let username = if verified_claims.sub.is_empty() {
        format!("oidc_{}", verified_claims.user_id)
    } else {
        verified_claims.sub.clone()
    };

    debug!(
        provider = %provider.provider_name,
        sub = %verified_claims.sub,
        iss = %verified_claims.iss,
        default_db = %default_db.as_u64(),
        "OIDC login succeeded"
    );

    Ok(AuthenticatedIdentity {
        // Use a sentinel range for OIDC ephemeral identities to avoid colliding
        // with trust-mode user_id == 0 checks. The real user record, if any,
        // is identified by the `sub` claim's username.
        user_id: verified_claims.user_id,
        username,
        tenant_id: TenantId::new(tenant_id),
        auth_method: AuthMethod::OidcBearer,
        roles,
        is_superuser: false,
        default_database: Some(default_db),
        accessible_databases: DatabaseSet::Some(accessible),
    })
}

/// Select one catalog provider for the token's unverified issuer and audience.
///
/// A `None` or empty audience is an issuer-only wildcard, valid only when it
/// is the sole provider for that issuer. Catalog corruption or legacy data
/// cannot silently change routing because every duplicate or wildcard-sharing
/// issuer route is rejected here.
fn select_catalog_provider(
    providers: Vec<StoredOidcProvider>,
    iss: &str,
    aud: &str,
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

    let mut exact_matches: Vec<StoredOidcProvider> = issuer_providers
        .iter()
        .filter(|provider| {
            provider
                .audience
                .as_deref()
                .is_some_and(|expected| !expected.is_empty() && expected == aud)
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
    if provider
        .audience
        .as_deref()
        .is_some_and(|expected| !expected.is_empty() && claims.aud != expected)
    {
        return Err(JwtError::InvalidAudience);
    }
    Ok(())
}

/// Collapse every pre-authentication JWT failure into one client-visible error.
/// Route, signature, algorithm, and token-state details remain non-observable.
fn jwt_error_to_crate_error(_error: JwtError) -> crate::Error {
    crate::Error::BadRequest {
        detail: "OIDC authentication failed".into(),
    }
}
