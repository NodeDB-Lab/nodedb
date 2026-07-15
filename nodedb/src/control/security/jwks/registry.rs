// SPDX-License-Identifier: BUSL-1.1

//! Multi-provider JWKS registry: routes JWT tokens to the correct provider,
//! fetches keys on demand, and validates signatures.
//!
//! All three public entry points (`validate`, `validate_with_provider`,
//! `validate_with_catalog_provider`) share the same token-decoding,
//! signature-verification, and time-claim-validation pipeline; they differ
//! only in how the verification key is resolved. The shared pipeline lives
//! in [`Self::decode_unverified`] and [`Self::verify_signature_and_time`].

mod cache_identity;

use std::sync::Arc;

use cache_identity::{catalog_cache_identity, static_cache_identity};
use tracing::{debug, warn};

use crate::config::auth::{JwtAuthConfig, JwtProviderConfig};
use crate::control::security::identity::{
    AuthMethod, AuthenticatedIdentity, roles_from_external_claims,
};
use crate::control::security::jwt::{JwtClaims, JwtError};
use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::cache::JwksCache;
use super::key::{VerificationKey, verify_signature};

/// Multi-provider JWKS registry.
///
/// Manages providers, caches keys, and validates JWT tokens.
/// Lives on the Control Plane (Send + Sync).
pub struct JwksRegistry {
    providers: Vec<JwtProviderConfig>,
    cache: Arc<JwksCache>,
    config: JwtAuthConfig,
    policy: Arc<super::url::JwksPolicy>,
    /// Background refresh task handle.
    _refresh_handle: Option<tokio::task::JoinHandle<()>>,
}

/// JWT broken into its three base64url-encoded parts plus the decoded
/// header and payload. Produced by [`JwksRegistry::decode_unverified`].
///
/// The `parts` slices borrow from the original token string and are reused
/// when reconstructing the signing input for signature verification — no
/// re-split, no re-decode.
struct DecodedToken<'a> {
    parts: [&'a str; 3],
    header: JwtHeader,
    claims: JwtClaims,
}

impl JwksRegistry {
    /// Create and initialize the registry.
    ///
    /// Fetches JWKS from all providers on startup, loads disk cache as fallback,
    /// and spawns the periodic refresh task.
    pub async fn init(config: JwtAuthConfig) -> crate::Result<Self> {
        // Registry construction is also a public entry point, so it must not
        // rely on the server-config loader to reject unsafe static providers.
        // Validate before creating cache state, fetching remote keys, or
        // spawning a refresh task.
        config.validate()?;
        let policy = Arc::new(config.jwks_policy().map_err(|e| crate::Error::Config {
            detail: format!("auth.jwt allow-list is invalid: {e}"),
        })?);
        let cache = Arc::new(JwksCache::new(config.jwks_cache_path.clone()));

        // Load disk cache first (offline fallback).
        cache.load_from_disk();

        // Fetch from all providers (best-effort — failures use disk cache).
        for provider in &config.providers {
            let cache_identity = static_cache_identity(&provider.name);
            super::fetch::fetch_and_cache(
                &cache_identity,
                &provider.name,
                &provider.jwks_url,
                &cache,
                &policy,
            )
            .await;
        }

        // Spawn periodic refresh.
        let refresh_handle = if !config.providers.is_empty() {
            let pairs: Vec<(String, String, String)> = config
                .providers
                .iter()
                .map(|p| {
                    (
                        static_cache_identity(&p.name),
                        p.name.clone(),
                        p.jwks_url.clone(),
                    )
                })
                .collect();
            Some(super::fetch::spawn_refresh_task(
                pairs,
                cache.clone(),
                config.jwks_refresh_secs,
                policy.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            providers: config.providers.clone(),
            cache,
            config,
            policy,
            _refresh_handle: refresh_handle,
        })
    }

    /// Validate a JWT token using JWKS, routing by the `iss` and `aud` claims.
    ///
    /// Flow:
    /// 1. Decode header + payload (no signature) via [`Self::decode_unverified`].
    /// 2. Match `iss` and `aud` to a configured provider via [`Self::find_provider`].
    /// 3. Resolve the verification key (cache lookup + on-demand re-fetch).
    /// 4. Verify signature, `exp`, `nbf` via [`Self::verify_signature_and_time`].
    /// 5. Validate `iss`, `aud` against the matched provider.
    /// 6. Build and return an `AuthenticatedIdentity` bound to that provider's tenant.
    pub async fn validate(&self, token: &str) -> Result<AuthenticatedIdentity, JwtError> {
        let decoded = self.decode_unverified(token)?;
        let provider = self.find_provider(&decoded.claims.iss, &decoded.claims.aud)?;
        let key = self.resolve_key(provider, &decoded).await?;
        self.verify_signature_and_time(&decoded, &key, &provider.name)?;
        validate_provider_claims(provider, &decoded.claims)?;

        let claims = decoded.claims;
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let identity = build_identity(&claims, provider.tenant_id);

        debug!(
            username = %identity.username,
            tenant_id = provider.tenant_id,
            provider = %provider.name,
            kid = %kid,
            "JWKS JWT validated"
        );

        Ok(identity)
    }

    /// Validate a JWT token using a specific named static provider.
    ///
    /// Like `validate`, but skips the `iss`-based provider lookup — the caller
    /// supplies the resolved provider name (from the OIDC provider catalog).
    /// Returns the decoded, verified claims on success.
    pub async fn validate_with_provider(
        &self,
        provider_name: &str,
        token: &str,
    ) -> Result<JwtClaims, JwtError> {
        let decoded = self.decode_unverified(token)?;
        let provider = self
            .providers
            .iter()
            .find(|p| p.name == provider_name)
            .ok_or(JwtError::InvalidIssuer)?;
        let key = self.resolve_key(provider, &decoded).await?;
        self.verify_signature_and_time(&decoded, &key, provider_name)?;
        validate_provider_claims(provider, &decoded.claims)?;

        debug!(
            provider = %provider_name,
            kid = %decoded.header.kid.as_deref().unwrap_or(""),
            sub = %decoded.claims.sub,
            "JWKS JWT validated via validate_with_provider"
        );
        Ok(decoded.claims)
    }

    /// Validate a JWT using a named catalog provider whose JWKS endpoint is
    /// provided dynamically (catalog OIDC providers not in the static config).
    ///
    /// Catalog keysets use a separate cache identity bound to their endpoint,
    /// so they cannot reuse a static provider's keys or a prior endpoint's
    /// keys after a catalog provider is recreated.
    pub async fn validate_with_catalog_provider(
        &self,
        provider_name: &str,
        jwks_uri: &str,
        token: &str,
    ) -> Result<JwtClaims, JwtError> {
        let decoded = self.decode_unverified(token)?;
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let cache_identity = catalog_cache_identity(provider_name, jwks_uri);
        let key = match self.cache.get(&cache_identity, kid) {
            Some(k) => k,
            None => {
                self.refetch_catalog_key(provider_name, jwks_uri, &cache_identity, kid)
                    .await?
            }
        };
        self.verify_signature_and_time(&decoded, &key, provider_name)?;

        debug!(
            provider = %provider_name,
            kid = %kid,
            sub = %decoded.claims.sub,
            "JWKS JWT validated via catalog provider"
        );
        Ok(decoded.claims)
    }

    /// Decode JWT claims without signature verification (for AuthContext building).
    pub fn decode_claims(&self, token: &str) -> Result<JwtClaims, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::MalformedToken);
        }
        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        sonic_rs::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)
    }

    /// Check if any providers are configured.
    pub fn is_configured(&self) -> bool {
        !self.providers.is_empty()
    }

    // ── Internal pipeline ───────────────────────────────────────────────

    /// Split the token, decode the header + payload, and check that the
    /// algorithm is non-`none` and on the allow-list. Does NOT verify the
    /// signature, the `iss`, the `aud`, or the time claims.
    fn decode_unverified<'a>(&self, token: &'a str) -> Result<DecodedToken<'a>, JwtError> {
        let raw: Vec<&str> = token.split('.').collect();
        if raw.len() != 3 {
            return Err(JwtError::MalformedToken);
        }
        let parts = [raw[0], raw[1], raw[2]];

        let header = decode_jwt_header(parts[0])?;

        // Check algorithm.
        if header.alg == "none" {
            return Err(JwtError::UnsupportedAlgorithm);
        }
        if !self.config.allowed_algorithms.is_empty()
            && !self
                .config
                .allowed_algorithms
                .iter()
                .any(|a| a == &header.alg)
        {
            return Err(JwtError::UnsupportedAlgorithm);
        }

        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        let claims: JwtClaims =
            sonic_rs::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)?;

        Ok(DecodedToken {
            parts,
            header,
            claims,
        })
    }

    /// Verify signature + `exp` + `nbf`. Assumes the algorithm has already
    /// been allow-listed by [`Self::decode_unverified`]. The `provider_name`
    /// is used only for log context on rejection.
    fn verify_signature_and_time(
        &self,
        decoded: &DecodedToken<'_>,
        key: &VerificationKey,
        provider_name: &str,
    ) -> Result<(), JwtError> {
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        if key.algorithm != decoded.header.alg {
            // HMAC-when-RSA-expected attack prevention.
            warn!(
                expected = %key.algorithm,
                actual = %decoded.header.alg,
                kid = %kid,
                provider = %provider_name,
                "JWT algorithm mismatch — possible algorithm confusion attack"
            );
            return Err(JwtError::UnsupportedAlgorithm);
        }

        let signing_input = format!("{}.{}", decoded.parts[0], decoded.parts[1]);
        let signature = base64_url_decode(decoded.parts[2]).ok_or(JwtError::DecodingError)?;
        if !verify_signature(key, signing_input.as_bytes(), &signature) {
            return Err(JwtError::InvalidSignature);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if decoded.claims.exp > 0 && now > decoded.claims.exp + self.config.clock_skew_secs {
            return Err(JwtError::Expired);
        }
        if decoded.claims.nbf > 0 && now + self.config.clock_skew_secs < decoded.claims.nbf {
            return Err(JwtError::NotYetValid);
        }
        Ok(())
    }

    /// Resolve the verification key for a static-config provider, refetching
    /// from the provider's JWKS URL on cache miss (rate-limited).
    async fn resolve_key(
        &self,
        provider: &JwtProviderConfig,
        decoded: &DecodedToken<'_>,
    ) -> Result<VerificationKey, JwtError> {
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let cache_identity = static_cache_identity(&provider.name);
        match self.cache.get(&cache_identity, kid) {
            Some(k) => Ok(k),
            None => {
                self.refetch_for_unknown_kid(provider, &cache_identity, kid)
                    .await
            }
        }
    }

    /// Find the provider matching a token's issuer and audience.
    ///
    /// Static configuration validation ensures a route is unique. A provider
    /// with an empty audience is a wildcard only when it is the sole provider
    /// for its issuer; validation forbids it from sharing that issuer. There
    /// is no single-provider fallback for a token whose issuer is empty or
    /// does not match a configured provider. For a known issuer with a
    /// mismatched audience, return `InvalidAudience` rather than accepting
    /// the first provider.
    fn find_provider(&self, issuer: &str, audience: &str) -> Result<&JwtProviderConfig, JwtError> {
        if issuer.is_empty() {
            return Err(JwtError::InvalidIssuer);
        }

        let mut issuer_matched = false;
        let mut wildcard_provider = None;
        for provider in &self.providers {
            if provider.issuer == issuer {
                issuer_matched = true;
                if provider.audience == audience {
                    return Ok(provider);
                }
                if provider.audience.is_empty() {
                    wildcard_provider = Some(provider);
                }
            }
        }

        match (issuer_matched, wildcard_provider) {
            (_, Some(provider)) => Ok(provider),
            (true, None) => Err(JwtError::InvalidAudience),
            (false, None) => Err(JwtError::InvalidIssuer),
        }
    }

    /// On-demand re-fetch for unknown `kid` against a static-config provider.
    async fn refetch_for_unknown_kid(
        &self,
        provider: &JwtProviderConfig,
        cache_identity: &str,
        kid: &str,
    ) -> Result<VerificationKey, JwtError> {
        if !self
            .cache
            .can_refetch(cache_identity, self.config.jwks_min_refetch_secs)
        {
            warn!(
                provider = %provider.name,
                kid = %kid,
                "unknown kid — re-fetch rate-limited"
            );
            return Err(JwtError::InvalidSignature);
        }

        self.cache.mark_refetch_attempted(cache_identity);
        super::fetch::fetch_and_cache(
            cache_identity,
            &provider.name,
            &provider.jwks_url,
            &self.cache,
            &self.policy,
        )
        .await;

        self.cache
            .get(cache_identity, kid)
            .ok_or(JwtError::InvalidSignature)
    }

    /// On-demand re-fetch for a catalog provider whose JWKS URI is supplied
    /// dynamically (not part of static config).
    async fn refetch_catalog_key(
        &self,
        provider_name: &str,
        jwks_uri: &str,
        cache_identity: &str,
        kid: &str,
    ) -> Result<VerificationKey, JwtError> {
        if !self
            .cache
            .can_refetch(cache_identity, self.config.jwks_min_refetch_secs)
        {
            warn!(
                provider = %provider_name,
                kid = %kid,
                "unknown kid — re-fetch rate-limited (catalog provider)"
            );
            return Err(JwtError::InvalidSignature);
        }
        self.cache.mark_refetch_attempted(cache_identity);
        super::fetch::fetch_and_cache(
            cache_identity,
            provider_name,
            jwks_uri,
            &self.cache,
            &self.policy,
        )
        .await;
        self.cache
            .get(cache_identity, kid)
            .ok_or(JwtError::InvalidSignature)
    }
}

/// Validate the issuer and audience constraints of a selected static provider.
fn validate_provider_claims(
    provider: &JwtProviderConfig,
    claims: &JwtClaims,
) -> Result<(), JwtError> {
    if claims.iss != provider.issuer {
        return Err(JwtError::InvalidIssuer);
    }
    if !provider.audience.is_empty() && claims.aud != provider.audience {
        return Err(JwtError::InvalidAudience);
    }
    Ok(())
}

/// Build an `AuthenticatedIdentity` from a verified static-provider JWT.
///
/// Static-provider roles are parsed by [`Role::from_str`]. Tenant ownership comes
/// from the provider's server-side binding, never the JWT. The catalog path uses
/// [`crate::control::security::oidc`] instead, which applies stored
/// claim-mapping rules.
fn build_identity(claims: &JwtClaims, tenant_id: u64) -> AuthenticatedIdentity {
    let roles = roles_from_external_claims(&claims.roles, claims.is_superuser);
    let username = if claims.sub.is_empty() {
        format!("jwt_user_{}", claims.user_id)
    } else {
        claims.sub.clone()
    };
    AuthenticatedIdentity {
        user_id: claims.user_id,
        username,
        tenant_id: TenantId::new(tenant_id),
        auth_method: AuthMethod::OidcBearer,
        roles,
        is_superuser: false,
        default_database: None,
        accessible_databases: AuthenticatedIdentity::default_database_set(false),
    }
}

// ── JWT Header Parsing ──────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

fn decode_jwt_header(encoded: &str) -> Result<JwtHeader, JwtError> {
    let bytes = base64_url_decode(encoded).ok_or(JwtError::DecodingError)?;
    sonic_rs::from_slice(&bytes).map_err(|_| JwtError::InvalidClaims)
}
