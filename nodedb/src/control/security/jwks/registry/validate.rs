// SPDX-License-Identifier: BUSL-1.1

//! The three public entry points — `validate`, `validate_with_claims`,
//! `validate_with_catalog_provider` — plus the claim-policy gate both bearer
//! routes converge on.

use tracing::debug;

use crate::config::auth::JwtAuthConfig;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::jwt::{JwtClaims, JwtError, validate_time_claims};

use super::cache_identity::catalog_cache_identity;
use super::claims::{build_identity, validate_provider_claims};
use super::state::JwksRegistry;
use super::verified::VerifiedJwtClaims;

impl JwksRegistry {
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
        self.validate_with_claims(token)
            .await
            .map(|(identity, _)| identity)
    }

    /// Validate a JWT and retain an opaque proof for rich session claims.
    pub(crate) async fn validate_with_claims(
        &self,
        token: &str,
    ) -> Result<(AuthenticatedIdentity, VerifiedJwtClaims), JwtError> {
        let decoded = self.decode_unverified(token)?;
        let provider = self.find_provider(&decoded.claims.iss, &decoded.claims.aud)?;
        let key = self.resolve_key(provider, &decoded).await?;
        self.verify_signature_and_time(&decoded, &key, &provider.name)?;
        validate_provider_claims(provider, &decoded.claims)?;

        let mut claims = decoded.claims;
        self.apply_claim_policy(&mut claims)?;
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let identity = build_identity(&claims, provider.tenant_id);

        debug!(
            username = %identity.username,
            tenant_id = provider.tenant_id,
            provider = %provider.name,
            kid = %kid,
            "JWKS JWT validated"
        );

        Ok((identity, VerifiedJwtClaims(claims)))
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
    ) -> Result<VerifiedJwtClaims, JwtError> {
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

        let mut claims = decoded.claims;
        self.apply_claim_policy(&mut claims)?;

        debug!(
            provider = %provider_name,
            kid = %kid,
            sub = %claims.sub,
            "JWKS JWT validated via catalog provider"
        );
        Ok(VerifiedJwtClaims(claims))
    }

    /// Check if any providers are configured.
    pub fn is_configured(&self) -> bool {
        !self.providers.is_empty()
    }

    /// The `[auth.jwt]` section this registry was built from.
    ///
    /// Exposed so the post-verification gate that needs server state
    /// (`jwt_policy::enforce_stateful_jwt_policy`) reads the same config the
    /// verification pipeline does, instead of a second copy that could drift.
    pub(crate) fn jwt_config(&self) -> &JwtAuthConfig {
        &self.config
    }

    /// Apply the config-only claim policy to a token that has passed
    /// signature, route, and time validation: remap the provider's claim names
    /// onto the fields NodeDB reads, then refuse a token whose status claim
    /// carries a blocked value.
    ///
    /// Both bearer routes converge here — the HTTP static-provider path via
    /// [`Self::validate_with_claims`] and the native/OIDC catalog path via
    /// [`Self::validate_with_catalog_provider`] — so neither can skip it.
    fn apply_claim_policy(&self, claims: &mut JwtClaims) -> Result<(), JwtError> {
        crate::control::security::jwt_policy::remap_claims(&self.config.claims, claims);
        crate::control::security::jwt_policy::check_blocked_status(
            self.config.status_claim.as_deref(),
            &self.config.blocked_statuses,
            claims,
        )
    }

    /// Re-check the `exp` (and the rest of the time-claim envelope) of
    /// previously verified claims against the current clock.
    ///
    /// `exp` is validated once, inside [`Self::verify_signature_and_time`],
    /// at the moment a token is authenticated. A caller that retains a
    /// [`VerifiedJwtClaims`] beyond that single check — e.g. a native
    /// session, which keeps it for the connection's lifetime to re-derive
    /// `$auth.*` enrichment on every request — must call this once per use
    /// so a token that expires mid-connection is caught instead of being
    /// re-applied indefinitely. Reuses [`validate_time_claims`] with this
    /// registry's configured clock-skew tolerance — the exact comparison and
    /// skew allowance the original authentication used.
    pub(crate) fn check_not_expired(&self, verified: &VerifiedJwtClaims) -> Result<(), JwtError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        validate_time_claims(
            verified.claims(),
            now,
            self.config.clock_skew_secs,
            self.config.max_token_lifetime_secs,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::auth::JwtAuthConfig;
    use crate::control::security::egress::EgressPolicy;

    fn claims(iat: u64, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 999,
            roles: Vec::new(),
            exp,
            nbf: 0,
            iat,
            iss: String::new(),
            aud: Vec::new(),
            user_id: 1,
            is_superuser: false,
            extra: std::collections::HashMap::new(),
        }
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock must be after epoch")
            .as_secs()
    }

    /// A native session retains `VerifiedJwtClaims` for the connection's
    /// lifetime (see `native::session::request::handle_request`) and must
    /// re-check `exp` on every request via this method, instead of trusting
    /// the one-time check `verify_signature_and_time` ran at authentication.
    /// A session whose stored claims have since expired must be rejected.
    #[tokio::test]
    async fn check_not_expired_rejects_claims_past_exp() {
        let registry = JwksRegistry::init(JwtAuthConfig::default(), strict())
            .await
            .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        let expired = VerifiedJwtClaims(claims(now - 2_000, now - 1_000));

        assert_eq!(registry.check_not_expired(&expired), Err(JwtError::Expired));
    }

    /// A session whose stored claims have not expired keeps passing the
    /// check request after request — no regression of the claim-enrichment
    /// path this check now gates.
    #[tokio::test]
    async fn check_not_expired_accepts_claims_before_exp() {
        let registry = JwksRegistry::init(JwtAuthConfig::default(), strict())
            .await
            .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        let valid = VerifiedJwtClaims(claims(now - 10, now + 3_600));

        assert_eq!(registry.check_not_expired(&valid), Ok(()));
    }

    /// Parse an `[auth.jwt]` section exactly as the server-config loader does,
    /// so these tests prove the knobs travel from a config file into the
    /// verification pipeline — not merely that a hand-built struct works.
    fn config_from_toml(body: &str) -> JwtAuthConfig {
        let parsed: JwtAuthConfig =
            toml::from_str(body).expect("the [auth.jwt] section must deserialize");
        parsed
            .validate(&EgressPolicy::strict())
            .expect("the section must be valid");
        parsed
    }

    fn strict() -> std::sync::Arc<EgressPolicy> {
        std::sync::Arc::new(EgressPolicy::strict())
    }

    fn claims_with_extra(pairs: &[(&str, serde_json::Value)]) -> JwtClaims {
        let mut claims = claims(1, 9_999_999_999);
        for (key, value) in pairs {
            claims.extra.insert((*key).to_owned(), value.clone());
        }
        claims
    }

    /// The assertion that would have caught the original defect: a
    /// `status_claim` / `blocked_statuses` pair supplied through server config
    /// must actually reject a token carrying a blocked value.
    #[tokio::test]
    async fn blocked_status_from_server_config_rejects_the_token() {
        let registry = JwksRegistry::init(
            config_from_toml(
                r#"
            status_claim = "account_status"
            blocked_statuses = ["suspended", "banned"]
            "#,
            ),
            strict(),
        )
        .await
        .expect("registry with no configured providers must still initialize");

        let mut blocked = claims_with_extra(&[("account_status", serde_json::json!("suspended"))]);
        assert_eq!(
            registry.apply_claim_policy(&mut blocked),
            Err(JwtError::BlockedStatus)
        );

        let mut allowed = claims_with_extra(&[("account_status", serde_json::json!("active"))]);
        assert_eq!(registry.apply_claim_policy(&mut allowed), Ok(()));

        // A token that never carries the claim is not blocked.
        let mut absent = claims_with_extra(&[]);
        assert_eq!(registry.apply_claim_policy(&mut absent), Ok(()));
    }

    /// A `[auth.jwt.claims]` table supplied through server config must move a
    /// provider-named claim onto the field NodeDB reads.
    #[tokio::test]
    async fn claim_remap_from_server_config_reaches_the_verification_pipeline() {
        let registry = JwksRegistry::init(
            config_from_toml(
                r#"
            [claims]
            upn = "email"
            "#,
            ),
            strict(),
        )
        .await
        .expect("registry with no configured providers must still initialize");

        let mut remapped = claims_with_extra(&[("upn", serde_json::json!("alice@example.com"))]);
        assert_eq!(registry.apply_claim_policy(&mut remapped), Ok(()));
        assert_eq!(
            remapped.extra.get("email").and_then(|v| v.as_str()),
            Some("alice@example.com")
        );
    }

    /// Remapping runs before the status check, so an operator may block on a
    /// provider claim after renaming it onto `status`.
    #[tokio::test]
    async fn remap_feeds_the_status_check() {
        let registry = JwksRegistry::init(
            config_from_toml(
                r#"
            status_claim = "status"
            blocked_statuses = ["deactivated"]

            [claims]
            acct_state = "status"
            "#,
            ),
            strict(),
        )
        .await
        .expect("registry with no configured providers must still initialize");

        let mut claims = claims_with_extra(&[("acct_state", serde_json::json!("deactivated"))]);
        assert_eq!(
            registry.apply_claim_policy(&mut claims),
            Err(JwtError::BlockedStatus)
        );
    }

    /// `check_not_expired` must apply the registry's own configured clock
    /// skew — the same tolerance `verify_signature_and_time` used at
    /// authentication — not a hand-rolled or zero tolerance.
    #[tokio::test]
    async fn check_not_expired_honors_configured_clock_skew() {
        let registry = JwksRegistry::init(
            JwtAuthConfig {
                clock_skew_secs: 120,
                ..JwtAuthConfig::default()
            },
            strict(),
        )
        .await
        .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        // Expired 60s ago: within the 120s skew tolerance, so still accepted.
        let just_expired = VerifiedJwtClaims(claims(now - 200, now - 60));

        assert_eq!(registry.check_not_expired(&just_expired), Ok(()));
    }
}
