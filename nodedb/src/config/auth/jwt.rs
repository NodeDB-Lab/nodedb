// SPDX-License-Identifier: BUSL-1.1

//! JWT authentication configuration (`[auth.jwt]` and `[[auth.jwt.providers]]`).

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// JWT authentication configuration.
///
/// Supports multiple identity providers (Auth0, Clerk, Keycloak, etc.),
/// each with its own JWKS endpoint and claim mapping.
///
/// ```toml
/// [auth.jwt]
/// allowed_algorithms = ["RS256", "ES256"]
///
/// [[auth.jwt.providers]]
/// name = "nodedb-auth"
/// jwks_url = "https://auth.example.com/.well-known/jwks.json"
/// issuer = "https://auth.example.com"
/// audience = "nodedb"
/// tenant_id = 42
/// ```
///
/// JWKS fetches go through the `[egress]` policy. A provider on a private
/// network needs its host on that allow-list.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtAuthConfig {
    /// JWKS refresh interval in seconds (default: 3600 = 1 hour).
    #[serde(default = "default_jwks_refresh")]
    pub jwks_refresh_secs: u64,

    /// Minimum interval between on-demand JWKS re-fetches for unknown `kid`
    /// (default: 60 seconds). Prevents abuse of unknown-kid triggering floods.
    #[serde(default = "default_jwks_min_refetch")]
    pub jwks_min_refetch_secs: u64,

    /// Allowed JWT algorithms. Tokens using other algorithms are rejected.
    /// Empty = allow RS256 + ES256 (safe defaults). `"none"` is always rejected.
    #[serde(default = "default_allowed_algorithms")]
    pub allowed_algorithms: Vec<String>,

    /// Clock skew tolerance in seconds for `exp`/`nbf`/`iat` validation.
    #[serde(default = "default_clock_skew")]
    pub clock_skew_secs: u64,

    /// Maximum accepted JWT lifetime (`exp - iat`) in seconds.
    #[serde(default = "default_max_token_lifetime")]
    pub max_token_lifetime_secs: u64,

    /// Path to cache JWKS on disk for offline fallback.
    /// If set, JWKS responses are persisted and used when providers are unreachable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_cache_path: Option<String>,

    /// Identity providers. Each has its own JWKS endpoint, issuer, and audience.
    #[serde(default)]
    pub providers: Vec<JwtProviderConfig>,

    /// Enable JIT (Just-In-Time) user provisioning from JWT claims.
    /// When true, `_system.auth_users` records are auto-created on first JWT auth.
    #[serde(default)]
    pub jit_provisioning: bool,

    /// Sync claims from JWT to `_system.auth_users` on each request.
    /// Updates email, status, roles, groups, etc. when they change in the JWT.
    /// Only has an effect while `jit_provisioning` is on — it is the sync half
    /// of the same feature. Read by
    /// `control::security::jwt_policy::provision_and_check_status`.
    #[serde(default = "default_true")]
    pub jit_sync_claims: bool,

    /// Claim mapping: renames provider-specific claim names onto the fields
    /// NodeDB reads (`email`, `groups`, `metadata`, `org_id`, `org_ids`,
    /// `permissions`, `roles`, `scope_expires`, `status`). Applied after
    /// signature, route, and time validation by
    /// `control::security::jwt_policy::remap_claims`, on both bearer routes.
    ///
    /// Entries are `"<provider claim>" = "<NodeDB field>"`. The source claim is
    /// kept alongside the copy. A source name resolves as an exact claim key
    /// first, then as a dotted path through nested claims
    /// (`"realm_access.roles"`), so a claim name containing a dot needs no
    /// escaping. A target outside the field list above, or two claims competing
    /// for one target, fails startup.
    #[serde(default)]
    pub claims: std::collections::HashMap<String, String>,

    /// Claim name for account status (e.g., "account_status", "status").
    /// If present in the JWT, its value is checked against `blocked_statuses`
    /// and a match rejects the token before any identity is issued.
    /// A token that does not carry the claim is not blocked.
    /// Resolved as an exact claim key first, then as a dotted path through
    /// nested claims.
    #[serde(default)]
    pub status_claim: Option<String>,

    /// Status values that block access (e.g., ["suspended", "banned", "deactivated"]).
    /// If the JWT status claim matches any of these — case-insensitively — the
    /// token is rejected. Enforced by
    /// `control::security::jwt_policy::check_blocked_status`.
    #[serde(default)]
    pub blocked_statuses: Vec<String>,

    /// Enforce scope declaration: reject a token whose `permissions` claim
    /// names a scope this server has never defined. Effective scope *grants*
    /// always come from the scope-grant store; this closes the gap where an
    /// undefined name still reaches `$auth.permissions` RLS predicates.
    /// Enforced by `control::security::jwt_policy::enforce_declared_scopes`.
    #[serde(default)]
    pub enforce_scopes: bool,
}

/// Configuration for a single JWT identity provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtProviderConfig {
    /// Provider name (for logging and diagnostics).
    pub name: String,

    /// JWKS endpoint URL. It must pass the `[egress]` policy: `https://`,
    /// or `http://` for a host on its allow-list.
    pub jwks_url: String,

    /// Expected `iss` claim. Empty = don't validate issuer for this provider.
    #[serde(default)]
    pub issuer: String,

    /// Expected `aud` claim. Empty = don't validate audience for this provider.
    #[serde(default)]
    pub audience: String,

    /// Tenant assigned to identities authenticated through this provider.
    ///
    /// This is deliberately required: it is a server-side binding, never a
    /// tenant selection supplied by untrusted JWT claims.
    pub tenant_id: u64,
}

impl JwtProviderConfig {
    /// Validate provider config against the `[egress]` policy. Fail-closed:
    /// empty `issuer` is rejected; `jwks_url` must pass the policy.
    pub fn validate(
        &self,
        policy: &crate::control::security::egress::EgressPolicy,
    ) -> crate::Result<()> {
        if self.name.trim().is_empty() {
            return Err(crate::Error::Config {
                detail: "auth.jwt provider must have a non-empty name".into(),
            });
        }
        if self.issuer.trim().is_empty() {
            return Err(crate::Error::Config {
                detail: format!(
                    "auth.jwt provider '{}' must set a non-empty `issuer`; \
                     empty issuer would disable issuer validation and allow \
                     cross-tenant token acceptance",
                    self.name
                ),
            });
        }
        policy
            .check_url(&self.jwks_url)
            .map_err(|e| crate::Error::Config {
                detail: format!("auth.jwt provider '{}' has unsafe jwks_url: {e}", self.name),
            })?;
        Ok(())
    }
}

impl JwtAuthConfig {
    /// Validate all providers against the `[egress]` policy. Called by the
    /// server-config loader and JWKS registry construction so
    /// misconfiguration fails before authentication.
    pub fn validate(
        &self,
        policy: &crate::control::security::egress::EgressPolicy,
    ) -> crate::Result<()> {
        if self.jwks_refresh_secs == 0 {
            return Err(crate::Error::Config {
                detail: "auth.jwt jwks_refresh_secs must be greater than zero".into(),
            });
        }
        if self.max_token_lifetime_secs == 0 {
            return Err(crate::Error::Config {
                detail: "auth.jwt max_token_lifetime_secs must be greater than zero".into(),
            });
        }
        crate::control::security::jwt_policy::validate_claim_remap(&self.claims)?;
        let mut provider_names = HashSet::with_capacity(self.providers.len());
        for provider in &self.providers {
            provider.validate(policy)?;
            if !provider_names.insert(provider.name.as_str()) {
                return Err(crate::Error::Config {
                    detail: format!(
                        "auth.jwt providers contain duplicate static provider name {:?}",
                        provider.name
                    ),
                });
            }
        }

        for (index, provider) in self.providers.iter().enumerate() {
            for other in self.providers.iter().skip(index + 1) {
                if provider.issuer != other.issuer {
                    continue;
                }

                if provider.audience.is_empty() || other.audience.is_empty() {
                    return Err(crate::Error::Config {
                        detail: format!(
                            "auth.jwt providers '{}' and '{}' share issuer '{}' but \
                             every shared-issuer route must use a distinct non-empty audience",
                            provider.name, other.name, provider.issuer
                        ),
                    });
                }
                if provider.audience == other.audience {
                    return Err(crate::Error::Config {
                        detail: format!(
                            "auth.jwt providers '{}' and '{}' duplicate the issuer/audience \
                             route ({:?}, {:?})",
                            provider.name, other.name, provider.issuer, provider.audience
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

fn default_jwks_refresh() -> u64 {
    3600
}
fn default_jwks_min_refetch() -> u64 {
    60
}
fn default_clock_skew() -> u64 {
    60
}
fn default_max_token_lifetime() -> u64 {
    86_400
}
fn default_allowed_algorithms() -> Vec<String> {
    vec!["RS256".into(), "ES256".into()]
}
fn default_true() -> bool {
    true
}

impl Default for JwtAuthConfig {
    fn default() -> Self {
        Self {
            jwks_refresh_secs: default_jwks_refresh(),
            jwks_min_refetch_secs: default_jwks_min_refetch(),
            allowed_algorithms: default_allowed_algorithms(),
            clock_skew_secs: default_clock_skew(),
            max_token_lifetime_secs: default_max_token_lifetime(),
            jwks_cache_path: None,
            providers: Vec::new(),
            jit_provisioning: false,
            jit_sync_claims: true,
            claims: std::collections::HashMap::new(),
            status_claim: None,
            blocked_statuses: Vec::new(),
            enforce_scopes: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt_section(body: &str) -> JwtAuthConfig {
        toml::from_str(body).expect("the [auth.jwt] section must deserialize")
    }

    /// The enforcement knobs must survive the config file → struct hop that
    /// the server-config loader performs; a knob that never arrives cannot be
    /// enforced no matter what the enforcement site does.
    #[test]
    fn jwt_policy_knobs_load_from_a_config_section() {
        let config = jwt_section(
            r#"
            jit_provisioning = true
            jit_sync_claims = false
            enforce_scopes = true
            status_claim = "account_status"
            blocked_statuses = ["suspended"]

            [claims]
            upn = "email"
            "#,
        );
        config
            .validate(&crate::control::security::egress::EgressPolicy::strict())
            .expect("section must be valid");

        assert!(config.jit_provisioning);
        assert!(!config.jit_sync_claims);
        assert!(config.enforce_scopes);
        assert_eq!(config.status_claim.as_deref(), Some("account_status"));
        assert_eq!(config.blocked_statuses, vec!["suspended".to_owned()]);
        assert_eq!(config.claims.get("upn").map(String::as_str), Some("email"));
    }

    #[test]
    fn claim_remap_onto_an_unread_field_fails_validation() {
        let config = jwt_section(
            r#"
            [claims]
            upn = "e_mail"
            "#,
        );
        let err = config
            .validate(&crate::control::security::egress::EgressPolicy::strict())
            .expect_err("a mapping no reader consults must fail startup");
        assert!(err.to_string().contains("unknown field"));
    }
}
