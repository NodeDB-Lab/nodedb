// SPDX-License-Identifier: BUSL-1.1

//! Core authentication configuration types.
//!
//! `Argon2Config` is **cluster-wide**. There is intentionally no
//! per-database override: a `DatabaseDescriptor` does not carry password
//! / Argon2 parameters, and downstream code must not branch hashing
//! parameters on `database_id`. Hash verification and rehash-on-login
//! both read this single config; per-database tuning would require
//! versioning the hash format and a migration path that does not yet
//! exist. If a per-database override is ever added, it must thread
//! through every site that constructs an `argon2::Argon2` instance —
//! a wide ripple, not a field on the descriptor.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::session::SessionHandleConfig;

// ── OWASP Argon2id 2024+ minimum recommended parameters ──────────────────────
// https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
// m=19456 KiB / t=2 / p=1 is the stated OWASP minimum for Argon2id.
// NodeDB ships with those minimums as defaults. Operators may increase them;
// decreasing below these values is their responsibility.

fn default_argon2_memory_kib() -> u32 {
    19_456
}
fn default_argon2_time_cost() -> u32 {
    2
}
fn default_argon2_parallelism() -> u32 {
    1
}
fn default_argon2_output_len() -> usize {
    32
}

/// Argon2id hashing parameters.
///
/// Defaults follow OWASP Argon2id 2024+ guidance (m=19456 KiB / t=2 / p=1).
///
/// **Upgrade rule**: on successful login, the stored hash is transparently
/// rehashed if *any* stored parameter is strictly weaker than the configured
/// one. If the stored hash is *stronger* (operator tuned the dial down), the
/// hash is left unchanged — no silent downgrade.
///
/// **Existing config files**: all fields have serde defaults so existing files
/// that omit `[auth.argon2]` continue to load and use the OWASP defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Argon2Config {
    /// Memory cost in KiB. OWASP minimum: 19456 (19 MiB).
    #[serde(default = "default_argon2_memory_kib")]
    pub memory_kib: u32,
    /// Number of iterations (time cost). OWASP minimum: 2.
    #[serde(default = "default_argon2_time_cost")]
    pub time_cost: u32,
    /// Degree of parallelism (lanes). OWASP minimum: 1.
    #[serde(default = "default_argon2_parallelism")]
    pub parallelism: u32,
    /// Output length in bytes. 32 bytes = 256-bit key material.
    #[serde(default = "default_argon2_output_len")]
    pub output_len: usize,
}

impl Default for Argon2Config {
    fn default() -> Self {
        Self {
            memory_kib: default_argon2_memory_kib(),
            time_cost: default_argon2_time_cost(),
            parallelism: default_argon2_parallelism(),
            output_len: default_argon2_output_len(),
        }
    }
}

/// Authentication mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// No authentication. Development/testing only.
    Trust,
    /// Username + password (SCRAM-SHA-256 over pgwire, cleartext over HTTP).
    Password,
    /// mTLS client certificate authentication.
    Certificate,
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Clock skew tolerance in seconds for `exp`/`nbf` validation.
    #[serde(default = "default_clock_skew")]
    pub clock_skew_secs: u64,

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
    /// Updates email, roles, groups, etc. when they change in the JWT.
    #[serde(default = "default_true")]
    pub jit_sync_claims: bool,

    /// Claim mapping: maps provider-specific claim names to NodeDB fields.
    #[serde(default)]
    pub claims: std::collections::HashMap<String, String>,

    /// Claim name for account status (e.g., "account_status", "status").
    /// If present in the JWT, its value is checked against `blocked_statuses`.
    #[serde(default)]
    pub status_claim: Option<String>,

    /// Status values that block access (e.g., ["suspended", "banned", "deactivated"]).
    /// If the JWT status claim matches any of these, the request is denied.
    #[serde(default)]
    pub blocked_statuses: Vec<String>,

    /// Enforce scope validation: reject unknown scopes from JWT `permissions` claim.
    /// When true, JWT tokens with permissions not matching defined scopes are denied.
    #[serde(default)]
    pub enforce_scopes: bool,

    /// SSRF relaxation: allow `http://` scheme for JWKS URLs whose host
    /// is in [`Self::allow_jwks_hosts`]. Off by default.
    #[serde(default)]
    pub allow_http_jwks: bool,

    /// SSRF relaxation: hostnames that may resolve to addresses inside
    /// [`Self::allow_jwks_cidrs`]. Exact-match, lowercase. IP literals
    /// remain forbidden regardless of this list.
    #[serde(default)]
    pub allow_jwks_hosts: Vec<String>,

    /// SSRF relaxation: CIDR ranges that [`Self::allow_jwks_hosts`] are
    /// permitted to resolve into, in addition to global unicast.
    /// Example: `["10.42.0.0/16"]` for an in-cluster Keycloak.
    #[serde(default)]
    pub allow_jwks_cidrs: Vec<String>,
}

/// Configuration for a single JWT identity provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtProviderConfig {
    /// Provider name (for logging and diagnostics).
    pub name: String,

    /// JWKS endpoint URL. Must be HTTPS in production.
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
    /// Validate provider config against a [`JwksPolicy`]. Fail-closed:
    /// empty `issuer` is rejected; `jwks_url` must pass the policy.
    pub fn validate(
        &self,
        policy: &crate::control::security::jwks::url::JwksPolicy,
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
    /// Build the effective [`JwksPolicy`] from the allow-list fields.
    pub fn jwks_policy(
        &self,
    ) -> Result<
        crate::control::security::jwks::url::JwksPolicy,
        crate::control::security::jwks::url::UrlValidationError,
    > {
        crate::control::security::jwks::url::JwksPolicy::from_parts(
            self.allow_http_jwks,
            &self.allow_jwks_hosts,
            &self.allow_jwks_cidrs,
        )
    }

    /// Validate all providers. Called by the server-config loader and JWKS
    /// registry construction so misconfiguration fails before authentication.
    pub fn validate(&self) -> crate::Result<()> {
        let policy = self.jwks_policy().map_err(|e| crate::Error::Config {
            detail: format!("auth.jwt allow-list is invalid: {e}"),
        })?;
        let mut provider_names = HashSet::with_capacity(self.providers.len());
        for provider in &self.providers {
            provider.validate(&policy)?;
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
            jwks_cache_path: None,
            providers: Vec::new(),
            jit_provisioning: false,
            jit_sync_claims: true,
            claims: std::collections::HashMap::new(),
            status_claim: None,
            blocked_statuses: Vec::new(),
            enforce_scopes: false,
            allow_http_jwks: false,
            allow_jwks_hosts: Vec::new(),
            allow_jwks_cidrs: Vec::new(),
        }
    }
}

/// Authentication and authorization configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication mode.
    pub mode: AuthMode,

    /// Superuser username (used on first-run bootstrap).
    pub superuser_name: String,

    /// Superuser password. Prefer `NODEDB_SUPERUSER_PASSWORD` env var over this field —
    /// passwords in config files risk exposure in logs, backups, and version control.
    /// If neither env var nor this field is set and mode is not "trust", startup fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superuser_password: Option<String>,

    /// Minimum password length for new users.
    pub min_password_length: usize,

    /// Maximum consecutive failed logins before lockout.
    pub max_failed_logins: u32,

    /// Lockout duration in seconds after max failed logins.
    pub lockout_duration_secs: u64,

    /// Idle session timeout in seconds (0 = no timeout).
    pub idle_timeout_secs: u64,

    /// Absolute session lifetime in seconds (0 = disabled).
    /// When set, a session is forcibly closed after this many seconds
    /// regardless of activity (SQLSTATE 57P01). HTTP is stateless — N/A.
    #[serde(default)]
    pub session_absolute_timeout_secs: u64,

    /// Maximum connections per user (0 = unlimited).
    pub max_connections_per_user: u32,

    /// Password expiry in days (0 = no expiry).
    /// When set, users must change their password before it expires.
    /// Expired passwords are rejected at SCRAM auth time.
    pub password_expiry_days: u32,

    /// Grace period after password expiry during which login is still allowed
    /// but a warning is emitted (0 = hard cutoff, no grace).
    #[serde(default)]
    pub password_expiry_grace_days: u32,

    /// Audit retention in days (0 = keep forever).
    /// Entries older than this are pruned during periodic flush.
    pub audit_retention_days: u32,

    /// Maximum total audit entries to retain in the catalog (0 = unlimited).
    /// When the catalog exceeds this count, the oldest entries are pruned
    /// at flush time. Age-based pruning (`audit_retention_days`) runs first,
    /// then count-based pruning trims to this ceiling.
    #[serde(default)]
    pub audit_max_entries: u64,

    /// Argon2id hashing parameters used for new hashes and rehash decisions.
    /// Existing config files that omit this section use the OWASP defaults.
    #[serde(default)]
    pub argon2: Argon2Config,

    /// JWT authentication configuration (JWKS providers, algorithms, etc.).
    /// If not present, JWT auth is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<JwtAuthConfig>,

    /// Rate limiting configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<crate::control::security::ratelimit::config::RateLimitConfig>,

    /// Usage metering configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metering: Option<crate::control::security::metering::config::MeteringConfig>,

    /// Opaque session handle configuration: fingerprint binding, resolve
    /// rate-limit, miss-spike detection.
    #[serde(default)]
    pub session: SessionHandleConfig,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Password,
            superuser_name: "nodedb".into(),
            superuser_password: None,
            min_password_length: 8,
            max_failed_logins: 5,
            lockout_duration_secs: 300,
            idle_timeout_secs: 3600,
            session_absolute_timeout_secs: 0,
            max_connections_per_user: 0,
            password_expiry_days: 0,
            password_expiry_grace_days: 0,
            audit_retention_days: 0,
            audit_max_entries: 0,
            argon2: Argon2Config::default(),
            jwt: None,
            rate_limit: None,
            metering: None,
            session: SessionHandleConfig::default(),
        }
    }
}
