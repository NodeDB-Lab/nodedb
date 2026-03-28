use serde::{Deserialize, Serialize};

/// Authentication mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// No authentication. Development/testing only.
    Trust,
    /// Username + password (SCRAM-SHA-256 over pgwire, cleartext over HTTP).
    Password,
    /// Username + password (MD5 over pgwire). For legacy PostgreSQL clients.
    Md5Password,
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

    /// Maximum connections per user (0 = unlimited).
    pub max_connections_per_user: u32,

    /// Password expiry in days (0 = no expiry).
    /// When set, users must change their password before it expires.
    /// Expired passwords are rejected at SCRAM auth time.
    pub password_expiry_days: u32,

    /// Audit retention in days (0 = keep forever).
    /// Entries older than this are pruned during periodic flush.
    pub audit_retention_days: u32,

    /// JWT authentication configuration (JWKS providers, algorithms, etc.).
    /// If not present, JWT auth is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwt: Option<JwtAuthConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Trust,
            superuser_name: "admin".into(),
            superuser_password: None,
            min_password_length: 8,
            max_failed_logins: 5,
            lockout_duration_secs: 300,
            idle_timeout_secs: 3600,
            max_connections_per_user: 0,
            password_expiry_days: 0,
            audit_retention_days: 0,
            jwt: None,
        }
    }
}

impl AuthConfig {
    /// Resolve the superuser password from config or environment variable.
    /// Returns None in trust mode (no password needed).
    pub fn resolve_superuser_password(&self) -> crate::Result<Option<String>> {
        if self.mode == AuthMode::Trust {
            return Ok(None);
        }

        // Check env var first (higher priority, avoids storing in config file).
        if let Ok(env_pw) = std::env::var("NODEDB_SUPERUSER_PASSWORD")
            && !env_pw.is_empty()
        {
            return Ok(Some(env_pw));
        }

        // Fall back to config file.
        if let Some(ref pw) = self.superuser_password
            && !pw.is_empty()
        {
            return Ok(Some(pw.clone()));
        }

        Err(crate::Error::Config {
            detail: format!(
                "auth mode is '{:?}' but no superuser password provided. \
                 Set 'auth.superuser_password' in config or NODEDB_SUPERUSER_PASSWORD env var.",
                self.mode
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_trust() {
        let cfg = AuthConfig::default();
        assert_eq!(cfg.mode, AuthMode::Trust);
    }

    #[test]
    fn trust_mode_no_password_needed() {
        let cfg = AuthConfig {
            mode: AuthMode::Trust,
            ..Default::default()
        };
        let result = cfg.resolve_superuser_password();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn password_mode_requires_password() {
        let cfg = AuthConfig {
            mode: AuthMode::Password,
            superuser_password: None,
            ..Default::default()
        };
        // Clear env var if set.
        // SAFETY: This test is single-threaded and no other thread reads this var.
        unsafe { std::env::remove_var("NODEDB_SUPERUSER_PASSWORD") };
        let result = cfg.resolve_superuser_password();
        assert!(result.is_err());
    }

    #[test]
    fn password_from_config() {
        let cfg = AuthConfig {
            mode: AuthMode::Password,
            superuser_password: Some("secret123".into()),
            ..Default::default()
        };
        let pw = cfg.resolve_superuser_password().unwrap();
        assert_eq!(pw, Some("secret123".into()));
    }

    #[test]
    fn toml_roundtrip() {
        let cfg = AuthConfig::default();
        let toml_str = toml::to_string_pretty(&cfg).unwrap();
        let parsed: AuthConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.mode, cfg.mode);
        assert_eq!(parsed.superuser_name, cfg.superuser_name);
    }
}
