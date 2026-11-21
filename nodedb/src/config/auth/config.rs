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

use serde::{Deserialize, Serialize};

use super::jwt::JwtAuthConfig;
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

    /// SIEM export configuration (webhook destination, HMAC secret, buffer
    /// ceiling, flush cadence). Absent leaves the exporter unconfigured: no
    /// events are buffered and no delivery loop is spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub siem: Option<crate::control::security::siem::SiemConfig>,

    /// Adaptive-auth risk scoring configuration (signal weights, allow/deny
    /// thresholds, known-IP cache bound). Absent — and, when present, absent
    /// `enabled = true` — leaves scoring off: no request is scored,
    /// `$auth.risk_score` stays unresolvable, and no request is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<crate::control::security::risk::RiskConfig>,

    /// Auto-escalation configuration (violation thresholds, rolling window,
    /// tracked-user bound). Absent — and, when present, absent
    /// `enabled = true` — leaves escalation off: violations are still
    /// audited, but no account is suspended or banned automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<crate::control::security::escalation::EscalationConfig>,

    /// TLS enforcement configuration (minimum negotiated version, cleartext
    /// rejection). Absent — and, when present, absent `enabled = true` —
    /// leaves enforcement off: no connection is refused on transport grounds,
    /// which is what every plaintext deployment depends on. An unparseable
    /// `min_tls_version` fails startup rather than falling back to a default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_policy: Option<crate::control::security::tls_policy::TlsPolicyConfig>,

    /// Opaque session handle configuration: fingerprint binding, resolve
    /// rate-limit, miss-spike detection.
    #[serde(default)]
    pub session: SessionHandleConfig,

    /// How schema DDL is authorized: `open` (default, no check),
    /// `tenant_admin`, `permission`, `owner` or `mix`. Every node of a
    /// cluster must carry the same value.
    #[serde(default)]
    pub ddl_authorization: super::DdlAuthorizationMode,
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
            siem: None,
            risk: None,
            escalation: None,
            tls_policy: None,
            session: SessionHandleConfig::default(),
            ddl_authorization: super::DdlAuthorizationMode::default(),
        }
    }
}
