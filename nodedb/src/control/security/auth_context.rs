// SPDX-License-Identifier: BUSL-1.1

//! Session-scoped authentication context derived from JWT claims or DB identity.
//!
//! `AuthContext` is the rich session context built after authentication. It
//! carries all claims needed for RLS predicate substitution, scope checks,
//! rate-limit tier resolution, and audit attribution.
//!
//! **Lifecycle**: Built once per session/request in the Control Plane. Passed to
//! the planner for `$auth.*` substitution. Never crosses the SPSC bridge — the
//! Data Plane receives only concrete, substituted `ScanFilter` values.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::identity::{AuthMethod, AuthenticatedIdentity, roles_from_external_claims};
use super::jwt::JwtClaims;

/// Account status for access-control decisions.
///
/// Determines what actions a user can perform. Checked after authentication
/// (valid JWT/password) but before authorization (RLS, scopes).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AuthStatus {
    /// Normal access — all granted permissions apply.
    #[default]
    Active,
    /// Temporarily blocked — all requests denied except status queries.
    Suspended,
    /// Permanently blocked — all requests denied. Requires admin intervention.
    Banned,
    /// Limited access — only permissions in the user's restriction set apply.
    Restricted,
    /// Can read but not write. Used for billing holds / grace periods.
    ReadOnly,
}

impl std::fmt::Display for AuthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Suspended => write!(f, "suspended"),
            Self::Banned => write!(f, "banned"),
            Self::Restricted => write!(f, "restricted"),
            Self::ReadOnly => write!(f, "read_only"),
        }
    }
}

impl std::str::FromStr for AuthStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "suspended" => Ok(Self::Suspended),
            "banned" => Ok(Self::Banned),
            "restricted" => Ok(Self::Restricted),
            "read_only" | "readonly" => Ok(Self::ReadOnly),
            other => Err(format!("unknown auth status: '{other}'")),
        }
    }
}

/// Rich session context built from JWT claims or DB user record.
///
/// All `$auth.*` references in RLS predicates resolve against this struct.
/// The planner substitutes `$auth.id` -> `"user_7291"` at plan time so the
/// Data Plane never parses JWT or resolves session variables.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// Unique user identifier (JWT `sub` or DB user_id).
    pub id: String,
    /// Display username.
    pub username: String,
    /// Email address (if available from claims or DB).
    pub email: Option<String>,
    /// Tenant this session belongs to.
    pub tenant_id: TenantId,
    /// Current organization context (from JWT `org_id` or session SET).
    pub org_id: Option<String>,
    /// All organization memberships (from JWT `org_ids` array claim).
    pub org_ids: Vec<String>,
    /// Role names as strings (for RLS predicate matching).
    pub roles: Vec<String>,
    /// Group memberships (from JWT `groups` claim).
    pub groups: Vec<String>,
    /// Granted permissions/scopes (from JWT `permissions` or `scope` claim).
    pub permissions: Vec<String>,
    /// Account status.
    pub status: AuthStatus,
    /// Arbitrary key-value metadata from JWT or DB (plan tier, region, etc.).
    pub metadata: HashMap<String, String>,
    /// How the user authenticated.
    pub auth_method: AuthMethod,
    /// When the user last authenticated (Unix epoch seconds).
    /// Used for step-up auth: `$auth.auth_time > (now() - 15min)`.
    pub auth_time: Option<u64>,
    /// Opaque session identifier for audit correlation.
    pub session_id: String,
    /// Per-request ON DENY override: `None` = use policy default.
    /// Set via `SET LOCAL nodedb.on_deny = 'error'` (pgwire) or `X-On-Deny: error` (HTTP).
    pub on_deny_override: Option<super::deny::DenyMode>,
    /// The database this session is connected to.
    ///
    /// Populated from the session's active database at request time. Used by
    /// `$auth.database_id` RLS predicates for per-database row isolation.
    /// `None` when the session has no explicit database (cluster-level queries).
    pub database_id: Option<DatabaseId>,
}

impl AuthContext {
    /// Build `AuthContext` from validated JWT claims.
    ///
    /// Maps standard and NodeDB-specific claims to context fields.
    /// Missing optional claims default to empty/None (deny by default in RLS).
    pub fn from_jwt(claims: &JwtClaims, session_id: String) -> Self {
        let username = if claims.sub.is_empty() {
            format!("jwt_user_{}", claims.user_id)
        } else {
            claims.sub.clone()
        };

        // Extract extended claims from the `extra` map if present.
        let email = claims
            .extra
            .get("email")
            .and_then(|v| v.as_str())
            .map(String::from);
        let org_id = claims
            .extra
            .get("org_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let org_ids = extract_string_array(&claims.extra, "org_ids");
        let groups = extract_string_array(&claims.extra, "groups");
        let permissions = extract_string_array(&claims.extra, "permissions");

        let status = claims
            .extra
            .get("status")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<AuthStatus>().ok())
            .unwrap_or(AuthStatus::Active);

        let mut metadata: HashMap<String, String> = claims
            .extra
            .get("metadata")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        // Parse scope_expires claim: { "pro:all": 1735689600, "basic": 0 }
        if let Some(scope_expires) = claims
            .extra
            .get("scope_expires")
            .and_then(|v| v.as_object())
        {
            for (scope_name, ts) in scope_expires {
                if let Some(ts_val) = ts.as_u64() {
                    metadata.insert(format!("scope_expires_at.{scope_name}"), ts_val.to_string());
                }
            }
        }

        Self {
            id: if claims.user_id != 0 {
                claims.user_id.to_string()
            } else {
                claims.sub.clone()
            },
            username,
            email,
            tenant_id: TenantId::new(claims.tenant_id),
            org_id,
            org_ids,
            roles: roles_from_external_claims(&claims.roles, claims.is_superuser)
                .into_iter()
                .map(|role| role.to_string())
                .collect(),
            groups,
            permissions,
            status,
            metadata,
            auth_method: AuthMethod::ApiKey, // JWT is bearer-token variant
            auth_time: if claims.iat > 0 {
                Some(claims.iat)
            } else {
                None
            },
            session_id,
            on_deny_override: None,
            database_id: None,
        }
    }

    /// Build `AuthContext` from an already-verified JWT and its bound identity.
    ///
    /// JWT claims retain non-authoritative session detail such as organization,
    /// group, permission, and metadata fields. Identity fields that control
    /// authorization are taken from the verified identity, whose tenant and
    /// roles may be provider-bound rather than token-claim-derived.
    pub fn from_verified_jwt(
        claims: &JwtClaims,
        identity: &AuthenticatedIdentity,
        session_id: String,
    ) -> Self {
        let mut context = Self::from_jwt(claims, session_id);
        if identity.user_id != 0 {
            context.id = identity.user_id.to_string();
        }
        context.username = identity.username.clone();
        context.tenant_id = identity.tenant_id;
        context.roles = identity.roles.iter().map(ToString::to_string).collect();
        context.auth_method = identity.auth_method.clone();
        context
    }

    /// Build `AuthContext` from a DB-authenticated identity (SCRAM, password, API key).
    ///
    /// This is the fallback path when no JWT is presented. The context is
    /// populated from the `AuthenticatedIdentity` and credential store data.
    /// Extended fields (email, groups, org) are empty — they require JWT or
    /// JIT-provisioned `_system.auth_users` records.
    pub fn from_identity(identity: &AuthenticatedIdentity, session_id: String) -> Self {
        Self {
            id: identity.user_id.to_string(),
            username: identity.username.clone(),
            email: None,
            tenant_id: identity.tenant_id,
            org_id: None,
            org_ids: Vec::new(),
            roles: identity.roles.iter().map(|r| r.to_string()).collect(),
            groups: Vec::new(),
            permissions: Vec::new(),
            status: AuthStatus::Active,
            metadata: HashMap::new(),
            auth_method: identity.auth_method.clone(),
            auth_time: None,
            session_id,
            on_deny_override: None,
            database_id: None,
        }
    }

    /// Check if the account status allows the request to proceed.
    ///
    /// Returns `Ok(())` for active accounts, `Err` with reason for blocked.
    pub fn check_status(&self) -> crate::Result<()> {
        match self.status {
            AuthStatus::Active | AuthStatus::Restricted | AuthStatus::ReadOnly => Ok(()),
            AuthStatus::Suspended => Err(crate::Error::RejectedAuthz {
                tenant_id: self.tenant_id,
                resource: "account suspended".into(),
            }),
            AuthStatus::Banned => Err(crate::Error::RejectedAuthz {
                tenant_id: self.tenant_id,
                resource: "account banned".into(),
            }),
        }
    }

    /// Check if the account allows write operations.
    pub fn allows_write(&self) -> bool {
        matches!(self.status, AuthStatus::Active | AuthStatus::Restricted)
    }

    /// Resolve a `$auth.<field>` reference to its concrete value.
    ///
    /// Returns `None` if the field doesn't exist or is empty (deny by default).
    pub fn resolve_variable(&self, field: &str) -> Option<serde_json::Value> {
        match field {
            "id" => Some(serde_json::Value::String(self.id.clone())),
            "username" => Some(serde_json::Value::String(self.username.clone())),
            "email" => self
                .email
                .as_ref()
                .map(|e| serde_json::Value::String(e.clone())),
            "tenant_id" => Some(serde_json::json!(self.tenant_id.as_u64())),
            "org_id" => self
                .org_id
                .as_ref()
                .map(|o| serde_json::Value::String(o.clone())),
            "org_ids" => Some(serde_json::Value::Array(
                self.org_ids
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "roles" => Some(serde_json::Value::Array(
                self.roles
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "groups" => Some(serde_json::Value::Array(
                self.groups
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "permissions" => Some(serde_json::Value::Array(
                self.permissions
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            )),
            "status" => Some(serde_json::Value::String(self.status.to_string())),
            "auth_method" => Some(serde_json::Value::String(format!("{:?}", self.auth_method))),
            "auth_time" => self.auth_time.map(|t| serde_json::json!(t)),
            "session_id" => Some(serde_json::Value::String(self.session_id.clone())),
            "database_id" => self.database_id.map(|d| serde_json::json!(d.as_u64())),
            // Metadata sub-fields: $auth.metadata.<key>
            other if other.starts_with("metadata.") => {
                let key = &other["metadata.".len()..];
                self.metadata
                    .get(key)
                    .map(|v| serde_json::Value::String(v.clone()))
            }
            _ => None,
        }
    }

    /// Whether this context represents a superuser.
    pub fn is_superuser(&self) -> bool {
        self.roles.iter().any(|r| r == "superuser")
    }
}

/// Extract a string array from a JSON object by key.
fn extract_string_array(obj: &HashMap<String, serde_json::Value>, key: &str) -> Vec<String> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Generate a cryptographically random session ID (`s_<128-bit hex>`).
///
/// Used as `$auth.session_id` in RLS predicates and as the audit
/// correlation key — must be unguessable.
pub fn generate_session_id() -> String {
    super::random::generate_tagged_random_hex("s_")
}

#[cfg(test)]
mod tests {
    use super::super::identity::Role;
    use super::*;

    fn test_identity() -> AuthenticatedIdentity {
        use crate::control::security::identity::DatabaseSet;
        AuthenticatedIdentity {
            user_id: 42,
            username: "alice".into(),
            tenant_id: TenantId::new(1),
            auth_method: AuthMethod::ScramSha256,
            roles: vec![Role::ReadWrite],
            is_superuser: false,
            default_database: None,
            accessible_databases: DatabaseSet::Some(smallvec::smallvec![
                nodedb_types::id::DatabaseId::DEFAULT
            ]),
        }
    }

    #[test]
    fn from_identity_populates_core_fields() {
        let identity = test_identity();
        let ctx = AuthContext::from_identity(&identity, "s_test_001".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "alice");
        assert_eq!(ctx.tenant_id, TenantId::new(1));
        assert_eq!(ctx.roles, vec!["readwrite"]);
        assert_eq!(ctx.status, AuthStatus::Active);
        assert!(ctx.email.is_none());
        assert!(ctx.org_id.is_none());
        assert!(ctx.groups.is_empty());
    }

    #[test]
    fn resolve_variable_core_fields() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_002".into());

        assert_eq!(ctx.resolve_variable("id"), Some(serde_json::json!("42")));
        assert_eq!(
            ctx.resolve_variable("username"),
            Some(serde_json::json!("alice"))
        );
        assert_eq!(
            ctx.resolve_variable("tenant_id"),
            Some(serde_json::json!(1))
        );
        assert_eq!(
            ctx.resolve_variable("roles"),
            Some(serde_json::json!(["readwrite"]))
        );
        assert_eq!(
            ctx.resolve_variable("status"),
            Some(serde_json::json!("active"))
        );
    }

    #[test]
    fn resolve_variable_metadata() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_003".into());
        ctx.metadata.insert("plan".into(), "pro".into());

        assert_eq!(
            ctx.resolve_variable("metadata.plan"),
            Some(serde_json::json!("pro"))
        );
        assert_eq!(ctx.resolve_variable("metadata.missing"), None);
    }

    #[test]
    fn resolve_variable_unknown() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_004".into());
        assert_eq!(ctx.resolve_variable("nonexistent"), None);
    }

    #[test]
    fn resolve_variable_database_id_none() {
        // When no database context is stamped, $auth.database_id resolves to
        // None (fail-closed: predicates that require a database_id deny access).
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_db_none".into());
        assert_eq!(ctx.resolve_variable("database_id"), None);
    }

    #[test]
    fn resolve_variable_database_id_some() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_db_some".into());
        ctx.database_id = Some(nodedb_types::id::DatabaseId::new(42));
        assert_eq!(
            ctx.resolve_variable("database_id"),
            Some(serde_json::json!(42u64))
        );
    }

    #[test]
    fn check_status_active_ok() {
        let ctx = AuthContext::from_identity(&test_identity(), "s_test_005".into());
        assert!(ctx.check_status().is_ok());
    }

    #[test]
    fn check_status_suspended_err() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_006".into());
        ctx.status = AuthStatus::Suspended;
        assert!(ctx.check_status().is_err());
    }

    #[test]
    fn check_status_banned_err() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_007".into());
        ctx.status = AuthStatus::Banned;
        assert!(ctx.check_status().is_err());
    }

    #[test]
    fn allows_write_by_status() {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test_008".into());
        assert!(ctx.allows_write()); // Active

        ctx.status = AuthStatus::ReadOnly;
        assert!(!ctx.allows_write());

        ctx.status = AuthStatus::Restricted;
        assert!(ctx.allows_write());
    }

    #[test]
    fn auth_status_display_roundtrip() {
        for status in [
            AuthStatus::Active,
            AuthStatus::Suspended,
            AuthStatus::Banned,
            AuthStatus::Restricted,
            AuthStatus::ReadOnly,
        ] {
            let s = status.to_string();
            let parsed: AuthStatus = s.parse().unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn session_id_generation_unique() {
        let id1 = generate_session_id();
        let id2 = generate_session_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("s_"));
    }

    /// Sanity check that `generate_session_id` uses the CSPRNG helper and
    /// carries the `s_` tag. Entropy / leak / enumerability guarantees are
    /// tested on the shared helper in `super::random`.
    #[test]
    fn session_id_uses_shared_csprng_helper_with_s_prefix() {
        let id = generate_session_id();
        assert!(id.starts_with("s_"));
        let rest = id.strip_prefix("s_").unwrap();
        assert_eq!(rest.len(), 32, "expected 128-bit (32 hex char) payload");
        assert!(rest.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn from_verified_jwt_uses_identity_for_authorization_fields() {
        let claims = JwtClaims {
            sub: "claim-user".into(),
            tenant_id: 999,
            roles: vec!["readonly".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "provider".into(),
            aud: "nodedb".into(),
            user_id: 7,
            is_superuser: false,
            extra: HashMap::from([
                ("groups".into(), serde_json::json!(["engineering"])),
                ("metadata".into(), serde_json::json!({"region": "us-west"})),
            ]),
        };
        let mut identity = test_identity();
        identity.user_id = 42;
        identity.username = "provider-user".into();
        identity.tenant_id = TenantId::new(1);
        identity.auth_method = AuthMethod::OidcBearer;
        identity.roles = vec![Role::TenantAdmin];

        let ctx = AuthContext::from_verified_jwt(&claims, &identity, "s_jwt_bound".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "provider-user");
        assert_eq!(ctx.tenant_id, TenantId::new(1));
        assert_eq!(ctx.roles, vec!["tenant_admin"]);
        assert_eq!(ctx.auth_method, AuthMethod::OidcBearer);
        assert_eq!(ctx.groups, vec!["engineering"]);
        assert_eq!(ctx.metadata.get("region"), Some(&"us-west".into()));
    }

    #[test]
    fn from_verified_jwt_uses_distinct_subject_ids_when_identity_id_is_zero() {
        let first_claims = JwtClaims {
            sub: "oidc-subject-alice".into(),
            tenant_id: 999,
            roles: vec!["readonly".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "https://issuer.example".into(),
            aud: "nodedb".into(),
            user_id: 0,
            is_superuser: false,
            extra: HashMap::new(),
        };
        let mut second_claims = first_claims.clone();
        second_claims.sub = "oidc-subject-bob".into();

        let mut identity = test_identity();
        identity.user_id = 0;
        identity.auth_method = AuthMethod::OidcBearer;

        let first = AuthContext::from_verified_jwt(&first_claims, &identity, "s_oidc_1".into());
        let second = AuthContext::from_verified_jwt(&second_claims, &identity, "s_oidc_2".into());

        assert_eq!(first.id, first_claims.sub);
        assert_eq!(second.id, second_claims.sub);
        assert_ne!(first.id, second.id);
        assert_ne!(first.id, "0");
        assert_eq!(first.tenant_id, identity.tenant_id);
        assert_eq!(first.username, identity.username);
        assert_eq!(first.roles, vec!["readwrite"]);
        assert_eq!(first.auth_method, AuthMethod::OidcBearer);
    }

    #[test]
    fn from_jwt_removes_externally_asserted_superuser_authority() {
        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["superuser".into(), "readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 0,
            iss: "nodedb-auth".into(),
            aud: "nodedb".into(),
            user_id: 42,
            is_superuser: true,
            extra: HashMap::new(),
        };

        let context = AuthContext::from_jwt(&claims, "s_jwt_roles".into());

        assert!(!context.is_superuser());
        assert_eq!(context.roles, vec!["readwrite"]);
    }

    #[test]
    fn from_jwt_populates_extended_fields() {
        let mut extra = HashMap::new();
        extra.insert("email".into(), serde_json::json!("alice@example.com"));
        extra.insert("org_id".into(), serde_json::json!("org_acme"));
        extra.insert(
            "org_ids".into(),
            serde_json::json!(["org_acme", "org_beta"]),
        );
        extra.insert("groups".into(), serde_json::json!(["engineering", "leads"]));
        extra.insert(
            "permissions".into(),
            serde_json::json!(["profile:read", "data:write"]),
        );
        extra.insert("status".into(), serde_json::json!("active"));
        extra.insert(
            "metadata".into(),
            serde_json::json!({"plan": "enterprise", "region": "us-west"}),
        );

        let claims = JwtClaims {
            sub: "alice".into(),
            tenant_id: 1,
            roles: vec!["readwrite".into()],
            exp: 9_999_999_999,
            nbf: 0,
            iat: 1_700_000_000,
            iss: "nodedb-auth".into(),
            aud: "nodedb".into(),
            user_id: 42,
            is_superuser: false,
            extra,
        };

        let ctx = AuthContext::from_jwt(&claims, "s_jwt_001".into());

        assert_eq!(ctx.id, "42");
        assert_eq!(ctx.username, "alice");
        assert_eq!(ctx.email, Some("alice@example.com".into()));
        assert_eq!(ctx.org_id, Some("org_acme".into()));
        assert_eq!(ctx.org_ids, vec!["org_acme", "org_beta"]);
        assert_eq!(ctx.groups, vec!["engineering", "leads"]);
        assert_eq!(ctx.permissions, vec!["profile:read", "data:write"]);
        assert_eq!(ctx.auth_time, Some(1_700_000_000));
        assert_eq!(ctx.metadata.get("plan"), Some(&"enterprise".into()));
        assert_eq!(ctx.metadata.get("region"), Some(&"us-west".into()));
    }
}
