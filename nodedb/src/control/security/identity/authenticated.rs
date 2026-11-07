// SPDX-License-Identifier: BUSL-1.1

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_types::id::DatabaseId;

use crate::types::TenantId;

use super::database_set::DatabaseSet;
use super::role::Role;

/// A verified identity bound to a session after authentication.
///
/// This is the single source of truth for "who is this connection?"
/// Created during auth handshake, immutable for the session lifetime.
/// Tenant ID comes from here — never from client payload.
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    /// Unique user identifier.
    pub user_id: u64,
    /// Username (for display, logging, audit).
    pub username: String,
    /// Tenant this user belongs to.
    ///
    /// Single-tenant per user; the database is the multi-axis.
    /// Cross-tenant access requires separate user accounts per tenant, or superuser.
    /// No code path branches on "user belongs to multiple tenants" —
    /// the single-tenant invariant holds throughout the codebase.
    pub tenant_id: TenantId,
    /// How the user authenticated.
    pub auth_method: AuthMethod,
    /// Assigned roles.
    pub roles: Vec<Role>,
    /// Server-issued privilege authority. Private so external claims and
    /// transport code cannot assert or mutate superuser state.
    authority: IdentityAuthority,
    /// Per-user default database. `None` means fall through to tenant default,
    /// then `DatabaseId::DEFAULT`.
    ///
    /// Set via `ALTER USER <name> SET DEFAULT DATABASE <db>` and stored in
    /// the credential store alongside the user record.
    pub default_database: Option<DatabaseId>,
    /// Which databases this identity may access.
    ///
    /// Superusers carry `DatabaseSet::All`. Regular users start with
    /// `DatabaseSet::Some([DatabaseId::DEFAULT])` and gain additional entries
    /// via `GRANT … ON DATABASE …`. Session bind rejects `current_database`
    /// values not in this set with `ACCESS_DENIED`.
    pub accessible_databases: DatabaseSet,
    /// The API key this identity authenticated with. Revoking the key ends the
    /// sessions it opened, and a session rebuilds its identity through the key.
    pub api_key_id: Option<String>,
    /// When a bearer token's session ends, in milliseconds since the epoch, or
    /// `None` for an identity no token bounds. The value is the first instant the
    /// per-request expiry check refuses the token, clock skew included.
    pub token_expiry_ms: Option<u64>,
}

/// Read-only privilege view exposed through `Deref` for compatibility with
/// authorization checks. There is deliberately no `DerefMut` implementation.
#[derive(Debug, Clone)]
pub struct IdentityAuthority {
    pub is_superuser: bool,
    /// Whether this identity is server-owned work (triggers, Raft apply,
    /// CRDT sync, scheduler, WAL replay) rather than an external client.
    ///
    /// This is deliberately *not* derived from `auth_method == AuthMethod::Trust`.
    /// `new_internal_service` sets `AuthMethod::Trust`, but so do
    /// `trust_identity` and `configured_trust_identity`
    /// (`control/server/session_auth/identity.rs`) for real external clients
    /// connecting under trust-auth mode. Exempting on `auth_method == Trust`
    /// would silently exempt every trust-mode external client from blacklist
    /// and rate-limit enforcement — a security hole. Only the
    /// `new_internal_service` constructor may set this `true`.
    pub is_internal_service: bool,
}

impl std::ops::Deref for AuthenticatedIdentity {
    type Target = IdentityAuthority;

    fn deref(&self) -> &Self::Target {
        &self.authority
    }
}

/// Server-owned principal material loaded from the credential catalog.
pub(crate) struct CatalogPrincipal {
    pub(crate) user_id: u64,
    pub(crate) username: String,
    pub(crate) tenant_id: TenantId,
    pub(crate) auth_method: AuthMethod,
    pub(crate) roles: Vec<Role>,
    pub(crate) is_superuser: bool,
    pub(crate) default_database: Option<DatabaseId>,
    pub(crate) accessible_databases: DatabaseSet,
}

impl AuthenticatedIdentity {
    /// Construct a regular identity. Superuser role strings are discarded;
    /// only the credential catalog or a named internal-service constructor may
    /// create superuser authority.
    pub fn new_regular(
        user_id: u64,
        username: impl Into<String>,
        tenant_id: TenantId,
        auth_method: AuthMethod,
        mut roles: Vec<Role>,
        default_database: Option<DatabaseId>,
        accessible_databases: DatabaseSet,
    ) -> Self {
        roles.retain(|role| !matches!(role, Role::Superuser));
        Self {
            user_id,
            username: username.into(),
            tenant_id,
            auth_method,
            roles,
            authority: IdentityAuthority {
                is_superuser: false,
                is_internal_service: false,
            },
            default_database,
            accessible_databases,
            api_key_id: None,
            token_expiry_ms: None,
        }
    }

    /// Name the API key this identity authenticated with.
    pub(crate) fn with_api_key_id(mut self, key_id: String) -> Self {
        self.api_key_id = Some(key_id);
        self
    }

    /// Bound this identity by its bearer token's expiry.
    pub(crate) fn with_token_expiry_ms(mut self, expiry_ms: u64) -> Self {
        self.token_expiry_ms = Some(expiry_ms);
        self
    }

    /// Construct an identity from a NodeDB credential-catalog record.
    pub(crate) fn from_catalog_principal(principal: CatalogPrincipal) -> Self {
        Self {
            user_id: principal.user_id,
            username: principal.username,
            tenant_id: principal.tenant_id,
            auth_method: principal.auth_method,
            roles: principal.roles,
            authority: IdentityAuthority {
                is_superuser: principal.is_superuser,
                is_internal_service: false,
            },
            default_database: principal.default_database,
            accessible_databases: principal.accessible_databases,
            api_key_id: None,
            token_expiry_ms: None,
        }
    }

    /// Construct a trusted internal service identity.
    ///
    /// This crate-private path is reserved for replay, triggers, schedulers,
    /// and other server-owned work that has no external claims.
    pub(crate) fn new_internal_service(
        user_id: u64,
        username: impl Into<String>,
        tenant_id: TenantId,
        roles: Vec<Role>,
        is_superuser: bool,
        default_database: Option<DatabaseId>,
        accessible_databases: DatabaseSet,
    ) -> Self {
        Self {
            user_id,
            username: username.into(),
            tenant_id,
            auth_method: AuthMethod::Trust,
            roles,
            authority: IdentityAuthority {
                is_superuser,
                is_internal_service: true,
            },
            default_database,
            accessible_databases,
            api_key_id: None,
            token_expiry_ms: None,
        }
    }

    /// Whether this server-issued identity has catalog/internal superuser authority.
    pub fn is_superuser(&self) -> bool {
        self.authority.is_superuser
    }

    /// Whether this identity represents server-owned work (triggers, Raft
    /// apply, CRDT sync, scheduler, replay) rather than an external client.
    ///
    /// Unforgeable the same way `is_superuser` is: the flag lives on the
    /// private `IdentityAuthority`, exposed only through this read-only
    /// accessor, and only [`Self::new_internal_service`] can set it `true`.
    /// See [`IdentityAuthority::is_internal_service`] for why this must not
    /// be derived from `auth_method == AuthMethod::Trust`.
    pub fn is_internal_service(&self) -> bool {
        self.authority.is_internal_service
    }

    /// Check if this identity has a specific role.
    pub fn has_role(&self, role: &Role) -> bool {
        self.authority.is_superuser || self.roles.contains(role)
    }

    /// The names of the roles this identity holds, as a write carries them to
    /// every replica that judges a role-guarded state transition.
    pub fn role_names(&self) -> Vec<String> {
        self.roles.iter().map(|role| role.to_string()).collect()
    }

    /// Check if this identity has any of the specified roles.
    pub fn has_any_role(&self, roles: &[Role]) -> bool {
        self.authority.is_superuser || roles.iter().any(|r| self.roles.contains(r))
    }

    /// Returns `true` if this identity is Superuser or carries `Role::ClusterAdmin`.
    pub fn has_cluster_admin(&self) -> bool {
        self.authority.is_superuser || self.roles.iter().any(|r| matches!(r, Role::ClusterAdmin))
    }

    /// Returns `true` if this identity is the owner of `db` (or is Superuser).
    pub fn is_database_owner(&self, db: DatabaseId) -> bool {
        self.authority.is_superuser
            || self
                .roles
                .iter()
                .any(|r| matches!(r, Role::DatabaseOwner(d) if *d == db))
    }

    /// Returns `true` if this identity may access the given database.
    ///
    /// Superusers always return `true`. Regular users return `true` only if
    /// the database is in `accessible_databases`. This is enforced at session
    /// bind — the session is rejected with `ACCESS_DENIED` if the resolved
    /// `current_database` fails this check.
    pub fn can_access_database(&self, db: DatabaseId) -> bool {
        self.authority.is_superuser || self.accessible_databases.contains(db)
    }

    /// Derive the appropriate `DatabaseSet` for a superuser identity.
    ///
    /// Superusers receive `DatabaseSet::All`; regular users start with
    /// `DatabaseSet::Some([DatabaseId::DEFAULT])`.
    pub fn default_database_set(is_superuser: bool) -> DatabaseSet {
        if is_superuser {
            DatabaseSet::All
        } else {
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT])
        }
    }
}

/// How the client proved their identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// SCRAM-SHA-256 via pgwire.
    ScramSha256,
    /// Cleartext password (dev/testing only).
    CleartextPassword,
    /// API key (bearer token).
    ApiKey,
    /// mTLS client certificate.
    Certificate,
    /// Trust mode (no authentication — dev only).
    Trust,
    /// OIDC bearer token (native / HTTP clients only; NOT pgwire).
    OidcBearer,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity(roles: Vec<Role>, superuser: bool) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "test",
            TenantId::new(1),
            roles,
            superuser,
            None,
            AuthenticatedIdentity::default_database_set(superuser),
        )
    }

    #[test]
    fn superuser_has_all_roles() {
        let id = test_identity(vec![], true);
        assert!(id.has_role(&Role::ReadOnly));
        assert!(id.has_role(&Role::TenantAdmin));
        assert!(id.has_role(&Role::Custom("anything".into())));
    }

    #[test]
    fn readonly_only_has_readonly() {
        let id = test_identity(vec![Role::ReadOnly], false);
        assert!(id.has_role(&Role::ReadOnly));
        assert!(!id.has_role(&Role::ReadWrite));
        assert!(!id.has_role(&Role::TenantAdmin));
    }

    #[test]
    fn superuser_can_access_any_database() {
        let id = test_identity(vec![], true);
        assert!(id.can_access_database(DatabaseId::new(99)));
    }

    #[test]
    fn regular_user_only_default_database() {
        let id = test_identity(vec![Role::ReadOnly], false);
        assert!(id.can_access_database(DatabaseId::DEFAULT));
        assert!(!id.can_access_database(DatabaseId::new(99)));
    }
}
