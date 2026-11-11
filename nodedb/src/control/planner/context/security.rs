// SPDX-License-Identifier: BUSL-1.1

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;

/// Security context for query planning — bundles identity + permission stores.
///
/// Used by `plan_sql_with_rls` to check EXECUTE permissions on user UDFs
/// and inject RLS predicates.
pub struct PlanSecurityContext<'a> {
    pub identity: &'a AuthenticatedIdentity,
    pub auth: &'a crate::control::security::auth_context::AuthContext,
    pub rls_store: &'a crate::control::security::rls::RlsPolicyStore,
    /// Redaction policy registry, consulted to refuse plans whose results the
    /// result-path masking hook cannot rewrite (aggregates over a redacted
    /// column, graph traversals).
    pub redaction_store: &'a crate::control::security::redaction::RedactionStore,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    /// Where planning reads the permission tree cache for hierarchical ACL
    /// injection.
    pub permission_tree: PermissionTreeSource<'a>,
}

impl PlanSecurityContext<'_> {
    /// This context with `auth` in place of its own auth context. A request
    /// of several statements plans each statement under its own `ON DENY`
    /// override this way.
    pub fn with_auth<'b>(
        &'b self,
        auth: &'b crate::control::security::auth_context::AuthContext,
    ) -> PlanSecurityContext<'b> {
        PlanSecurityContext {
            identity: self.identity,
            auth,
            rls_store: self.rls_store,
            redaction_store: self.redaction_store,
            permissions: self.permissions,
            roles: self.roles,
            permission_tree: self.permission_tree,
        }
    }
}

/// Where planning reads the permission tree cache for hierarchical ACL
/// injection.
#[derive(Clone, Copy)]
pub enum PermissionTreeSource<'a> {
    /// Skip permission tree filtering (e.g., internal queries).
    None,
    /// The node's live permission tree cache. The caller runs the
    /// authorization fence first (`auth_fence::admit_permission_view`).
    /// Planning takes the read lock only after its last await, so no lock is
    /// held across a request to a surrogate's collection home.
    Live(&'a tokio::sync::RwLock<crate::control::security::permission_tree::PermissionCache>),
}

impl<'a> PermissionTreeSource<'a> {
    /// The permission tree cache to inject from, read-locked. `None` when
    /// this source skips permission tree filtering.
    pub async fn read(
        self,
    ) -> Option<
        tokio::sync::RwLockReadGuard<
            'a,
            crate::control::security::permission_tree::PermissionCache,
        >,
    > {
        match self {
            Self::None => None,
            Self::Live(cache) => Some(cache.read().await),
        }
    }
}
