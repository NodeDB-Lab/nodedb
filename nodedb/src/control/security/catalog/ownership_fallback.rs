// SPDX-License-Identifier: BUSL-1.1

//! Resolution of the authoritative principal that receives orphaned ownership.

use super::SystemCatalog;

impl SystemCatalog {
    /// Return the designated lifecycle administrator for a persisted tenant.
    /// Legacy rows derive the historical `<tenant_name>_admin` identity.
    pub fn authoritative_tenant_admin(&self, tenant_id: u64) -> crate::Result<Option<String>> {
        Ok(self
            .load_all_tenants()?
            .into_iter()
            .find(|tenant| tenant.tenant_id == tenant_id)
            .map(|tenant| {
                if tenant.admin_username.is_empty() {
                    format!("{}_admin", tenant.name)
                } else {
                    tenant.admin_username
                }
            }))
    }

    /// Resolve a valid administrative principal for `tenant_id`, excluding the
    /// identity being removed. New tenant rows name the principal explicitly;
    /// legacy/default tenants fall back deterministically to an active tenant
    /// admin, then an active superuser in the same tenant.
    pub fn resolve_ownership_fallback(
        &self,
        tenant_id: u64,
        excluded_username: &str,
    ) -> crate::Result<Option<String>> {
        let tenant = self
            .load_all_tenants()?
            .into_iter()
            .find(|tenant| tenant.tenant_id == tenant_id);
        let mut users = self.load_all_users()?;
        users.sort_by(|left, right| {
            left.user_id
                .cmp(&right.user_id)
                .then_with(|| left.username.cmp(&right.username))
        });

        if let Some(admin_username) = tenant
            .as_ref()
            .map(|tenant| tenant.admin_username.as_str())
            .filter(|username| !username.is_empty())
        {
            return Ok(users
                .iter()
                .find(|user| {
                    user.username == admin_username
                        && user.username != excluded_username
                        && user.tenant_id == tenant_id
                        && user.is_active
                        && (user.is_superuser
                            || user.roles.iter().any(|role| role == "tenant_admin"))
                })
                .map(|user| user.username.clone()));
        }

        let tenant_admin = users.iter().find(|user| {
            user.username != excluded_username
                && user.tenant_id == tenant_id
                && user.is_active
                && user.roles.iter().any(|role| role == "tenant_admin")
        });
        if let Some(user) = tenant_admin {
            return Ok(Some(user.username.clone()));
        }

        Ok(users
            .iter()
            .find(|user| {
                user.username != excluded_username
                    && user.tenant_id == tenant_id
                    && user.is_active
                    && user.is_superuser
            })
            .map(|user| user.username.clone()))
    }
}
