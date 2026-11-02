// SPDX-License-Identifier: BUSL-1.1

//! Resolution of the authoritative principal that receives orphaned ownership.

use super::SystemCatalog;
use super::auth_types::StoredTenant;

/// The admin a stored tenant row names. `CREATE TENANT` always names one, so
/// an empty name is a corrupt row.
fn named_admin(tenant: &StoredTenant) -> crate::Result<&str> {
    if tenant.admin_username.is_empty() {
        return Err(crate::Error::Storage {
            engine: "catalog".into(),
            detail: format!("tenant {} row names no admin", tenant.tenant_id),
        });
    }
    Ok(&tenant.admin_username)
}

impl SystemCatalog {
    /// Return the designated lifecycle administrator for a persisted tenant.
    pub fn authoritative_tenant_admin(&self, tenant_id: u64) -> crate::Result<Option<String>> {
        let Some(tenant) = self
            .load_all_tenants()?
            .into_iter()
            .find(|tenant| tenant.tenant_id == tenant_id)
        else {
            return Ok(None);
        };
        Ok(Some(named_admin(&tenant)?.to_string()))
    }

    /// Resolve a valid administrative principal for `tenant_id`, excluding the
    /// identity being removed. A stored tenant row names the principal
    /// explicitly. A tenant with no stored row falls back deterministically to
    /// an active tenant admin, then an active superuser in the same tenant.
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

        if let Some(tenant) = tenant.as_ref() {
            let admin_username = named_admin(tenant)?;
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
