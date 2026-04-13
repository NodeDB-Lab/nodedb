//! Post-apply side effects for tenant `CatalogEntry` variants.
//!
//! `PutTenant` seeds the in-memory `TenantStore` with default quota
//! so reads work immediately on every node. Quota replication itself
//! is not part of `StoredTenant` — that is a separate concern.
//!
//! `DeleteTenant` removes the in-memory quota entry. Tenant data is
//! left in place — operators must call `PURGE TENANT <id> CONFIRM`
//! separately if they want it gone.

use std::sync::Arc;

use crate::control::security::catalog::StoredTenant;
use crate::control::security::tenant::TenantQuota;
use crate::control::state::SharedState;
use crate::types::TenantId;

pub fn put(stored: StoredTenant, shared: Arc<SharedState>) {
    let tid = TenantId::new(stored.tenant_id);
    let mut tenants = match shared.tenants.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    // Only seed default quota if the tenant has no quota yet — never
    // clobber an `ALTER TENANT SET QUOTA` value the operator already set.
    if !tenants.has_quota(tid) {
        tenants.set_quota(tid, TenantQuota::default());
    }
    tracing::debug!(
        tenant = stored.tenant_id,
        name = %stored.name,
        "post_apply: tenant identity replicated"
    );
}

pub fn delete(tenant_id: u32, shared: Arc<SharedState>) {
    let tid = TenantId::new(tenant_id);
    let mut tenants = match shared.tenants.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    tenants.remove_quota(tid);
    tracing::debug!(tenant = tenant_id, "post_apply: tenant identity removed");
}
