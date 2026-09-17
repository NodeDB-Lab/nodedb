// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for RLS policy `CatalogEntry` variants.
//!
//! After the synchronous `apply::rls` step has written the redb row,
//! this rehydrates the runtime `RlsPolicy` (compiling the stored `USING`
//! text against the collection's declared columns) and installs it into
//! the in-memory `RlsPolicyStore` on every node so the read/write
//! evaluators see the new policy on their next request. A row that cannot
//! be compiled installs as a restrictive deny-all
//! (`StoredRlsPolicy::rehydrate`).

use std::sync::Arc;

use crate::control::security::catalog::StoredRlsPolicy;
use crate::control::state::SharedState;

pub fn put(stored: StoredRlsPolicy, shared: Arc<SharedState>) {
    let runtime = stored.rehydrate(shared.credentials.catalog());
    shared.rls.install_replicated_policy(runtime);
    tracing::debug!(
        policy = %stored.name,
        collection = %stored.collection,
        tenant = stored.tenant_id,
        "post_apply: RLS policy replicated"
    );
}

pub fn delete(tenant_id: u64, collection: String, name: String, shared: Arc<SharedState>) {
    let removed = shared
        .rls
        .install_replicated_drop_policy(tenant_id, &collection, &name);
    tracing::debug!(
        policy = %name,
        collection = %collection,
        tenant = tenant_id,
        removed,
        "post_apply: RLS policy drop replicated"
    );
}
