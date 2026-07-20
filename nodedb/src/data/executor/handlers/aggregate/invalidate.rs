// SPDX-License-Identifier: BUSL-1.1

//! Shared aggregate-cache invalidation sweep, used by every write path that
//! changes row counts for a collection (PUT, point DELETE, bulk DELETE,
//! TRUNCATE, columnar insert) and therefore must not leave a stale
//! `COUNT(*)` / `GROUP BY` result cached.
//!
//! Keyed on `(tenant, "{collection}\0...")` — every cached entry for a given
//! collection shares that null-byte-terminated prefix (see
//! `aggregate_cache_key` in `cache_key.rs`), so a prefix sweep evicts exactly
//! that collection's entries without touching other collections' cached
//! aggregates for the same tenant.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Evict every cached aggregate result for `(tid, collection)`.
    ///
    /// Callers gate this on "did this operation actually change the
    /// collection's rows" (e.g. `prior.is_some()`, `affected > 0`) — the
    /// sweep itself is unconditional once called.
    pub(in crate::data::executor) fn invalidate_aggregate_cache_for_collection(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) {
        let database_key = crate::types::DatabaseId::new(database_id);
        let tid_key = crate::types::TenantId::new(tid);
        let coll_prefix = format!("{collection}\0");
        self.aggregate_cache.retain(|(d, t, rest), _| {
            !(*d == database_key && *t == tid_key && rest.starts_with(&coll_prefix))
        });
    }
}
