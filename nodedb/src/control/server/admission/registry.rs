// SPDX-License-Identifier: BUSL-1.1

//! Per-database and per-tenant connection semaphore registries.
//!
//! The registry lazily creates a `tokio::sync::Semaphore` for each database
//! (keyed by `DatabaseId`) and each tenant within a database
//! (keyed by `(DatabaseId, TenantId)`) on first quota configuration.
//! Semaphore capacity comes from the `max_connections` field of the matching
//! `QuotaRecord`; zero means "no cap", and the entry stays only while live
//! connections still hold permits.
//!
//! Cap changes resize the semaphore in place. The `Arc<Semaphore>` a live
//! permit holds is never replaced, so the live count stays exact.
//!
//! All operations take a read lock on the fast path (admission) and a short
//! write lock when a cap changes.
//!
//! ## Lock-poisoning policy
//!
//! The `RwLock`-guarded maps store `Arc<Semaphore>` handles. Map updates
//! are single insertions; they cannot leave a partially-constructed
//! invariant if a different thread panics. We therefore recover poisoned
//! locks via `unwrap_or_else(|p| p.into_inner())` rather than propagate
//! the poison — keeping admission live across an unrelated panic is
//! strictly better than failing every future connection until restart.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tracing::debug;

use nodedb_types::{DatabaseId, TenantId};

/// Reason a connection was rejected at admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    /// The target database has exhausted its `max_connections` quota.
    DatabaseCapExhausted { db: DatabaseId, limit: u32 },
    /// The tenant has exhausted its `max_connections` quota within the database.
    TenantCapExhausted {
        db: DatabaseId,
        tenant: TenantId,
        limit: u32,
    },
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseCapExhausted { db, limit } => {
                write!(
                    f,
                    "database {db:?} has reached its maximum connection limit ({limit})"
                )
            }
            Self::TenantCapExhausted { db, tenant, limit } => {
                write!(
                    f,
                    "tenant {tenant:?} in database {db:?} has reached its maximum \
                     connection limit ({limit})"
                )
            }
        }
    }
}

impl std::error::Error for AdmissionError {}

/// Permits a cap configures. An uncapped scope holds `Semaphore::MAX_PERMITS`.
fn capacity_of(limit: Option<u32>) -> usize {
    match limit {
        Some(limit) => limit as usize,
        None => Semaphore::MAX_PERMITS,
    }
}

/// Entry in a connection-limit map, shared by the database and tenant layers.
///
/// The `Arc<Semaphore>` outlives every cap change, so permits taken under an
/// older cap still count against the new one.
struct LimitEntry {
    semaphore: Arc<Semaphore>,
    /// Configured cap. `None` means uncapped but still counted.
    limit: Option<u32>,
    /// Permits owed back to a shrink that could not complete immediately.
    pending_shrink: AtomicU32,
}

impl LimitEntry {
    fn new(limit: Option<u32>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity_of(limit))),
            limit,
            pending_shrink: AtomicU32::new(0),
        }
    }

    /// Move the semaphore to `limit` without replacing it.
    ///
    /// Lowering a cap below the live count cannot evict connections. The
    /// excess becomes `pending_shrink` and takes effect as they close.
    fn resize(&mut self, limit: Option<u32>) {
        // `&mut self` is exclusive here (held under the map's write lock), so
        // a plain read-and-reset avoids an unneeded atomic RMW.
        let owed = std::mem::replace(self.pending_shrink.get_mut(), 0) as usize;
        let current = capacity_of(self.limit).saturating_add(owed);
        let target = capacity_of(limit);
        if target > current {
            self.semaphore.add_permits(target - current);
        } else {
            self.forget_up_to(current - target);
        }
        self.limit = limit;
    }

    /// Absorb outstanding debt with permits freed since the last attempt.
    fn settle_shrink(&self) {
        let owed = self.pending_shrink.swap(0, Ordering::AcqRel) as usize;
        self.forget_up_to(owed);
    }

    /// Forget up to `amount` permits, recording whatever could not be forgotten.
    fn forget_up_to(&self, amount: usize) {
        if amount == 0 {
            return;
        }
        let forgot = self.semaphore.forget_permits(amount);
        self.record_debt(amount - forgot);
    }

    /// Return unabsorbed debt to the counter, saturating at `u32::MAX`.
    fn record_debt(&self, owed: usize) {
        if owed == 0 {
            return;
        }
        let owed = u32::try_from(owed).unwrap_or(u32::MAX);
        let _ = self
            .pending_shrink
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(owed))
            });
    }

    /// Permits held by live connections, cap or no cap.
    fn live(&self) -> u32 {
        let total = capacity_of(self.limit)
            .saturating_add(self.pending_shrink.load(Ordering::Relaxed) as usize);
        let live = total.saturating_sub(self.semaphore.available_permits());
        u32::try_from(live).unwrap_or(u32::MAX)
    }
}

/// Apply `limit` to `key` in place. `limit = 0` drops the cap.
fn set_limit<K: Eq + Hash>(map: &mut HashMap<K, LimitEntry>, key: K, limit: u32) {
    if limit == 0 {
        clear_limit(map, &key);
        return;
    }
    match map.get_mut(&key) {
        Some(entry) => entry.resize(Some(limit)),
        None => {
            map.insert(key, LimitEntry::new(Some(limit)));
        }
    }
}

/// Drop the cap for `key`. The entry survives while a live permit holds its
/// semaphore; the write lock excludes readers, so `strong_count` is exact.
fn clear_limit<K: Eq + Hash>(map: &mut HashMap<K, LimitEntry>, key: &K) {
    let idle = map
        .get(key)
        .is_some_and(|entry| Arc::strong_count(&entry.semaphore) == 1);
    if idle {
        map.remove(key);
    } else if let Some(entry) = map.get_mut(key) {
        entry.resize(None);
    }
}

/// Registry of per-database and per-tenant connection semaphores.
///
/// Created once at server startup and shared (via `Arc`) with every
/// `Listener` instance. `set_database_limit` / `set_tenant_limit` resize an
/// existing scope in place, so a quota change never resets the live count.
pub struct AdmissionRegistry {
    db_semaphores: RwLock<HashMap<DatabaseId, LimitEntry>>,
    tenant_semaphores: RwLock<HashMap<(DatabaseId, TenantId), LimitEntry>>,
}

impl AdmissionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            db_semaphores: RwLock::new(HashMap::new()),
            tenant_semaphores: RwLock::new(HashMap::new()),
        }
    }

    // ── Quota setters ─────────────────────────────────────────────────────────

    /// Configure the maximum connections for a database.
    ///
    /// `limit = 0` removes the cap; live connections stay counted.
    pub fn set_database_limit(&self, db: DatabaseId, limit: u32) {
        let mut map = self
            .db_semaphores
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_limit(&mut map, db, limit);
    }

    /// Configure the maximum connections for a tenant within a database.
    ///
    /// `limit = 0` removes the cap; live connections stay counted.
    pub fn set_tenant_limit(&self, db: DatabaseId, tenant: TenantId, limit: u32) {
        let mut map = self
            .tenant_semaphores
            .write()
            .unwrap_or_else(|p| p.into_inner());
        set_limit(&mut map, (db, tenant), limit);
    }

    /// Remove every connection entry of a dropped database, tenants included.
    /// A dropped scope is never re-capped, so live permits keep no entry alive.
    pub fn clear_database_scope(&self, db: DatabaseId) {
        {
            let mut map = self
                .db_semaphores
                .write()
                .unwrap_or_else(|p| p.into_inner());
            map.remove(&db);
        }
        let mut map = self
            .tenant_semaphores
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(entry_db, _), _| *entry_db != db);
    }

    /// Remove a dropped tenant's connection entries in every database.
    /// A dropped scope is never re-capped, so live permits keep no entry alive.
    pub fn clear_tenant_scope(&self, tenant: TenantId) {
        let mut map = self
            .tenant_semaphores
            .write()
            .unwrap_or_else(|p| p.into_inner());
        map.retain(|(_, entry_tenant), _| *entry_tenant != tenant);
    }

    // ── Live counts ───────────────────────────────────────────────────────────

    /// Connections holding a database permit. `None` when the database has no
    /// entry.
    pub fn database_live_connections(&self, db: DatabaseId) -> Option<u32> {
        let map = self.db_semaphores.read().unwrap_or_else(|p| p.into_inner());
        map.get(&db).map(LimitEntry::live)
    }

    /// Connections holding a tenant permit. `None` when the tenant has no
    /// entry.
    pub fn tenant_live_connections(&self, db: DatabaseId, tenant: TenantId) -> Option<u32> {
        let map = self
            .tenant_semaphores
            .read()
            .unwrap_or_else(|p| p.into_inner());
        map.get(&(db, tenant)).map(LimitEntry::live)
    }

    // ── Admission ─────────────────────────────────────────────────────────────

    /// Attempt to acquire a database-level permit for a new connection.
    ///
    /// Returns `Ok(Some(permit))` if an entry exists and a slot was acquired,
    /// `Ok(None)` if the database has no entry, or `Err(AdmissionError)` if the
    /// database is at capacity.
    pub fn try_acquire_database(
        &self,
        db: DatabaseId,
    ) -> Result<Option<OwnedSemaphorePermit>, AdmissionError> {
        let has_entry = {
            let map = self.db_semaphores.read().unwrap_or_else(|p| p.into_inner());
            map.contains_key(&db)
        };
        if !has_entry {
            let mut map = self
                .db_semaphores
                .write()
                .unwrap_or_else(|p| p.into_inner());
            map.entry(db).or_insert_with(|| LimitEntry::new(None));
        }
        let map = self.db_semaphores.read().unwrap_or_else(|p| p.into_inner());
        let permit = try_acquire_entry(map.get(&db), |limit| {
            AdmissionError::DatabaseCapExhausted { db, limit }
        })?;
        if permit.is_some() {
            debug!(db = ?db, "database admission permit acquired");
        }
        Ok(permit)
    }

    /// Attempt to acquire a tenant-level permit for a new connection.
    ///
    /// Returns `Ok(Some(permit))` if an entry exists and a slot was acquired,
    /// `Ok(None)` if the tenant has no entry, or `Err(AdmissionError)` if the
    /// tenant is at capacity.
    pub fn try_acquire_tenant(
        &self,
        db: DatabaseId,
        tenant: TenantId,
    ) -> Result<Option<OwnedSemaphorePermit>, AdmissionError> {
        let map = self
            .tenant_semaphores
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let permit = try_acquire_entry(map.get(&(db, tenant)), |limit| {
            AdmissionError::TenantCapExhausted { db, tenant, limit }
        })?;
        if permit.is_some() {
            debug!(db = ?db, tenant = ?tenant, "tenant admission permit acquired");
        }
        Ok(permit)
    }
}

/// Shared acquire path for a single `LimitEntry`. `on_exhausted` builds the
/// scope-specific error from the configured cap.
fn try_acquire_entry(
    entry: Option<&LimitEntry>,
    on_exhausted: impl FnOnce(u32) -> AdmissionError,
) -> Result<Option<OwnedSemaphorePermit>, AdmissionError> {
    let Some(entry) = entry else {
        return Ok(None); // No entry configured.
    };
    entry.settle_shrink();
    match entry.semaphore.clone().try_acquire_owned() {
        Ok(permit) => Ok(Some(permit)),
        Err(TryAcquireError::NoPermits) => match entry.limit {
            Some(limit) => Err(on_exhausted(limit)),
            // An uncapped entry holds `Semaphore::MAX_PERMITS`.
            None => Ok(None),
        },
        // Semaphore was closed (registry teardown) — treat as no-limit.
        Err(TryAcquireError::Closed) => Ok(None),
    }
}

impl Default for AdmissionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for AdmissionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let db_count = self.db_semaphores.read().map(|m| m.len()).unwrap_or(0);
        let tenant_count = self.tenant_semaphores.read().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("AdmissionRegistry")
            .field("db_entries", &db_count)
            .field("tenant_entries", &tenant_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::{AdmissionError, AdmissionRegistry};

    fn db(n: u64) -> DatabaseId {
        DatabaseId::new(n)
    }

    fn tenant(n: u64) -> TenantId {
        TenantId::new(n)
    }

    // ── Database cap tests ────────────────────────────────────────────────────

    #[test]
    fn no_database_cap_allows_unlimited() {
        let reg = AdmissionRegistry::new();
        // An uncapped database admits and tracks live connections.
        let p = reg.try_acquire_database(db(0)).unwrap();
        assert!(p.is_some());
        assert_eq!(reg.database_live_connections(db(0)), Some(1));
        drop(p);
        assert_eq!(reg.database_live_connections(db(0)), Some(0));
    }

    #[test]
    fn database_cap_allows_up_to_limit() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(0), 2);

        let p1 = reg.try_acquire_database(db(0)).unwrap();
        let p2 = reg.try_acquire_database(db(0)).unwrap();
        assert!(p1.is_some());
        assert!(p2.is_some());

        // Third attempt must fail.
        let err = reg.try_acquire_database(db(0)).unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::DatabaseCapExhausted { limit: 2, .. }
        ));

        // Drop one permit — the slot is released.
        drop(p1);
        let p3 = reg.try_acquire_database(db(0)).unwrap();
        assert!(p3.is_some());
    }

    #[test]
    fn separate_databases_do_not_share_a_cap() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(1), 1);
        reg.set_database_limit(db(2), 1);

        let _a = reg.try_acquire_database(db(1)).unwrap().unwrap();
        assert!(reg.try_acquire_database(db(1)).is_err());

        let b = reg.try_acquire_database(db(2)).unwrap();
        assert!(b.is_some());
        assert_eq!(reg.database_live_connections(db(1)), Some(1));
        assert_eq!(reg.database_live_connections(db(2)), Some(1));
    }

    // ── Scope teardown tests ──────────────────────────────────────────────────

    #[test]
    fn clear_database_scope_drops_the_database_and_its_tenants() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(1), 2);
        reg.set_tenant_limit(db(1), tenant(1), 2);
        reg.set_database_limit(db(2), 2);
        reg.set_tenant_limit(db(2), tenant(1), 2);
        let _held = reg.try_acquire_database(db(1)).unwrap().unwrap();

        reg.clear_database_scope(db(1));

        assert_eq!(reg.database_live_connections(db(1)), None);
        assert_eq!(reg.tenant_live_connections(db(1), tenant(1)), None);
        assert_eq!(reg.database_live_connections(db(2)), Some(0));
        assert_eq!(reg.tenant_live_connections(db(2), tenant(1)), Some(0));
    }

    #[test]
    fn clear_tenant_scope_drops_that_tenant_in_every_database() {
        let reg = AdmissionRegistry::new();
        reg.set_tenant_limit(db(1), tenant(1), 2);
        reg.set_tenant_limit(db(2), tenant(1), 2);
        reg.set_tenant_limit(db(1), tenant(2), 2);

        reg.clear_tenant_scope(tenant(1));

        assert_eq!(reg.tenant_live_connections(db(1), tenant(1)), None);
        assert_eq!(reg.tenant_live_connections(db(2), tenant(1)), None);
        assert_eq!(reg.tenant_live_connections(db(1), tenant(2)), Some(0));
    }

    // ── Tenant cap tests ──────────────────────────────────────────────────────

    #[test]
    fn tenant_cap_isolates_tenants() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(0), 100); // generous DB cap
        reg.set_tenant_limit(db(0), tenant(1), 1);

        // T1 gets its single slot.
        let t1_permit = reg.try_acquire_tenant(db(0), tenant(1)).unwrap();
        assert!(t1_permit.is_some());

        // T1 is now at capacity.
        let err = reg.try_acquire_tenant(db(0), tenant(1)).unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::TenantCapExhausted { limit: 1, .. }
        ));

        // T2 (different tenant) is unaffected.
        let t2_permit = reg.try_acquire_tenant(db(0), tenant(2)).unwrap();
        assert!(t2_permit.is_none()); // T2 has no entry configured → None
    }

    // ── Resize tests ──────────────────────────────────────────────────────────

    #[test]
    fn raising_a_live_limit_keeps_existing_permits_counted() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(7), 2);
        let _p1 = reg.try_acquire_database(db(7)).unwrap().unwrap();
        let _p2 = reg.try_acquire_database(db(7)).unwrap().unwrap();

        reg.set_database_limit(db(7), 4);
        assert_eq!(reg.database_live_connections(db(7)), Some(2));

        let _p3 = reg.try_acquire_database(db(7)).unwrap().unwrap();
        let _p4 = reg.try_acquire_database(db(7)).unwrap().unwrap();
        let err = reg.try_acquire_database(db(7)).unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::DatabaseCapExhausted { limit: 4, .. }
        ));
    }

    #[test]
    fn raising_a_live_tenant_limit_keeps_existing_permits_counted() {
        let reg = AdmissionRegistry::new();
        reg.set_tenant_limit(db(7), tenant(3), 2);
        let _p1 = reg.try_acquire_tenant(db(7), tenant(3)).unwrap().unwrap();
        let _p2 = reg.try_acquire_tenant(db(7), tenant(3)).unwrap().unwrap();

        reg.set_tenant_limit(db(7), tenant(3), 4);
        assert_eq!(reg.tenant_live_connections(db(7), tenant(3)), Some(2));

        let _p3 = reg.try_acquire_tenant(db(7), tenant(3)).unwrap().unwrap();
        let _p4 = reg.try_acquire_tenant(db(7), tenant(3)).unwrap().unwrap();
        let err = reg.try_acquire_tenant(db(7), tenant(3)).unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::TenantCapExhausted { limit: 4, .. }
        ));
    }

    #[test]
    fn lowering_a_limit_below_the_live_count_settles_as_connections_close() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(9), 5);
        let mut permits = Vec::new();
        for _ in 0..5 {
            permits.push(reg.try_acquire_database(db(9)).unwrap().unwrap());
        }

        reg.set_database_limit(db(9), 2);
        assert!(reg.try_acquire_database(db(9)).is_err());

        // Four connections close: one live, cap two → exactly one slot free.
        permits.truncate(1);
        let _p = reg.try_acquire_database(db(9)).unwrap().unwrap();
        assert_eq!(reg.database_live_connections(db(9)), Some(2));
        assert!(reg.try_acquire_database(db(9)).is_err());
    }

    #[test]
    fn clearing_a_limit_keeps_counting_live_connections() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(4), 2);
        let _p1 = reg.try_acquire_database(db(4)).unwrap().unwrap();
        let _p2 = reg.try_acquire_database(db(4)).unwrap().unwrap();

        // Uncapped acquisitions still take a permit.
        reg.set_database_limit(db(4), 0);
        let _p3 = reg.try_acquire_database(db(4)).unwrap().unwrap();
        assert_eq!(reg.database_live_connections(db(4)), Some(3));

        // Re-applying a cap counts the three live connections against it.
        reg.set_database_limit(db(4), 5);
        assert_eq!(reg.database_live_connections(db(4)), Some(3));
        let _p4 = reg.try_acquire_database(db(4)).unwrap().unwrap();
        let _p5 = reg.try_acquire_database(db(4)).unwrap().unwrap();
        let err = reg.try_acquire_database(db(4)).unwrap_err();
        assert!(matches!(
            err,
            AdmissionError::DatabaseCapExhausted { limit: 5, .. }
        ));
    }

    #[test]
    fn clearing_a_limit_with_no_live_permits_drops_the_entry() {
        let reg = AdmissionRegistry::new();
        reg.set_database_limit(db(5), 1);
        reg.set_database_limit(db(5), 0);

        assert_eq!(reg.database_live_connections(db(5)), None);
        assert!(reg.try_acquire_database(db(5)).unwrap().is_some());
    }

    #[test]
    fn clearing_a_tenant_limit_with_no_live_permits_drops_the_entry() {
        let reg = AdmissionRegistry::new();
        reg.set_tenant_limit(db(5), tenant(8), 1);
        reg.set_tenant_limit(db(5), tenant(8), 0);

        assert_eq!(reg.tenant_live_connections(db(5), tenant(8)), None);
        assert!(reg.try_acquire_tenant(db(5), tenant(8)).unwrap().is_none());
    }
}
