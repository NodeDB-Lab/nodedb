//! CP-side helper that turns a `(collection, pk_bytes)` into a stable
//! `Surrogate`, allocating from the registry on the first call and
//! returning the persisted value on every subsequent call (UPSERT
//! preserves the surrogate).
//!
//! Cross-cutting flush trigger: every successful allocation runs the
//! registry's `should_flush()` check; if true, we persist the new
//! high-watermark to both the catalog row (`_system.surrogate_hwm`)
//! and the WAL (`SurrogateAlloc` record) before returning. The two
//! writes form one logical checkpoint — if either fails we surface
//! the error to the caller rather than silently letting the registry
//! advance past a non-durable hwm.

use std::sync::{Arc, RwLock};

use nodedb_types::Surrogate;

use super::persist::SurrogateHwmPersist;
use super::registry::SurrogateRegistry;
use super::wal_appender::SurrogateWalAppender;
use crate::control::security::catalog::SystemCatalog;

/// Shared handle to the surrogate registry. Lives on `SharedState`
/// and is cloned (cheaply) into every CP path that allocates
/// surrogates.
///
/// The inner `RwLock` is held only for the duration of one
/// `assign_surrogate` call (write lock) — the registry's hot-path
/// `alloc_one` uses atomics, so the lock is uncontended.
pub type SurrogateRegistryHandle = Arc<RwLock<SurrogateRegistry>>;

/// CP-side surrogate assigner. Bundles the registry, the catalog,
/// and the WAL appender so call sites only need to pass
/// `(collection, pk_bytes)`.
pub struct SurrogateAssigner<'a> {
    registry: &'a SurrogateRegistryHandle,
    catalog: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
}

impl<'a> SurrogateAssigner<'a> {
    pub fn new(
        registry: &'a SurrogateRegistryHandle,
        catalog: &'a SystemCatalog,
        wal_appender: &'a dyn SurrogateWalAppender,
    ) -> Self {
        Self {
            registry,
            catalog,
            wal_appender,
        }
    }

    /// Resolve `(collection, pk_bytes)` to a stable surrogate.
    ///
    /// - If a binding already exists, return it (no allocation, no flush).
    /// - Else: allocate one surrogate, persist the binding, and check
    ///   the registry's flush threshold; flush durably if tripped.
    ///
    /// Allocation + catalog write happen inside one critical section
    /// on the registry write-lock so the registry hwm and the
    /// persisted PK row cannot diverge under concurrent assigners.
    pub fn assign(&self, collection: &str, pk_bytes: &[u8]) -> crate::Result<Surrogate> {
        // Fast-path: existing binding. Done under a read lock — most
        // production calls land here once the per-collection working
        // set has been observed.
        if let Some(s) = self.catalog.get_surrogate_for_pk(collection, pk_bytes)? {
            return Ok(s);
        }

        // Slow path: allocate + persist + maybe flush. The write lock
        // guards the (allocate, write-pk-row) pair so two concurrent
        // assigners can't both observe "missing", both allocate, and
        // both write — the second would silently overwrite the
        // first's binding with a different surrogate.
        let registry = self.registry.write().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })?;
        // Re-check inside the lock: another assigner may have raced
        // us between the read above and the lock acquisition.
        if let Some(s) = self.catalog.get_surrogate_for_pk(collection, pk_bytes)? {
            return Ok(s);
        }
        let surrogate = registry.alloc_one()?;
        self.catalog
            .put_surrogate(collection, pk_bytes, surrogate)?;

        // Flush trigger: durably checkpoint the new hwm if either the
        // ops or elapsed-time threshold has tripped. Both writes are
        // idempotent so a crash between them on a re-run replays
        // cleanly.
        if registry.should_flush() {
            let combined = CombinedPersist {
                catalog: self.catalog,
                wal_appender: self.wal_appender,
            };
            registry.flush(&combined)?;
        }

        Ok(surrogate)
    }
}

/// `SurrogateHwmPersist` impl that writes the catalog row AND emits
/// the WAL record on every checkpoint. Used by `SurrogateAssigner`
/// to satisfy the registry's flush boundary.
struct CombinedPersist<'a> {
    catalog: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)?;
        self.wal_appender.record_alloc_to_wal(hwm)?;
        Ok(())
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::surrogate::wal_appender::NoopWalAppender;

    fn open_test() -> (
        tempfile::TempDir,
        SystemCatalog,
        SurrogateRegistryHandle,
        NoopWalAppender,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        (dir, cat, reg, NoopWalAppender)
    }

    #[test]
    fn assign_is_idempotent_for_same_pk() {
        let (_dir, cat, reg, wal) = open_test();
        let a = SurrogateAssigner::new(&reg, &cat, &wal);
        let s1 = a.assign("users", b"alice").unwrap();
        let s2 = a.assign("users", b"alice").unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
    }

    #[test]
    fn assign_distinct_pks_returns_distinct_surrogates() {
        let (_dir, cat, reg, wal) = open_test();
        let a = SurrogateAssigner::new(&reg, &cat, &wal);
        let s1 = a.assign("users", b"alice").unwrap();
        let s2 = a.assign("users", b"bob").unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_writes_reverse_binding() {
        let (_dir, cat, reg, wal) = open_test();
        let a = SurrogateAssigner::new(&reg, &cat, &wal);
        let s = a.assign("users", b"alice").unwrap();
        assert_eq!(
            cat.get_pk_for_surrogate("users", s).unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn assign_persists_hwm_at_flush_threshold() {
        let (_dir, cat, reg, wal) = open_test();
        let a = SurrogateAssigner::new(&reg, &cat, &wal);
        // Allocate just up to and across the 1024 ops threshold.
        let n = super::super::registry::FLUSH_OPS_THRESHOLD as usize;
        for i in 0..n {
            let pk = format!("u{i}");
            let _ = a.assign("users", pk.as_bytes()).unwrap();
        }
        // Either threshold (1024 ops or 200 ms elapsed) may fire
        // first; assert only that the catalog persisted *some*
        // checkpoint inside the (0, n] band.
        let persisted = cat.get_surrogate_hwm().unwrap();
        assert!(persisted > 0 && persisted <= n as u32);
    }
}
