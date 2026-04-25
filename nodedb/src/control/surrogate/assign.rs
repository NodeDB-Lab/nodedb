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

use std::sync::{Arc, RwLock, Weak};

use nodedb_types::Surrogate;

use super::persist::SurrogateHwmPersist;
use super::registry::SurrogateRegistry;
use super::wal_appender::SurrogateWalAppender;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Shared handle to the surrogate registry. Lives on `SharedState`
/// and is cloned (cheaply) into every CP path that allocates
/// surrogates.
///
/// The inner `RwLock` is held only for the duration of one
/// `assign_surrogate` call (write lock) — the registry's hot-path
/// `alloc_one` uses atomics, so the lock is uncontended.
pub type SurrogateRegistryHandle = Arc<RwLock<SurrogateRegistry>>;

/// CP-side surrogate assigner. Owning shape — bundles the registry,
/// the credential store (which exposes the catalog), and the WAL
/// appender so call sites only need to pass `(collection, pk_bytes)`.
///
/// Stored as `Arc<SurrogateAssigner>` on `SharedState`.
pub struct SurrogateAssigner {
    registry: SurrogateRegistryHandle,
    credential_store: Arc<CredentialStore>,
    wal_appender: Arc<dyn SurrogateWalAppender>,
    /// Weak handle to SharedState for Raft-mediated HWM proposals.
    /// Set after SharedState construction to break the Arc cycle.
    /// When set and a Raft cluster is active, the flush path proposes
    /// `MetadataEntry::SurrogateAlloc { hwm }` in addition to the
    /// local WAL record so all followers advance their HWM.
    shared: std::sync::OnceLock<Weak<SharedState>>,
}

impl SurrogateAssigner {
    pub fn new(
        registry: SurrogateRegistryHandle,
        credential_store: Arc<CredentialStore>,
        wal_appender: Arc<dyn SurrogateWalAppender>,
    ) -> Self {
        Self {
            registry,
            credential_store,
            wal_appender,
            shared: std::sync::OnceLock::new(),
        }
    }

    /// Install a weak SharedState handle so the flush path can
    /// propose to Raft when in cluster mode. Called by `start_raft`
    /// after SharedState is fully wired.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Resolve `(collection, pk_bytes)` to a stable surrogate.
    ///
    /// - If the credential store has no catalog (in-memory test fixture),
    ///   returns `Surrogate::ZERO`. Production state always wires a
    ///   redb-backed `CredentialStore::open` so this branch never fires.
    /// - If a binding already exists, return it (no allocation, no flush).
    /// - Else: allocate one surrogate, persist the binding, and check
    ///   the registry's flush threshold; flush durably if tripped.
    ///
    /// Allocation + catalog write happen inside one critical section
    /// on the registry write-lock so the registry hwm and the
    /// persisted PK row cannot diverge under concurrent assigners.
    pub fn assign(&self, collection: &str, pk_bytes: &[u8]) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Surrogate::ZERO),
        };

        // Fast-path: existing binding. Done under a read lock — most
        // production calls land here once the per-collection working
        // set has been observed.
        if let Some(s) = catalog.get_surrogate_for_pk(collection, pk_bytes)? {
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
        if let Some(s) = catalog.get_surrogate_for_pk(collection, pk_bytes)? {
            return Ok(s);
        }
        let surrogate = registry.alloc_one()?;
        catalog.put_surrogate(collection, pk_bytes, surrogate)?;
        // Emit a durable WAL bind before the lock releases. Order is
        // load-bearing: a crash between catalog write and bind append
        // is invisible (the catalog row is already on disk via redb's
        // own WAL); a crash before the catalog write leaves nothing
        // to recover; a crash between bind append and lock release is
        // recovered by replaying the bind into the catalog (idempotent
        // via the two-table overwrite).
        self.wal_appender
            .record_bind_to_wal(surrogate.as_u32(), collection, pk_bytes)?;

        // Flush trigger: durably checkpoint the new hwm if either the
        // ops or elapsed-time threshold has tripped. Both writes are
        // idempotent so a crash between them on a re-run replays
        // cleanly.
        if registry.should_flush() {
            let raft_shared = self.shared.get().and_then(|w| w.upgrade());
            let combined = CombinedPersist {
                catalog,
                wal_appender: self.wal_appender.as_ref(),
                raft_shared: raft_shared.as_deref(),
            };
            registry.flush(&combined)?;
        }

        Ok(surrogate)
    }

    /// Read-only lookup: return the surrogate previously bound to
    /// `(collection, pk_bytes)` without ever allocating or writing.
    /// Used by point-read/update/delete planning where a missing
    /// binding means the row does not exist (semantic no-op).
    ///
    /// When the credential store has no catalog (in-memory test
    /// fixture), returns `Some(Surrogate::ZERO)` — mirroring the
    /// `Surrogate::ZERO` allocation `assign` performs in the same
    /// catalog-less mode, so a write/read pair against an unwired
    /// catalog still resolves to the same identity.
    pub fn lookup(&self, collection: &str, pk_bytes: &[u8]) -> crate::Result<Option<Surrogate>> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Some(Surrogate::ZERO)),
        };
        catalog.get_surrogate_for_pk(collection, pk_bytes)
    }

    /// Expose the registry handle for read access by the Raft applier.
    ///
    /// The returned `Arc<RwLock<SurrogateRegistry>>` is used by
    /// `MetadataCommitApplier` to call `restore_hwm` when a
    /// `SurrogateAlloc` entry commits on a follower.
    pub fn registry_handle(&self) -> &SurrogateRegistryHandle {
        &self.registry
    }

    /// Allocate a fresh surrogate for an entity that has no user-facing
    /// primary key (e.g. headless vector inserts). The surrogate is
    /// self-keyed in the catalog (`pk_bytes = surrogate.as_u32().to_be_bytes()`)
    /// so the binding round-trips homogeneously with named-PK rows: a
    /// later lookup via the self-bytes returns the same surrogate, and
    /// the reverse lookup returns the self-bytes back. Keeps the
    /// catalog single-shaped — no special-case "unbound" rows.
    pub fn assign_anonymous(&self, collection: &str) -> crate::Result<Surrogate> {
        let catalog = match self.credential_store.catalog().as_ref() {
            Some(c) => c,
            None => return Ok(Surrogate::ZERO),
        };

        let registry = self.registry.write().map_err(|_| crate::Error::Internal {
            detail: "surrogate registry lock poisoned".into(),
        })?;
        let surrogate = registry.alloc_one()?;
        let self_bytes = surrogate.as_u32().to_be_bytes();
        catalog.put_surrogate(collection, &self_bytes, surrogate)?;
        self.wal_appender
            .record_bind_to_wal(surrogate.as_u32(), collection, &self_bytes)?;

        if registry.should_flush() {
            let raft_shared = self.shared.get().and_then(|w| w.upgrade());
            let combined = CombinedPersist {
                catalog,
                wal_appender: self.wal_appender.as_ref(),
                raft_shared: raft_shared.as_deref(),
            };
            registry.flush(&combined)?;
        }

        Ok(surrogate)
    }
}

/// `SurrogateHwmPersist` impl that writes the catalog row AND emits
/// the WAL record on every checkpoint. When `raft_shared` is set and
/// the node is in cluster mode, also proposes `SurrogateAlloc { hwm }`
/// to the metadata Raft group so followers advance their in-memory HWM.
struct CombinedPersist<'a> {
    catalog: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
    /// Present when the Raft cluster is active; drives the Raft propose.
    raft_shared: Option<&'a SharedState>,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)?;
        self.wal_appender.record_alloc_to_wal(hwm)?;
        // Propose to Raft when in cluster mode so followers advance
        // their in-memory HWM. Failure is non-fatal for the local
        // write (which is already durable via the catalog and WAL);
        // the follower will catch up on the next flush cycle or via
        // snapshot. We log at warn so operators can detect systemic
        // issues without breaking the local write path.
        if let Some(shared) = self.raft_shared
            && let Err(e) = crate::control::metadata_proposer::propose_surrogate_hwm(shared, hwm)
        {
            tracing::warn!(hwm, error = %e, "surrogate hwm raft propose failed; followers may lag");
        }
        Ok(())
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::wal_appender::NoopWalAppender;

    fn open_test() -> (tempfile::TempDir, Arc<SurrogateAssigner>) {
        let dir = tempfile::tempdir().unwrap();
        let credentials = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
        let reg = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let a = Arc::new(SurrogateAssigner::new(reg, credentials, wal));
        (dir, a)
    }

    #[test]
    fn assign_is_idempotent_for_same_pk() {
        let (_dir, a) = open_test();
        let s1 = a.assign("users", b"alice").unwrap();
        let s2 = a.assign("users", b"alice").unwrap();
        assert_eq!(s1, s2);
        assert_eq!(s1, Surrogate::new(1));
    }

    #[test]
    fn assign_distinct_pks_returns_distinct_surrogates() {
        let (_dir, a) = open_test();
        let s1 = a.assign("users", b"alice").unwrap();
        let s2 = a.assign("users", b"bob").unwrap();
        assert_ne!(s1, s2);
    }

    #[test]
    fn assign_writes_reverse_binding() {
        let (_dir, a) = open_test();
        let s = a.assign("users", b"alice").unwrap();
        let cat = a.credential_store.catalog().as_ref().unwrap();
        assert_eq!(
            cat.get_pk_for_surrogate("users", s).unwrap(),
            Some(b"alice".to_vec())
        );
    }

    #[test]
    fn assign_persists_hwm_at_flush_threshold() {
        let (_dir, a) = open_test();
        // Allocate just up to and across the 1024 ops threshold.
        let n = super::super::registry::FLUSH_OPS_THRESHOLD as usize;
        for i in 0..n {
            let pk = format!("u{i}");
            let _ = a.assign("users", pk.as_bytes()).unwrap();
        }
        // Either threshold (1024 ops or 200 ms elapsed) may fire
        // first; assert only that the catalog persisted *some*
        // checkpoint inside the (0, n] band.
        let cat = a.credential_store.catalog().as_ref().unwrap();
        let persisted = cat.get_surrogate_hwm().unwrap();
        assert!(persisted > 0 && persisted <= n as u32);
    }
}
