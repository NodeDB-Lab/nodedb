//! Integration tests for `assign_surrogate` semantics over a real
//! `SystemCatalog`, including UPSERT idempotency, allocation
//! divergence, drop-collection cleanup, and the WAL-flush trigger.

use std::sync::{Arc, RwLock};

use nodedb::control::security::credential::CredentialStore;
use nodedb::control::surrogate::{
    FLUSH_OPS_THRESHOLD, NoopWalAppender, SurrogateAssigner, SurrogateRegistry,
    SurrogateRegistryHandle, SurrogateWalAppender,
};
use nodedb_types::Surrogate;

fn open_credentials() -> (tempfile::TempDir, Arc<CredentialStore>) {
    let dir = tempfile::tempdir().unwrap();
    let creds = Arc::new(CredentialStore::open(&dir.path().join("system.redb")).unwrap());
    (dir, creds)
}

fn fresh_registry() -> SurrogateRegistryHandle {
    Arc::new(RwLock::new(SurrogateRegistry::new()))
}

fn make_assigner(
    creds: Arc<CredentialStore>,
    wal: Arc<dyn SurrogateWalAppender>,
) -> SurrogateAssigner {
    SurrogateAssigner::new(fresh_registry(), creds, wal)
}

#[test]
fn assign_is_idempotent_for_same_pk() {
    let (_dir, creds) = open_credentials();
    let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
    let a = make_assigner(creds.clone(), wal);
    let s1 = a.assign("users", b"alice").unwrap();
    let s2 = a.assign("users", b"alice").unwrap();
    let s3 = a.assign("users", b"alice").unwrap();
    assert_eq!(s1, s2);
    assert_eq!(s2, s3);
    let cat = creds.catalog().as_ref().unwrap();
    assert_eq!(
        cat.get_surrogate_for_pk("users", b"alice").unwrap(),
        Some(s1)
    );
    assert_eq!(
        cat.get_pk_for_surrogate("users", s1).unwrap(),
        Some(b"alice".to_vec())
    );
}

#[test]
fn assign_distinct_pks_returns_distinct_surrogates() {
    let (_dir, creds) = open_credentials();
    let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
    let a = make_assigner(creds, wal);
    let s1 = a.assign("users", b"alice").unwrap();
    let s2 = a.assign("users", b"bob").unwrap();
    let s3 = a.assign("users", b"carol").unwrap();
    assert_ne!(s1, s2);
    assert_ne!(s2, s3);
    assert_ne!(s1, s3);
    assert!(s1.as_u32() < s2.as_u32());
    assert!(s2.as_u32() < s3.as_u32());
}

#[test]
fn drop_collection_wipes_surrogate_map() {
    let (_dir, creds) = open_credentials();
    let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
    let a = make_assigner(creds.clone(), wal);
    let _ = a.assign("users", b"alice").unwrap();
    let _ = a.assign("users", b"bob").unwrap();
    let s_other = a.assign("orders", b"o1").unwrap();
    let cat = creds.catalog().as_ref().unwrap();
    assert_eq!(
        cat.scan_surrogates_for_collection("users").unwrap().len(),
        2
    );

    cat.delete_all_surrogates_for_collection("users").unwrap();
    assert!(
        cat.scan_surrogates_for_collection("users")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        cat.get_surrogate_for_pk("orders", b"o1").unwrap(),
        Some(s_other)
    );
}

/// Counting WAL appender — verifies `assign_surrogate` actually
/// triggers a `SurrogateAlloc` WAL emission when the registry's
/// 1024-ops threshold is crossed.
struct CountingAppender {
    allocs: std::sync::atomic::AtomicU32,
    binds: std::sync::atomic::AtomicU32,
}

impl CountingAppender {
    fn new() -> Self {
        Self {
            allocs: std::sync::atomic::AtomicU32::new(0),
            binds: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl SurrogateWalAppender for CountingAppender {
    fn record_alloc_to_wal(&self, _hi: u32) -> nodedb::Result<()> {
        self.allocs
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    fn record_bind_to_wal(
        &self,
        _surrogate: u32,
        _collection: &str,
        _pk_bytes: &[u8],
    ) -> nodedb::Result<()> {
        self.binds.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }
}

#[test]
fn flush_emits_wal_record_at_threshold() {
    let (_dir, creds) = open_credentials();
    let wal_concrete = Arc::new(CountingAppender::new());
    let wal_dyn: Arc<dyn SurrogateWalAppender> = wal_concrete.clone();
    let a = make_assigner(creds.clone(), wal_dyn);

    let n = FLUSH_OPS_THRESHOLD as usize;
    for i in 0..n {
        let pk = format!("u{i:08}");
        let _ = a.assign("users", pk.as_bytes()).unwrap();
    }

    let alloc_calls = wal_concrete
        .allocs
        .load(std::sync::atomic::Ordering::Acquire);
    assert!(
        alloc_calls >= 1,
        "expected at least one SurrogateAlloc WAL emission after {n} allocations, got {alloc_calls}"
    );
    let bind_calls = wal_concrete
        .binds
        .load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(
        bind_calls as usize, n,
        "expected one SurrogateBind per fresh allocation"
    );
    let cat = creds.catalog().as_ref().unwrap();
    let persisted = cat.get_surrogate_hwm().unwrap();
    assert!(
        persisted > 0 && persisted <= n as u32,
        "expected persisted hwm in (0, {n}], got {persisted}"
    );
}

#[test]
fn assigns_persist_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");
    let s_persisted: Surrogate;
    {
        let creds = Arc::new(CredentialStore::open(&path).unwrap());
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let a = make_assigner(creds, wal);
        s_persisted = a.assign("users", b"alice").unwrap();
    }
    // Reopen — the binding row must survive.
    let creds = CredentialStore::open(&path).unwrap();
    let cat = creds.catalog().as_ref().unwrap();
    assert_eq!(
        cat.get_surrogate_for_pk("users", b"alice").unwrap(),
        Some(s_persisted)
    );
    assert_eq!(
        cat.get_pk_for_surrogate("users", s_persisted).unwrap(),
        Some(b"alice".to_vec())
    );
}
