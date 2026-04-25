//! Surrogate WAL crash-recovery: every `(collection, pk)` binding emitted
//! before a force-kill must be re-derivable from the WAL on restart, and
//! the registry hwm must not regress.
//!
//! Single-node only. Cluster snapshot inclusion + 3-node replay are
//! covered separately under the cluster harness.

use std::sync::{Arc, RwLock};

use nodedb::control::security::catalog::SystemCatalog;
use nodedb::control::security::credential::CredentialStore;
use nodedb::control::surrogate::{
    SurrogateAssigner, SurrogateRegistry, SurrogateRegistryHandle, SurrogateWalAppender,
    WalSurrogateAppender,
};
use nodedb::wal::WalManager;
use nodedb::wal::replay::replay_surrogate_records;
use nodedb_types::Surrogate;

fn open_wal_appender(path: &std::path::Path) -> (Arc<WalManager>, Arc<dyn SurrogateWalAppender>) {
    let wal = Arc::new(WalManager::open_for_testing(path).unwrap());
    let appender: Arc<dyn SurrogateWalAppender> =
        Arc::new(WalSurrogateAppender::new(Arc::clone(&wal)));
    (wal, appender)
}

fn fresh_registry() -> SurrogateRegistryHandle {
    Arc::new(RwLock::new(SurrogateRegistry::new()))
}

#[test]
fn kill_restart_recovers_all_bindings_and_hwm() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");

    // Mix of fresh inserts and UPSERTs (same (collection, pk) repeated).
    // Each fresh insert must allocate a unique surrogate; UPSERTs must
    // return the same surrogate as the original insert.
    let inserts: Vec<(&str, &[u8])> = vec![
        ("users", b"alice"),
        ("users", b"bob"),
        ("orders", b"o-1"),
        ("orders", b"o-2"),
        ("orders", b"o-3"),
        ("users", b"carol"),
        ("users", b"alice"),
        ("orders", b"o-1"),
    ];

    let mut expected: Vec<(String, Vec<u8>, Surrogate)> = Vec::new();

    // Phase 1: open WAL + catalog, allocate, then drop without graceful
    // shutdown — closures form an implicit kill (no flush, no fsync from
    // higher layers).
    {
        let (_wal, appender) = open_wal_appender(&wal_path);
        let credentials = Arc::new(CredentialStore::open(&catalog_path).unwrap());
        let registry = fresh_registry();
        let assigner = SurrogateAssigner::new(registry, credentials, appender);

        for (coll, pk) in &inserts {
            let s = assigner.assign(coll, pk).unwrap();
            // Track the FIRST allocation per (coll, pk); UPSERTs reuse.
            let already = expected
                .iter()
                .find(|(c, p, _)| c == *coll && p.as_slice() == *pk);
            match already {
                Some((_, _, prior)) => assert_eq!(s, *prior, "UPSERT must preserve surrogate"),
                None => expected.push((coll.to_string(), pk.to_vec(), s)),
            }
        }
        // No explicit flush, no fsync from outer layer — drop is the kill.
    }

    // Phase 2: reopen WAL + catalog, replay surrogate records into a
    // fresh registry seeded from the persisted hwm row, assert state.
    let (wal, _appender) = open_wal_appender(&wal_path);
    let catalog = SystemCatalog::open(&catalog_path).unwrap();
    let initial_hwm = catalog.get_surrogate_hwm().unwrap();
    let registry: SurrogateRegistryHandle = Arc::new(RwLock::new(
        SurrogateRegistry::from_persisted_hwm(initial_hwm),
    ));

    let records = wal.replay().unwrap();
    let stats = replay_surrogate_records(&records, &catalog, &registry).unwrap();
    assert_eq!(
        stats.binds,
        expected.len(),
        "expected one bind per fresh allocation"
    );

    // Every previously-bound (collection, pk) resolves to the original
    // surrogate. The reverse map agrees too.
    for (coll, pk, surrogate) in &expected {
        assert_eq!(
            catalog.get_surrogate_for_pk(coll, pk).unwrap(),
            Some(*surrogate),
            "binding for ({coll}, {pk:?}) lost across crash",
        );
        assert_eq!(
            catalog.get_pk_for_surrogate(coll, *surrogate).unwrap(),
            Some(pk.clone()),
            "reverse binding for ({coll}, {surrogate:?}) lost across crash",
        );
    }

    // Hwm recovery: registry must be advanced past every surrogate ever
    // issued, so the next allocation strictly exceeds them all.
    let max_issued = expected.iter().map(|(_, _, s)| s.as_u32()).max().unwrap();
    let post_replay_hwm = registry.read().unwrap().current_hwm();
    assert!(
        post_replay_hwm >= max_issued,
        "post-replay hwm {post_replay_hwm} must cover max issued {max_issued}",
    );

    let next = registry.read().unwrap().alloc_one().unwrap();
    assert_eq!(
        next.as_u32(),
        post_replay_hwm + 1,
        "next allocation must be exactly hwm+1",
    );
    assert!(
        next.as_u32() > max_issued,
        "next allocation {} must exceed all pre-crash surrogates {max_issued}",
        next.as_u32(),
    );
}

#[test]
fn kill_restart_after_hwm_flush_threshold_recovers_via_alloc_record() {
    // Force the 1024-ops flush threshold to fire so a `SurrogateAlloc`
    // record actually lands in the WAL. Then kill+reopen and verify
    // that BOTH the catalog hwm row and the WAL alloc record are
    // honoured (replay is idempotent on the higher of the two).
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");

    let total = nodedb::control::surrogate::FLUSH_OPS_THRESHOLD as usize + 4;

    let mut last_surrogate: Option<Surrogate> = None;
    {
        let (_wal, appender) = open_wal_appender(&wal_path);
        let credentials = Arc::new(CredentialStore::open(&catalog_path).unwrap());
        let registry = fresh_registry();
        let assigner = SurrogateAssigner::new(registry, credentials, appender);

        for i in 0..total {
            let pk = format!("u{i:08}");
            last_surrogate = Some(assigner.assign("users", pk.as_bytes()).unwrap());
        }
    }

    let (wal, _appender) = open_wal_appender(&wal_path);
    let catalog = SystemCatalog::open(&catalog_path).unwrap();
    let initial_hwm = catalog.get_surrogate_hwm().unwrap();
    let registry: SurrogateRegistryHandle = Arc::new(RwLock::new(
        SurrogateRegistry::from_persisted_hwm(initial_hwm),
    ));

    let records = wal.replay().unwrap();
    let stats = replay_surrogate_records(&records, &catalog, &registry).unwrap();
    assert!(stats.allocs >= 1, "expected ≥1 SurrogateAlloc in WAL");
    assert_eq!(stats.binds, total);

    let last = last_surrogate.unwrap().as_u32();
    let post_replay_hwm = registry.read().unwrap().current_hwm();
    assert!(post_replay_hwm >= last);

    let next = registry.read().unwrap().alloc_one().unwrap();
    assert!(next.as_u32() > last);
}
