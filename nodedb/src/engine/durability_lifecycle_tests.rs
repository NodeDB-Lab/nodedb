// SPDX-License-Identifier: BUSL-1.1

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};

use super::GatedDatabase;

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("lifecycle");

fn put(db: &GatedDatabase, key: u64) {
    let txn = db.begin_write().unwrap();
    txn.open_table(TABLE).unwrap().insert(key, key).unwrap();
    txn.commit().unwrap();
}

fn keys(path: &std::path::Path) -> Vec<u64> {
    let db = Database::create(path).unwrap();
    let read = db.begin_read().unwrap();
    read.open_table(TABLE)
        .unwrap()
        .iter()
        .unwrap()
        .map(|entry| entry.unwrap().0.value())
        .collect()
}

#[test]
fn unresolved_deferred_close_preserves_prior_durable_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unresolved.redb");
    let db = GatedDatabase::create(&path).unwrap();
    put(&db, 1);
    db.defer();
    put(&db, 2);
    db.resume();
    drop(db);
    assert_eq!(keys(&path), [1]);
}

#[test]
fn resolved_durable_session_survives_normal_close() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resolved.redb");
    let db = GatedDatabase::create(&path).unwrap();
    put(&db, 1);
    db.defer();
    put(&db, 2);
    db.persist().unwrap();
    db.resume();
    db.resolve_journal();
    drop(db);
    assert_eq!(keys(&path), [1, 2]);
}

#[test]
fn outstanding_transactions_retain_fence_after_wrapper_drop() {
    for durability in [Durability::Immediate, Durability::None] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outstanding.redb");
        let db = GatedDatabase::create(&path).unwrap();
        put(&db, 1);
        db.defer();
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(durability).unwrap();
        txn.open_table(TABLE).unwrap().insert(2, 2).unwrap();
        drop(db);
        let result = txn.commit();
        if matches!(durability, Durability::Immediate) {
            assert!(result.is_err());
        }
        // A None commit can succeed in memory. Close still cannot persist it.
        assert_eq!(keys(&path), [1]);
    }
}

#[test]
fn sealed_database_releases_locks_and_rejects_new_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("locks.redb");
    let db = GatedDatabase::create(&path).unwrap();
    put(&db, 1);
    assert!(matches!(
        GatedDatabase::create(&path),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    db.seal();
    db.seal();
    db.resolve_journal();
    assert!(db.begin_write().is_err());
    assert!(db.begin_durable_write().is_err());
    assert!(db.persist().is_err());
    drop(db);
    assert_eq!(keys(&path), [1]);
}

#[test]
fn sealed_large_transaction_cannot_write_evicted_or_direct_pages() {
    for value_size in [256, 1024 * 1024] {
        check_sealed_transaction(value_size);
    }
}

fn check_sealed_transaction(value_size: usize) {
    use super::guarded_backend::{GuardedBackend, WriteFence};
    use std::fs::OpenOptions;
    use std::sync::atomic::AtomicBool;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.redb");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();
    let fence = WriteFence::default();
    let backend = GuardedBackend::new(
        redb::backends::FileBackend::new(file).unwrap(),
        fence.clone(),
    );
    let db = GatedDatabase {
        db: Database::builder()
            .set_cache_size(4096)
            .create_with_backend(backend)
            .unwrap(),
        fence,
        unresolved: AtomicBool::new(false),
        deferred: AtomicBool::new(false),
        pending: AtomicBool::new(false),
    };
    put(&db, 1);
    let large: TableDefinition<u64, &[u8]> = TableDefinition::new("large");
    let setup = db.begin_write().unwrap();
    setup.open_table(large).unwrap();
    setup.commit().unwrap();
    db.defer();
    let txn = db.begin_write().unwrap();
    let before = std::fs::read(&path).unwrap();
    db.seal();
    let result = (|| -> Result<(), redb::Error> {
        let mut table = txn.open_table(large)?;
        let value = vec![7; value_size];
        for key in 0..128 {
            table.insert(key, value.as_slice())?;
        }
        drop(table);
        txn.commit()?;
        Ok(())
    })();
    assert!(result.is_err());
    drop(db);
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(keys(&path), [1]);
}
