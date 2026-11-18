// SPDX-License-Identifier: BUSL-1.1

//! Deferred redb commits with an irreversible backend write fence.
//!
//! An unresolved journal seals storage before normal close can promote deferred commits.
//! Outstanding transactions retain the same fence through redb's backend ownership.

#[path = "guarded_backend.rs"]
mod guarded_backend;

use std::fs::OpenOptions;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use guarded_backend::{GuardedBackend, WriteFence};

use redb::{Database, Durability, WriteTransaction};

/// A redb database with a durability gate.
#[derive(Debug)]
pub struct GatedDatabase {
    db: Database,
    fence: WriteFence,
    unresolved: AtomicBool,
    deferred: AtomicBool,
    /// Whether a transaction began deferred since the last [`Self::persist`]
    /// or successful [`DurableWrite::commit`].
    pending: AtomicBool,
}

impl GatedDatabase {
    /// Open or create a database with guarded storage mutations.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, redb::DatabaseError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let fence = WriteFence::default();
        let backend = GuardedBackend::new(redb::backends::FileBackend::new(file)?, fence.clone());
        let db = Database::builder().create_with_backend(backend)?;
        Ok(Self {
            db,
            fence,
            unresolved: AtomicBool::new(false),
            deferred: AtomicBool::new(false),
            pending: AtomicBool::new(false),
        })
    }

    /// Begin a write transaction. While the gate defers, it commits without
    /// persisting.
    pub fn begin_write(&self) -> Result<WriteTransaction, redb::Error> {
        self.fence.check()?;
        let mut txn = self.db.begin_write()?;
        if self.deferred.load(Ordering::Acquire) {
            txn.set_durability(Durability::None)?;
            self.pending.store(true, Ordering::Release);
        }
        Ok(txn)
    }

    /// Begin a write transaction that persists its commit and every deferred
    /// commit before it, whatever the gate says.
    pub fn begin_durable_write(&self) -> Result<DurableWrite<'_>, redb::Error> {
        self.fence.check()?;
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        Ok(DurableWrite {
            txn,
            pending: &self.pending,
        })
    }

    /// Whether a commit began deferred and no durable commit persisted it
    /// yet.
    pub fn has_unpersisted(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// Defer the durability of every write transaction begun from now on.
    pub fn defer(&self) {
        self.unresolved.store(true, Ordering::Release);
        self.deferred.store(true, Ordering::Release);
    }

    /// Stop deferring without resolving the current journal session.
    pub fn resume(&self) {
        self.deferred.store(false, Ordering::Release);
    }

    /// Mark the journal resolved after its complete healthy finalization.
    pub fn resolve_journal(&self) {
        self.unresolved.store(false, Ordering::Release);
    }

    /// Permanently reject backend mutations, including redb shutdown writes.
    pub fn seal(&self) {
        self.fence.seal();
    }

    /// Whether the gate defers.
    pub fn is_deferred(&self) -> bool {
        self.deferred.load(Ordering::Acquire)
    }

    /// Persist every deferred commit. A database with no transaction begun
    /// deferred since the last persist has nothing to persist.
    pub fn persist(&self) -> Result<(), redb::Error> {
        self.fence.check()?;
        if !self.pending.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let persisted = self
            .begin_durable_write()
            .and_then(|txn| txn.commit().map_err(redb::Error::from));
        if persisted.is_err() {
            self.pending.store(true, Ordering::Release);
        }
        persisted
    }
}

/// A write transaction whose commit persists every commit before it. redb
/// has one writer, so its successful commit leaves nothing deferred.
pub struct DurableWrite<'a> {
    txn: WriteTransaction,
    pending: &'a AtomicBool,
}

impl DurableWrite<'_> {
    /// Commit and persist. On success no deferred commit stays unpersisted.
    pub fn commit(self) -> Result<(), redb::CommitError> {
        self.txn.commit()?;
        self.pending.store(false, Ordering::Release);
        Ok(())
    }
}

impl Deref for DurableWrite<'_> {
    type Target = WriteTransaction;

    fn deref(&self) -> &WriteTransaction {
        &self.txn
    }
}

impl Drop for GatedDatabase {
    fn drop(&mut self) {
        if self.unresolved.load(Ordering::Acquire) {
            self.seal();
        }
    }
}

impl Deref for GatedDatabase {
    type Target = Database;

    fn deref(&self) -> &Database {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};

    use super::*;

    const T: TableDefinition<u64, u64> = TableDefinition::new("t");

    fn put(db: &GatedDatabase, key: u64) {
        let txn = db.begin_write().unwrap();
        txn.open_table(T).unwrap().insert(key, key).unwrap();
        txn.commit().unwrap();
    }

    /// The keys a crash at this instant leaves: the file as it stands, opened
    /// from a copy while the live database keeps its own open.
    fn keys_after_crash(path: &std::path::Path, copy: &std::path::Path) -> Vec<u64> {
        std::fs::copy(path, copy).unwrap();
        let db = Database::create(copy).unwrap();
        let txn = db.begin_read().unwrap();
        let Ok(table) = txn.open_table(T) else {
            return Vec::new();
        };
        table
            .iter()
            .unwrap()
            .map(|entry| entry.unwrap().0.value())
            .collect()
    }

    /// A deferred commit is visible at once, and a crash rolls it back. The
    /// durable commit after it persists both.
    #[test]
    fn a_deferred_commit_persists_only_with_a_durable_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gated.redb");
        let db = GatedDatabase::create(&path).unwrap();
        put(&db, 1);
        db.defer();
        put(&db, 2);
        {
            let read = db.begin_read().unwrap();
            assert_eq!(
                read.open_table(T)
                    .unwrap()
                    .get(2)
                    .unwrap()
                    .map(|v| v.value()),
                Some(2),
                "a deferred commit is visible to reads"
            );
        }
        assert_eq!(
            keys_after_crash(&path, &dir.path().join("crash-1.redb")),
            [1],
            "a crash rolls a deferred commit back"
        );

        db.persist().unwrap();
        db.resume();
        db.resolve_journal();
        assert!(!db.is_deferred());
        assert_eq!(
            keys_after_crash(&path, &dir.path().join("crash-2.redb")),
            [1, 2]
        );
    }

    /// A durable commit persists the deferred commits before it, so the
    /// database holds nothing unpersisted after it.
    #[test]
    fn a_durable_commit_clears_pending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gated.redb");
        let db = GatedDatabase::create(&path).unwrap();
        assert!(!db.has_unpersisted());
        db.defer();
        put(&db, 1);
        assert!(db.has_unpersisted());

        let txn = db.begin_durable_write().unwrap();
        txn.open_table(T).unwrap().insert(2, 2).unwrap();
        txn.commit().unwrap();
        assert!(!db.has_unpersisted());
        assert_eq!(
            keys_after_crash(&path, &dir.path().join("crash.redb")),
            [1, 2]
        );

        put(&db, 3);
        assert!(db.has_unpersisted(), "a later deferred commit is pending");
        db.resume();
        db.resolve_journal();
    }
}

#[cfg(test)]
#[path = "durability_lifecycle_tests.rs"]
mod lifecycle_tests;
