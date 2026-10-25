// SPDX-License-Identifier: BUSL-1.1

//! SystemCatalog: redb-backed persistent catalog database.
//!
//! Opens or creates the system.redb file and initializes all tables.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase};
use tracing::info;

use super::types::*;

/// Cloneable handle to the redb-backed system catalog. Clones share one
/// underlying `redb::Database` (one file open), so the catalog can be shared
/// between the credential store and the sync producer registry without a
/// second `Database::create` on the same path (which redb rejects).
#[derive(Clone)]
pub struct SystemCatalog {
    pub(super) db: Arc<Database>,
    pub(super) crdt_signing_root: Arc<std::sync::RwLock<Option<[u8; 32]>>>,
    /// Committed DEFINE EVENT definitions, for readers that must not read
    /// redb.
    pub(super) event_defs: Arc<super::event_defs_index::EventDefsIndex>,
    /// The user-defined CRS set, loaded at open and swapped on each write.
    pub(super) crs_set: Arc<std::sync::RwLock<Arc<nodedb_crs::CustomCrsSet>>>,
    /// Whether a barrier log row committed without persisting since the
    /// last `persist_calvin_barrier_logs`.
    pub(super) calvin_barrier_logs_pending: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only fault switches and read counters.
    #[cfg(test)]
    pub(super) faults: super::test_faults::CatalogTestFaults,
}

impl crate::storage::RedbBacked for SystemCatalog {
    fn redb_database(&self) -> &Database {
        &self.db
    }
}

impl SystemCatalog {
    /// Open or create the system catalog at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| catalog_err("open", e))?;

        if Self::ensure_bootstrapped(&db)? {
            info!(path = %path.display(), "system catalog opened (bootstrapped)");
        } else {
            info!(path = %path.display(), "system catalog opened");
        }

        Self::from_bootstrapped(db)
    }

    /// Open a non-persistent system catalog backed by an in-memory redb
    /// database. Used by in-process credential stores that need a real,
    /// fully-bootstrapped catalog without touching the filesystem.
    pub fn open_in_memory() -> crate::Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| catalog_err("open in-memory", e))?;
        Self::ensure_bootstrapped(&db)?;
        Self::from_bootstrapped(db)
    }

    /// The catalog over `db`, whose tables exist: seeds the default database
    /// and loads the event definitions and the custom CRS set.
    fn from_bootstrapped(db: Database) -> crate::Result<Self> {
        let catalog = Self {
            db: Arc::new(db),
            crdt_signing_root: Arc::new(std::sync::RwLock::new(None)),
            event_defs: Arc::new(super::event_defs_index::EventDefsIndex::new()),
            crs_set: Arc::new(std::sync::RwLock::new(Arc::new(
                nodedb_crs::CustomCrsSet::new(),
            ))),
            calvin_barrier_logs_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            faults: super::test_faults::CatalogTestFaults::default(),
        };
        catalog.bootstrap_default_database()?;
        catalog.reload_event_definitions()?;
        catalog.reload_custom_crs_set()?;
        Ok(catalog)
    }

    /// Bootstrap every `_system.*` table from the canonical registry —
    /// but only if at least one is actually missing. Probing read-only
    /// first keeps `open` byte-idempotent on an already-bootstrapped
    /// catalog: a write transaction + commit stamps a fresh meta/commit
    /// page on redb every time, so an unconditional bootstrap rewrites
    /// `system.redb` on every boot (changing its size/md5) even when
    /// nothing changed — and a boot that then fails its integrity check
    /// mutates persistent catalog state on its way out.
    /// Opening a table in a write transaction creates it if absent; the
    /// registry is the single source of truth, so a table cannot be
    /// read in production code without being bootstrapped here. Returns
    /// `true` when a bootstrap write actually ran.
    fn ensure_bootstrapped(db: &Database) -> crate::Result<bool> {
        let needs_bootstrap = match db.begin_read() {
            Ok(read_txn) => super::bootstrap_tables::BOOTSTRAP_TABLES
                .iter()
                .any(|table| (table.probe)(&read_txn).is_err()),
            // A read transaction on a brand-new database can fail before
            // the first commit; treat that as "bootstrap needed".
            Err(_) => true,
        };
        if needs_bootstrap {
            let write_txn = db.begin_write().map_err(|e| catalog_err("init txn", e))?;
            for table in super::bootstrap_tables::BOOTSTRAP_TABLES {
                (table.create)(&write_txn)
                    .map_err(|e| catalog_err(&format!("init {} table", table.label), e))?;
            }
            write_txn
                .commit()
                .map_err(|e| catalog_err("init commit", e))?;
        }
        Ok(needs_bootstrap)
    }
}

#[cfg(test)]
mod tests {
    use super::super::auth_types::StoredUser;
    use super::*;

    #[test]
    fn open_and_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "alice".into(),
            tenant_id: 1,
            password_hash: "$argon2id$test".into(),
            scram_salt: vec![1, 2, 3, 4],
            scram_salted_password: vec![5, 6, 7, 8],
            roles: vec!["readwrite".into()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: vec![],
        };

        catalog.put_user(&user).unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].username, "alice");
        assert_eq!(loaded[0].tenant_id, 1);
    }

    #[test]
    fn delete_user() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        let user = StoredUser {
            user_id: 1,
            username: "bob".into(),
            tenant_id: 1,
            password_hash: "hash".into(),
            scram_salt: vec![],
            scram_salted_password: vec![],
            roles: vec![],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: vec![],
        };

        catalog.put_user(&user).unwrap();
        catalog.delete_user("bob").unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn bootstrap_creates_every_registered_table() {
        // A fresh catalog must already contain every table in the
        // bootstrap registry, so boot-time readers (integrity walk,
        // continuous-aggregate replay, …) open existing empty tables
        // instead of hitting "table does not exist". Re-opening each
        // entry read-only fails with `TableDoesNotExist` if the
        // init path ever stopped iterating the registry.
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let txn = catalog.db.begin_read().unwrap();
        for table in super::super::bootstrap_tables::BOOTSTRAP_TABLES {
            (table.probe)(&txn)
                .unwrap_or_else(|e| panic!("table `{}` missing after bootstrap: {e}", table.label));
        }
    }

    #[test]
    fn reading_a_bootstrapped_catalog_does_not_mutate_the_file() {
        // A boot that only *reads* the catalog must not rewrite
        // `system.redb`: operators verify a catalog (or a backup of one)
        // by hash, and a boot that later fails its integrity check must
        // not already have mutated persistent catalog state on its way
        // out.
        //
        // This cannot be delivered by a read-write handle. redb commits
        // its allocator state table when a `Database` drops (so the next
        // open can skip a full repair), which stamps the god byte at
        // offset 9 regardless of whether the caller wrote anything. The
        // read-only handle wraps `redb::ReadOnlyDatabase`, which never
        // writes — so read-only boots take that, and this test pins the
        // guarantee to it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        // Bootstrap once so the file reaches steady state.
        drop(SystemCatalog::open(&path).unwrap());
        let before = std::fs::read(&path).unwrap();

        // A pure "bring the catalog up to read it" boot. Must not touch a byte.
        let catalog = super::super::ReadOnlySystemCatalog::open(&path)
            .expect("cleanly-closed catalog opens read-only")
            .expect("catalog file exists");
        catalog.list_databases().unwrap();
        drop(catalog);
        let after = std::fs::read(&path).unwrap();

        let first_diff = before
            .iter()
            .zip(after.iter())
            .position(|(a, b)| a != b)
            .map(|i| i.to_string())
            .unwrap_or_else(|| "len".to_string());
        assert!(
            before == after,
            "a read-only catalog open rewrote system.redb (len {} → {}, \
             first differing offset: {first_diff}): read-only boots must \
             leave the file byte-identical so it stays verifiable by hash.",
            before.len(),
            after.len(),
        );
    }

    #[test]
    fn read_write_open_reports_repair_needed_state_to_read_only_callers() {
        // The read-only handle refuses a catalog that was not cleanly
        // closed rather than silently repairing it (repair writes). A
        // cleanly-closed catalog must open read-only.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        drop(SystemCatalog::open(&path).unwrap());

        assert!(
            super::super::ReadOnlySystemCatalog::open(&path)
                .expect("clean catalog opens read-only")
                .is_some()
        );

        // No file at all is "nothing to load", not an error.
        let missing = dir.path().join("absent.redb");
        assert!(
            super::super::ReadOnlySystemCatalog::open(&missing)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn next_user_id_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_next_user_id().unwrap(), 1);
            catalog.save_next_user_id(42).unwrap();
        }

        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_next_user_id().unwrap(), 42);
    }

    #[test]
    fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_user(&StoredUser {
                    user_id: 5,
                    username: "persistent".into(),
                    tenant_id: 3,
                    password_hash: "hash".into(),
                    scram_salt: vec![1],
                    scram_salted_password: vec![2],
                    roles: vec!["readonly".into(), "monitor".into()],
                    is_superuser: false,
                    is_active: true,
                    is_service_account: false,
                    created_at: 0,
                    updated_at: 0,
                    password_expires_at: 0,
                    must_change_password: false,
                    password_changed_at: 0,
                    default_database_id: 0,
                    accessible_databases: vec![],
                })
                .unwrap();
        }

        let catalog = SystemCatalog::open(&path).unwrap();
        let users = catalog.load_all_users().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "persistent");
        assert_eq!(users[0].user_id, 5);
        assert_eq!(users[0].roles, vec!["readonly", "monitor"]);
    }
}
