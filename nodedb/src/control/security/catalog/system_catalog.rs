//! SystemCatalog: redb-backed persistent catalog database.
//!
//! Opens or creates the system.redb file, initializes all tables,
//! and provides raw WASM module storage methods.

use std::path::Path;

use redb::Database;
use tracing::info;

use super::types::*;

pub struct SystemCatalog {
    pub(super) db: Database,
}

impl SystemCatalog {
    /// Open or create the system catalog at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| catalog_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| catalog_err("init txn", e))?;
        {
            let _ = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("init users table", e))?;
            let _ = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("init api_keys table", e))?;
            let _ = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("init roles table", e))?;
            let _ = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("init permissions table", e))?;
            let _ = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("init owners table", e))?;
            let _ = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("init tenants table", e))?;
            let _ = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("init audit_log table", e))?;
            let _ = write_txn
                .open_table(COLLECTIONS)
                .map_err(|e| catalog_err("init collections table", e))?;
            let _ = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("init metadata table", e))?;
            let _ = write_txn
                .open_table(BLACKLIST)
                .map_err(|e| catalog_err("init blacklist table", e))?;
            let _ = write_txn
                .open_table(AUTH_USERS)
                .map_err(|e| catalog_err("init auth_users table", e))?;
            let _ = write_txn
                .open_table(ORGS)
                .map_err(|e| catalog_err("init orgs table", e))?;
            let _ = write_txn
                .open_table(ORG_MEMBERS)
                .map_err(|e| catalog_err("init org_members table", e))?;
            let _ = write_txn
                .open_table(SCOPES)
                .map_err(|e| catalog_err("init scopes table", e))?;
            let _ = write_txn
                .open_table(SCOPE_GRANTS)
                .map_err(|e| catalog_err("init scope_grants table", e))?;
            let _ = write_txn
                .open_table(MATERIALIZED_VIEWS)
                .map_err(|e| catalog_err("init materialized_views table", e))?;
            let _ = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("init functions table", e))?;
            let _ = write_txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("init triggers table", e))?;
            let _ = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("init arrays table", e))?;
            let _ = write_txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("init procedures table", e))?;
            let _ = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("init dependencies table", e))?;
            let _ = write_txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err("init wasm_modules table", e))?;
            let _ = write_txn
                .open_table(CHANGE_STREAMS)
                .map_err(|e| catalog_err("init change_streams table", e))?;
            let _ = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("init consumer_groups table", e))?;
            let _ = write_txn
                .open_table(SCHEDULES)
                .map_err(|e| catalog_err("init schedules table", e))?;
            let _ = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("init topics_ep table", e))?;
            let _ = write_txn
                .open_table(STREAMING_MVS)
                .map_err(|e| catalog_err("init streaming_mvs table", e))?;
            let _ = write_txn
                .open_table(super::rls::RLS_POLICIES)
                .map_err(|e| catalog_err("init rls_policies table", e))?;
            let _ = write_txn
                .open_table(ALERT_RULES)
                .map_err(|e| catalog_err("init alert_rules table", e))?;
            let _ = write_txn
                .open_table(RETENTION_POLICIES)
                .map_err(|e| catalog_err("init retention_policies table", e))?;
            let _ = write_txn
                .open_table(SEQUENCES)
                .map_err(|e| catalog_err("init sequences table", e))?;
            let _ = write_txn
                .open_table(SEQUENCE_STATE)
                .map_err(|e| catalog_err("init sequence_state table", e))?;
            let _ = write_txn
                .open_table(COLUMN_STATS)
                .map_err(|e| catalog_err("init column_stats table", e))?;
            let _ = write_txn
                .open_table(VECTOR_MODEL_METADATA)
                .map_err(|e| catalog_err("init vector_model_metadata table", e))?;
            let _ = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("init checkpoints table", e))?;
            let _ = write_txn
                .open_table(WAL_TOMBSTONES)
                .map_err(|e| catalog_err("init wal_tombstones table", e))?;
            let _ = write_txn
                .open_table(super::types::L2_CLEANUP_QUEUE)
                .map_err(|e| catalog_err("init l2_cleanup_queue table", e))?;
            let _ = write_txn
                .open_table(super::surrogate_hwm::SURROGATE_HWM)
                .map_err(|e| catalog_err("init surrogate_hwm table", e))?;
            let _ = write_txn
                .open_table(super::types::SURROGATE_PK)
                .map_err(|e| catalog_err("init surrogate_pk table", e))?;
            let _ = write_txn
                .open_table(super::types::SURROGATE_PK_REV)
                .map_err(|e| catalog_err("init surrogate_pk_rev table", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("init commit", e))?;

        info!(path = %path.display(), "system catalog opened");

        Ok(Self { db })
    }

    /// Execute a write transaction on the WASM_MODULES table.
    fn wasm_write<F, T>(&self, op: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut redb::Table<&str, &[u8]>) -> crate::Result<T>,
    {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err(&format!("{op} txn"), e))?;
        let result = {
            let mut table = txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err(&format!("{op} open"), e))?;
            f(&mut table)?
        };
        txn.commit()
            .map_err(|e| catalog_err(&format!("{op} commit"), e))?;
        Ok(result)
    }

    /// Store raw bytes under a string key in the WASM_MODULES table.
    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("put_raw key", e))?;
        self.wasm_write("put_raw", |table| {
            table
                .insert(key_str, value)
                .map_err(|e| catalog_err("put_raw insert", e))?;
            Ok(())
        })
    }

    /// Load raw bytes by string key from the WASM_MODULES table.
    pub fn get_raw(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("get_raw key", e))?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("get_raw txn", e))?;
        let table = txn
            .open_table(WASM_MODULES)
            .map_err(|e| catalog_err("get_raw open", e))?;
        match table
            .get(key_str)
            .map_err(|e| catalog_err("get_raw get", e))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Delete raw bytes by string key from the WASM_MODULES table.
    pub fn delete_raw(&self, key: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("delete_raw key", e))?;
        self.wasm_write("delete_raw", |table| {
            table
                .remove(key_str)
                .map_err(|e| catalog_err("delete_raw remove", e))?;
            Ok(())
        })
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
            md5_hash: String::new(),
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
            md5_hash: String::new(),
        };

        catalog.put_user(&user).unwrap();
        catalog.delete_user("bob").unwrap();

        let loaded = catalog.load_all_users().unwrap();
        assert!(loaded.is_empty());
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
                    md5_hash: String::new(),
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
