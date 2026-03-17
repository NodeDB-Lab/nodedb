//! System catalog — redb-backed persistent storage for auth metadata.
//!
//! Stores users, roles, and permissions in `{data_dir}/system.redb`.
//! Lives on the Control Plane (Send + Sync). Uses redb's ACID transactions
//! for crash-safe writes, same technology as the sparse engine.

use std::path::Path;

use redb::{Database, TableDefinition};
use tracing::info;

/// Table: username (string) → MessagePack-serialized user record.
const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: "next_user_id" → u64 (single key for ID generation).
const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Serializable user record for redb storage.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct StoredUser {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: u32,
    pub password_hash: String,
    pub scram_salt: Vec<u8>,
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub is_active: bool,
}

/// Persistent system catalog backed by redb.
pub struct SystemCatalog {
    db: Database,
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
                .open_table(METADATA)
                .map_err(|e| catalog_err("init metadata table", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("init commit", e))?;

        info!(path = %path.display(), "system catalog opened");

        Ok(Self { db })
    }

    /// Load all active users from the catalog.
    pub fn load_all_users(&self) -> crate::Result<Vec<StoredUser>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(USERS)
            .map_err(|e| catalog_err("open users", e))?;

        let mut users = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range users", e))?;
        for entry in range {
            let (_, value) = entry.map_err(|e| catalog_err("read entry", e))?;
            let user: StoredUser = rmp_serde::from_slice(value.value())
                .map_err(|e| catalog_err("deserialize user", e))?;
            users.push(user);
        }

        Ok(users)
    }

    /// Write a user record to the catalog (insert or update).
    pub fn put_user(&self, user: &StoredUser) -> crate::Result<()> {
        let bytes = rmp_serde::to_vec(user).map_err(|e| catalog_err("serialize user", e))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            table
                .insert(user.username.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert user", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Delete a user record from the catalog.
    pub fn delete_user(&self, username: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            table
                .remove(username)
                .map_err(|e| catalog_err("remove user", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the next_user_id counter.
    pub fn load_next_user_id(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(METADATA)
            .map_err(|e| catalog_err("open metadata", e))?;

        match table
            .get("next_user_id")
            .map_err(|e| catalog_err("get next_user_id", e))?
        {
            Some(val) => {
                let bytes = val.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    Ok(u64::from_le_bytes(arr))
                } else {
                    Ok(1)
                }
            }
            None => Ok(1),
        }
    }

    /// Persist the next_user_id counter.
    pub fn save_next_user_id(&self, id: u64) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("open metadata", e))?;
            table
                .insert("next_user_id", id.to_le_bytes().as_slice())
                .map_err(|e| catalog_err("insert next_user_id", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
            assert_eq!(catalog.load_next_user_id().unwrap(), 1); // Default.
            catalog.save_next_user_id(42).unwrap();
        }

        // Reopen — ID should persist.
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
                })
                .unwrap();
        }

        // Reopen — user should survive.
        let catalog = SystemCatalog::open(&path).unwrap();
        let users = catalog.load_all_users().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "persistent");
        assert_eq!(users[0].user_id, 5);
        assert_eq!(users[0].roles, vec!["readonly", "monitor"]);
    }
}
