//! User CRUD operations for the system catalog.

use super::types::{StoredUser, SystemCatalog, USERS, catalog_err};

impl SystemCatalog {
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
            let user: StoredUser = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize user", e))?;
            users.push(user);
        }

        Ok(users)
    }

    /// Look up a single user by username. Matches the shape of
    /// `get_collection` / `get_trigger` / etc. Used by the
    /// replication applier to implement `DeactivateUser`.
    pub fn get_user(&self, username: &str) -> crate::Result<Option<StoredUser>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(USERS)
            .map_err(|e| catalog_err("open users", e))?;
        match table.get(username) {
            Ok(Some(value)) => {
                let user: StoredUser = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize user", e))?;
                Ok(Some(user))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get user", e)),
        }
    }

    /// Write a user record to the catalog (insert or update).
    pub fn put_user(&self, user: &StoredUser) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(user).map_err(|e| catalog_err("serialize user", e))?;

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
}
