// SPDX-License-Identifier: BUSL-1.1

//! Catalog operations for the `_system.database_grants` table.
//!
//! Grants are stored as composite string keys:
//! `"{database_id}:{user_id}:{privilege}"` → empty value.
//!
//! This layout lets callers range-scan by database prefix (`"{db}:"`) or
//! enumerate all privileges for one user without secondary indexes.

use nodedb_types::id::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};

use super::types::{DATABASE_GRANTS, SystemCatalog, catalog_err};

/// A single database-level privilege grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseGrant {
    pub database_id: DatabaseId,
    pub user_id: u64,
    pub privilege: String,
}

impl DatabaseGrant {
    fn key(database_id: DatabaseId, user_id: u64, privilege: &str) -> String {
        format!("{}:{}:{}", database_id.as_u64(), user_id, privilege)
    }
}

impl SystemCatalog {
    /// Persist a database-level privilege grant. Idempotent (overwrites silently).
    pub fn put_database_grant(
        &self,
        database_id: DatabaseId,
        user_id: u64,
        privilege: &str,
    ) -> crate::Result<()> {
        let key = DatabaseGrant::key(database_id, user_id, privilege);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("database_grants write txn", e))?;
        {
            let mut table = txn
                .open_table(DATABASE_GRANTS)
                .map_err(|e| catalog_err("open database_grants", e))?;
            table
                .insert(key.as_str(), b"".as_slice())
                .map_err(|e| catalog_err("insert database_grants", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("database_grants commit", e))
    }

    /// Remove a specific database-level privilege grant.
    pub fn delete_database_grant(
        &self,
        database_id: DatabaseId,
        user_id: u64,
        privilege: &str,
    ) -> crate::Result<()> {
        let key = DatabaseGrant::key(database_id, user_id, privilege);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("database_grants delete txn", e))?;
        {
            let mut table = txn
                .open_table(DATABASE_GRANTS)
                .map_err(|e| catalog_err("open database_grants for delete", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete database_grants", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("database_grants delete commit", e))
    }

    /// Return all grants for a given database.
    pub fn list_database_grants(
        &self,
        database_id: DatabaseId,
    ) -> crate::Result<Vec<DatabaseGrant>> {
        let prefix = format!("{}:", database_id.as_u64());
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("database_grants read txn", e))?;
        let table = match txn.open_table(DATABASE_GRANTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(catalog_err("open database_grants for list", e)),
        };
        let mut grants = Vec::new();
        for entry in table
            .range(prefix.as_str()..)
            .map_err(|e| catalog_err("scan database_grants", e))?
        {
            let (k, _v) = entry.map_err(|e| catalog_err("read database_grants entry", e))?;
            let key_str = k.value();
            if !key_str.starts_with(&prefix) {
                break;
            }
            // Parse key: "{database_id}:{user_id}:{privilege}"
            let without_db = &key_str[prefix.len()..];
            if let Some(colon) = without_db.find(':') {
                let user_id: u64 = without_db[..colon].parse().unwrap_or(0);
                let privilege = without_db[colon + 1..].to_string();
                grants.push(DatabaseGrant {
                    database_id,
                    user_id,
                    privilege,
                });
            }
        }
        Ok(grants)
    }

    /// Return the distinct set of database IDs that a user has any grant on.
    /// Always includes `DatabaseId::DEFAULT`.
    pub fn list_user_grant_databases(&self, user_id: u64) -> crate::Result<Vec<DatabaseId>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("database_grants read txn (user)", e))?;
        let table = match txn.open_table(DATABASE_GRANTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(vec![DatabaseId::DEFAULT]);
            }
            Err(e) => return Err(catalog_err("open database_grants for user list", e)),
        };
        let mut db_ids = std::collections::HashSet::new();
        db_ids.insert(DatabaseId::DEFAULT);
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iter database_grants for user", e))?
        {
            let (k, _v) = entry.map_err(|e| catalog_err("read database_grants entry", e))?;
            let key_str = k.value();
            // Key format: "{database_id}:{user_id}:{privilege}"
            let parts: Vec<&str> = key_str.splitn(3, ':').collect();
            if parts.len() == 3
                && let (Ok(db_raw), Ok(uid)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                && uid == user_id
            {
                db_ids.insert(DatabaseId::new(db_raw));
            }
        }
        Ok(db_ids.into_iter().collect())
    }

    /// Check whether a specific grant exists.
    pub fn has_database_grant(
        &self,
        database_id: DatabaseId,
        user_id: u64,
        privilege: &str,
    ) -> crate::Result<bool> {
        let key = DatabaseGrant::key(database_id, user_id, privilege);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("database_grants check txn", e))?;
        let table = match txn.open_table(DATABASE_GRANTS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(catalog_err("open database_grants for check", e)),
        };
        Ok(table
            .get(key.as_str())
            .map_err(|e| catalog_err("get database_grants", e))?
            .is_some())
    }
}
