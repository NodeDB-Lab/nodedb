// SPDX-License-Identifier: BUSL-1.1

//! API key, role, permission, and ownership operations for the system catalog.

use super::types::{
    API_KEYS, OWNERS, PERMISSIONS, ROLES, StoredApiKey, StoredOwner, StoredPermission, StoredRole,
    SystemCatalog, catalog_err, owner_key,
};

impl SystemCatalog {
    // ── API Key operations ──────────────────────────────────────────

    /// Write an API key record.
    pub fn put_api_key(&self, key: &StoredApiKey) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(key).map_err(|e| catalog_err("serialize api key", e))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("open api_keys", e))?;
            table
                .insert(key.key_id.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert api key", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load all API keys.
    pub fn load_all_api_keys(&self) -> crate::Result<Vec<StoredApiKey>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(API_KEYS)
            .map_err(|e| catalog_err("open api_keys", e))?;

        let mut keys = Vec::new();
        let range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range api_keys", e))?;
        for entry in range {
            let (_, value) = entry.map_err(|e| catalog_err("read entry", e))?;
            let key: StoredApiKey = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize api key", e))?;
            keys.push(key);
        }

        Ok(keys)
    }

    /// Look up a single API key by id. Used by the replication
    /// applier to implement `RevokeApiKey` without re-loading the
    /// full key table.
    pub fn get_api_key(&self, key_id: &str) -> crate::Result<Option<StoredApiKey>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(API_KEYS)
            .map_err(|e| catalog_err("open api_keys", e))?;
        match table.get(key_id) {
            Ok(Some(value)) => {
                let key: StoredApiKey = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize api key", e))?;
                Ok(Some(key))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get api key", e)),
        }
    }

    /// Delete an API key by key_id.
    pub fn delete_api_key(&self, key_id: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(API_KEYS)
                .map_err(|e| catalog_err("open api_keys", e))?;
            table
                .remove(key_id)
                .map_err(|e| catalog_err("remove api key", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    // ── Role operations ──────────────────────────────────────────────

    pub fn put_role(&self, role: &StoredRole) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(role).map_err(|e| catalog_err("serialize role", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("open roles", e))?;
            table
                .insert(role.name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert role", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_role(&self, name: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ROLES)
                .map_err(|e| catalog_err("open roles", e))?;
            table
                .remove(name)
                .map_err(|e| catalog_err("remove role", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_roles(&self) -> crate::Result<Vec<StoredRole>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ROLES)
            .map_err(|e| catalog_err("open roles", e))?;
        let mut roles = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range roles", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read role", e))?;
            roles.push(
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser role", e))?,
            );
        }
        Ok(roles)
    }

    // ── Permission operations ───────────────────────────────────────

    /// Key format: "{target}:{grantee}:{permission}"
    fn permission_key(target: &str, grantee: &str, permission: &str) -> String {
        format!("{target}:{grantee}:{permission}")
    }

    pub fn put_permission(&self, perm: &StoredPermission) -> crate::Result<()> {
        let key = Self::permission_key(&perm.target, &perm.grantee, &perm.permission);
        let bytes = zerompk::to_msgpack_vec(perm).map_err(|e| catalog_err("serialize perm", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("open perms", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert perm", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_permission(
        &self,
        target: &str,
        grantee: &str,
        permission: &str,
    ) -> crate::Result<()> {
        let key = Self::permission_key(target, grantee, permission);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(PERMISSIONS)
                .map_err(|e| catalog_err("open perms", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove perm", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_permissions(&self) -> crate::Result<Vec<StoredPermission>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(PERMISSIONS)
            .map_err(|e| catalog_err("open perms", e))?;
        let mut perms = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range perms", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read perm", e))?;
            perms.push(
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser perm", e))?,
            );
        }
        Ok(perms)
    }

    // ── Ownership operations ────────────────────────────────────────

    pub fn put_owner(&self, owner: &StoredOwner) -> crate::Result<()> {
        let key = owner_key(
            &owner.object_type,
            owner.database_id,
            owner.tenant_id,
            &owner.object_name,
        );
        let bytes =
            zerompk::to_msgpack_vec(owner).map_err(|e| catalog_err("serialize owner", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("open owners", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert owner", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn delete_owner(
        &self,
        object_type: &str,
        database_id: u64,
        tenant_id: u64,
        object_name: &str,
    ) -> crate::Result<()> {
        let key = owner_key(object_type, database_id, tenant_id, object_name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(OWNERS)
                .map_err(|e| catalog_err("open owners", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete owner", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    pub fn load_all_owners(&self) -> crate::Result<Vec<StoredOwner>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(OWNERS)
            .map_err(|e| catalog_err("open owners", e))?;
        let mut owners = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range owners", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read owner", e))?;
            owners.push(
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser owner", e))?,
            );
        }
        Ok(owners)
    }

    /// Every `StoredOwner` row owned by `owner_username` within
    /// `tenant_id`, across every `object_type`. Used by `DROP USER` to
    /// enumerate the objects that must be reassigned before the user
    /// row is removed, so no dangling `owner → user` reference can
    /// survive the drop and brick the next boot's integrity check.
    pub fn owners_for_user(
        &self,
        owner_username: &str,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredOwner>> {
        Ok(self
            .load_all_owners()?
            .into_iter()
            .filter(|o| o.owner_username == owner_username && o.tenant_id == tenant_id)
            .collect())
    }
}
