// SPDX-License-Identifier: BUSL-1.1

//! Metadata counter and tenant operations for the system catalog.

use redb::ReadableTable;

use super::tenant_id_hwm::{HWM_KEY, TENANT_ID_HWM};
use super::types::{
    METADATA, StoredTenant, StoredUser, SystemCatalog, TENANTS, USERS, catalog_err,
};

impl SystemCatalog {
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

    // ── Tenant operations ────────────────────────────────────────────

    pub fn put_tenant(&self, tenant: &StoredTenant) -> crate::Result<()> {
        let key = tenant.tenant_id.to_string();
        let bytes =
            zerompk::to_msgpack_vec(tenant).map_err(|e| catalog_err("serialize tenant", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("open tenants", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert tenant", e))?;
        }
        // Advance the durable tenant-id high-water-mark in the same
        // transaction so the id is never reissued by a later
        // auto-allocation — covers explicitly-chosen ids and ids
        // replicated from a leader on follower-side applies alike.
        {
            let mut hwm = write_txn
                .open_table(TENANT_ID_HWM)
                .map_err(|e| catalog_err("open tenant_id_hwm", e))?;
            let cur = hwm
                .get(HWM_KEY)
                .map_err(|e| catalog_err("get tenant_id_hwm", e))?
                .map(|v| v.value())
                .unwrap_or(0);
            if tenant.tenant_id > cur {
                hwm.insert(HWM_KEY, tenant.tenant_id)
                    .map_err(|e| catalog_err("insert tenant_id_hwm", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Atomically persist a tenant and its authoritative administrator.
    pub fn put_tenant_with_admin(
        &self,
        tenant: &StoredTenant,
        admin: &StoredUser,
    ) -> crate::Result<()> {
        if tenant.tenant_id != admin.tenant_id || tenant.admin_username != admin.username {
            return Err(catalog_err(
                "validate tenant administrator",
                "tenant and administrator identities do not match",
            ));
        }
        let tenant_key = tenant.tenant_id.to_string();
        let tenant_bytes =
            zerompk::to_msgpack_vec(tenant).map_err(|e| catalog_err("serialize tenant", e))?;
        let admin_bytes =
            zerompk::to_msgpack_vec(admin).map_err(|e| catalog_err("serialize admin", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let admin_exists = {
            let users = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            match users
                .get(admin.username.as_str())
                .map_err(|e| catalog_err("get admin", e))?
            {
                Some(existing) if existing.value() == admin_bytes.as_slice() => true,
                Some(_) => {
                    return Err(catalog_err(
                        "insert admin",
                        format!("user '{}' already exists", admin.username),
                    ));
                }
                None => false,
            }
        };
        let tenant_exists = {
            let tenants = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("open tenants", e))?;
            let existing_id = tenants
                .get(tenant_key.as_str())
                .map_err(|e| catalog_err("get tenant", e))?;
            let exact_id = match existing_id {
                Some(existing) if existing.value() == tenant_bytes.as_slice() => true,
                Some(_) => {
                    return Err(catalog_err(
                        "insert tenant",
                        format!("tenant id '{}' already exists", tenant.tenant_id),
                    ));
                }
                None => false,
            };
            for row in tenants
                .iter()
                .map_err(|e| catalog_err("iterate tenants", e))?
            {
                let (_, value) = row.map_err(|e| catalog_err("read tenant", e))?;
                let existing: StoredTenant = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode tenant", e))?;
                if existing.name == tenant.name && existing.tenant_id != tenant.tenant_id {
                    return Err(catalog_err(
                        "insert tenant",
                        format!("tenant name '{}' already exists", tenant.name),
                    ));
                }
            }
            exact_id
        };
        match (tenant_exists, admin_exists) {
            (true, true) => return Ok(()),
            (true, false) => {
                return Err(catalog_err(
                    "insert tenant admin",
                    "tenant exists without its atomic administrator",
                ));
            }
            (false, true) => {
                return Err(catalog_err(
                    "insert tenant admin",
                    "administrator exists without its atomic tenant",
                ));
            }
            (false, false) => {}
        }
        {
            let mut users = write_txn
                .open_table(USERS)
                .map_err(|e| catalog_err("open users", e))?;
            users
                .insert(admin.username.as_str(), admin_bytes.as_slice())
                .map_err(|e| catalog_err("insert admin", e))?;
        }
        {
            let mut metadata = write_txn
                .open_table(METADATA)
                .map_err(|e| catalog_err("open metadata", e))?;
            let current = metadata
                .get("next_user_id")
                .map_err(|e| catalog_err("get next_user_id", e))?
                .and_then(|value| {
                    let bytes = value.value();
                    (bytes.len() == 8).then(|| {
                        let mut array = [0u8; 8];
                        array.copy_from_slice(bytes);
                        u64::from_le_bytes(array)
                    })
                })
                .unwrap_or(1);
            let next = admin.user_id.saturating_add(1);
            if next > current {
                metadata
                    .insert("next_user_id", next.to_le_bytes().as_slice())
                    .map_err(|e| catalog_err("insert next_user_id", e))?;
            }
        }
        {
            let mut tenants = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("open tenants", e))?;
            tenants
                .insert(tenant_key.as_str(), tenant_bytes.as_slice())
                .map_err(|e| catalog_err("insert tenant", e))?;
        }
        {
            let mut hwm = write_txn
                .open_table(TENANT_ID_HWM)
                .map_err(|e| catalog_err("open tenant_id_hwm", e))?;
            let current = hwm
                .get(HWM_KEY)
                .map_err(|e| catalog_err("get tenant_id_hwm", e))?
                .map(|value| value.value())
                .unwrap_or(0);
            if tenant.tenant_id > current {
                hwm.insert(HWM_KEY, tenant.tenant_id)
                    .map_err(|e| catalog_err("insert tenant_id_hwm", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Hard-delete a tenant identity record. Returns `true` if a row existed.
    pub fn delete_tenant(&self, tenant_id: u64) -> crate::Result<bool> {
        let key = tenant_id.to_string();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(TENANTS)
                .map_err(|e| catalog_err("open tenants", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove tenant", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Look up a tenant identity record by its display name.
    ///
    /// Tenants are keyed by numeric id in the catalog, so a name lookup is a
    /// full scan; the tenant set is small (admin-managed) so this is cheap.
    pub fn find_tenant_by_name(&self, name: &str) -> crate::Result<Option<StoredTenant>> {
        Ok(self
            .load_all_tenants()?
            .into_iter()
            .find(|t| t.name == name))
    }

    pub fn load_all_tenants(&self) -> crate::Result<Vec<StoredTenant>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TENANTS)
            .map_err(|e| catalog_err("open tenants", e))?;
        let mut tenants = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range tenants", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read tenant", e))?;
            tenants.push(
                zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser tenant", e))?,
            );
        }
        Ok(tenants)
    }
}
