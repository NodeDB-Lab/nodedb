//! Metadata counter and tenant operations for the system catalog.

use super::types::{METADATA, StoredTenant, SystemCatalog, TENANTS, catalog_err};

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
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Hard-delete a tenant identity record. Returns `true` if a row existed.
    pub fn delete_tenant(&self, tenant_id: u32) -> crate::Result<bool> {
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
