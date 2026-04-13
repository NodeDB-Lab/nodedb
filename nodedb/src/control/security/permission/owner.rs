//! Object ownership CRUD on `PermissionStore`.

use crate::control::security::catalog::{StoredOwner, SystemCatalog};
use crate::types::TenantId;

use super::store::PermissionStore;
use super::types::owner_key;

impl PermissionStore {
    /// Set the owner of an object. Direct CRUD path used by
    /// single-node mode and tests; cluster mode flows through
    /// `CatalogEntry::PutOwner` (or via `install_replicated_owner`
    /// from a parent variant's post_apply).
    pub fn set_owner(
        &self,
        object_type: &str,
        tenant_id: TenantId,
        object_name: &str,
        owner_username: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let key = owner_key(object_type, tenant_id.as_u32(), object_name);

        if let Some(catalog) = catalog {
            catalog.put_owner(&StoredOwner {
                object_type: object_type.to_string(),
                object_name: object_name.to_string(),
                tenant_id: tenant_id.as_u32(),
                owner_username: owner_username.to_string(),
            })?;
        }

        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned — recovering data");
                p.into_inner()
            }
        };
        owners.insert(key, owner_username.to_string());
        Ok(())
    }

    /// Remove an ownership record.
    pub fn remove_owner(
        &self,
        object_type: &str,
        tenant_id: TenantId,
        object_name: &str,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let key = owner_key(object_type, tenant_id.as_u32(), object_name);

        if let Some(catalog) = catalog {
            catalog.delete_owner(object_type, tenant_id.as_u32(), object_name)?;
        }

        let mut owners = match self.owners.write() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned — recovering data");
                p.into_inner()
            }
        };
        owners.remove(&key);
        Ok(())
    }

    /// Get the owner of an object.
    pub fn get_owner(
        &self,
        object_type: &str,
        tenant_id: TenantId,
        object_name: &str,
    ) -> Option<String> {
        let key = owner_key(object_type, tenant_id.as_u32(), object_name);
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(p) => {
                tracing::error!("owner store lock poisoned — recovering data");
                p.into_inner()
            }
        };
        owners.get(&key).cloned()
    }

    /// List all objects of a given type owned in a tenant.
    /// Returns `(object_name, owner_username)` pairs.
    pub fn list_owners(&self, object_type: &str, tenant_id: TenantId) -> Vec<(String, String)> {
        let prefix = format!("{object_type}:{}:", tenant_id.as_u32());
        let owners = match self.owners.read() {
            Ok(o) => o,
            Err(p) => p.into_inner(),
        };
        owners
            .iter()
            .filter_map(|(key, owner)| {
                key.strip_prefix(&prefix)
                    .map(|name| (name.to_string(), owner.clone()))
            })
            .collect()
    }
}
