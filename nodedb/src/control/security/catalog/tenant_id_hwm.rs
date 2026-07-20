// SPDX-License-Identifier: BUSL-1.1

//! Durable tenant-id allocator for the `_system.tenant_id_hwm` table.
//!
//! Singleton table — one row keyed `"global"` holding the highest
//! tenant id ever allocated. The high-water-mark is monotonic: it is
//! advanced on every tenant write and never lowered by `DROP TENANT`,
//! so a tenant id is never reused for a different tenant across a drop
//! or a restart. This is the same durable-counter idiom the global
//! surrogate identity uses; see [`super::surrogate_hwm`].
//!
//! Auto-allocation derives the next id from the max of this counter
//! and the ids actually present in `_system.tenants`, so a database
//! whose tenants predate the counter self-heals on first allocation
//! instead of colliding with an existing tenant.

use redb::ReadableTable;

use super::types::{SystemCatalog, TENANTS, catalog_err};

/// Redb table: singleton `"global"` -> highest allocated tenant id (`u64`).
pub(super) const TENANT_ID_HWM: redb::TableDefinition<&str, u64> =
    redb::TableDefinition::new("_system.tenant_id_hwm");

/// Singleton row key.
pub(super) const HWM_KEY: &str = "global";

/// Lowest id handed to an auto-allocated tenant. `0` is the system
/// tenant and `1` is the bootstrap/default tenant; user tenants start
/// at `2` so neither reserved slot is ever handed out by allocation.
pub(crate) const FIRST_USER_TENANT_ID: u64 = 2;

impl SystemCatalog {
    /// Atomically allocate the next tenant id.
    ///
    /// Durable and monotonic: the returned id is strictly greater than
    /// every id previously allocated or currently stored, and the
    /// counter is never reused after a `DROP TENANT`. Self-heals on
    /// first use by taking the max of the stored hwm and the highest id
    /// in `_system.tenants`, so tenants created before this counter
    /// existed can never collide with a fresh allocation.
    pub fn allocate_tenant_id(&self) -> crate::Result<u64> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("tenant_id_hwm write txn", e))?;
        let next;
        {
            // Highest id currently materialised in the tenants table.
            let mut max_existing = 0u64;
            {
                let tenants = write_txn
                    .open_table(TENANTS)
                    .map_err(|e| catalog_err("open tenants", e))?;
                for entry in tenants
                    .range::<&str>(..)
                    .map_err(|e| catalog_err("range tenants", e))?
                {
                    let (key, _) = entry.map_err(|e| catalog_err("read tenant", e))?;
                    if let Ok(id) = key.value().parse::<u64>() {
                        max_existing = max_existing.max(id);
                    }
                }
            }
            let mut hwm = write_txn
                .open_table(TENANT_ID_HWM)
                .map_err(|e| catalog_err("open tenant_id_hwm", e))?;
            let stored = hwm
                .get(HWM_KEY)
                .map_err(|e| catalog_err("get tenant_id_hwm", e))?
                .map(|v| v.value())
                .unwrap_or(0);
            next = stored.max(max_existing).max(FIRST_USER_TENANT_ID - 1) + 1;
            hwm.insert(HWM_KEY, next)
                .map_err(|e| catalog_err("insert tenant_id_hwm", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("tenant_id_hwm commit", e))?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::{StoredTenant, StoredUser};

    fn open() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let catalog = SystemCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    #[test]
    fn first_allocation_skips_reserved_slots() {
        let (_dir, catalog) = open();
        assert_eq!(catalog.allocate_tenant_id().unwrap(), FIRST_USER_TENANT_ID);
    }

    #[test]
    fn allocations_are_strictly_increasing_and_distinct() {
        let (_dir, catalog) = open();
        let a = catalog.allocate_tenant_id().unwrap();
        let b = catalog.allocate_tenant_id().unwrap();
        let c = catalog.allocate_tenant_id().unwrap();
        assert!(a < b && b < c, "{a} {b} {c}");
    }

    #[test]
    fn never_reuses_id_after_delete() {
        let (_dir, catalog) = open();
        let id = catalog.allocate_tenant_id().unwrap();
        catalog
            .put_tenant(&StoredTenant {
                tenant_id: id,
                name: "t".into(),
                created_at: 0,
                is_active: true,
                admin_username: String::new(),
            })
            .unwrap();
        catalog.delete_tenant(id).unwrap();
        // The dropped id must not be handed out again.
        assert!(catalog.allocate_tenant_id().unwrap() > id);
    }

    #[test]
    fn self_heals_against_preexisting_tenants() {
        let (_dir, catalog) = open();
        // A tenant row written without ever bumping the counter (the
        // pre-fix world): insert directly so the hwm stays at 0, then
        // assert allocation still skips past the existing id.
        let bytes = zerompk::to_msgpack_vec(&StoredTenant {
            tenant_id: 500,
            name: "legacy".into(),
            created_at: 0,
            is_active: true,
            admin_username: String::new(),
        })
        .unwrap();
        let txn = catalog.db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TENANTS).unwrap();
            t.insert("500", bytes.as_slice()).unwrap();
        }
        txn.commit().unwrap();
        assert!(catalog.allocate_tenant_id().unwrap() > 500);
    }

    #[test]
    fn atomic_tenant_admin_write_advances_user_id_hwm() {
        let (_dir, catalog) = open();
        let tenant = StoredTenant {
            tenant_id: 10,
            name: "atomic".into(),
            created_at: 0,
            is_active: true,
            admin_username: "atomic_admin".into(),
        };
        let admin = StoredUser {
            user_id: 41,
            username: "atomic_admin".into(),
            tenant_id: 10,
            password_hash: String::new(),
            scram_salt: Vec::new(),
            scram_salted_password: Vec::new(),
            roles: vec!["tenant_admin".into()],
            is_superuser: false,
            is_active: true,
            is_service_account: false,
            created_at: 0,
            updated_at: 0,
            password_expires_at: 0,
            must_change_password: false,
            password_changed_at: 0,
            default_database_id: 0,
            accessible_databases: Vec::new(),
        };
        let mut mismatched_admin = admin.clone();
        mismatched_admin.tenant_id = 11;
        assert!(
            catalog
                .put_tenant_with_admin(&tenant, &mismatched_admin)
                .is_err()
        );

        catalog.put_tenant_with_admin(&tenant, &admin).unwrap();
        catalog.put_tenant_with_admin(&tenant, &admin).unwrap();

        let mut conflicting_tenant = tenant.clone();
        conflicting_tenant.tenant_id = 11;
        conflicting_tenant.name = "conflict".into();
        let mut conflicting_admin = admin.clone();
        conflicting_admin.user_id = 42;
        conflicting_admin.tenant_id = 11;
        assert!(
            catalog
                .put_tenant_with_admin(&conflicting_tenant, &conflicting_admin)
                .is_err()
        );

        assert_eq!(catalog.load_next_user_id().unwrap(), 42);
        assert_eq!(
            catalog.get_user("atomic_admin").unwrap().unwrap().tenant_id,
            10
        );
        assert!(
            catalog
                .load_all_tenants()
                .unwrap()
                .into_iter()
                .all(|stored| stored.tenant_id != 11)
        );
    }

    #[test]
    fn put_tenant_with_explicit_id_advances_hwm() {
        let (_dir, catalog) = open();
        // An explicitly-chosen high id must push the hwm forward so a
        // later auto-allocation never collides with it.
        catalog
            .put_tenant(&StoredTenant {
                tenant_id: 10,
                name: "explicit".into(),
                created_at: 0,
                is_active: true,
                admin_username: String::new(),
            })
            .unwrap();
        assert_eq!(catalog.allocate_tenant_id().unwrap(), 11);
    }
}
