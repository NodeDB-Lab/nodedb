// SPDX-License-Identifier: BUSL-1.1

//! Apply tenant catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{
    StoredCollection, StoredTenant, StoredUser, SystemCatalog, catalog_err,
};
use crate::types::DatabaseId;

pub fn put(stored: &StoredTenant, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_tenant(stored).map_err(|e| {
        catalog_err(
            &format!("put_tenant '{}' (tenant {})", stored.name, stored.tenant_id),
            e,
        )
    })
}

/// Apply a `PutTenantWithAdmin` entry.
///
/// Reports `false` only when both rows already exist unchanged, which ends the
/// DDL without post-apply. A write error raises so the applier wedges.
pub fn put_with_admin(
    tenant: &StoredTenant,
    admin: &StoredUser,
    catalog: &SystemCatalog,
) -> crate::Result<bool> {
    catalog.put_tenant_with_admin(tenant, admin).map_err(|e| {
        catalog_err(
            &format!(
                "put_tenant_with_admin '{}' (tenant {}, admin '{}')",
                tenant.name, tenant.tenant_id, admin.username
            ),
            e,
        )
    })
}

/// Apply a `DeleteTenant` entry — remove the tenant row and its quota
/// rows in every database.
pub fn delete(tenant_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_tenant(tenant_id)
        .map_err(|e| catalog_err(&format!("delete_tenant (tenant {tenant_id})"), e))?;
    // A stale quota row keeps consuming the database's tenant ceiling.
    super::quota::purge_tenant_scope(tenant_id, catalog)
}

/// Apply `MoveTenantCutover`: atomically re-key all `collections` from
/// `source_db_id` to `target_db_id`, then delete each one from the source.
///
/// This is the single Raft proposal that makes the cutover phase of
/// `MOVE TENANT` atomic on every node. A per-collection failure raises: a
/// tenant split across two databases is divergence from the quorum.
pub fn move_cutover(
    tenant_id: u64,
    source_db_id: u64,
    target_db_id: u64,
    collections: &[StoredCollection],
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let src = DatabaseId::new(source_db_id);
    let tgt = DatabaseId::new(target_db_id);

    for coll in collections {
        // Write to target database.
        let mut target_coll = coll.clone();
        target_coll.database_id = tgt;
        // The cutover re-issued the rows as new writes in the row's mode.
        target_coll.storage_conversion = None;
        catalog.put_collection(tgt, &target_coll).map_err(|e| {
            catalog_err(
                &format!(
                    "move_cutover write of '{}' into target database {target_db_id} \
                     (tenant {tenant_id})",
                    coll.name
                ),
                e,
            )
        })?;
        super::owner::put_parent_owner(
            object_type::COLLECTION,
            tgt.as_u64(),
            coll.tenant_id,
            &coll.name,
            &coll.owner,
            catalog,
        )?;
        // Delete from source database using the collection's own tenant_id,
        // which is the actual storage key component.
        catalog
            .delete_collection(src, coll.tenant_id, &coll.name)
            .map_err(|e| {
                catalog_err(
                    &format!(
                        "move_cutover delete of '{}' from source database {source_db_id} \
                         (tenant {})",
                        coll.name, coll.tenant_id
                    ),
                    e,
                )
            })?;
        // Remove the stale source owner row so it does not outlive the
        // collection it was for — the primary row is already gone from
        // `src` above.
        super::owner::delete_parent_owner(
            object_type::COLLECTION,
            src.as_u64(),
            coll.tenant_id,
            &coll.name,
            catalog,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::credential::store::CredentialStore;

    fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
        );
        (store, tmp)
    }

    fn tenant(tenant_id: u64, name: &str, admin: &str) -> StoredTenant {
        StoredTenant {
            tenant_id,
            name: name.into(),
            created_at: 0,
            is_active: true,
            admin_username: admin.into(),
        }
    }

    fn admin_user(user_id: u64, username: &str, tenant_id: u64) -> StoredUser {
        StoredUser {
            user_id,
            username: username.into(),
            tenant_id,
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
        }
    }

    /// A rejected write must not read as the benign "row already existed"
    /// outcome — `Ok(false)` acks a committed entry this node never applied.
    #[test]
    fn put_with_admin_raises_when_the_catalog_write_fails() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let stored = tenant(10, "acme", "acme_admin");
        // Identity mismatch: `put_tenant_with_admin` rejects the pair.
        let mismatched = admin_user(41, "acme_admin", 11);

        let error = put_with_admin(&stored, &mismatched, catalog)
            .expect_err("a rejected tenant/admin write must raise");
        assert!(error.to_string().contains("acme"), "{error}");
        assert!(catalog.find_tenant_by_name("acme").unwrap().is_none());
    }

    /// The `false` report is reserved for an idempotent replay of rows that
    /// already exist byte-for-byte.
    #[test]
    fn put_with_admin_reports_false_only_for_an_existing_row() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let stored = tenant(12, "globex", "globex_admin");
        let admin = admin_user(42, "globex_admin", 12);

        assert!(
            put_with_admin(&stored, &admin, catalog).expect("first write"),
            "the first write applies the entry"
        );
        assert!(
            !put_with_admin(&stored, &admin, catalog).expect("idempotent replay"),
            "a replay of identical rows wrote nothing"
        );
        assert!(catalog.find_tenant_by_name("globex").unwrap().is_some());
    }

    /// A failed cutover leaves the tenant split across two databases, so the
    /// arm raises instead of finishing the remaining collections.
    #[test]
    fn move_cutover_raises_instead_of_completing_partially() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();
        let src = DatabaseId::new(1);
        let mut first = StoredCollection::stamped_for_test(5, "orders", "mover");
        first.database_id = src;
        let mut second = StoredCollection::stamped_for_test(5, "invoices", "mover");
        second.database_id = src;
        for coll in [&first, &second] {
            apply_to(
                &CatalogEntry::PutCollection(Box::new(coll.clone())),
                catalog,
            )
            .expect("seed source collection");
        }

        catalog.fail_next_collection_write_for_test();
        let error = move_cutover(5, 1, 2, &[first, second], catalog)
            .expect_err("a failed cutover write must raise");
        assert!(error.to_string().contains("orders"), "{error}");

        // Neither collection reached the target database.
        assert!(
            catalog
                .get_collection(DatabaseId::new(2), 5, "orders")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_collection(DatabaseId::new(2), 5, "invoices")
                .unwrap()
                .is_none()
        );
        // The source keeps both rows rather than losing half the tenant.
        assert!(catalog.get_collection(src, 5, "orders").unwrap().is_some());
        assert!(
            catalog
                .get_collection(src, 5, "invoices")
                .unwrap()
                .is_some()
        );
    }
}
