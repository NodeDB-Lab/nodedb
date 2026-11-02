// SPDX-License-Identifier: BUSL-1.1

//! Array-catalog redb ops for the `_system.arrays` table.
//!
//! Mirrors the `triggers.rs` shape: typed put/get/delete plus a bulk
//! loader for startup. Keyed by `name` (already globally scoped in the
//! `ArrayCatalogEntry` via `ArrayId`'s tenant field — a second-level
//! tenant prefix only duplicates that information).

use redb::{ReadableDatabase, ReadableTable};

use crate::control::array_catalog::ArrayCatalogEntry;

use super::surrogate_scope::{
    assign_scope, drain_scope, fwd_key, rev_key, take_scope, write_scope,
};
use super::types::{ARRAYS, SURROGATE_PK, SURROGATE_PK_REV, SystemCatalog, catalog_err};

/// Version-two catalog key.  It includes both tenant and database because a
/// database id is only meaningful inside a tenant.
fn array_storage_key(
    tenant_id: nodedb_types::TenantId,
    database_id: nodedb_types::DatabaseId,
    name: &str,
) -> String {
    format!(
        "\u{0}v2:{}:{}:{name}",
        tenant_id.as_u64(),
        database_id.as_u64()
    )
}

fn array_storage_key_for_entry(entry: &ArrayCatalogEntry) -> String {
    array_storage_key(
        entry.array_id.tenant_id,
        entry.array_id.database_id,
        &entry.name,
    )
}

impl SystemCatalog {
    /// Insert or overwrite an array catalog entry.
    pub fn put_array(&self, entry: &ArrayCatalogEntry) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(entry).map_err(|e| catalog_err("serialize array", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key_for_entry(entry);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert array", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Fetch an array by its explicit tenant/database identity.
    pub fn get_array_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<Option<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(ARRAYS)
            .map_err(|e| catalog_err("open arrays", e))?;
        let key = array_storage_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get array", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser array", e))
    }

    /// Delete by explicit tenant/database identity.
    pub fn delete_array_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key(tenant_id, database_id, name);
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove array", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Atomically delete an array catalog row and all of its surrogate
    /// bindings. Keeping these mutations in one redb write transaction means
    /// a failed DROP cannot leave an array without its identity mappings (or
    /// vice versa).
    pub fn delete_array_and_surrogates_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<bool> {
        self.remove_array_row(tenant_id, database_id, name, None)
    }

    /// Atomically delete the array row under `from` and move its surrogate
    /// bindings to `to`. The cells keep their surrogates across a MOVE
    /// TENANT rekey.
    pub fn move_array_surrogates_in_database(
        &self,
        tenant_id: nodedb_types::TenantId,
        from: nodedb_types::DatabaseId,
        to: nodedb_types::DatabaseId,
        name: &str,
    ) -> crate::Result<bool> {
        self.remove_array_row(tenant_id, from, name, Some(to))
    }

    fn remove_array_row(
        &self,
        tenant_id: nodedb_types::TenantId,
        database_id: nodedb_types::DatabaseId,
        name: &str,
        move_to: Option<nodedb_types::DatabaseId>,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("array/surrogate delete txn", e))?;
        let existed;
        {
            let mut arrays = write_txn
                .open_table(ARRAYS)
                .map_err(|e| catalog_err("open arrays", e))?;
            let key = array_storage_key(tenant_id, database_id, name);
            existed = arrays
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove array", e))?
                .is_some();
        }
        let source = nodedb_types::CollectionKey::from_bare(database_id, name);
        if let Some(scope) = take_scope(&write_txn, source, tenant_id)? {
            match move_to {
                None => {
                    drain_scope(&write_txn, scope)?;
                }
                Some(to) => {
                    let target = nodedb_types::CollectionKey::from_bare(to, name);
                    match write_scope(&write_txn, target, tenant_id)? {
                        // The rows keep their scope id: naming it under the
                        // target moves every binding at once.
                        None => assign_scope(&write_txn, target, tenant_id, scope)?,
                        // The target already holds bindings: the moved rows
                        // overwrite it key by key.
                        Some(existing) => {
                            let moved = drain_scope(&write_txn, scope)?;
                            let mut forward = write_txn
                                .open_table(SURROGATE_PK)
                                .map_err(|e| catalog_err("open surrogate_pk", e))?;
                            let mut reverse = write_txn
                                .open_table(SURROGATE_PK_REV)
                                .map_err(|e| catalog_err("open surrogate_pk_rev", e))?;
                            for (pk, surrogate) in moved {
                                forward
                                    .insert(fwd_key(existing, &pk).as_slice(), surrogate)
                                    .map_err(|e| catalog_err("move surrogate_pk", e))?;
                                reverse
                                    .insert(rev_key(existing, surrogate), pk.as_slice())
                                    .map_err(|e| catalog_err("move surrogate_pk_rev", e))?;
                            }
                        }
                    }
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit array/surrogate delete", e))?;
        Ok(existed)
    }

    /// Load every entry. Used by `ArrayCatalog::load_from_catalog` at
    /// startup.
    pub fn load_all_arrays(&self) -> crate::Result<Vec<ArrayCatalogEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        load_all_arrays_in(&read_txn)
    }
}

/// Body of [`SystemCatalog::load_all_arrays`], over an already-open read
/// transaction so the read-only catalog handle can reuse it verbatim.
pub(super) fn load_all_arrays_in(
    read_txn: &redb::ReadTransaction,
) -> crate::Result<Vec<ArrayCatalogEntry>> {
    let table = read_txn
        .open_table(ARRAYS)
        .map_err(|e| catalog_err("open arrays", e))?;
    let mut rows = Vec::new();
    let iter = table.iter().map_err(|e| catalog_err("iter arrays", e))?;
    for row in iter {
        let (_, value) = row.map_err(|e| catalog_err("iter row", e))?;
        let entry: ArrayCatalogEntry =
            zerompk::from_msgpack(value.value()).map_err(|e| catalog_err("deser array", e))?;
        rows.push(entry);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_types::{DatabaseId, TenantId};

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn entry(
        tenant_id: u64,
        database_id: DatabaseId,
        name: &str,
        schema_hash: u64,
    ) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(tenant_id), database_id, name),
            name: name.into(),
            schema_msgpack: vec![0x80],
            schema_hash,
            created_at_ms: 1,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: nodedb_types::Hlc::ZERO,
            incarnation: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn same_name_is_isolated_across_tenants_and_databases() {
        let catalog = catalog();
        let tenant_one_db_one = entry(1, DatabaseId::new(1), "same", 1);
        let tenant_two_db_one = entry(2, DatabaseId::new(1), "same", 2);
        let tenant_one_db_two = entry(1, DatabaseId::new(2), "same", 3);
        catalog.put_array(&tenant_one_db_one).unwrap();
        catalog.put_array(&tenant_two_db_one).unwrap();
        catalog.put_array(&tenant_one_db_two).unwrap();

        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::new(1), "same")
                .unwrap(),
            Some(tenant_one_db_one)
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(2), DatabaseId::new(1), "same")
                .unwrap(),
            Some(tenant_two_db_one)
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::new(2), "same")
                .unwrap(),
            Some(tenant_one_db_two)
        );
    }

    #[test]
    fn a_replacing_put_is_visible_to_both_read_paths() {
        let catalog = catalog();
        catalog
            .put_array(&entry(1, DatabaseId::DEFAULT, "same", 1))
            .unwrap();
        let replacement = entry(1, DatabaseId::DEFAULT, "same", 2);
        catalog.put_array(&replacement).unwrap();

        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, "same")
                .unwrap(),
            Some(replacement.clone())
        );
        assert_eq!(catalog.load_all_arrays().unwrap(), vec![replacement]);
    }

    #[test]
    fn delete_is_scoped_to_one_tenant_and_database() {
        let catalog = catalog();
        let kept = entry(2, DatabaseId::DEFAULT, "same", 1);
        catalog.put_array(&kept).unwrap();

        assert!(
            !catalog
                .delete_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, "same")
                .unwrap()
        );
        assert!(
            !catalog
                .delete_array_in_database(TenantId::new(2), DatabaseId::new(7), "same")
                .unwrap()
        );
        assert_eq!(
            catalog
                .get_array_in_database(TenantId::new(2), DatabaseId::DEFAULT, "same")
                .unwrap(),
            Some(kept)
        );
        assert!(
            catalog
                .delete_array_in_database(TenantId::new(2), DatabaseId::DEFAULT, "same")
                .unwrap()
        );
    }

    /// A MOVE TENANT rekey carries every surrogate binding to the target
    /// database and leaves none under the source.
    #[test]
    fn move_carries_surrogates_to_the_target_database() {
        let catalog = catalog();
        let (from, to) = (DatabaseId::new(3), DatabaseId::new(4));
        catalog.put_array(&entry(1, from, "cells", 1)).unwrap();
        let key = |db| nodedb_types::CollectionKey::from_bare(db, "cells");
        catalog
            .put_surrogate(
                key(from),
                TenantId::new(1),
                b"coord:1",
                nodedb_types::Surrogate::new(9),
            )
            .unwrap();

        assert!(
            catalog
                .move_array_surrogates_in_database(TenantId::new(1), from, to, "cells")
                .unwrap()
        );

        assert!(
            catalog
                .get_array_in_database(TenantId::new(1), from, "cells")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_surrogate_for_pk(key(from), TenantId::new(1), b"coord:1")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            catalog
                .get_surrogate_for_pk(key(to), TenantId::new(1), b"coord:1")
                .unwrap(),
            Some(nodedb_types::Surrogate::new(9))
        );
        assert_eq!(
            catalog
                .get_pk_for_surrogate(key(to), TenantId::new(1), nodedb_types::Surrogate::new(9))
                .unwrap(),
            Some(b"coord:1".to_vec())
        );
    }

    /// A move into a database that already binds keys of the same name merges
    /// the moved bindings into it, both directions.
    #[test]
    fn move_into_a_bound_target_merges_both_directions() {
        let catalog = catalog();
        let (from, to) = (DatabaseId::new(3), DatabaseId::new(4));
        let t1 = TenantId::new(1);
        catalog.put_array(&entry(1, from, "cells", 1)).unwrap();
        let key = |db| nodedb_types::CollectionKey::from_bare(db, "cells");
        let s = nodedb_types::Surrogate::new;
        catalog
            .put_surrogate(key(from), t1, b"moved", s(9))
            .unwrap();
        catalog.put_surrogate(key(to), t1, b"kept", s(4)).unwrap();

        catalog
            .move_array_surrogates_in_database(t1, from, to, "cells")
            .unwrap();

        let mut got = catalog.scan_surrogates_for_collection(key(to), t1).unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![(b"kept".to_vec(), s(4)), (b"moved".to_vec(), s(9))]
        );
        assert_eq!(
            catalog.get_pk_for_surrogate(key(to), t1, s(9)).unwrap(),
            Some(b"moved".to_vec())
        );
        assert!(
            catalog
                .scan_surrogates_for_collection(key(from), t1)
                .unwrap()
                .is_empty()
        );
    }
}
