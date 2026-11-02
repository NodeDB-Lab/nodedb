// SPDX-License-Identifier: BUSL-1.1

//! Persistence glue for the array catalog.
//!
//! Mirrors the `trigger` / `sequence` registry pattern: the
//! [`SystemCatalog`] owns the redb table and exposes typed read/write
//! helpers (see `control/security/catalog/arrays.rs`). This module
//! provides the bulk `load_all` entry and the `persist` / `remove`
//! wrappers the `PutArray` / `DeleteArray` apply writes through.

use nodedb_types::NodeDbError;

use crate::control::security::catalog::types::SystemCatalog;

use super::entry::ArrayCatalogEntry;
use super::registry::ArrayCatalog;

/// Load every persisted array entry into an in-memory registry.
/// Called once at server startup.
pub fn load_all(catalog: &SystemCatalog) -> Result<ArrayCatalog, NodeDbError> {
    let mut reg = ArrayCatalog::new();
    let entries = catalog.load_all_arrays().map_err(NodeDbError::from)?;
    for entry in entries {
        reg.register(entry)?;
    }
    Ok(reg)
}

/// Register every entry of a boot-time `load_all_arrays` read into `reg`.
///
/// Boot fails open: a read error or an entry that does not register logs a
/// warning and the rest boots. `register` refuses an id already present, so
/// a second load never duplicates an entry.
pub fn register_loaded(reg: &mut ArrayCatalog, loaded: crate::Result<Vec<ArrayCatalogEntry>>) {
    match loaded {
        Ok(entries) => {
            for entry in entries {
                if let Err(e) = reg.register(entry) {
                    tracing::warn!(error = %e, "failed to register array at startup");
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to load _system.arrays at startup");
        }
    }
}

/// Persist (or overwrite) a single entry.
pub fn persist(catalog: &SystemCatalog, entry: &ArrayCatalogEntry) -> Result<(), NodeDbError> {
    catalog.put_array(entry).map_err(NodeDbError::from)
}

/// Remove by explicit array identity.
pub fn remove(
    catalog: &SystemCatalog,
    array_id: &nodedb_array::types::ArrayId,
) -> Result<(), NodeDbError> {
    catalog
        .delete_array_in_database(array_id.tenant_id, array_id.database_id, &array_id.name)
        .map(|_existed| ())
        .map_err(NodeDbError::from)
}

/// Remove the array under `array_id` and move every surrogate mapping to
/// `to`, in one durable transaction.
pub fn move_with_surrogates(
    catalog: &SystemCatalog,
    array_id: &nodedb_array::types::ArrayId,
    to: nodedb_types::DatabaseId,
) -> Result<(), NodeDbError> {
    catalog
        .move_array_surrogates_in_database(
            array_id.tenant_id,
            array_id.database_id,
            to,
            &array_id.name,
        )
        .map(|_existed| ())
        .map_err(NodeDbError::from)
}

/// Remove an array and every surrogate mapping in one durable transaction.
pub fn remove_with_surrogates(
    catalog: &SystemCatalog,
    array_id: &nodedb_array::types::ArrayId,
) -> Result<(), NodeDbError> {
    catalog
        .delete_array_and_surrogates_in_database(
            array_id.tenant_id,
            array_id.database_id,
            &array_id.name,
        )
        .map(|_existed| ())
        .map_err(NodeDbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::SystemCatalog;
    use nodedb_array::types::ArrayId;
    use nodedb_types::TenantId;
    use tempfile::TempDir;

    fn entry(name: &str) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::new(TenantId::new(7), name),
            name: name.to_string(),
            schema_msgpack: vec![0x80],
            schema_hash: 0xCAFE_F00D,
            created_at_ms: 1_700_000_000_000,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: nodedb_types::Hlc::ZERO,
            incarnation: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn same_tenant_same_name_in_two_databases_survives_catalog_roundtrip() {
        let dir = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let mut db1 = entry("same");
        db1.array_id =
            ArrayId::in_database(TenantId::new(7), nodedb_types::DatabaseId::new(1), "same");
        let mut db2 = entry("same");
        db2.array_id =
            ArrayId::in_database(TenantId::new(7), nodedb_types::DatabaseId::new(2), "same");
        persist(&cat, &db1).unwrap();
        persist(&cat, &db2).unwrap();
        assert_eq!(
            cat.get_array_in_database(TenantId::new(7), nodedb_types::DatabaseId::new(1), "same")
                .unwrap(),
            Some(db1.clone())
        );
        assert_eq!(
            cat.get_array_in_database(TenantId::new(7), nodedb_types::DatabaseId::new(2), "same")
                .unwrap(),
            Some(db2.clone())
        );
        remove(&cat, &db1.array_id).unwrap();
        assert!(
            cat.get_array_in_database(TenantId::new(7), nodedb_types::DatabaseId::new(1), "same")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            cat.get_array_in_database(TenantId::new(7), nodedb_types::DatabaseId::new(2), "same")
                .unwrap(),
            Some(db2)
        );
    }

    #[test]
    fn persist_then_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        persist(&cat, &entry("a")).unwrap();
        persist(&cat, &entry("b")).unwrap();

        let reg = load_all(&cat).unwrap();
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.lookup_by_id(&entry("a").array_id), Some(entry("a")));
        assert_eq!(reg.lookup_by_id(&entry("b").array_id), Some(entry("b")));

        remove(&cat, &entry("a").array_id).unwrap();
        let reg = load_all(&cat).unwrap();
        assert_eq!(reg.len(), 1);
        assert!(reg.lookup_by_id(&entry("a").array_id).is_none());
        assert!(reg.lookup_by_id(&entry("b").array_id).is_some());
    }

    #[test]
    fn drop_finalization_removes_array_and_surrogate_bindings_together() {
        let dir = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        let array = entry("cells");
        persist(&cat, &array).unwrap();
        cat.put_surrogate(
            nodedb_types::CollectionKey::from_bare(array.array_id.database_id, &array.name),
            array.array_id.tenant_id,
            b"coord:1",
            nodedb_types::Surrogate::new(42),
        )
        .unwrap();

        // Before successful Data-Plane DROP, the deferred finalizer leaves
        // both durable sides present, so a failed broadcast needs no mapping
        // reconstruction.
        assert!(
            cat.get_array_in_database(
                array.array_id.tenant_id,
                array.array_id.database_id,
                &array.name
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            cat.get_surrogate_for_pk(
                nodedb_types::CollectionKey::from_bare(array.array_id.database_id, &array.name),
                array.array_id.tenant_id,
                b"coord:1"
            )
            .unwrap(),
            Some(nodedb_types::Surrogate::new(42))
        );

        remove_with_surrogates(&cat, &array.array_id).unwrap();
        assert!(
            cat.get_array_in_database(
                array.array_id.tenant_id,
                array.array_id.database_id,
                &array.name
            )
            .unwrap()
            .is_none()
        );
        assert!(
            cat.get_surrogate_for_pk(
                nodedb_types::CollectionKey::from_bare(array.array_id.database_id, &array.name),
                array.array_id.tenant_id,
                b"coord:1"
            )
            .unwrap()
            .is_none()
        );
    }
}
