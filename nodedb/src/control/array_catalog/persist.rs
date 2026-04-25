//! Persistence glue for the array catalog.
//!
//! Mirrors the `trigger` / `sequence` registry pattern: the
//! [`SystemCatalog`] owns the redb table and exposes typed read/write
//! helpers (see `control/security/catalog/arrays.rs`). This module
//! provides the bulk `load_all` entry used at server startup and the
//! `persist` / `remove` wrappers used by DDL handlers.

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

/// Persist (or overwrite) a single entry.
pub fn persist(catalog: &SystemCatalog, entry: &ArrayCatalogEntry) -> Result<(), NodeDbError> {
    catalog.put_array(entry).map_err(NodeDbError::from)
}

/// Remove by name.
pub fn remove(catalog: &SystemCatalog, name: &str) -> Result<(), NodeDbError> {
    catalog
        .delete_array(name)
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
        }
    }

    #[test]
    fn persist_then_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();

        persist(&cat, &entry("a")).unwrap();
        persist(&cat, &entry("b")).unwrap();

        let reg = load_all(&cat).unwrap();
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.lookup_by_name("a"), Some(entry("a")));
        assert_eq!(reg.lookup_by_name("b"), Some(entry("b")));

        remove(&cat, "a").unwrap();
        let reg = load_all(&cat).unwrap();
        assert_eq!(reg.len(), 1);
        assert!(reg.lookup_by_name("a").is_none());
        assert!(reg.lookup_by_name("b").is_some());
    }
}
