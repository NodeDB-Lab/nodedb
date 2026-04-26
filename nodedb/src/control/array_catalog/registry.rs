//! In-memory registry of all arrays, mirrored to the system catalog.
//!
//! The registry is keyed by logical array name and maintains a reverse
//! map `ArrayId -> name` so Data-Plane dispatch can
//! resolve either direction without scanning. All public methods return
//! cloned [`ArrayCatalogEntry`] values — the handle is an `Arc<RwLock>`
//! shared between Control and the Data Plane, and callers must not
//! hold the internal lock across engine calls.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_array::types::ArrayId;
use nodedb_types::NodeDbError;

use super::entry::ArrayCatalogEntry;

/// Shared-ownership handle. `RwLock` (not `Mutex`) because the Data
/// Plane does read-mostly lookups during scans while DDL is rare.
pub type ArrayCatalogHandle = Arc<RwLock<ArrayCatalog>>;

/// Purely in-memory index; persistence is the caller's concern (see
/// [`super::persist`]).
#[derive(Debug, Default)]
pub struct ArrayCatalog {
    entries_by_name: HashMap<String, ArrayCatalogEntry>,
    name_by_id: HashMap<ArrayId, String>,
}

impl ArrayCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn handle() -> ArrayCatalogHandle {
        Arc::new(RwLock::new(Self::new()))
    }

    /// Insert a new entry. Duplicate name or id is rejected — DDL must
    /// drop the existing array first.
    pub fn register(&mut self, entry: ArrayCatalogEntry) -> Result<(), NodeDbError> {
        if self.entries_by_name.contains_key(&entry.name) {
            return Err(NodeDbError::array(
                entry.name.clone(),
                "array already registered",
            ));
        }
        if self.name_by_id.contains_key(&entry.array_id) {
            return Err(NodeDbError::array(
                entry.name.clone(),
                "array id already registered under a different name",
            ));
        }
        self.name_by_id
            .insert(entry.array_id.clone(), entry.name.clone());
        self.entries_by_name.insert(entry.name.clone(), entry);
        Ok(())
    }

    pub fn lookup_by_name(&self, name: &str) -> Option<ArrayCatalogEntry> {
        self.entries_by_name.get(name).cloned()
    }

    pub fn lookup_by_id(&self, id: &ArrayId) -> Option<ArrayCatalogEntry> {
        let name = self.name_by_id.get(id)?;
        self.entries_by_name.get(name).cloned()
    }

    /// Remove an entry by name. Returns the removed entry if it existed.
    pub fn unregister(&mut self, name: &str) -> Option<ArrayCatalogEntry> {
        let entry = self.entries_by_name.remove(name)?;
        self.name_by_id.remove(&entry.array_id);
        Some(entry)
    }

    pub fn all_entries(&self) -> Vec<ArrayCatalogEntry> {
        self.entries_by_name.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries_by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries_by_name.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::TenantId;

    fn entry(name: &str) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::new(TenantId::new(1), name),
            name: name.to_string(),
            schema_msgpack: vec![0x80], // empty msgpack map
            schema_hash: 0xDEAD_BEEF,
            created_at_ms: 1_700_000_000_000,
            prefix_bits: 8,
            audit_retain_ms: None,
        }
    }

    #[test]
    fn register_lookup_roundtrip() {
        let mut cat = ArrayCatalog::new();
        let e = entry("genomes");
        cat.register(e.clone()).unwrap();

        assert_eq!(cat.lookup_by_name("genomes"), Some(e.clone()));
        assert_eq!(cat.lookup_by_id(&e.array_id), Some(e.clone()));
        assert_eq!(cat.all_entries(), vec![e]);
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let mut cat = ArrayCatalog::new();
        cat.register(entry("a")).unwrap();
        let err = cat.register(entry("a")).unwrap_err();
        assert!(err.to_string().contains("already registered"));
    }

    #[test]
    fn unregister_removes_both_sides() {
        let mut cat = ArrayCatalog::new();
        let e = entry("x");
        cat.register(e.clone()).unwrap();
        let removed = cat.unregister("x").expect("existed");
        assert_eq!(removed.array_id, e.array_id);
        assert!(cat.lookup_by_name("x").is_none());
        assert!(cat.lookup_by_id(&e.array_id).is_none());
    }
}
