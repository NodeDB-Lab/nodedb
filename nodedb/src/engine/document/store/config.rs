//! Collection configuration for the Document Engine.

use super::index_path::IndexPath;

/// Collection configuration for the Document Engine.
#[derive(Debug, Clone)]
pub struct CollectionConfig {
    pub name: String,
    /// Declared secondary index paths.
    pub index_paths: Vec<IndexPath>,
    /// Whether this collection uses CRDT-backed storage (Loro).
    pub crdt_enabled: bool,
    /// Storage encoding mode (schemaless MessagePack or strict Binary Tuple).
    pub storage_mode: crate::bridge::physical_plan::StorageMode,
    /// Collection enforcement options (append-only, period lock, retention, etc.).
    pub enforcement: crate::bridge::physical_plan::EnforcementOptions,
}

impl CollectionConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            index_paths: Vec::new(),
            crdt_enabled: false,
            storage_mode: crate::bridge::physical_plan::StorageMode::Schemaless,
            enforcement: crate::bridge::physical_plan::EnforcementOptions::default(),
        }
    }

    pub fn with_index(mut self, path: &str) -> Self {
        self.index_paths.push(IndexPath::new(path));
        self
    }

    pub fn with_crdt(mut self) -> Self {
        self.crdt_enabled = true;
        self
    }

    pub fn with_storage_mode(mut self, mode: crate::bridge::physical_plan::StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}
