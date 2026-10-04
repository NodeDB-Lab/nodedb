// SPDX-License-Identifier: BUSL-1.1

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
    pub storage_mode: nodedb_physical::physical_plan::StorageMode,
    /// Collection enforcement options (append-only, period lock, retention, etc.).
    pub enforcement: nodedb_physical::physical_plan::EnforcementOptions,
    /// Bitemporal storage: every write goes to the versioned document
    /// table, keyed by `system_from_ms`. Enables `FOR SYSTEM_TIME AS OF`
    /// queries.
    pub bitemporal: bool,
    /// Durable CRDT conflict-resolution policy (JSON-serialized
    /// `CollectionPolicy`), carried through from the catalog so
    /// `execute_register_document_collection` can rehydrate it into this
    /// core's `PolicyRegistry`. `None` = no explicit policy persisted.
    pub conflict_policy: Option<String>,
    /// Declared columns + designated `TIME_KEY` for a timeseries collection.
    /// `Some` only for `engine='timeseries'`. Read by the timeseries ingest
    /// and scan paths so the collection's storage layout and its time column
    /// come from the DDL rather than from whatever the first ingested batch
    /// happened to look like.
    pub timeseries: Option<Box<nodedb_physical::physical_plan::TimeseriesSchema>>,
    /// Vector-primary access-path config. `Some` only for a
    /// `WITH (primary='vector')` collection, whose sparse rows are `zerompk`
    /// TAGGED metadata sidecars (`Value::String("r1")` → `[4,"r1"]`) rather
    /// than ordinary document bodies.
    ///
    /// The read path decodes by this marker and never by inspecting the
    /// stored bytes: a tagged map and a plain document map are both valid
    /// MessagePack maps with the same header byte, so byte sniffing silently
    /// returns tag arrays to the client.
    pub vector_primary: Option<Box<nodedb_types::VectorPrimaryConfig>>,
    /// Declared `VECTOR(n)` columns of a schemaless collection, as
    /// `(column, n)`. A document write indexes each one into its field's
    /// vector index. Empty for every other collection: a strict schema
    /// carries its vector columns in `storage_mode`.
    pub vector_fields: Vec<(String, usize)>,
}

impl CollectionConfig {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            index_paths: Vec::new(),
            crdt_enabled: false,
            storage_mode: nodedb_physical::physical_plan::StorageMode::Schemaless,
            enforcement: nodedb_physical::physical_plan::EnforcementOptions::default(),
            bitemporal: false,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
            vector_fields: Vec::new(),
        }
    }

    pub fn with_bitemporal(mut self, on: bool) -> Self {
        self.bitemporal = on;
        self
    }

    pub fn with_index(mut self, path: &str) -> Self {
        self.index_paths.push(IndexPath::new(path));
        self
    }

    pub fn with_crdt(mut self) -> Self {
        self.crdt_enabled = true;
        self
    }

    pub fn with_storage_mode(mut self, mode: nodedb_physical::physical_plan::StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}
