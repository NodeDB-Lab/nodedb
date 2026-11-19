// SPDX-License-Identifier: BUSL-1.1

//! The document config fields a config's wire form fixes, mapped in one
//! place.
//!
//! A `Register` and a config pin decode both map the wire form here, so one
//! wire form gives one config on every path.

use nodedb_physical::physical_plan::{
    DeclaredColumn, DocConfigWire, EnforcementOptions, StorageMode,
};

use crate::data::executor::enforcement::typeguard::CompiledTypeGuard;
use crate::engine::document::store::{CollectionConfig, IndexPath};

/// The `CollectionConfig` fields a wire form fixes.
pub(in crate::data::executor) struct WireConfigFields {
    pub index_paths: Vec<IndexPath>,
    pub storage_mode: StorageMode,
    pub enforcement: EnforcementOptions,
    pub bitemporal: bool,
    pub vector_primary: Option<Box<nodedb_types::VectorPrimaryConfig>>,
    pub vector_fields: Vec<(String, usize)>,
    pub declared_columns: Vec<DeclaredColumn>,
    pub declared_key: Option<String>,
    pub type_guards: Vec<CompiledTypeGuard>,
}

impl WireConfigFields {
    /// The fields `wire` fixes. A type guard violation names
    /// `user_collection`, the name the user wrote.
    pub(in crate::data::executor) fn from_wire(wire: DocConfigWire, user_collection: &str) -> Self {
        // Every field named, so a field added to the wire form is a compile
        // error here.
        let DocConfigWire {
            bitemporal,
            enforcement,
            storage_mode,
            index_paths,
            declared_key,
            vector_primary,
            declared_columns,
            vector_fields,
        } = wire;
        let type_guards = CompiledTypeGuard::compile_all(user_collection, &enforcement.type_guards);
        Self {
            index_paths: index_paths.iter().map(IndexPath::from_registered).collect(),
            storage_mode,
            enforcement,
            bitemporal,
            vector_primary: vector_primary.map(Box::new),
            vector_fields,
            declared_columns,
            declared_key,
            type_guards,
        }
    }

    /// Write every field this form fixes onto `config`. The fields the wire
    /// form does not fix keep their values.
    pub(in crate::data::executor) fn overwrite(self, config: &mut CollectionConfig) {
        let Self {
            index_paths,
            storage_mode,
            enforcement,
            bitemporal,
            vector_primary,
            vector_fields,
            declared_columns,
            declared_key,
            type_guards,
        } = self;
        config.index_paths = index_paths;
        config.storage_mode = storage_mode;
        config.enforcement = enforcement;
        config.bitemporal = bitemporal;
        config.vector_primary = vector_primary;
        config.vector_fields = vector_fields;
        config.declared_columns = declared_columns;
        config.declared_key = declared_key;
        config.type_guards = type_guards;
    }
}
