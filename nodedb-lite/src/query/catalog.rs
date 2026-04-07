//! SqlCatalog implementation for Lite.
//!
//! Resolves collection metadata from the CRDT, strict, and columnar engines.

use std::sync::{Arc, Mutex};

use nodedb_sql::types::*;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::strict::StrictEngine;
use crate::storage::engine::StorageEngine;

/// Catalog adapter for Lite that resolves collections from local engines.
pub struct LiteCatalog<S: StorageEngine> {
    crdt: Arc<Mutex<CrdtEngine>>,
    strict: Arc<Mutex<StrictEngine<S>>>,
    columnar: Arc<Mutex<ColumnarEngine<S>>>,
}

impl<S: StorageEngine> LiteCatalog<S> {
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<Mutex<StrictEngine<S>>>,
        columnar: Arc<Mutex<ColumnarEngine<S>>>,
    ) -> Self {
        Self {
            crdt,
            strict,
            columnar,
        }
    }
}

impl<S: StorageEngine> SqlCatalog for LiteCatalog<S> {
    fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        // Check strict collections first.
        if let Ok(strict) = self.strict.lock() {
            if let Some(schema) = strict.schema(name) {
                let columns = schema
                    .columns
                    .iter()
                    .map(|c| ColumnInfo {
                        name: c.name.clone(),
                        data_type: convert_column_type(&c.column_type),
                        nullable: c.nullable,
                        is_primary_key: c.primary_key,
                    })
                    .collect();
                let pk = schema
                    .columns
                    .iter()
                    .find(|c| c.primary_key)
                    .map(|c| c.name.clone());
                return Some(CollectionInfo {
                    name: name.into(),
                    engine: EngineType::DocumentStrict,
                    columns,
                    primary_key: pk,
                    has_auto_tier: false,
                });
            }
        }

        // Check columnar collections.
        if let Ok(columnar) = self.columnar.lock() {
            if columnar.schema(name).is_some() {
                return Some(CollectionInfo {
                    name: name.into(),
                    engine: EngineType::Columnar,
                    columns: Vec::new(),
                    primary_key: None,
                    has_auto_tier: false,
                });
            }
        }

        // Check CRDT (schemaless) collections.
        if let Ok(crdt) = self.crdt.lock() {
            if crdt.collection_names().iter().any(|n| n == name) {
                return Some(CollectionInfo {
                    name: name.into(),
                    engine: EngineType::DocumentSchemaless,
                    columns: vec![ColumnInfo {
                        name: "id".into(),
                        data_type: SqlDataType::String,
                        nullable: false,
                        is_primary_key: true,
                    }],
                    primary_key: Some("id".into()),
                    has_auto_tier: false,
                });
            }
        }

        None
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry => SqlDataType::Bytes,
        ColumnType::Timestamp => SqlDataType::Timestamp,
        ColumnType::Decimal | ColumnType::Uuid => SqlDataType::String,
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
    }
}
