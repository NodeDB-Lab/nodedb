//! Implements `nodedb_sql::SqlCatalog` for Origin using CredentialStore.

use std::sync::Arc;

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType};

use crate::control::security::credential::CredentialStore;

/// Adapter bridging the NodeDB catalog to the `SqlCatalog` trait.
pub struct OriginCatalog {
    credentials: Arc<CredentialStore>,
    tenant_id: u32,
    retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
}

impl OriginCatalog {
    pub fn new(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials,
            tenant_id,
            retention_policy_registry,
        }
    }

    fn has_auto_tier(&self, collection: &str) -> bool {
        let registry = match &self.retention_policy_registry {
            Some(r) => r,
            None => return false,
        };
        registry
            .get(self.tenant_id, collection)
            .is_some_and(|p| p.auto_tier)
    }
}

impl SqlCatalog for OriginCatalog {
    fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        let catalog = self.credentials.catalog().as_ref()?;
        let stored = catalog.get_collection(self.tenant_id, name).ok()??;
        if !stored.is_active {
            return None;
        }

        let (engine, columns, primary_key) = convert_collection_type(&stored);

        Some(CollectionInfo {
            name: stored.name,
            engine,
            columns,
            primary_key,
            has_auto_tier: self.has_auto_tier(name),
        })
    }
}

/// Convert a StoredCollection to engine type, columns, and primary key.
fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
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
            (EngineType::DocumentStrict, columns, pk)
        }

        CollectionType::Document(DocumentMode::Schemaless) => {
            let mut columns = vec![ColumnInfo {
                name: "id".into(),
                data_type: SqlDataType::String,
                nullable: false,
                is_primary_key: true,
            }];
            // Add tracked fields from catalog.
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                });
            }
            (EngineType::DocumentSchemaless, columns, Some("id".into()))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            (EngineType::KeyValue, columns, pk)
        }

        CollectionType::Columnar(profile) => {
            let engine = if profile.is_timeseries() {
                EngineType::Timeseries
            } else if profile.is_spatial() {
                EngineType::Spatial
            } else {
                EngineType::Columnar
            };
            let mut columns = Vec::new();
            if !profile.is_timeseries() {
                columns.push(ColumnInfo {
                    name: "id".into(),
                    data_type: SqlDataType::String,
                    nullable: false,
                    is_primary_key: true,
                });
            }
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                });
            }
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some("id".into())
            };
            (engine, columns, pk)
        }
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

fn parse_type_str(s: &str) -> SqlDataType {
    match s.to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => SqlDataType::Int64,
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => SqlDataType::Float64,
        "BOOL" | "BOOLEAN" => SqlDataType::Bool,
        "BYTES" | "BYTEA" | "BLOB" => SqlDataType::Bytes,
        "TIMESTAMP" | "TIMESTAMPTZ" => SqlDataType::Timestamp,
        _ => SqlDataType::String,
    }
}
