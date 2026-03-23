//! Custom DataFusion catalog provider for NodeDB collections.
//!
//! Makes NodeDB collections visible to DataFusion's SQL planner so that
//! `SELECT * FROM orders` resolves correctly instead of returning
//! "table not found". The actual execution still goes through our
//! `PlanConverter` — this provider only supplies schema metadata.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::control::security::credential::CredentialStore;

/// A DataFusion `SchemaProvider` backed by NodeDB's system catalog.
///
/// Returns `NodeDbTableStub` instances for each collection. These stubs
/// provide schema information to DataFusion for planning but do not
/// execute — the `PlanConverter` handles physical execution.
pub struct NodeDbSchemaProvider {
    credentials: Arc<CredentialStore>,
    tenant_id: u32,
}

impl std::fmt::Debug for NodeDbSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDbSchemaProvider")
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl NodeDbSchemaProvider {
    pub fn new(credentials: Arc<CredentialStore>, tenant_id: u32) -> Self {
        Self {
            credentials,
            tenant_id,
        }
    }

    fn load_collections(&self) -> Vec<crate::control::security::catalog::StoredCollection> {
        let catalog = self.credentials.catalog();
        match catalog {
            Some(c) => c
                .load_collections_for_tenant(self.tenant_id)
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }
}

#[async_trait]
impl SchemaProvider for NodeDbSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.load_collections()
            .into_iter()
            .map(|c| c.name)
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let name = name.to_lowercase();
        let name = name.as_str();
        let catalog = self.credentials.catalog();
        match catalog {
            Some(c) => c
                .get_collection(self.tenant_id, name)
                .ok()
                .flatten()
                .is_some_and(|c| c.is_active),
            None => false,
        }
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let name = name.to_lowercase();
        let name = name.as_str();
        let catalog = self.credentials.catalog();
        let coll = match catalog {
            Some(c) => c
                .get_collection(self.tenant_id, name)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            None => return Ok(None),
        };

        match coll {
            Some(stored) if stored.is_active => {
                let schema = collection_to_arrow_schema(&stored);
                Ok(Some(Arc::new(NodeDbTableStub {
                    name: stored.name,
                    schema,
                })))
            }
            _ => Ok(None),
        }
    }
}

/// A stub `TableProvider` that gives DataFusion schema info for planning.
///
/// Does not scan data — the `PlanConverter` extracts the collection name
/// from the `TableScan` node and dispatches to the Data Plane.
#[derive(Debug)]
struct NodeDbTableStub {
    name: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for NodeDbTableStub {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // This should never be called — the PlanConverter intercepts
        // TableScan nodes before DataFusion tries to execute them.
        Err(DataFusionError::NotImplemented(format!(
            "NodeDB collection '{}' is executed via the Data Plane, not DataFusion scan",
            self.name
        )))
    }
}

/// Convert a `StoredCollection` field list to an Arrow schema.
fn collection_to_arrow_schema(
    coll: &crate::control::security::catalog::StoredCollection,
) -> SchemaRef {
    if coll.fields.is_empty() {
        return Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("document", DataType::Utf8, true),
        ]));
    }

    let mut fields = vec![Field::new("id", DataType::Utf8, false)];

    for (name, type_str) in &coll.fields {
        let dt = match type_str.to_uppercase().as_str() {
            "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => DataType::Int64,
            "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "REAL" => DataType::Float64,
            "VARCHAR" | "TEXT" | "STRING" => DataType::Utf8,
            "BOOL" | "BOOLEAN" => DataType::Boolean,
            "BYTES" | "BYTEA" | "BLOB" => DataType::Binary,
            "JSON" | "JSONB" => DataType::Utf8,
            "TIMESTAMP" | "TIMESTAMPTZ" => DataType::Utf8,
            t if t.starts_with("VECTOR") => DataType::Utf8,
            _ => DataType::Utf8,
        };
        fields.push(Field::new(name, dt, true));
    }

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;

    #[test]
    fn schemaless_collection_schema() {
        let coll = StoredCollection {
            tenant_id: 1,
            name: "test".into(),
            owner: "admin".into(),
            created_at: 0,
            fields: vec![],
            is_active: true,
        };
        let schema = collection_to_arrow_schema(&coll);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "document");
    }

    #[test]
    fn typed_collection_schema() {
        let coll = StoredCollection {
            tenant_id: 1,
            name: "users".into(),
            owner: "admin".into(),
            created_at: 0,
            fields: vec![
                ("name".into(), "VARCHAR".into()),
                ("age".into(), "INT".into()),
                ("score".into(), "FLOAT".into()),
                ("active".into(), "BOOL".into()),
            ],
            is_active: true,
        };
        let schema = collection_to_arrow_schema(&coll);
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(2).data_type(), DataType::Int64);
        assert_eq!(*schema.field(3).data_type(), DataType::Float64);
        assert_eq!(*schema.field(4).data_type(), DataType::Boolean);
    }
}
