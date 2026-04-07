//! DDL handlers for columnar collection operations.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::parser::parse_strict_create_sql;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: CREATE COLLECTION <name> (<col_defs>) WITH storage = 'columnar'
    pub(in crate::query) async fn handle_create_columnar(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        // Reuse the same parser as strict — column defs are the same syntax.
        let (name, strict_schema) = parse_strict_create_sql(sql)?;

        // Convert StrictSchema → ColumnarSchema (same column defs, different wrapper).
        let mut columnar_schema =
            nodedb_types::columnar::ColumnarSchema::new(strict_schema.columns)
                .map_err(|e| LiteError::Query(e.to_string()))?;

        // Determine profile from SQL (plain by default).
        let upper = sql.to_uppercase();
        let profile = if upper.contains("PROFILE") && upper.contains("SPATIAL") {
            // Find the geometry column.
            let geom_col = columnar_schema
                .columns
                .iter()
                .find(|c| matches!(c.column_type, nodedb_types::columnar::ColumnType::Geometry))
                .map(|c| c.name.clone())
                .unwrap_or_default();

            // Auto-add hidden _geohash column for Point proximity queries.
            let has_geohash = columnar_schema.columns.iter().any(|c| c.name == "_geohash");
            if !has_geohash {
                columnar_schema
                    .columns
                    .push(nodedb_types::columnar::ColumnDef::nullable(
                        "_geohash",
                        nodedb_types::columnar::ColumnType::String,
                    ));
            }

            nodedb_types::columnar::ColumnarProfile::Spatial {
                geometry_column: geom_col,
                auto_rtree: true,
                auto_geohash: true,
            }
        } else {
            nodedb_types::columnar::ColumnarProfile::Plain
        };

        {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(columnar.create_collection(&name, columnar_schema, profile))
            })?;
        }

        self.register_columnar_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "columnar collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: DROP COLLECTION <name> (for columnar collections).
    pub(in crate::query) async fn handle_drop_columnar(
        &self,
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(columnar.drop_collection(name))
            })?;
        }

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "columnar collection '{name}' dropped"
            ))]],
            rows_affected: 0,
        })
    }
}
