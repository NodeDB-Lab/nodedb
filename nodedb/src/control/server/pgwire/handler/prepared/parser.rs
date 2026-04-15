//! NodeDbQueryParser — pgwire `QueryParser` implementation.
//!
//! Converts incoming SQL (from a Parse message) into a `ParsedStatement`
//! with inferred parameter types and result schema. Uses nodedb-sql for
//! schema resolution instead of DataFusion.

use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::PgWireResult;

use crate::control::state::SharedState;

use super::statement::ParsedStatement;

/// Implements pgwire's `QueryParser` trait for NodeDB.
///
/// On Parse message: parses SQL via sqlparser, extracts placeholder types
/// from the catalog schema, and computes the result schema.
pub struct NodeDbQueryParser {
    state: Arc<SharedState>,
}

impl NodeDbQueryParser {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Infer parameter and result types using nodedb-sql catalog, scoped to
    /// the connecting user's tenant so a tenant-N user's Parse message
    /// resolves against tenant-N's catalog (not tenant 1).
    fn try_infer_types(
        &self,
        sql: &str,
        client_types: &[Option<Type>],
        tenant_id: u32,
    ) -> (Vec<Option<Type>>, Vec<FieldInfo>) {
        let catalog = crate::control::planner::catalog_adapter::OriginCatalog::new(
            Arc::clone(&self.state.credentials),
            tenant_id,
            Some(Arc::clone(&self.state.retention_policy_registry)),
        );

        // Placeholder inference runs unconditionally so an unplannable
        // SQL string (e.g. `WHERE id = $1` where the planner needs bound
        // params to typecheck) still reports the right number of
        // parameter slots in Describe.
        let param_count = count_placeholders(sql);
        let mut param_types = vec![None; param_count.max(client_types.len())];
        for (i, ct) in client_types.iter().enumerate() {
            if let Some(t) = ct {
                param_types[i] = Some(t.clone());
            }
        }

        // Parse and plan to get collection info for result schema. A plan
        // failure here is not fatal — Describe callers only need the
        // parameter count to bind, and execution re-plans with bound
        // params anyway.
        let plans = match nodedb_sql::plan_sql(sql, &catalog) {
            Ok(p) => p,
            Err(_) => return (param_types, Vec::new()),
        };

        // Infer result fields from the first plan.
        let result_fields = if let Some(plan) = plans.first() {
            infer_result_fields(plan, &catalog)
        } else {
            Vec::new()
        };

        (param_types, result_fields)
    }
}

#[async_trait]
impl QueryParser for NodeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Resolve the connecting user's tenant from pgwire metadata so
        // parse-time catalog lookups are scoped to the right tenant.
        // Unknown users fall back to tenant 1 only during bootstrap
        // (credential store empty) — otherwise parse-time inference
        // returns empty field info, which is the safe default.
        let tenant_id = client
            .metadata()
            .get("user")
            .and_then(|u| {
                self.state
                    .credentials
                    .to_identity(u, crate::control::security::identity::AuthMethod::Trust)
                    .or_else(|| {
                        self.state.credentials.to_identity(
                            u,
                            crate::control::security::identity::AuthMethod::ScramSha256,
                        )
                    })
            })
            .map(|id| id.tenant_id.as_u32())
            .unwrap_or(1);
        let (param_types, result_fields) = self.try_infer_types(sql, types, tenant_id);

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            param_types,
            result_fields,
        })
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(stmt
            .param_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        _column_format: Option<&pgwire::api::portal::Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(stmt.result_fields.clone())
    }
}

/// Count $1, $2, ... placeholders in SQL text.
fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    for (_, _, idx) in super::sql_placeholder::placeholder_ranges(sql) {
        if idx > max_idx {
            max_idx = max_idx.max(idx);
        }
    }
    max_idx
}

/// Infer result FieldInfo from a SqlPlan by looking up collection schema.
fn infer_result_fields(
    plan: &nodedb_sql::SqlPlan,
    catalog: &dyn nodedb_sql::SqlCatalog,
) -> Vec<FieldInfo> {
    use nodedb_sql::types::*;
    use pgwire::api::results::FieldFormat;

    let collection = match plan {
        SqlPlan::Scan { collection, .. } => collection,
        SqlPlan::PointGet { collection, .. } => collection,
        SqlPlan::Aggregate { input, .. } => {
            return infer_result_fields(input, catalog);
        }
        SqlPlan::Join { left, .. } => {
            return infer_result_fields(left, catalog);
        }
        _ => return Vec::new(),
    };

    // `infer_result_fields` runs on a successful plan so any
    // `RetryableSchemaChanged` drain signal would have surfaced
    // earlier. Treat any error here as "no field info", same as
    // None — the caller is non-critical (prepared-stmt
    // description fallback).
    let info = match catalog.get_collection(collection) {
        Ok(Some(i)) => i,
        Ok(None) | Err(_) => return Vec::new(),
    };

    // Check if projection specifies columns.
    let projected_cols = match plan {
        SqlPlan::Scan { projection, .. } => projection,
        _ => return columns_to_field_info(&info.columns),
    };

    if projected_cols.is_empty() || projected_cols.iter().any(|p| matches!(p, Projection::Star)) {
        return columns_to_field_info(&info.columns);
    }

    projected_cols
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => {
                let col = info.columns.iter().find(|c| c.name == *name);
                let pg_type = col
                    .map(|c| sql_data_type_to_pg(&c.data_type))
                    .unwrap_or(Type::TEXT);
                Some(FieldInfo::new(
                    name.clone(),
                    None,
                    None,
                    pg_type,
                    FieldFormat::Text,
                ))
            }
            Projection::Computed { alias, .. } => Some(FieldInfo::new(
                alias.clone(),
                None,
                None,
                Type::TEXT,
                FieldFormat::Text,
            )),
            _ => None,
        })
        .collect()
}

fn columns_to_field_info(columns: &[nodedb_sql::ColumnInfo]) -> Vec<FieldInfo> {
    use pgwire::api::results::FieldFormat;
    columns
        .iter()
        .map(|c| {
            FieldInfo::new(
                c.name.clone(),
                None,
                None,
                sql_data_type_to_pg(&c.data_type),
                FieldFormat::Text,
            )
        })
        .collect()
}

fn sql_data_type_to_pg(dt: &nodedb_sql::SqlDataType) -> Type {
    use nodedb_sql::types::SqlDataType;
    match dt {
        SqlDataType::Int64 => Type::INT8,
        SqlDataType::Float64 => Type::FLOAT8,
        SqlDataType::String => Type::TEXT,
        SqlDataType::Bool => Type::BOOL,
        SqlDataType::Bytes => Type::BYTEA,
        SqlDataType::Timestamp => Type::TIMESTAMP,
        SqlDataType::Decimal => Type::NUMERIC,
        SqlDataType::Uuid => Type::TEXT,
        SqlDataType::Vector(_) => Type::BYTEA,
        SqlDataType::Geometry => Type::BYTEA,
    }
}
