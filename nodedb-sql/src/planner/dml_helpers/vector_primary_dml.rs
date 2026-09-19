// SPDX-License-Identifier: Apache-2.0

//! Plan construction for `UPDATE` / `DELETE` against a vector-primary
//! collection, plus the refusals for the write shapes it does not carry.

use super::vector_primary_insert::sql_values_to_vector;
use crate::error::{Result, SqlError};
use crate::types::*;

/// Whether `info` names a vector-primary collection.
pub(crate) fn is_vector_primary(info: &CollectionInfo) -> bool {
    info.primary == nodedb_types::PrimaryEngine::Vector && info.vector_primary.is_some()
}

/// Refuse a write shape a vector-primary collection cannot execute.
///
/// `UPDATE ... FROM` and `MERGE` expand into document point writes on the
/// Control Plane. A vector-primary row has no document store behind it, so
/// those expansions would write nothing the collection can read back.
pub(crate) fn refuse_vector_primary_shape(info: &CollectionInfo, shape: &str) -> Result<()> {
    if !is_vector_primary(info) {
        return Ok(());
    }
    Err(SqlError::Unsupported {
        detail: format!(
            "{shape} is not supported on vector-primary collection '{}'; use UPDATE ... WHERE, \
             DELETE ... WHERE, or UPSERT INTO",
            info.name
        ),
    })
}

/// Inputs to [`build_vector_primary_update_plan`].
pub(crate) struct VectorPrimaryUpdateParams<'a> {
    pub collection: &'a str,
    pub info: &'a CollectionInfo,
    pub vpc: &'a nodedb_types::VectorPrimaryConfig,
    pub assignments: Vec<(String, SqlExpr)>,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    pub returning: bool,
}

/// Build a `SqlPlan::VectorPrimaryUpdate`.
///
/// The vector-column assignment is peeled into `new_vector`. It must be an
/// array literal: the HNSW node is rebuilt from it, and the Data Plane has
/// no row expression evaluator for a vector. An assignment to the primary
/// key is refused: the key is the row's surrogate identity.
pub(crate) fn build_vector_primary_update_plan(
    params: VectorPrimaryUpdateParams<'_>,
) -> Result<Vec<SqlPlan>> {
    let VectorPrimaryUpdateParams {
        collection,
        info,
        vpc,
        assignments,
        filters,
        target_keys,
        returning,
    } = params;
    let mut new_vector: Option<Vec<f32>> = None;
    let mut payload_assignments = Vec::with_capacity(assignments.len());
    for (column, expr) in assignments {
        if info.primary_key.as_deref() == Some(column.as_str()) {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "UPDATE of primary key '{column}' on vector-primary collection \
                     '{collection}' is not supported; DELETE the row and INSERT it under \
                     the new key"
                ),
            });
        }
        if column == vpc.vector_field {
            new_vector = Some(vector_literal(&column, &expr)?);
            continue;
        }
        payload_assignments.push((column, expr));
    }
    Ok(vec![SqlPlan::VectorPrimaryUpdate {
        collection: collection.to_string(),
        field: vpc.vector_field.clone(),
        quantization: vpc.quantization,
        storage_dtype: vpc.storage_dtype,
        payload_indexes: vpc.payload_indexes.clone(),
        new_vector,
        assignments: payload_assignments,
        filters,
        target_keys,
        returning,
        primary_key: info.primary_key.clone(),
    }])
}

/// Build a `SqlPlan::VectorPrimaryDelete`.
pub(crate) fn build_vector_primary_delete_plan(
    collection: &str,
    info: &CollectionInfo,
    vpc: &nodedb_types::VectorPrimaryConfig,
    filters: Vec<Filter>,
    target_keys: Vec<SqlValue>,
) -> Vec<SqlPlan> {
    vec![SqlPlan::VectorPrimaryDelete {
        collection: collection.to_string(),
        field: vpc.vector_field.clone(),
        filters,
        target_keys,
        primary_key: info.primary_key.clone(),
    }]
}

/// The `f32` components of an assigned vector expression.
///
/// Accepts `ARRAY[...]` of numeric literals and a pre-folded array literal.
fn vector_literal(field: &str, expr: &SqlExpr) -> Result<Vec<f32>> {
    match expr {
        SqlExpr::Literal(SqlValue::Array(items)) => sql_values_to_vector(field, items),
        SqlExpr::ArrayLiteral(elems) => {
            let items = elems
                .iter()
                .map(|e| match e {
                    SqlExpr::Literal(v) => Ok(v.clone()),
                    other => Err(SqlError::Unsupported {
                        detail: format!(
                            "vector field '{field}' must be assigned an array of numeric \
                             literals, got element {other:?}"
                        ),
                    }),
                })
                .collect::<Result<Vec<SqlValue>>>()?;
            sql_values_to_vector(field, &items)
        }
        other => Err(SqlError::Unsupported {
            detail: format!(
                "vector field '{field}' must be assigned an array literal, got {other:?}"
            ),
        }),
    }
}
