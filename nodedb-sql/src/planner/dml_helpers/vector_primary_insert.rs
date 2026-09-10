// SPDX-License-Identifier: Apache-2.0

//! Plan construction for `INSERT` against a vector-primary collection.

use crate::error::{Result, SqlError};
use crate::types::*;

/// Build a `SqlPlan::VectorPrimaryInsert` from parsed rows.
///
/// Extracts the vector-field column into `vector: Vec<f32>` and collects
/// all remaining columns into `payload_fields`. Rows missing the vector
/// column are rejected.
///
/// `rows` arrive with every declared DEFAULT already materialized, so a
/// defaulted key or payload column reaches `payload_fields` like a supplied
/// one. `volatile_defaults` reports whether any of those defaults was volatile,
/// which keeps the plan out of the physical-plan cache.
pub(crate) fn build_vector_primary_insert_plan(
    collection: &str,
    vpc: &nodedb_types::VectorPrimaryConfig,
    _columns: &[String],
    rows: Vec<Vec<(String, SqlValue)>>,
    volatile_defaults: bool,
) -> Result<Vec<SqlPlan>> {
    let mut result_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut vector: Option<Vec<f32>> = None;
        let mut payload_fields = std::collections::HashMap::new();

        for (col, val) in row {
            if col == vpc.vector_field {
                match val {
                    SqlValue::Array(items) => {
                        let floats: Result<Vec<f32>> = items
                            .iter()
                            .map(|v| match v {
                                SqlValue::Float(f) => Ok(*f as f32),
                                SqlValue::Int(i) => Ok(*i as f32),
                                SqlValue::Decimal(d) => {
                                    use rust_decimal::prelude::ToPrimitive;
                                    d.to_f32().ok_or_else(|| SqlError::Parse {
                                        detail: format!(
                                            "vector element decimal '{d}' is out of f32 range"
                                        ),
                                    })
                                }
                                other => Err(SqlError::Parse {
                                    detail: format!(
                                        "vector field must contain numbers, got {other:?}"
                                    ),
                                }),
                            })
                            .collect();
                        vector = Some(floats?);
                    }
                    other => {
                        return Err(SqlError::Parse {
                            detail: format!(
                                "vector field '{}' must be an array literal, got {other:?}",
                                vpc.vector_field
                            ),
                        });
                    }
                }
            } else {
                payload_fields.insert(col, val);
            }
        }

        let vector = vector.ok_or_else(|| SqlError::Parse {
            detail: format!(
                "vector-primary INSERT missing required vector field '{}'",
                vpc.vector_field
            ),
        })?;

        result_rows.push(VectorPrimaryRow {
            surrogate: nodedb_types::Surrogate::ZERO,
            vector,
            payload_fields,
        });
    }

    Ok(vec![SqlPlan::VectorPrimaryInsert {
        collection: collection.to_string(),
        field: vpc.vector_field.clone(),
        quantization: vpc.quantization,
        storage_dtype: vpc.storage_dtype,
        payload_indexes: vpc.payload_indexes.clone(),
        rows: result_rows,
        volatile_defaults,
    }])
}
