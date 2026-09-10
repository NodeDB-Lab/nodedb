// SPDX-License-Identifier: Apache-2.0

//! Conversion between the evaluator's value type and the planner's literal type.

use crate::error::SqlError;
use crate::types::SqlValue;

/// Convert an evaluated SQL literal into the engine-facing value type.
pub(super) fn sql_value_to_ndb(v: SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Bool(b) => nodedb_types::Value::Bool(b),
        SqlValue::Int(i) => nodedb_types::Value::Integer(i),
        SqlValue::Float(f) => nodedb_types::Value::Float(f),
        SqlValue::Decimal(d) => nodedb_types::Value::Decimal(d),
        SqlValue::String(s) => nodedb_types::Value::String(s),
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b),
        SqlValue::Array(a) => {
            nodedb_types::Value::Array(a.into_iter().map(sql_value_to_ndb).collect())
        }
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(dt),
    }
}

/// Convert an evaluated default back into the planner's literal type.
///
/// The inverse of `sql_value_to_ndb` above, which is the only producer of
/// these values — so every shape the evaluator can emit has an exact
/// counterpart here. Anything else raises rather than rendering through
/// `Debug`: a `DEFAULT` that stored `Uuid("…")` as its own debug text is the
/// same class of defect as dropping it, and harder to notice because the
/// column looks populated.
pub fn default_value_to_sql(column: &str, value: nodedb_types::Value) -> crate::Result<SqlValue> {
    Ok(match value {
        nodedb_types::Value::Null => SqlValue::Null,
        nodedb_types::Value::Bool(b) => SqlValue::Bool(b),
        nodedb_types::Value::Integer(i) => SqlValue::Int(i),
        nodedb_types::Value::Float(f) => SqlValue::Float(f),
        nodedb_types::Value::Decimal(d) => SqlValue::Decimal(d),
        nodedb_types::Value::String(s) => SqlValue::String(s),
        nodedb_types::Value::Bytes(b) => SqlValue::Bytes(b),
        nodedb_types::Value::NaiveDateTime(dt) => SqlValue::Timestamp(dt),
        nodedb_types::Value::DateTime(dt) => SqlValue::Timestamptz(dt),
        nodedb_types::Value::Array(items) => SqlValue::Array(
            items
                .into_iter()
                .map(|item| default_value_to_sql(column, item))
                .collect::<crate::Result<Vec<_>>>()?,
        ),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "default for column '{column}' evaluates to a value with no SQL literal \
                     form: {other:?}"
                ),
            });
        }
    })
}
