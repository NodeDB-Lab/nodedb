// SPDX-License-Identifier: BUSL-1.1

//! The SET-list of a columnar UPDATE, bound to schema column indexes.
//!
//! Every columnar UPDATE path builds post-images here: the autocommit and
//! COMMIT-replay handler, the write-policy resolve, and the in-transaction
//! staging. One binding decides what a row becomes on every path.
//!
//! A literal decodes once per statement. An expression evaluates once per
//! matched row with the shared `SqlExpr` evaluator. Each expression reads
//! the row's pre-image, so one assignment never observes another. This is
//! PostgreSQL's rule, and the document UPDATE paths follow it too.
//!
//! The post-image then meets the declared column rule. A value past a
//! declared width is an error, so the caller refuses the statement before
//! the first row changes.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_query::expr::SqlExpr;
use nodedb_types::Value;
use nodedb_types::columnar::ColumnarSchema;

use crate::data::executor::handlers::columnar_read::convert::row_to_value;
use crate::data::executor::handlers::columnar_write::coerce_columnar_row;

/// The value one assignment writes.
enum AssignedValue {
    /// A constant, decoded once for the statement.
    Literal(Value),
    /// An expression over the row's pre-image.
    Expr(SqlExpr),
}

/// A columnar UPDATE's SET-list, bound to the schema it writes.
pub(in crate::data::executor) struct ColumnarAssignments {
    /// `(column index, value)` per assignment, in SET-list order.
    entries: Vec<(usize, AssignedValue)>,
}

impl ColumnarAssignments {
    /// Bind `updates` to `schema`. An assignment to a column the schema
    /// does not hold writes nothing. A literal that does not decode is an
    /// `Internal` error.
    pub(in crate::data::executor) fn bind(
        schema: &ColumnarSchema,
        updates: &[(String, UpdateValue)],
    ) -> crate::Result<Self> {
        let mut entries = Vec::with_capacity(updates.len());
        for (field_name, update) in updates {
            let Some(col_idx) = schema.columns.iter().position(|c| c.name == *field_name) else {
                continue;
            };
            let value = match update {
                UpdateValue::Literal(bytes) => {
                    let value = nodedb_types::value_from_msgpack(bytes).map_err(|e| {
                        crate::Error::Internal {
                            detail: format!(
                                "failed to decode update value for field '{field_name}': {e}"
                            ),
                        }
                    })?;
                    AssignedValue::Literal(value)
                }
                UpdateValue::Expr(expr) => AssignedValue::Expr(expr.clone()),
            };
            entries.push((col_idx, value));
        }
        Ok(Self { entries })
    }

    /// Build the post-image of `row`, a schema-ordered pre-image.
    ///
    /// `Err` when an expression fails to evaluate, such as a division by
    /// zero, or when a cell does not meet its declared column, such as a
    /// value past a `SMALLINT` width (SQLSTATE 22003). Geo functions in an
    /// expression resolve SRIDs through `crs`.
    pub(in crate::data::executor) fn apply(
        &self,
        schema: &ColumnarSchema,
        row: Vec<Value>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Vec<Value>> {
        // Every value is computed from the pre-image before any cell
        // changes. The evaluation context is built on the first expression.
        let mut context: Option<Value> = None;
        let mut values = Vec::with_capacity(self.entries.len());
        for (col_idx, assigned) in &self.entries {
            let value = match assigned {
                AssignedValue::Literal(value) => value.clone(),
                AssignedValue::Expr(expr) => {
                    let context = match context {
                        Some(ref context) => context,
                        None => context.insert(row_to_value(&row, schema)),
                    };
                    expr.eval(context, crs)?
                }
            };
            values.push((*col_idx, value));
        }
        let mut new_row = row;
        for (col_idx, value) in values {
            let cells = new_row.len();
            let Some(cell) = new_row.get_mut(col_idx) else {
                return Err(crate::Error::Internal {
                    detail: format!("columnar UPDATE: row holds {cells} cells, no cell {col_idx}"),
                });
            };
            *cell = value;
        }
        coerce_columnar_row(schema, &mut new_row)?;
        Ok(new_row)
    }
}
