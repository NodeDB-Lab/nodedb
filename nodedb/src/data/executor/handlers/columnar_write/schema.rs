// SPDX-License-Identifier: BUSL-1.1

//! Schema inference, field coercion, bitemporal column injection, and
//! schema-ordered row <-> object conversion.

use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Ensure a `MutationEngine` is registered for `engine_key`, creating an
    /// empty one (schema resolved from `schema_bytes`, falling back to
    /// inference from `first_row`) when absent, then return its schema.
    ///
    /// Shared by the durable insert path (`execute_columnar_insert`) and the
    /// in-transaction staging path (`stage_columnar_insert`) so a staged
    /// `INSERT` into a collection with no prior durable write registers the
    /// SAME schema a same-transaction `SELECT` will find — without that, an
    /// in-transaction `INSERT` immediately followed by a `SELECT` on a
    /// brand-new collection would hit `execute_columnar_scan`'s "missing
    /// engine -> empty result" branch and never see the staged row, breaking
    /// read-your-own-writes for the first insert into a collection.
    ///
    /// Creating the engine here (rather than only at COMMIT) is safe on
    /// ROLLBACK: an empty, zero-row `MutationEngine` is indistinguishable
    /// from "not yet created" for every read path, and the durable insert
    /// path already treats engine creation as idempotent
    /// (`if !self.columnar_engines.contains_key(...)`).
    ///
    /// Fails when `schema_bytes` is present but does not decode. A sampled
    /// row never stands in for a declared schema.
    pub(in crate::data::executor) fn ensure_columnar_engine_schema(
        &mut self,
        engine_key: &(DatabaseId, TenantId, String),
        collection: &str,
        bitemporal: bool,
        first_row: &Value,
        schema_bytes: &[u8],
    ) -> crate::Result<ColumnarSchema> {
        if let Some(engine) = self.columnar_engines.get(engine_key) {
            return Ok(engine.schema().clone());
        }
        let base_schema = if schema_bytes.is_empty() {
            infer_schema_from_value(first_row)
        } else {
            zerompk::from_msgpack::<ColumnarSchema>(schema_bytes).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!("columnar schema of '{collection}' does not decode: {e}"),
                }
            })?
        };
        let schema = if bitemporal {
            prepend_bitemporal_columns(base_schema)
        } else {
            base_schema
        };
        let engine = nodedb_columnar::MutationEngine::with_flush_threshold(
            collection.to_string(),
            schema.clone(),
            self.query_tuning.columnar_flush_threshold,
        );
        self.columnar_engines.insert(engine_key.clone(), engine);
        Ok(schema)
    }
}

/// Build a `nodedb_types::Value::Object` from a schema-ordered row, every
/// cell as stored. Used by the ON CONFLICT DO UPDATE path to present
/// `existing` and `EXCLUDED` rows to `apply_on_conflict_updates`, and by a
/// row image that is stored again. A row a user or a policy sees goes
/// through [`row_values_to_user_object`].
pub(in crate::data::executor) fn row_values_to_object(
    schema: &ColumnarSchema,
    row: &[Value],
) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col, val) in schema.columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), val.clone());
    }
    nodedb_types::Value::Object(map)
}

/// [`row_values_to_object`] with `_ts_system` rendered in milliseconds
/// (`render_system_ms`). The object a WHERE predicate, a write policy, a
/// `RETURNING` projection or a scan reads.
pub(in crate::data::executor) fn row_values_to_user_object(
    schema: &ColumnarSchema,
    row: &[Value],
) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col, val) in schema.columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), render_system_ms(&col.name, val));
    }
    nodedb_types::Value::Object(map)
}

/// The cell a user reads for column `column`. A bitemporal columnar row
/// stores a system-time ordinal in `_ts_system`, and a user reads its
/// millisecond (`ordinal_to_ms`). Every other cell reads as stored.
pub(in crate::data::executor) fn render_system_ms(column: &str, cell: &Value) -> Value {
    match cell {
        Value::Integer(ordinal) if column == TS_SYSTEM => {
            Value::Integer(nodedb_types::ordinal_to_ms(*ordinal))
        }
        other => other.clone(),
    }
}

/// Coerce a `nodedb_types::Value` field to the value `column` stores.
///
/// Every column converts by the strict document coercion rule, declared
/// numeric width included, so columnar and strict collections accept and
/// refuse the same values. A refusal names the column. Text the type cannot
/// read is `InvalidTextRepresentation`, a value of the wrong kind is
/// `DatatypeMismatch`, and a value past the type's range or the declared
/// width is `NumericValueOutOfRange`. The memtable stores every shape the
/// coercion yields.
///
/// A `SYSTEM_TIMESTAMP` column is the one exception. The columnar write path
/// assigns no value to it, so it stores the instant the row carries, in the
/// two forms the planner's type guard admits: `DateTime` and `Integer`.
pub(in crate::data::executor) fn ndb_field_to_value(
    val: Option<&Value>,
    column: &ColumnDef,
) -> crate::Result<Value> {
    match (&column.column_type, val) {
        (_, None | Some(Value::Null)) => Ok(Value::Null),
        (ColumnType::SystemTimestamp, Some(v @ (Value::DateTime(_) | Value::Integer(_)))) => {
            Ok(v.clone())
        }
        (_, Some(val)) => crate::data::executor::strict_format::coerce_value(val, column),
    }
}

/// Coerce every cell of the schema-ordered `row` to the value its column
/// stores, in place. An UPDATE runs this on each post-image before the
/// engine stores it, the rule an INSERT applies to each new row.
pub(in crate::data::executor) fn coerce_columnar_row(
    schema: &ColumnarSchema,
    row: &mut [Value],
) -> crate::Result<()> {
    for (column, cell) in schema.columns.iter().zip(row.iter_mut()) {
        let coerced = ndb_field_to_value(Some(&*cell), column)?;
        *cell = coerced;
    }
    Ok(())
}

/// Infer a columnar schema from a `nodedb_types::Value::Object` (first row).
///
/// Last resort only: reached when the collection has no catalog schema to send
/// (`schema_bytes` empty) — a test fixture, or a WAL redo record replayed
/// before the boot-time schema seed. Types come from the row's own values;
/// a column's *name* says nothing about its type, so a column called
/// `timestamp` holding a string is a string column.
pub(in crate::data::executor) fn infer_schema_from_value(row: &Value) -> ColumnarSchema {
    let obj = match row {
        Value::Object(m) => m,
        _ => {
            return ColumnarSchema {
                columns: vec![ColumnDef::required("value", ColumnType::Float64)],
                version: 1,
            };
        }
    };

    let mut columns = Vec::new();
    for (key, val) in obj {
        let col_type = match val {
            Value::Float(_) => ColumnType::Float64,
            Value::Integer(_) => ColumnType::Int64,
            Value::Bool(_) => ColumnType::Bool,
            Value::DateTime(_) => ColumnType::Timestamptz,
            Value::NaiveDateTime(_) => ColumnType::Timestamp,
            Value::Geometry(_) => ColumnType::Geometry,
            Value::Bytes(_) => ColumnType::Bytes,
            Value::Object(_) | Value::Array(_) => ColumnType::Json,
            _ => ColumnType::String,
        };
        let lower = key.to_lowercase();
        if lower == "id" {
            columns.push(ColumnDef::required(key.clone(), col_type).with_primary_key());
        } else {
            columns.push(ColumnDef::nullable(key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(ColumnDef::required("value", ColumnType::Float64));
    }

    // Valid by construction: the names are the keys of one map, the key
    // column is required, the list is never empty, and no inferred type is a
    // vector.
    ColumnarSchema {
        columns,
        version: 1,
    }
}

/// Prepend the three reserved bitemporal columns (`_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until`) at positions 0/1/2 of a columnar
/// schema. All three are required Int64; `_ts_system` is engine-stamped
/// on every write, the valid-time pair is client-provided (or defaults
/// to the open interval).
///
/// A base schema that already lists a reserved column loses it, so the
/// triple appears once, at 0/1/2. A schema inferred from a row that carries
/// its temporal columns (a restored version, or a row with client valid
/// time) lists them. The result is valid by construction: `base` is a valid
/// schema, the reserved names leave it, and the triple adds three distinct,
/// non-key, non-null columns.
pub(in crate::data::executor) fn prepend_bitemporal_columns(
    base: ColumnarSchema,
) -> ColumnarSchema {
    let reserved = [TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL];
    let mut columns = Vec::with_capacity(3 + base.columns.len());
    columns.push(ColumnDef::required(TS_SYSTEM, ColumnType::Int64));
    columns.push(ColumnDef::required(TS_VALID_FROM, ColumnType::Int64));
    columns.push(ColumnDef::required(TS_VALID_UNTIL, ColumnType::Int64));
    columns.extend(
        base.columns
            .into_iter()
            .filter(|column| !reserved.contains(&column.name.as_str())),
    );
    ColumnarSchema {
        columns,
        version: base.version,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn column_type(schema: &ColumnarSchema, name: &str) -> ColumnType {
        schema
            .columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.column_type)
            .expect("column inferred")
    }

    #[test]
    fn field_coercion_matches_strict_for_every_column_type() {
        let coerce =
            |v: Value, t: ColumnType| ndb_field_to_value(Some(&v), &ColumnDef::nullable("c", t));

        assert_eq!(
            coerce(Value::Integer(5), ColumnType::String).expect("string"),
            Value::String("5".into())
        );
        assert_eq!(
            coerce(Value::String("7".into()), ColumnType::Int64).expect("int"),
            Value::Integer(7)
        );
        assert!(matches!(
            coerce(Value::Float(1.5), ColumnType::Int64),
            Err(crate::Error::DatatypeMismatch { ref detail }) if detail.contains("'c'")
        ));
        assert!(matches!(
            coerce(Value::Integer(1), ColumnType::Geometry),
            Err(crate::Error::DatatypeMismatch { .. })
        ));
        assert_eq!(
            coerce(
                Value::Array(vec![Value::Float(0.5), Value::Float(1.25)]),
                ColumnType::Vector(2)
            )
            .expect("vector"),
            Value::Bytes(
                [0.5f32, 1.25f32]
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect()
            )
        );
        assert!(matches!(
            coerce(Value::Array(vec![Value::Float(0.5)]), ColumnType::Vector(2)),
            Err(crate::Error::DataException { .. })
        ));
        assert_eq!(
            coerce(Value::Integer(9), ColumnType::SystemTimestamp).expect("system ts"),
            Value::Integer(9)
        );
        assert_eq!(
            ndb_field_to_value(None, &ColumnDef::nullable("c", ColumnType::Int64)).expect("absent"),
            Value::Null
        );
    }

    /// A columnar `SMALLINT` or `REAL` column refuses a value past its
    /// declared width, on a new row and on an UPDATE post-image alike.
    #[test]
    fn declared_width_is_enforced_on_insert_and_update_post_images() {
        let small = ColumnDef::nullable("v", ColumnType::Int64).with_declared_width("SMALLINT");
        let real = ColumnDef::nullable("r", ColumnType::Float64).with_declared_width("REAL");
        for (value, column) in [(Value::Integer(40000), &small), (Value::Float(1e39), &real)] {
            let err = ndb_field_to_value(Some(&value), column).expect_err("past the width");
            assert!(
                matches!(err, crate::Error::NumericValueOutOfRange { .. }),
                "{value:?}: {err:?}"
            );
        }

        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            small.clone(),
            real.clone(),
        ])
        .expect("valid schema");
        // A computed `SET v = v + 39999` over a stored `1`.
        let mut post_image = vec![Value::Integer(1), Value::Integer(40000), Value::Float(1.5)];
        let err = coerce_columnar_row(&schema, &mut post_image).expect_err("past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );

        let mut fits = vec![Value::Integer(1), Value::Integer(32767), Value::Integer(2)];
        coerce_columnar_row(&schema, &mut fits).expect("fits");
        assert_eq!(
            fits,
            vec![Value::Integer(1), Value::Integer(32767), Value::Float(2.0)]
        );
    }

    /// Every value `ndb_field_to_value` yields is a value the memtable holds.
    #[test]
    fn coerced_fields_append_to_the_memtable() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("s", ColumnType::String),
            ColumnDef::required("u", ColumnType::Uuid),
            ColumnDef::required("v", ColumnType::Vector(2)),
            ColumnDef::required("d", ColumnType::Duration),
            ColumnDef::required("b", ColumnType::Bytes),
        ])
        .expect("valid schema");
        let input = [
            Value::Integer(5),
            Value::String("67e55044-10b1-426f-9247-bb680e5fe0c8".into()),
            Value::Array(vec![Value::Float(0.5), Value::Float(1.25)]),
            Value::Duration(nodedb_types::NdbDuration::from_micros(10)),
            Value::String("AQI=".into()),
        ];
        let row: Vec<Value> = schema
            .columns
            .iter()
            .zip(input.iter())
            .map(|(col, v)| ndb_field_to_value(Some(v), col))
            .collect::<crate::Result<_>>()
            .expect("coerce");
        let mut mt = nodedb_columnar::ColumnarMemtable::new(&schema);
        mt.append_row(&row).expect("append");
        assert_eq!(
            mt.get_row(0).expect("read"),
            Some(vec![
                Value::String("5".into()),
                Value::Uuid("67e55044-10b1-426f-9247-bb680e5fe0c8".into()),
                Value::Array(vec![Value::Float(0.5), Value::Float(1.25)]),
                Value::Integer(10),
                Value::Bytes(vec![1, 2]),
            ])
        );
    }

    #[test]
    fn nested_and_geometry_fields_infer_columns_that_hold_them() {
        let row = Value::Object(HashMap::from([
            ("id".to_string(), Value::String("a".into())),
            (
                "geom".to_string(),
                Value::Geometry(nodedb_types::geometry::SridGeometry::wgs84(
                    nodedb_types::geometry::Geometry::point(1.0, 2.0),
                )),
            ),
            (
                "emb".to_string(),
                Value::Array(vec![Value::Float(0.5), Value::Float(1.5)]),
            ),
            ("meta".to_string(), Value::Object(HashMap::new())),
            ("raw".to_string(), Value::Bytes(vec![1, 2])),
        ]));
        let schema = infer_schema_from_value(&row);
        assert_eq!(column_type(&schema, "id"), ColumnType::String);
        assert_eq!(column_type(&schema, "geom"), ColumnType::Geometry);
        assert_eq!(column_type(&schema, "emb"), ColumnType::Json);
        assert_eq!(column_type(&schema, "meta"), ColumnType::Json);
        assert_eq!(column_type(&schema, "raw"), ColumnType::Bytes);
    }

    /// A base schema that already lists the reserved temporal columns keeps
    /// one copy of each, at 0/1/2, typed Int64.
    #[test]
    fn prepend_keeps_one_copy_of_each_reserved_column() {
        let base = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable(TS_SYSTEM, ColumnType::String),
            ColumnDef::nullable(TS_VALID_FROM, ColumnType::Int64),
        ])
        .expect("valid base");
        let schema = prepend_bitemporal_columns(base);
        let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec![TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL, "id"]);
        assert_eq!(column_type(&schema, TS_SYSTEM), ColumnType::Int64);
        assert!(ColumnarSchema::new(schema.columns.clone()).is_ok());
    }
}
