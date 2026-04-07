//! Arrow schema construction for strict collections.

use std::sync::Arc;

use nodedb_types::columnar::{ColumnType, StrictSchema};

/// Convert an Arrow `DataType` from a `ColumnType` (for schema construction).
pub fn column_type_to_arrow(ct: &ColumnType) -> arrow::datatypes::DataType {
    match ct {
        ColumnType::Int64 => arrow::datatypes::DataType::Int64,
        ColumnType::Float64 => arrow::datatypes::DataType::Float64,
        ColumnType::String => arrow::datatypes::DataType::Utf8,
        ColumnType::Bool => arrow::datatypes::DataType::Boolean,
        ColumnType::Bytes | ColumnType::Geometry => arrow::datatypes::DataType::Binary,
        ColumnType::Timestamp => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        ColumnType::Decimal => arrow::datatypes::DataType::Utf8, // Lossless string representation
        ColumnType::Uuid => arrow::datatypes::DataType::Utf8,
        ColumnType::Vector(_) => arrow::datatypes::DataType::Binary, // Packed f32 bytes
    }
}

/// Build an Arrow schema from a StrictSchema (for DataFusion table registration).
pub fn strict_schema_to_arrow(schema: &StrictSchema) -> arrow::datatypes::SchemaRef {
    use arrow::datatypes::{Field, Schema};
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|col| {
            Field::new(
                &col.name,
                column_type_to_arrow(&col.column_type),
                col.nullable,
            )
        })
        .collect();
    Arc::new(Schema::new(fields))
}
