// SPDX-License-Identifier: BUSL-1.1

//! DESCRIBE for an array (`CREATE ARRAY`).
//!
//! Arrays live in the array catalog, not in `StoredCollection`. The result
//! uses the DESCRIBE COLLECTION columns: one row per dim, one row per attr,
//! then `__storage`, `__tile_extents`, and `__georef` when the array has one.
//! An attr's type carries its metadata clauses in DDL form:
//! `UINT16 NODATA 0.0 SCALE 2.75e-5 OFFSET -0.2 UNITS 'reflectance'`.

use nodedb_array::schema::{ArraySchema, DimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_types::{DatabaseId, TenantId};
use serde_json::{Map, Value as JsonValue};

use crate::control::array_catalog::attr_meta::{meta_ast, meta_sql};
use crate::control::array_catalog::georef::{georef_ast, georef_sql};
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// DESCRIBE rows for array `name`, or `None` when no array has that name.
pub(super) fn describe_array(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let entry = state
        .array_catalog
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .lookup_by_name_in_database(tenant_id, database_id, name)?;
    let schema: ArraySchema = match zerompk::from_msgpack(&entry.schema_msgpack) {
        Ok(schema) => schema,
        Err(e) => {
            return Some(Err(DdlError::new(
                "XX000",
                format!("array '{name}': stored schema decode: {e}"),
            )));
        }
    };
    Some(Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        vec![
            "field".to_string(),
            "type".to_string(),
            "nullable".to_string(),
            "primary_key".to_string(),
        ],
        vec![
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Text,
            DdlColType::Bool,
        ],
        rows(&schema),
    ))]))
}

/// The DESCRIBE rows of `schema`.
fn rows(schema: &ArraySchema) -> Vec<Map<String, JsonValue>> {
    let mut rows = Vec::with_capacity(schema.dims.len() + schema.attrs.len() + 3);
    for dim in &schema.dims {
        let ty = format!(
            "{} DIM [{}..{}]",
            dim_type_name(dim.dtype),
            bound_text(&dim.domain.lo),
            bound_text(&dim.domain.hi)
        );
        rows.push(row(&dim.name, &ty, "false"));
    }
    for attr in &schema.attrs {
        let nullable = if attr.nullable { "true" } else { "false" };
        let ty = format!(
            "{}{}",
            attr.dtype.sql_name(),
            meta_sql(&meta_ast(&attr.meta))
        );
        rows.push(row(&attr.name, &ty, nullable));
    }
    rows.push(row("__storage", "array", "false"));
    let extents: Vec<String> = schema.tile_extents.iter().map(u64::to_string).collect();
    rows.push(row("__tile_extents", &extents.join(", "), "false"));
    if let Some(georef) = georef_ast(schema) {
        rows.push(row("__georef", &georef_sql(&georef), "false"));
    }
    rows
}

fn row(field: &str, ty: &str, nullable: &str) -> Map<String, JsonValue> {
    let mut row = Map::new();
    row.insert("field".to_string(), JsonValue::String(field.to_string()));
    row.insert("type".to_string(), JsonValue::String(ty.to_string()));
    row.insert(
        "nullable".to_string(),
        JsonValue::String(nullable.to_string()),
    );
    row.insert("primary_key".to_string(), JsonValue::Bool(false));
    row
}

fn dim_type_name(dtype: DimType) -> &'static str {
    match dtype {
        DimType::Int64 => "INT64",
        DimType::Float64 => "FLOAT64",
        DimType::TimestampMs => "TIMESTAMP_MS",
        DimType::String => "STRING",
    }
}

fn bound_text(bound: &DomainBound) -> String {
    match bound {
        DomainBound::Int64(v) | DomainBound::TimestampMs(v) => v.to_string(),
        DomainBound::Float64(v) => format!("{v:?}"),
        DomainBound::String(v) => format!("'{}'", v.replace('\'', "''")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{
        ArrayGeoref, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, GeoTransform,
    };
    use nodedb_array::types::domain::Domain;

    fn schema(georef: Option<ArrayGeoref>) -> ArraySchema {
        let dim = |name: &str| {
            DimSpec::new(
                name,
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            )
        };
        let mut builder = ArraySchemaBuilder::new("scene")
            .dim(dim("row"))
            .dim(dim("col"))
            .attr(AttrSpec::new("b1", AttrType::UInt16, false))
            .tile_extents(vec![256, 256]);
        if let Some(georef) = georef {
            builder = builder.georef(georef);
        }
        builder.build().unwrap()
    }

    fn field(rows: &[Map<String, JsonValue>], name: &str) -> Option<String> {
        rows.iter()
            .find(|r| r.get("field") == Some(&JsonValue::String(name.into())))
            .and_then(|r| r.get("type"))
            .and_then(|t| t.as_str().map(str::to_string))
    }

    #[test]
    fn rows_list_dims_attrs_and_storage() {
        let rows = rows(&schema(None));
        assert_eq!(field(&rows, "row").as_deref(), Some("INT64 DIM [0..1023]"));
        assert_eq!(field(&rows, "b1").as_deref(), Some("UINT16"));
        assert_eq!(field(&rows, "__storage").as_deref(), Some("array"));
        assert_eq!(field(&rows, "__tile_extents").as_deref(), Some("256, 256"));
        assert_eq!(field(&rows, "__georef"), None);
    }

    #[test]
    fn attr_rows_show_their_metadata() {
        let schema = ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "row",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(9)),
            ))
            .attr(AttrSpec::new("red", AttrType::UInt16, true).with_meta(
                nodedb_array::schema::AttrMeta {
                    nodata: Some(0.0),
                    scale: Some(0.0000275),
                    offset: Some(-0.2),
                    units: Some("reflectance".into()),
                },
            ))
            .attr(AttrSpec::new("qa", AttrType::Int8, true))
            .tile_extents(vec![10])
            .build()
            .unwrap();
        let rows = rows(&schema);
        assert_eq!(
            field(&rows, "red").as_deref(),
            Some("UINT16 NODATA 0.0 SCALE 2.75e-5 OFFSET -0.2 UNITS 'reflectance'")
        );
        assert_eq!(field(&rows, "qa").as_deref(), Some("INT8"));
    }

    #[test]
    fn rows_show_the_georef() {
        let rows = rows(&schema(Some(ArrayGeoref {
            srid: nodedb_types::geometry::Srid::new(32647).expect("srid"),
            x_dim: 1,
            y_dim: 0,
            transform: Some(GeoTransform([600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0])),
        })));
        assert_eq!(
            field(&rows, "__georef").as_deref(),
            Some("SRID 32647, X col, Y row, TRANSFORM (600000.0, 10.0, 0.0, 700000.0, 0.0, -10.0)")
        );
    }
}
