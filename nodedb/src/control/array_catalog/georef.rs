// SPDX-License-Identifier: BUSL-1.1

//! Conversions between the engine georef and its SQL form.
//!
//! The engine names the X and Y axes by dim index. SQL names them by dim
//! name. Every refusal carries its own SQLSTATE class: `42703` for an
//! unknown dim, `22000` for every other georef error.

use nodedb_array::schema::{ArrayGeoref, ArraySchema, DimSpec, GeoTransform};
use nodedb_sql::types_array::{ArrayGeorefAlter, ArrayGeorefAst};

use super::schema_recode::{decode_schema, reencode_checked, schema_error};

/// The SQL form of `schema`'s georef, axes named by dim.
///
/// `None` when the schema has no georef, or when an axis index names no dim.
/// The schema builder refuses such an index, so a stored schema never holds one.
pub(crate) fn georef_ast(schema: &ArraySchema) -> Option<ArrayGeorefAst> {
    let georef = schema.georef.as_ref()?;
    let dim_name = |index: u32| {
        usize::try_from(index)
            .ok()
            .and_then(|i| schema.dims.get(i))
            .map(|d| d.name.clone())
    };
    Some(ArrayGeorefAst {
        srid: georef.srid.code(),
        x_dim: dim_name(georef.x_dim)?,
        y_dim: dim_name(georef.y_dim)?,
        transform: georef.transform.map(|gt| gt.0),
    })
}

/// The engine georef `ast` names, its axes resolved against `dims`.
pub(crate) fn georef_from_ast(
    array: &str,
    ast: &ArrayGeorefAst,
    dims: &[DimSpec],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<ArrayGeoref> {
    let srid = crs
        .resolve_u32(ast.srid)
        .map_err(|e| crate::Error::DataException {
            detail: format!("array '{array}' GEOREF: {e}"),
        })?;
    let index = |name: &str| {
        dims.iter()
            .position(|d| d.name == name)
            .and_then(|i| u32::try_from(i).ok())
            .ok_or_else(|| crate::Error::UndefinedColumn {
                column: name.to_string(),
            })
    };
    Ok(ArrayGeoref {
        srid,
        x_dim: index(&ast.x_dim)?,
        y_dim: index(&ast.y_dim)?,
        transform: ast.transform.map(GeoTransform),
    })
}

/// The stored schema `stored_msgpack` with `change` applied, re-encoded.
///
/// The content bytes stay equal, so `schema_hash` and every segment stay valid.
pub(crate) fn regeoref_schema(
    statement: &str,
    stored_msgpack: &[u8],
    change: &ArrayGeorefAlter,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let schema = decode_schema(statement, stored_msgpack)?;
    let georef = match change {
        ArrayGeorefAlter::Set(ast) => Some(georef_from_ast(&schema.name, ast, &schema.dims, crs)?),
        ArrayGeorefAlter::Drop => None,
    };
    let altered = schema
        .with_georef(georef)
        .map_err(|e| schema_error(statement, e))?;
    reencode_checked(statement, stored_msgpack, &altered)
}

/// The clause body DESCRIBE shows, in the DDL form that recreates it.
pub(crate) fn georef_sql(ast: &ArrayGeorefAst) -> String {
    let mut out = format!("SRID {}, X {}, Y {}", ast.srid, ast.x_dim, ast.y_dim);
    if let Some(gt) = ast.transform {
        let coefficients: Vec<String> = gt.iter().map(|c| format!("{c:?}")).collect();
        out.push_str(&format!(", TRANSFORM ({})", coefficients.join(", ")));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::schema_recode::check_same_content;
    use super::*;
    use nodedb_array::ArrayError;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn dims() -> Vec<DimSpec> {
        ["row", "col"]
            .into_iter()
            .map(|name| {
                DimSpec::new(
                    name,
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
                )
            })
            .collect()
    }

    fn ast() -> ArrayGeorefAst {
        ArrayGeorefAst {
            srid: 4326,
            x_dim: "col".into(),
            y_dim: "row".into(),
            transform: Some([
                100.5,
                8.983152841195214e-05,
                0.0,
                6.5,
                0.0,
                -8.983152841195214e-05,
            ]),
        }
    }

    #[test]
    fn ast_round_trips_through_the_engine_georef() {
        let georef = georef_from_ast(
            "scene",
            &ast(),
            &dims(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(georef.x_dim, 1);
        assert_eq!(georef.y_dim, 0);
        let mut builder = ArraySchemaBuilder::new("scene");
        for d in dims() {
            builder = builder.dim(d);
        }
        let schema = builder
            .attr(AttrSpec::new("b1", AttrType::UInt16, true))
            .tile_extents(vec![256, 256])
            .georef(georef)
            .build()
            .unwrap();
        assert_eq!(georef_ast(&schema), Some(ast()));
    }

    #[test]
    fn unknown_dim_is_undefined_column() {
        let mut bad = ast();
        bad.x_dim = "lon".into();
        let err = georef_from_ast(
            "scene",
            &bad,
            &dims(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap_err();
        assert!(
            matches!(err, crate::Error::UndefinedColumn { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn unsupported_srid_is_data_exception() {
        let mut bad = ast();
        bad.srid = 999;
        let err = georef_from_ast(
            "scene",
            &bad,
            &dims(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::DataException { .. }), "{err:?}");
    }

    #[test]
    fn georef_refusal_is_data_exception() {
        let err = schema_error(
            "CREATE ARRAY scene",
            ArrayError::InvalidGeoref {
                array: "scene".into(),
                detail: "singular".into(),
            },
        );
        assert!(matches!(err, crate::Error::DataException { .. }), "{err:?}");
    }

    fn stored() -> ArraySchema {
        let mut builder = ArraySchemaBuilder::new("scene");
        for d in dims() {
            builder = builder.dim(d);
        }
        builder
            .attr(AttrSpec::new("b1", AttrType::UInt16, true))
            .tile_extents(vec![256, 256])
            .build()
            .unwrap()
    }

    #[test]
    fn regeoref_set_then_drop_keeps_content() {
        let plain = zerompk::to_msgpack_vec(&stored()).unwrap();
        let set = regeoref_schema(
            "ALTER ARRAY scene",
            &plain,
            &ArrayGeorefAlter::Set(ast()),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        let with: ArraySchema = zerompk::from_msgpack(&set).unwrap();
        assert_eq!(georef_ast(&with), Some(ast()));
        assert_eq!(
            with.content_msgpack().unwrap(),
            stored().content_msgpack().unwrap()
        );
        let dropped = regeoref_schema(
            "ALTER ARRAY scene",
            &set,
            &ArrayGeorefAlter::Drop,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(dropped, plain);
    }

    #[test]
    fn regeoref_refusals_carry_their_class() {
        let plain = zerompk::to_msgpack_vec(&stored()).unwrap();
        let mut unknown = ast();
        unknown.y_dim = "lat".into();
        let err = regeoref_schema(
            "ALTER ARRAY scene",
            &plain,
            &ArrayGeorefAlter::Set(unknown),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap_err();
        assert!(
            matches!(err, crate::Error::UndefinedColumn { .. }),
            "{err:?}"
        );
        let mut singular = ast();
        singular.transform = Some([0.0, 1.0, 1.0, 0.0, 1.0, 1.0]);
        let err = regeoref_schema(
            "ALTER ARRAY scene",
            &plain,
            &ArrayGeorefAlter::Set(singular),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::DataException { .. }), "{err:?}");
    }

    #[test]
    fn content_change_is_refused() {
        let plain = zerompk::to_msgpack_vec(&stored()).unwrap();
        let mut changed = stored();
        changed.attrs[0] = AttrSpec::new("b1", AttrType::UInt8, true);
        let changed = zerompk::to_msgpack_vec(&changed).unwrap();
        assert!(check_same_content("ALTER ARRAY scene", &plain, &changed).is_err());
        assert!(check_same_content("ALTER ARRAY scene", &plain, &plain).is_ok());
    }

    #[test]
    fn sql_form_keeps_full_precision() {
        let text = georef_sql(&ast());
        assert_eq!(
            text,
            "SRID 4326, X col, Y row, TRANSFORM (100.5, 8.983152841195213e-5, 0.0, 6.5, 0.0, \
             -8.983152841195213e-5)"
        );
        // The printed form is the shortest that parses back to the same bits.
        let printed = text
            .split_once("TRANSFORM (")
            .and_then(|(_, rest)| rest.strip_suffix(')'))
            .expect("transform list");
        let parsed: Vec<f64> = printed
            .split(", ")
            .map(|c| c.parse().expect("coefficient"))
            .collect();
        let original = ast().transform.expect("transform");
        assert_eq!(parsed.as_slice(), original.as_slice());
    }
}
