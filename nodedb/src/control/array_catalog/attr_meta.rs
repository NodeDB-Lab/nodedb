// SPDX-License-Identifier: BUSL-1.1

//! Conversions between the engine attr metadata and its SQL form, and the
//! schema re-encode an `ALTER ARRAY ... ALTER ATTR` makes.
//!
//! Attr metadata is not part of `content_msgpack`, so a change keeps
//! `schema_hash` and every segment valid, as a georef change does. A value
//! that does not fit the attr type is SQLSTATE `22000`.

use nodedb_array::schema::AttrMeta;
use nodedb_sql::types_array::{ArrayAttrMetaAlter, ArrayAttrMetaAst};

use super::schema_recode::{decode_schema, reencode_checked, schema_error};

/// The SQL form of `meta`.
pub(crate) fn meta_ast(meta: &AttrMeta) -> ArrayAttrMetaAst {
    ArrayAttrMetaAst {
        nodata: meta.nodata,
        scale: meta.scale,
        offset: meta.offset,
        units: meta.units.clone(),
    }
}

/// The engine form of `ast`.
pub(crate) fn meta_from_ast(ast: &ArrayAttrMetaAst) -> AttrMeta {
    AttrMeta {
        nodata: ast.nodata,
        scale: ast.scale,
        offset: ast.offset,
        units: ast.units.clone(),
    }
}

/// The stored schema `stored_msgpack` with `change` applied, re-encoded.
///
/// The content bytes stay equal, so `schema_hash` and every segment stay
/// valid.
pub(crate) fn remeta_schema(
    statement: &str,
    stored_msgpack: &[u8],
    change: &ArrayAttrMetaAlter,
) -> crate::Result<Vec<u8>> {
    let schema = decode_schema(statement, stored_msgpack)?;
    let current = schema
        .attr(&change.attr)
        .map(|a| meta_ast(&a.meta))
        .ok_or_else(|| crate::Error::UndefinedColumn {
            column: change.attr.clone(),
        })?;
    let next = meta_from_ast(&change.change.apply(&current));
    let altered = schema
        .with_attr_meta(&change.attr, next)
        .map_err(|e| schema_error(statement, e))?;
    reencode_checked(statement, stored_msgpack, &altered)
}

/// The metadata clauses DESCRIBE shows after an attr type, in the DDL form
/// that recreates them: ` NODATA 0.0 SCALE 2.75e-5 UNITS 'reflectance'`.
/// Empty when the attr has none.
pub(crate) fn meta_sql(meta: &ArrayAttrMetaAst) -> String {
    let number = |v: f64| {
        if v.is_nan() {
            "NAN".to_string()
        } else if v.is_infinite() {
            // The lexer reads `-` only before a digit, so negative
            // infinity takes the quoted form.
            if v > 0.0 { "INF" } else { "'-INF'" }.to_string()
        } else {
            format!("{v:?}")
        }
    };
    let mut out = String::new();
    if let Some(v) = meta.nodata {
        out.push_str(&format!(" NODATA {}", number(v)));
    }
    if let Some(v) = meta.scale {
        out.push_str(&format!(" SCALE {}", number(v)));
    }
    if let Some(v) = meta.offset {
        out.push_str(&format!(" OFFSET {}", number(v)));
    }
    if let Some(units) = &meta.units {
        out.push_str(&format!(" UNITS '{}'", units.replace('\'', "''")));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{
        ArraySchema, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType,
    };
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_sql::types_array::{ArrayAttrMetaChange, ArrayAttrMetaKey};

    fn stored() -> ArraySchema {
        ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "row",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            ))
            .attr(AttrSpec::new("red", AttrType::UInt16, true))
            .tile_extents(vec![256])
            .build()
            .unwrap()
    }

    fn set(meta: ArrayAttrMetaAst) -> ArrayAttrMetaAlter {
        ArrayAttrMetaAlter {
            attr: "red".into(),
            change: ArrayAttrMetaChange::Set(meta),
        }
    }

    #[test]
    fn set_then_drop_keeps_content() {
        let plain = zerompk::to_msgpack_vec(&stored()).unwrap();
        let landsat = ArrayAttrMetaAst {
            nodata: Some(0.0),
            scale: Some(0.0000275),
            offset: Some(-0.2),
            units: Some("reflectance".into()),
        };
        let with = remeta_schema("ALTER ARRAY scene", &plain, &set(landsat.clone())).unwrap();
        let schema: ArraySchema = zerompk::from_msgpack(&with).unwrap();
        assert_eq!(meta_ast(&schema.attrs[0].meta), landsat);
        assert_eq!(
            schema.content_msgpack().unwrap(),
            stored().content_msgpack().unwrap()
        );
        let drop_all = ArrayAttrMetaAlter {
            attr: "red".into(),
            change: ArrayAttrMetaChange::Drop(vec![
                ArrayAttrMetaKey::Nodata,
                ArrayAttrMetaKey::Scale,
                ArrayAttrMetaKey::Offset,
                ArrayAttrMetaKey::Units,
            ]),
        };
        let dropped = remeta_schema("ALTER ARRAY scene", &with, &drop_all).unwrap();
        assert_eq!(dropped, plain);
    }

    #[test]
    fn an_unrepresentable_nodata_is_data_exception() {
        let plain = zerompk::to_msgpack_vec(&stored()).unwrap();
        let err = remeta_schema(
            "ALTER ARRAY scene",
            &plain,
            &set(ArrayAttrMetaAst {
                nodata: Some(-1.0),
                ..ArrayAttrMetaAst::default()
            }),
        )
        .unwrap_err();
        assert!(matches!(err, crate::Error::DataException { .. }), "{err:?}");
    }

    #[test]
    fn sql_form_names_every_clause() {
        let text = meta_sql(&ArrayAttrMetaAst {
            nodata: Some(f64::NAN),
            scale: Some(0.5),
            offset: None,
            units: Some("it's".into()),
        });
        assert_eq!(text, " NODATA NAN SCALE 0.5 UNITS 'it''s'");
        assert_eq!(meta_sql(&ArrayAttrMetaAst::default()), "");
    }
}
