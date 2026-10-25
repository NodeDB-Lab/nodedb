// SPDX-License-Identifier: Apache-2.0

//! Where a stored definition names an SRID as an SRID.
//!
//! A code counts only in an SRID role:
//!
//! - an integer literal in the SRID argument of a geo function, at the
//!   position its catalog argument shape declares;
//! - a geometry literal that carries the code: an EWKT `SRID=<code>;`
//!   prefix, a GeoJSON `crs` member, hex EWKB, or a native geometry;
//! - a `GEOMETRY(<shape>, <code>)` or `GEOGRAPHY(<shape>, <code>)` type.
//!
//! The same number in any other role, such as `amount < 910001`, does not
//! count.

use nodedb_spatial::SridCheck;
use nodedb_types::Value;

use super::catalog::lookup;
use crate::expr::SqlExpr;

/// The zero-based SRID argument position of geo function `name`, when its
/// argument shape has one.
pub fn srid_arg_position(name: &str) -> Option<usize> {
    lookup(name).and_then(|spec| spec.args.srid_position())
}

/// The code a literal SRID argument holds, coerced as the evaluator
/// coerces it: an integer, an integral float, or integer text.
pub fn literal_srid_code(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(i) => Some(*i),
        Value::Float(f) if f.is_finite() && f.fract() == 0.0 => Some(*f as i64),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// The SRID a geometry literal carries. `None` for a value that is not a
/// geometry.
pub fn literal_geometry_srid(value: &Value) -> Option<u32> {
    match value {
        Value::Geometry(g) => Some(g.srid.code()),
        Value::String(s) => nodedb_spatial::geometry_from_text(s, SridCheck::Syntax)
            .ok()
            .map(|g| g.srid.code()),
        Value::Bytes(b) => nodedb_spatial::geometry_from_bytes(b, SridCheck::Syntax)
            .ok()
            .map(|g| g.srid.code()),
        _ => None,
    }
}

/// Whether `expr` names `code` in an SRID role.
pub fn expr_names_srid(expr: &SqlExpr, code: u32) -> bool {
    let names = |e: &SqlExpr| expr_names_srid(e, code);
    match expr {
        SqlExpr::Literal(value) => literal_geometry_srid(value) == Some(code),
        SqlExpr::Function { name, args } => {
            let at = srid_arg_position(name);
            args.iter().enumerate().any(|(i, arg)| {
                (Some(i) == at && srid_literal(arg) == Some(i64::from(code))) || names(arg)
            })
        }
        SqlExpr::BinaryOp { left, right, .. } => names(left) || names(right),
        SqlExpr::Negate(inner) | SqlExpr::Cast { expr: inner, .. } => names(inner),
        SqlExpr::IsNull { expr: inner, .. } => names(inner),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            operand.as_deref().is_some_and(names)
                || when_thens.iter().any(|(w, t)| names(w) || names(t))
                || else_expr.as_deref().is_some_and(names)
        }
        SqlExpr::Coalesce(items) => items.iter().any(names),
        SqlExpr::NullIf(a, b) => names(a) || names(b),
        SqlExpr::Column(_) | SqlExpr::OldColumn(_) | SqlExpr::ExcludedColumn(_) => false,
    }
}

/// The code an SRID argument holds when it is a literal, through casts.
fn srid_literal(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Literal(value) => literal_srid_code(value),
        SqlExpr::Cast { expr, .. } => srid_literal(expr),
        _ => None,
    }
}

/// Whether type text declares `code`: `GEOMETRY(<shape>, <code>)` or
/// `GEOGRAPHY(<shape>, <code>)`, alone or as one `|` alternative.
pub fn type_names_srid(type_text: &str, code: u32) -> bool {
    type_text
        .split('|')
        .any(|alternative| type_srid(alternative) == Some(code))
}

/// The SRID typmod of one geometry type.
fn type_srid(type_text: &str) -> Option<u32> {
    let text = type_text.trim();
    let open = text.find('(')?;
    let name = text[..open].trim();
    if !(name.eq_ignore_ascii_case("geometry") || name.eq_ignore_ascii_case("geography")) {
        return None;
    }
    let inner = text[open + 1..].strip_suffix(')')?;
    inner.rsplit(',').next()?.trim().parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_parse::parse_generated_expr;

    const CODE: u32 = 910_042;

    fn names(text: &str) -> bool {
        let (expr, _) = parse_generated_expr(text).expect("parses");
        expr_names_srid(&expr, CODE)
    }

    #[test]
    fn srid_arguments_name_the_code() {
        assert!(names("ST_Transform(geom, 910042)"));
        assert!(names("ST_SetSRID(geom, 910042)"));
        assert!(names("st_geomfromtext('POINT(1 2)', 910042)"));
        assert!(names("ST_SetSRID(geom, '910042')"));
        assert!(names("ST_SRID(ST_Transform(geom, 910042)) = 1"));
    }

    #[test]
    fn geometry_literals_name_their_srid() {
        assert!(names("ST_Distance(geom, 'SRID=910042;POINT(1 2)')"));
        assert!(names(
            "ST_Distance(geom, '{\"type\":\"Point\",\"coordinates\":[1,2],\
             \"crs\":{\"type\":\"name\",\"properties\":{\"name\":\"NODEDB:910042\"}}}')"
        ));
    }

    #[test]
    fn the_number_in_another_role_does_not_count() {
        assert!(!names("amount < 910042"));
        assert!(!names("ST_SRID(geom) = 910042"));
        assert!(!names("ST_Buffer(geom, 910042)"));
        assert!(!names("ST_Transform(geom, 910043)"));
        assert!(!names("'910042'"));
    }

    #[test]
    fn geometry_typmods_name_their_srid() {
        assert!(type_names_srid("GEOMETRY(Point, 910042)", CODE));
        assert!(type_names_srid("geography(POLYGON,910042)|NULL", CODE));
        assert!(!type_names_srid("GEOMETRY", CODE));
        assert!(!type_names_srid("DECIMAL(10, 910042)", CODE));
        assert!(!type_names_srid("GEOMETRY(Point, 4326)", CODE));
    }

    #[test]
    fn every_srid_shape_has_its_position() {
        assert_eq!(srid_arg_position("ST_Transform"), Some(1));
        assert_eq!(srid_arg_position("st_geomfromwkb"), Some(1));
        assert_eq!(srid_arg_position("ST_MakeEnvelope"), None);
        assert_eq!(srid_arg_position("ST_Buffer"), None);
        assert_eq!(srid_arg_position("upper"), None);
    }
}
