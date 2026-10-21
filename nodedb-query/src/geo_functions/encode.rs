// SPDX-License-Identifier: Apache-2.0

//! Geometry encodings: WKT, EWKT, GeoJSON, WKB and EWKB.

use nodedb_crs::CrsContext;
use nodedb_spatial::ByteOrder;
use nodedb_types::Value;

use super::helpers::{GeoEval, geom_arg, geometry_fault, str_arg};
use crate::expr::EvalError;

pub(super) fn eval(canonical: &'static str, args: &[Value], ctx: &CrsContext<'_>) -> GeoEval {
    let result =
        match canonical {
            // WKT stays plain: it has no SRID slot.
            "st_astext" => text(canonical, args, ctx, nodedb_spatial::geometry_to_wkt),
            "st_asewkt" => text(canonical, args, ctx, nodedb_spatial::geometry_to_ewkt),
            // GeoJSON carries the `crs` member for any SRID other than 4326,
            // Z as a third number, and no M.
            "st_asgeojson" => geom_arg(args, 0, canonical, ctx).and_then(|g| match g {
                Some(g) => sonic_rs::to_string(&g).map(Value::String).map_err(|e| {
                    EvalError::ArgumentValue {
                        function: canonical,
                        position: 1,
                        reason: format!("does not encode as GeoJSON: {e}"),
                    }
                }),
                None => Ok(Value::Null),
            }),
            "st_asbinary" => binary(
                canonical,
                args,
                ctx,
                nodedb_spatial::geometry_to_wkb_ordered,
            ),
            "st_asewkb" => binary(
                canonical,
                args,
                ctx,
                nodedb_spatial::geometry_to_ewkb_ordered,
            ),
            _ => return None,
        };
    Some(result)
}

fn text(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    f: fn(&nodedb_types::geometry::SridGeometry) -> String,
) -> Result<Value, EvalError> {
    Ok(match geom_arg(args, 0, function, ctx)? {
        Some(g) => Value::String(f(&g)),
        None => Value::Null,
    })
}

type Encoder = fn(
    &nodedb_types::geometry::SridGeometry,
    ByteOrder,
) -> Result<Vec<u8>, nodedb_spatial::WkbError>;

/// `ST_AsBinary(geom [, 'NDR' | 'XDR'])`: little-endian unless `'XDR'`.
fn binary(
    function: &'static str,
    args: &[Value],
    ctx: &CrsContext<'_>,
    encode: Encoder,
) -> Result<Value, EvalError> {
    let order = match str_arg(args, 1) {
        None => ByteOrder::LittleEndian,
        Some(s) if s.eq_ignore_ascii_case("NDR") => ByteOrder::LittleEndian,
        Some(s) if s.eq_ignore_ascii_case("XDR") => ByteOrder::BigEndian,
        Some(_) => {
            return Err(EvalError::ArgumentValue {
                function,
                position: 2,
                reason: "must be 'NDR' or 'XDR'".into(),
            });
        }
    };
    let Some(g) = geom_arg(args, 0, function, ctx)? else {
        return Ok(Value::Null);
    };
    encode(&g, order)
        .map(Value::Bytes)
        .map_err(|e| geometry_fault(function, e.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid, SridGeometry};

    fn ctx() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn run(canonical: &'static str, args: &[Value]) -> Value {
        eval(canonical, args, &ctx())
            .expect("geo name")
            .expect("no fault expected")
    }

    fn zm() -> Value {
        Value::Geometry(
            SridGeometry::with_ordinates(
                Srid::new(32648).unwrap(),
                Geometry::point(1.0, 2.0),
                Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
            )
            .unwrap(),
        )
    }

    #[test]
    fn text_forms_carry_z_and_m() {
        assert_eq!(
            run("st_astext", &[zm()]),
            Value::String("POINT ZM (1 2 3 4)".into())
        );
        assert_eq!(
            run("st_asewkt", &[zm()]),
            Value::String("SRID=32648;POINT ZM (1 2 3 4)".into())
        );
        let Value::String(json) = run("st_asgeojson", &[zm()]) else {
            panic!();
        };
        assert!(json.contains("[1.0,2.0,3.0]"), "{json}");
    }

    #[test]
    fn binary_forms_round_trip() {
        let Value::Bytes(wkb) = run("st_asbinary", &[zm()]) else {
            panic!();
        };
        assert_eq!(u32::from_le_bytes(wkb[1..5].try_into().unwrap()), 3001);
        let Value::Bytes(be) = run("st_asbinary", &[zm(), Value::String("XDR".into())]) else {
            panic!();
        };
        assert_eq!(be[0], 0);
        let Value::Bytes(ewkb) = run("st_asewkb", &[zm()]) else {
            panic!();
        };
        let back = nodedb_spatial::geometry_from_wkb(&ewkb).unwrap();
        assert_eq!(back.srid, Some(32648));
        assert!(matches!(
            eval("st_asbinary", &[zm(), Value::String("bad".into())], &ctx()),
            Some(Err(EvalError::ArgumentValue { .. }))
        ));
    }
}
