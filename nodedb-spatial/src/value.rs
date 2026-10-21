// SPDX-License-Identifier: Apache-2.0

//! The geometry a `Value` holds, and its EWKB.
//!
//! A geometry arrives as the native geometry, its text (GeoJSON, EWKT, WKT,
//! hex EWKB), WKB / EWKB bytes, or a GeoJSON object. Every storage codec
//! reads a geometry cell through these functions.

use nodedb_types::Value;
use nodedb_types::geometry::{SridGeometry, geojson_object, stored_geojson_object};

use crate::error::SpatialError;
use crate::text::{SridCheck, geometry_from_bytes, geometry_from_text};
use crate::wkb::geometry_to_ewkb;

/// The geometry `value` holds. `check` resolves or range-checks its SRID.
/// A value of any other type is [`SpatialError::NotGeometryValue`].
pub fn geometry_from_value(
    value: &Value,
    check: SridCheck<'_>,
) -> Result<SridGeometry, SpatialError> {
    match value {
        Value::Geometry(g) => {
            if let SridCheck::Resolve(ctx) = check {
                ctx.resolve_u32(g.srid.code())?;
            }
            Ok(g.clone())
        }
        Value::String(s) => geometry_from_text(s, check),
        Value::Bytes(b) => geometry_from_bytes(b, check),
        Value::Object(_) => Ok(match check {
            SridCheck::Resolve(ctx) => geojson_object(value, ctx)?,
            SridCheck::Syntax => stored_geojson_object(value)?,
        }),
        other => Err(SpatialError::NotGeometryValue {
            found: other.type_name(),
        }),
    }
}

/// The little-endian EWKB of the geometry `value` holds.
pub fn ewkb_of_value(value: &Value, check: SridCheck<'_>) -> Result<Vec<u8>, SpatialError> {
    Ok(geometry_to_ewkb(&geometry_from_value(value, check)?)?)
}

/// The geometry stored EWKB bytes hold, as a `Value`. The stored SRID is
/// checked for range only.
pub fn value_of_ewkb(bytes: &[u8]) -> Result<Value, SpatialError> {
    geometry_from_bytes(bytes, SridCheck::Syntax).map(Value::Geometry)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid};

    fn point_object(crs: Option<&str>) -> Value {
        let mut map = HashMap::new();
        map.insert("type".to_string(), Value::String("Point".into()));
        map.insert(
            "coordinates".to_string(),
            Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]),
        );
        if let Some(name) = crs {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String(name.into()));
            let mut member = HashMap::new();
            member.insert("type".to_string(), Value::String("name".into()));
            member.insert("properties".to_string(), Value::Object(props));
            map.insert("crs".to_string(), Value::Object(member));
        }
        Value::Object(map)
    }

    #[test]
    fn every_input_form_round_trips_through_ewkb() {
        let zm = SridGeometry::with_ordinates(
            Srid::new(32648).unwrap(),
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
        )
        .unwrap();
        let native = ewkb_of_value(&Value::Geometry(zm.clone()), SridCheck::Syntax).unwrap();
        assert_eq!(value_of_ewkb(&native), Ok(Value::Geometry(zm)));
        let text = Value::String("POINT M (1 2 9)".into());
        let Ok(Value::Geometry(g)) =
            value_of_ewkb(&ewkb_of_value(&text, SridCheck::Syntax).unwrap())
        else {
            panic!("text form decodes");
        };
        assert_eq!(g.ordinates().m(), Some(&[9.0][..]));
        let wkb = Value::Bytes(crate::geometry_to_wkb(&g.force_2d()).unwrap());
        assert!(ewkb_of_value(&wkb, SridCheck::Syntax).is_ok());
    }

    #[test]
    fn a_native_geojson_object_decodes_with_its_crs() {
        let wgs84 = geometry_from_value(&point_object(None), SridCheck::Syntax).unwrap();
        assert_eq!(wgs84, SridGeometry::wgs84(Geometry::point(1.0, 2.0)));
        let stored =
            geometry_from_value(&point_object(Some("EPSG:999")), SridCheck::Syntax).unwrap();
        assert_eq!(stored.srid.code(), 999);
        let ctx = nodedb_crs::CrsContext::builtin_only();
        assert!(matches!(
            geometry_from_value(&point_object(Some("EPSG:999")), SridCheck::Resolve(&ctx)),
            Err(SpatialError::GeoJson(_))
        ));
    }

    #[test]
    fn other_value_types_are_not_geometry() {
        assert_eq!(
            geometry_from_value(&Value::Integer(1), SridCheck::Syntax),
            Err(SpatialError::NotGeometryValue { found: "int" })
        );
        assert!(value_of_ewkb(b"{\"type\":\"Point\"}").is_err());
    }
}
