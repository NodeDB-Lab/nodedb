// SPDX-License-Identifier: Apache-2.0

//! The stored form of a `Geometry` column cell: little-endian EWKB, which
//! keeps the SRID, Z and M.
//!
//! A write accepts every shape a geometry takes on its way in: the native
//! geometry, its text (GeoJSON, EWKT, WKT, hex EWKB), WKB / EWKB bytes, or
//! a GeoJSON object. Stored SRIDs are checked for syntax only. Whether an
//! SRID is defined is a question for the planner that admitted the row.

use nodedb_spatial::{SpatialError, SridCheck};
use nodedb_types::value::Value;

use crate::error::ColumnarError;

/// The EWKB bytes of a value written to geometry column `column`.
pub(crate) fn encode_geometry_cell(value: &Value, column: &str) -> Result<Vec<u8>, ColumnarError> {
    nodedb_spatial::ewkb_of_value(value, SridCheck::Syntax).map_err(|e| match e {
        SpatialError::NotGeometryValue { .. } => ColumnarError::TypeMismatch {
            column: column.to_string(),
            expected: "Geometry".into(),
        },
        source => ColumnarError::Geometry {
            column: column.to_string(),
            source,
        },
    })
}

/// The reason a corruption error reports for a cell that is not EWKB.
pub(crate) fn not_ewkb(error: SpatialError) -> String {
    format!("geometry cell is not EWKB: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::{Geometry, Ordinates, Srid, SridGeometry};

    #[test]
    fn every_input_form_stores_as_ewkb_and_decodes() {
        let zm = SridGeometry::with_ordinates(
            Srid::new(32648).unwrap(),
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
        )
        .unwrap();
        let native = encode_geometry_cell(&Value::Geometry(zm.clone()), "g").unwrap();
        assert_eq!(
            nodedb_spatial::value_of_ewkb(&native),
            Ok(Value::Geometry(zm))
        );
        let text = encode_geometry_cell(&Value::String("POINT M (1 2 9)".into()), "g").unwrap();
        let Ok(Value::Geometry(g)) = nodedb_spatial::value_of_ewkb(&text) else {
            panic!();
        };
        assert_eq!(g.ordinates().m(), Some(&[9.0][..]));
    }

    #[test]
    fn malformed_input_is_refused() {
        assert!(matches!(
            encode_geometry_cell(&Value::String("POINT(".into()), "g"),
            Err(ColumnarError::Geometry { .. })
        ));
        assert!(matches!(
            encode_geometry_cell(&Value::Integer(1), "g"),
            Err(ColumnarError::TypeMismatch { .. })
        ));
        assert!(nodedb_spatial::value_of_ewkb(b"{\"type\":\"Point\"}").is_err());
        let mut object = std::collections::HashMap::new();
        object.insert("type".to_string(), Value::String("Point".into()));
        assert!(matches!(
            encode_geometry_cell(&Value::Object(object), "g"),
            Err(ColumnarError::Geometry { .. })
        ));
    }
}
