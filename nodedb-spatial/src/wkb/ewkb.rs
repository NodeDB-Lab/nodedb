// SPDX-License-Identifier: Apache-2.0

//! PostGIS EWKB writer.

use nodedb_types::geometry::SridGeometry;

use super::error::WkbError;
use super::write::{ByteOrder, Convention, encode};

/// Serialize to little-endian EWKB: Z / M flags on every type word, the
/// SRID in the top-level header (`ST_AsEWKB`).
pub fn geometry_to_ewkb(g: &SridGeometry) -> Result<Vec<u8>, WkbError> {
    geometry_to_ewkb_ordered(g, ByteOrder::LittleEndian)
}

/// [`geometry_to_ewkb`] in `order`.
pub fn geometry_to_ewkb_ordered(g: &SridGeometry, order: ByteOrder) -> Result<Vec<u8>, WkbError> {
    encode(
        g,
        Convention::Ewkb {
            srid: g.srid.code(),
        },
        order,
    )
}

#[cfg(test)]
mod tests {
    use super::super::read::geometry_from_wkb;
    use super::super::write::geometry_to_wkb;
    use super::*;
    use nodedb_types::geometry::{Geometry, Srid};

    #[test]
    fn ewkb_round_trip_keeps_srid() {
        for code in [4326, 32650, 990_001] {
            let srid = Srid::new(code).unwrap();
            let g = SridGeometry::new(
                srid,
                Geometry::polygon(vec![vec![
                    [0.0, 0.0],
                    [100.0, 0.0],
                    [100.0, 100.0],
                    [0.0, 0.0],
                ]]),
            );
            let back = geometry_from_wkb(&geometry_to_ewkb(&g).unwrap()).unwrap();
            assert_eq!(back.geometry.geometry(), g.geometry());
            assert_eq!(back.srid, Some(srid.code()));
        }
    }

    #[test]
    fn iso_wkb_carries_no_srid() {
        let iso = geometry_to_wkb(&SridGeometry::wgs84(Geometry::point(1.0, 2.0))).unwrap();
        assert_eq!(geometry_from_wkb(&iso).map(|w| w.srid), Ok(None));
    }
}
