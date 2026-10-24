// SPDX-License-Identifier: Apache-2.0

//! Msgpack ext encoding for geometry in standard-msgpack documents.
//!
//! Ext type `3` carries the MessagePack form of a [`SridGeometry`]
//! (`[srid, geometry, z | nil, m | nil]`) as its payload, so a document
//! field keeps its SRID, Z and M. JSON renderings write it as GeoJSON.

use crate::geometry::SridGeometry;

/// Ext type byte of a geometry.
pub const EXT_GEOMETRY: i8 = 3;

/// Write `g` as a geometry ext value.
pub fn write_geometry_ext<W: zerompk::Write>(
    writer: &mut W,
    g: &SridGeometry,
) -> zerompk::Result<()> {
    let payload = zerompk::to_msgpack_vec(g)?;
    writer.write_ext(EXT_GEOMETRY, &payload)
}

/// The geometry an ext value carries. `None` when `ext_type` is not
/// [`EXT_GEOMETRY`] or the payload does not decode.
pub fn geometry_from_ext(ext_type: i8, payload: &[u8]) -> Option<SridGeometry> {
    if ext_type != EXT_GEOMETRY {
        return None;
    }
    zerompk::from_msgpack(payload).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{Geometry, Ordinates, Srid};

    #[test]
    fn round_trip_keeps_z_m_and_srid() {
        let g = SridGeometry::with_ordinates(
            Srid::new(32648).unwrap(),
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
        )
        .unwrap();
        let payload = zerompk::to_msgpack_vec(&g).unwrap();
        assert_eq!(geometry_from_ext(EXT_GEOMETRY, &payload), Some(g));
        assert_eq!(geometry_from_ext(9, &payload), None);
    }
}
