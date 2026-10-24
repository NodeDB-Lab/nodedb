// SPDX-License-Identifier: Apache-2.0

//! MessagePack form of [`SridGeometry`]: `[srid, geometry, z | nil, m | nil]`.
//!
//! `z` and `m` are arrays of one float per XY coordinate in traversal
//! order, or nil when the geometry lacks that ordinate. The SRID is
//! checked for syntax only.

use super::super::ordinates::Ordinates;
use super::super::shape::Geometry;
use super::super::srid::Srid;
use super::srid_geometry::SridGeometry;
use zerompk::{FromMessagePack, ToMessagePack};

/// Number of fields after the array header.
pub const SRID_GEOMETRY_FIELDS: usize = 4;

/// Write the four fields, without the array header. `Value` writes them
/// after its own type tag.
pub fn write_fields<W: zerompk::Write>(g: &SridGeometry, writer: &mut W) -> zerompk::Result<()> {
    g.srid.write(writer)?;
    g.geometry().write(writer)?;
    write_ordinate(g.ordinates().z(), writer)?;
    write_ordinate(g.ordinates().m(), writer)
}

fn write_ordinate<W: zerompk::Write>(
    values: Option<&[f64]>,
    writer: &mut W,
) -> zerompk::Result<()> {
    match values {
        Some(values) => {
            writer.write_array_len(values.len())?;
            values.iter().try_for_each(|v| writer.write_f64(*v))
        }
        None => writer.write_nil(),
    }
}

/// Read the four fields, without the array header. Z or M arrays that do
/// not hold one finite value per coordinate fail the decode.
pub fn read_fields<'a, R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<SridGeometry> {
    let srid = Srid::read(reader)?;
    let geometry = Geometry::read(reader)?;
    let z = Option::<Vec<f64>>::read(reader)?;
    let m = Option::<Vec<f64>>::read(reader)?;
    let expected = geometry.coord_count();
    let actual = z
        .as_ref()
        .map(Vec::len)
        .filter(|len| *len != expected)
        .or_else(|| m.as_ref().map(Vec::len).filter(|len| *len != expected));
    SridGeometry::with_ordinates(srid, geometry, Ordinates::new(z, m)).map_err(|_| {
        zerompk::Error::ArrayLengthMismatch {
            expected,
            actual: actual.unwrap_or(expected),
        }
    })
}

impl zerompk::ToMessagePack for SridGeometry {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(SRID_GEOMETRY_FIELDS)?;
        write_fields(self, writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for SridGeometry {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != SRID_GEOMETRY_FIELDS {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: SRID_GEOMETRY_FIELDS,
                actual: len,
            });
        }
        read_fields(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_keeps_srid_z_and_m() {
        let g = SridGeometry::with_ordinates(
            Srid::new(3376).unwrap(),
            Geometry::line_string(vec![[1.0, 2.0], [3.0, 4.0]]),
            Ordinates::new(Some(vec![5.0, 6.0]), Some(vec![7.0, 8.0])),
        )
        .unwrap();
        let bytes = zerompk::to_msgpack_vec(&g).unwrap();
        let back: SridGeometry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, g);
        let xy = SridGeometry::new(Srid::WGS84, Geometry::point(1.0, 2.0));
        let bytes = zerompk::to_msgpack_vec(&xy).unwrap();
        assert_eq!(zerompk::from_msgpack::<SridGeometry>(&bytes).unwrap(), xy);
    }

    #[test]
    fn empty_point_round_trips() {
        let g = SridGeometry::wgs84(Geometry::empty_point());
        let bytes = zerompk::to_msgpack_vec(&g).unwrap();
        let back: SridGeometry = zerompk::from_msgpack(&bytes).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn wrong_z_length_fails_decode() {
        let g = SridGeometry::new(Srid::WGS84, Geometry::point(1.0, 2.0));
        let mut encoded = zerompk::to_msgpack_vec(&g).unwrap();
        // Replace the trailing `nil, nil` with `[1.0, 2.0], nil`.
        encoded.truncate(encoded.len() - 2);
        encoded.push(0x92);
        encoded.push(0xcb);
        encoded.extend_from_slice(&1.0f64.to_be_bytes());
        encoded.push(0xcb);
        encoded.extend_from_slice(&2.0f64.to_be_bytes());
        encoded.push(0xc0);
        assert!(zerompk::from_msgpack::<SridGeometry>(&encoded).is_err());
    }
}
