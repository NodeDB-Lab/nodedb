// SPDX-License-Identifier: Apache-2.0

//! WKB writers: ISO WKB and PostGIS EWKB, little-endian.
//!
//! Both writers carry Z and M. ISO WKB adds 1000 / 2000 / 3000 to the type
//! code. EWKB sets the Z / M flags on every type word and puts the SRID in
//! the top-level header only, as PostGIS writes it. An EMPTY point writes
//! NaN for every ordinate. Output is little-endian unless a caller asks
//! for big-endian.

use nodedb_types::geometry::{Dimension, Geometry, GeometryKind, Ordinates, SridGeometry};

use super::error::WkbError;
use super::format::{
    BYTE_ORDER_BE, BYTE_ORDER_LE, EWKB_SRID_FLAG, MAX_WKB_BYTES, MAX_WKB_DEPTH, MAX_WKB_ELEMENTS,
    ewkb_type_word, iso_type_word,
};

/// WKB byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ByteOrder {
    /// NDR, marker 1.
    #[default]
    LittleEndian,
    /// XDR, marker 0.
    BigEndian,
}

/// Serialize to little-endian ISO WKB. Carries Z and M, no SRID.
///
/// Fails when the geometry exceeds the decoder's element, size or nesting
/// limits, so every emitted value stays decodable.
pub fn geometry_to_wkb(g: &SridGeometry) -> Result<Vec<u8>, WkbError> {
    encode(g, Convention::Iso, ByteOrder::LittleEndian)
}

/// [`geometry_to_wkb`] in `order`.
pub fn geometry_to_wkb_ordered(g: &SridGeometry, order: ByteOrder) -> Result<Vec<u8>, WkbError> {
    encode(g, Convention::Iso, order)
}

/// The type-word convention of a writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Convention {
    Iso,
    /// EWKB with the SRID in the top-level header.
    Ewkb {
        srid: u32,
    },
}

pub(super) fn encode(
    g: &SridGeometry,
    convention: Convention,
    order: ByteOrder,
) -> Result<Vec<u8>, WkbError> {
    let dimension = g.dimension();
    let stride = 8 * usize::from(dimension.coord_dim());
    let srid_bytes = match convention {
        Convention::Iso => 0,
        Convention::Ewkb { .. } => 4,
    };
    let size = encoded_size(g.geometry(), stride, MAX_WKB_DEPTH)?
        .checked_add(srid_bytes)
        .filter(|s| *s <= MAX_WKB_BYTES)
        .ok_or(WkbError::TooLarge { max: MAX_WKB_BYTES })?;
    let mut writer = Writer {
        buf: Vec::with_capacity(size),
        ordinates: g.ordinates(),
        dimension,
        convention,
        order,
        index: 0,
    };
    writer.geometry(g.geometry(), true);
    debug_assert_eq!(writer.buf.len(), size);
    Ok(writer.buf)
}

/// Encoded size of `geom` whose coordinates take `stride` bytes each.
fn encoded_size(geom: &Geometry, stride: usize, depth: usize) -> Result<usize, WkbError> {
    if depth == 0 {
        return Err(WkbError::TooDeep { max: MAX_WKB_DEPTH });
    }
    let path = |len: usize| -> Result<usize, WkbError> {
        checked_count(len)?;
        add(4, mul(len, stride)?)
    };
    let size = match geom {
        Geometry::Point { .. } => 5 + stride,
        Geometry::LineString { coordinates } => add(5, path(coordinates.len())?)?,
        Geometry::Polygon { coordinates } => {
            checked_count(coordinates.len())?;
            coordinates
                .iter()
                .try_fold(9usize, |acc, ring| add(acc, path(ring.len())?))?
        }
        Geometry::MultiPoint { coordinates } => {
            checked_count(coordinates.len())?;
            add(9, mul(coordinates.len(), 5 + stride)?)?
        }
        Geometry::MultiLineString { coordinates } => {
            if depth == 1 {
                return Err(WkbError::TooDeep { max: MAX_WKB_DEPTH });
            }
            checked_count(coordinates.len())?;
            coordinates
                .iter()
                .try_fold(9usize, |acc, line| add(acc, add(5, path(line.len())?)?))?
        }
        Geometry::MultiPolygon { coordinates } => {
            if depth == 1 {
                return Err(WkbError::TooDeep { max: MAX_WKB_DEPTH });
            }
            checked_count(coordinates.len())?;
            coordinates.iter().try_fold(9usize, |acc, polygon| {
                checked_count(polygon.len())?;
                let rings = polygon
                    .iter()
                    .try_fold(9usize, |acc, ring| add(acc, path(ring.len())?))?;
                add(acc, rings)
            })?
        }
        Geometry::GeometryCollection { geometries } => {
            checked_count(geometries.len())?;
            geometries.iter().try_fold(9usize, |acc, member| {
                add(acc, encoded_size(member, stride, depth - 1)?)
            })?
        }
    };
    if size > MAX_WKB_BYTES {
        return Err(WkbError::TooLarge { max: MAX_WKB_BYTES });
    }
    Ok(size)
}

fn checked_count(count: usize) -> Result<(), WkbError> {
    if count > MAX_WKB_ELEMENTS || u32::try_from(count).is_err() {
        return Err(WkbError::TooManyElements {
            count,
            max: MAX_WKB_ELEMENTS,
        });
    }
    Ok(())
}

fn add(left: usize, right: usize) -> Result<usize, WkbError> {
    left.checked_add(right)
        .filter(|size| *size <= MAX_WKB_BYTES)
        .ok_or(WkbError::TooLarge { max: MAX_WKB_BYTES })
}

fn mul(left: usize, right: usize) -> Result<usize, WkbError> {
    left.checked_mul(right)
        .filter(|size| *size <= MAX_WKB_BYTES)
        .ok_or(WkbError::TooLarge { max: MAX_WKB_BYTES })
}

struct Writer<'a> {
    buf: Vec<u8>,
    ordinates: &'a Ordinates,
    dimension: Dimension,
    convention: Convention,
    order: ByteOrder,
    /// Index of the next coordinate in traversal order.
    index: usize,
}

impl Writer<'_> {
    fn header(&mut self, kind: GeometryKind, top: bool) {
        self.buf.push(match self.order {
            ByteOrder::LittleEndian => BYTE_ORDER_LE,
            ByteOrder::BigEndian => BYTE_ORDER_BE,
        });
        match self.convention {
            Convention::Iso => self.u32(iso_type_word(kind, self.dimension)),
            Convention::Ewkb { srid } if top => {
                self.u32(ewkb_type_word(kind, self.dimension) | EWKB_SRID_FLAG);
                self.u32(srid);
            }
            Convention::Ewkb { .. } => self.u32(ewkb_type_word(kind, self.dimension)),
        }
    }

    /// `encoded_size` runs first and bounds every count to
    /// `MAX_WKB_ELEMENTS`, so the count fits a `u32`.
    fn count(&mut self, count: usize) {
        self.u32(count as u32);
    }

    fn geometry(&mut self, geom: &Geometry, top: bool) {
        self.header(geom.kind(), top);
        match geom {
            Geometry::Point { .. } if geom.is_empty() => {
                for _ in 0..self.dimension.coord_dim() {
                    self.f64(f64::NAN);
                }
            }
            Geometry::Point { coordinates } => self.coord(*coordinates),
            Geometry::LineString { coordinates } => self.path(coordinates),
            Geometry::Polygon { coordinates } => self.rings(coordinates),
            Geometry::MultiPoint { coordinates } => {
                self.count(coordinates.len());
                for c in coordinates {
                    self.header(GeometryKind::Point, false);
                    self.coord(*c);
                }
            }
            Geometry::MultiLineString { coordinates } => {
                self.count(coordinates.len());
                for line in coordinates {
                    self.header(GeometryKind::LineString, false);
                    self.path(line);
                }
            }
            Geometry::MultiPolygon { coordinates } => {
                self.count(coordinates.len());
                for polygon in coordinates {
                    self.header(GeometryKind::Polygon, false);
                    self.rings(polygon);
                }
            }
            Geometry::GeometryCollection { geometries } => {
                self.count(geometries.len());
                for member in geometries {
                    self.geometry(member, false);
                }
            }
        }
    }

    fn rings(&mut self, rings: &[Vec<[f64; 2]>]) {
        self.count(rings.len());
        for ring in rings {
            self.path(ring);
        }
    }

    fn path(&mut self, coords: &[[f64; 2]]) {
        self.count(coords.len());
        for c in coords {
            self.coord(*c);
        }
    }

    fn coord(&mut self, c: [f64; 2]) {
        let (z, m) = self.ordinates.at(self.index);
        self.index += 1;
        self.f64(c[0]);
        self.f64(c[1]);
        if let Some(z) = z {
            self.f64(z);
        }
        if let Some(m) = m {
            self.f64(m);
        }
    }

    fn u32(&mut self, v: u32) {
        match self.order {
            ByteOrder::LittleEndian => self.buf.extend_from_slice(&v.to_le_bytes()),
            ByteOrder::BigEndian => self.buf.extend_from_slice(&v.to_be_bytes()),
        }
    }

    fn f64(&mut self, v: f64) {
        match self.order {
            ByteOrder::LittleEndian => self.buf.extend_from_slice(&v.to_le_bytes()),
            ByteOrder::BigEndian => self.buf.extend_from_slice(&v.to_be_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Srid;

    fn xy(g: Geometry) -> SridGeometry {
        SridGeometry::new(Srid::WGS84, g)
    }

    #[test]
    fn encoder_rejects_total_output_above_byte_limit() {
        let g = xy(Geometry::MultiLineString {
            coordinates: (0..5).map(|_| vec![[0.0, 0.0]; 840_000]).collect(),
        });
        assert!(matches!(
            geometry_to_wkb(&g),
            Err(WkbError::TooLarge { .. })
        ));
    }

    #[test]
    fn encoder_rejects_element_count_above_decoder_limit() {
        let g = xy(Geometry::LineString {
            coordinates: vec![[0.0, 0.0]; MAX_WKB_ELEMENTS + 1],
        });
        assert!(geometry_to_wkb(&g).is_err());
    }

    #[test]
    fn big_endian_round_trips() {
        use super::super::read::geometry_from_wkb;
        let g = xy(Geometry::line_string(vec![[1.0, 2.0], [3.0, 4.0]]));
        let be = geometry_to_wkb_ordered(&g, ByteOrder::BigEndian).unwrap();
        assert_eq!(be[0], BYTE_ORDER_BE);
        assert_eq!(geometry_from_wkb(&be).map(|w| w.geometry), Ok(g));
    }

    #[test]
    fn point_sizes() {
        assert_eq!(
            geometry_to_wkb(&xy(Geometry::point(0.0, 0.0)))
                .unwrap()
                .len(),
            21
        );
        let empty = geometry_to_wkb(&xy(Geometry::empty_point())).unwrap();
        assert_eq!(empty.len(), 21);
        assert!(f64::from_le_bytes(empty[5..13].try_into().unwrap()).is_nan());
    }
}
