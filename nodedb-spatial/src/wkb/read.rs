// SPDX-License-Identifier: Apache-2.0

//! WKB / EWKB reader.
//!
//! Accepts ISO WKB (Z / M / ZM as +1000 / +2000 / +3000) and PostGIS EWKB
//! (Z / M / SRID flags) in either byte order. An EMPTY point is all-NaN
//! ordinates. Every member of a multi or collection has the parent's
//! dimension. Only the top-level SRID is reported.
//!
//! Every declared element consumes input bytes. The reader keeps one
//! budget of input bytes promised to declared but unread elements across
//! all nesting levels. A count fits only into the unpromised remainder, so
//! the total reserved capacity stays a small multiple of the input size.

use nodedb_types::geometry::{
    Dimension, Geometry, GeometryKind, OrdinatesBuilder, Srid, SridGeometry,
};

use super::error::WkbError;
use super::format::{
    BYTE_ORDER_BE, BYTE_ORDER_LE, MAX_WKB_BYTES, MAX_WKB_DEPTH, MAX_WKB_ELEMENTS, TypeWordError,
    decode_type_word,
};

/// Bytes of a header: byte order marker and type word.
const HEADER_BYTES: usize = 5;
/// Bytes of an element count.
const COUNT_BYTES: usize = 4;
/// Bytes of a member header followed by its count.
const NESTED_MEMBER_BYTES: usize = HEADER_BYTES + COUNT_BYTES;

/// A decoded WKB value.
#[derive(Debug, Clone, PartialEq)]
pub struct Wkb {
    /// The SRID an EWKB header declares, unresolved. `None` for ISO WKB.
    pub srid: Option<u32>,
    /// The geometry, tagged WGS 84. The caller retags it once `srid`
    /// resolves.
    pub geometry: SridGeometry,
}

/// Decode WKB or EWKB bytes.
pub fn geometry_from_wkb(data: &[u8]) -> Result<Wkb, WkbError> {
    if data.len() > MAX_WKB_BYTES {
        return Err(WkbError::TooLarge { max: MAX_WKB_BYTES });
    }
    let mut reader = Reader {
        data,
        cursor: 0,
        promised: 0,
        ordinates: OrdinatesBuilder::default(),
    };
    let header = reader.header()?;
    reader.ordinates = OrdinatesBuilder::with_dimension(header.dimension);
    let geometry = reader.body(header, MAX_WKB_DEPTH)?;
    if reader.cursor != data.len() {
        return Err(WkbError::Trailing {
            count: data.len() - reader.cursor,
        });
    }
    let ordinates = reader.ordinates.finish();
    Ok(Wkb {
        srid: header.srid,
        geometry: SridGeometry::with_ordinates(Srid::WGS84, geometry, ordinates)?,
    })
}

/// Bounding box of WKB bytes, in geographic space.
pub fn wkb_bbox(data: &[u8]) -> Result<nodedb_types::BoundingBox, WkbError> {
    let wkb = geometry_from_wkb(data)?;
    Ok(nodedb_types::geometry_bbox(wkb.geometry.geometry()))
}

/// One header: byte order, type word and the optional SRID.
#[derive(Debug, Clone, Copy)]
struct Header {
    little_endian: bool,
    kind: GeometryKind,
    dimension: Dimension,
    srid: Option<u32>,
    /// Offset of the byte order marker.
    offset: usize,
}

struct Reader<'a> {
    data: &'a [u8],
    cursor: usize,
    /// Input bytes promised to declared elements not yet started.
    promised: usize,
    ordinates: OrdinatesBuilder,
}

impl Reader<'_> {
    fn header(&mut self) -> Result<Header, WkbError> {
        let offset = self.cursor;
        let byte = self.u8()?;
        let little_endian = match byte {
            BYTE_ORDER_LE => true,
            BYTE_ORDER_BE => false,
            byte => return Err(WkbError::ByteOrder { byte, offset }),
        };
        let word = self.u32(little_endian)?;
        let decoded = decode_type_word(word).map_err(|e| match e {
            TypeWordError::UnknownCode => WkbError::UnknownType {
                word,
                offset: offset + 1,
            },
            TypeWordError::ConflictingDimension => WkbError::ConflictingDimension {
                word,
                offset: offset + 1,
            },
        })?;
        let srid = if decoded.has_srid {
            Some(self.u32(little_endian)?)
        } else {
            None
        };
        Ok(Header {
            little_endian,
            kind: decoded.kind,
            dimension: decoded.dimension,
            srid,
            offset,
        })
    }

    /// A member header: its dimension must equal the parent's.
    fn member(&mut self, parent: Dimension) -> Result<Header, WkbError> {
        let header = self.header()?;
        if header.dimension != parent {
            return Err(WkbError::MemberDimension {
                parent,
                member: header.dimension,
                offset: header.offset,
            });
        }
        Ok(header)
    }

    /// A member of `parent_kind` that must be a `want`.
    fn typed_member(&mut self, parent: Header, want: GeometryKind) -> Result<Header, WkbError> {
        let header = self.member(parent.dimension)?;
        if header.kind != want {
            return Err(WkbError::MemberKind {
                parent: parent.kind.name(),
                found: header.kind.name(),
                offset: header.offset,
            });
        }
        Ok(header)
    }

    fn body(&mut self, h: Header, depth: usize) -> Result<Geometry, WkbError> {
        if depth == 0 {
            return Err(WkbError::TooDeep { max: MAX_WKB_DEPTH });
        }
        let stride = 8 * usize::from(h.dimension.coord_dim());
        let point_member = HEADER_BYTES + stride;
        Ok(match h.kind {
            GeometryKind::Point => match self.point(h)? {
                Some(coordinates) => Geometry::Point { coordinates },
                None => Geometry::empty_point(),
            },
            GeometryKind::LineString => Geometry::LineString {
                coordinates: self.coords(h)?,
            },
            GeometryKind::Polygon => Geometry::Polygon {
                coordinates: self.rings(h)?,
            },
            GeometryKind::MultiPoint => {
                let count = self.checked_wkb_capacity(h.little_endian, point_member)?;
                let mut points = Vec::with_capacity(count);
                for _ in 0..count {
                    self.start_element(point_member);
                    let member = self.typed_member(h, GeometryKind::Point)?;
                    let offset = self.cursor;
                    let coordinates = self.point(member)?.ok_or(WkbError::MemberKind {
                        parent: h.kind.name(),
                        found: "an EMPTY point",
                        offset,
                    })?;
                    points.push(coordinates);
                }
                Geometry::MultiPoint {
                    coordinates: points,
                }
            }
            GeometryKind::MultiLineString => {
                let count = self.checked_wkb_capacity(h.little_endian, NESTED_MEMBER_BYTES)?;
                let mut lines = Vec::with_capacity(count);
                for _ in 0..count {
                    self.start_element(NESTED_MEMBER_BYTES);
                    let member = self.typed_member(h, GeometryKind::LineString)?;
                    lines.push(self.coords(member)?);
                }
                Geometry::MultiLineString { coordinates: lines }
            }
            GeometryKind::MultiPolygon => {
                let count = self.checked_wkb_capacity(h.little_endian, NESTED_MEMBER_BYTES)?;
                let mut polygons = Vec::with_capacity(count);
                for _ in 0..count {
                    self.start_element(NESTED_MEMBER_BYTES);
                    let member = self.typed_member(h, GeometryKind::Polygon)?;
                    polygons.push(self.rings(member)?);
                }
                Geometry::MultiPolygon {
                    coordinates: polygons,
                }
            }
            GeometryKind::GeometryCollection => {
                let count = self.checked_wkb_capacity(h.little_endian, HEADER_BYTES)?;
                let mut geometries = Vec::with_capacity(count);
                for _ in 0..count {
                    self.start_element(HEADER_BYTES);
                    let member = self.member(h.dimension)?;
                    geometries.push(self.body(member, depth - 1)?);
                }
                Geometry::GeometryCollection { geometries }
            }
        })
    }

    /// A point body. All-NaN ordinates are the EMPTY point (`None`) and
    /// record no coordinate.
    fn point(&mut self, h: Header) -> Result<Option<[f64; 2]>, WkbError> {
        let offset = self.cursor;
        let values = self.ordinate_values(h)?;
        let used = usize::from(h.dimension.coord_dim());
        if values[..used].iter().all(|v| v.is_nan()) {
            return Ok(None);
        }
        self.record(values, h.dimension, offset).map(Some)
    }

    /// A polygon body: a ring count, then each ring.
    fn rings(&mut self, h: Header) -> Result<Vec<Vec<[f64; 2]>>, WkbError> {
        let count = self.checked_wkb_capacity(h.little_endian, COUNT_BYTES)?;
        let mut rings = Vec::with_capacity(count);
        for _ in 0..count {
            self.start_element(COUNT_BYTES);
            rings.push(self.coords(h)?);
        }
        Ok(rings)
    }

    fn coords(&mut self, h: Header) -> Result<Vec<[f64; 2]>, WkbError> {
        let stride = 8 * usize::from(h.dimension.coord_dim());
        let count = self.checked_wkb_capacity(h.little_endian, stride)?;
        let mut coords = Vec::with_capacity(count);
        for _ in 0..count {
            self.start_element(stride);
            let offset = self.cursor;
            let values = self.ordinate_values(h)?;
            coords.push(self.record(values, h.dimension, offset)?);
        }
        Ok(coords)
    }

    /// The 2 to 4 ordinates of one coordinate.
    fn ordinate_values(&mut self, h: Header) -> Result<[f64; 4], WkbError> {
        let mut values = [0.0; 4];
        for slot in values.iter_mut().take(usize::from(h.dimension.coord_dim())) {
            *slot = self.f64(h.little_endian)?;
        }
        Ok(values)
    }

    /// Check one coordinate's ordinates and record its Z and M.
    fn record(
        &mut self,
        values: [f64; 4],
        dimension: Dimension,
        offset: usize,
    ) -> Result<[f64; 2], WkbError> {
        let used = usize::from(dimension.coord_dim());
        if values[..used].iter().any(|v| !v.is_finite()) {
            return Err(WkbError::NonFinite { offset });
        }
        let z = dimension.has_z().then_some(values[2]);
        let m = dimension
            .has_m()
            .then_some(values[if dimension.has_z() { 3 } else { 2 }]);
        self.ordinates.push(z, m)?;
        Ok([values[0], values[1]])
    }

    /// An element count whose elements occupy at least `min_bytes` each.
    /// The elements claim their bytes from the unpromised input. A count
    /// that does not fit fails before any allocation.
    fn checked_wkb_capacity(
        &mut self,
        little_endian: bool,
        min_bytes: usize,
    ) -> Result<usize, WkbError> {
        let offset = self.cursor;
        let count = self.u32(little_endian)? as usize;
        if count > MAX_WKB_ELEMENTS {
            return Err(WkbError::TooManyElements {
                count,
                max: MAX_WKB_ELEMENTS,
            });
        }
        let required = count.saturating_mul(min_bytes);
        let unpromised = self
            .data
            .len()
            .saturating_sub(self.cursor)
            .saturating_sub(self.promised);
        if required > unpromised {
            return Err(WkbError::Truncated { offset });
        }
        self.promised += required;
        Ok(count)
    }

    /// Start reading one declared element: release its promised bytes.
    /// Its fixed part consumes at least these bytes before any nested
    /// count claims more.
    fn start_element(&mut self, min_bytes: usize) {
        self.promised = self.promised.saturating_sub(min_bytes);
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], WkbError> {
        let end = self.cursor.checked_add(N).ok_or(WkbError::Truncated {
            offset: self.cursor,
        })?;
        let bytes: [u8; N] = self
            .data
            .get(self.cursor..end)
            .and_then(|s| s.try_into().ok())
            .ok_or(WkbError::Truncated {
                offset: self.cursor,
            })?;
        self.cursor = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, WkbError> {
        self.take::<1>().map(|b| b[0])
    }

    fn u32(&mut self, little_endian: bool) -> Result<u32, WkbError> {
        let bytes = self.take::<4>()?;
        Ok(if little_endian {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    fn f64(&mut self, little_endian: bool) -> Result<f64, WkbError> {
        let bytes = self.take::<8>()?;
        Ok(if little_endian {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::ewkb::geometry_to_ewkb;
    use super::super::format::{EWKB_SRID_FLAG, EWKB_Z_FLAG};
    use super::super::write::geometry_to_wkb;
    use super::*;
    use nodedb_types::geometry::Ordinates;

    fn xy(g: Geometry) -> SridGeometry {
        SridGeometry::wgs84(g)
    }

    fn decode(bytes: &[u8]) -> Result<SridGeometry, WkbError> {
        geometry_from_wkb(bytes).map(|w| w.geometry)
    }

    fn header(buf: &mut Vec<u8>, word: u32) {
        buf.push(1);
        buf.extend_from_slice(&word.to_le_bytes());
    }

    fn shapes() -> Vec<Geometry> {
        vec![
            Geometry::point(-73.9857, 40.7484),
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]),
            Geometry::polygon(vec![
                vec![
                    [0.0, 0.0],
                    [10.0, 0.0],
                    [10.0, 10.0],
                    [0.0, 10.0],
                    [0.0, 0.0],
                ],
                vec![[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]],
            ]),
            Geometry::MultiPoint {
                coordinates: vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            },
            Geometry::MultiLineString {
                coordinates: vec![vec![[0.0, 0.0], [1.0, 1.0]], vec![[2.0, 2.0], [4.0, 2.0]]],
            },
            Geometry::MultiPolygon {
                coordinates: vec![
                    vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                    vec![vec![[5.0, 5.0], [6.0, 5.0], [6.0, 6.0], [5.0, 5.0]]],
                ],
            },
            Geometry::GeometryCollection {
                geometries: vec![
                    Geometry::point(1.0, 2.0),
                    Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
                ],
            },
        ]
    }

    #[test]
    fn xy_shapes_round_trip() {
        for g in shapes() {
            let g = xy(g);
            let wkb = geometry_to_wkb(&g).unwrap();
            assert_eq!(decode(&wkb), Ok(g));
        }
    }

    #[test]
    fn z_m_and_zm_round_trip_in_iso_and_ewkb() {
        for g in shapes() {
            let n = g.coord_count();
            let seq = |k: f64| (0..n).map(|i| k + i as f64).collect::<Vec<_>>();
            for ordinates in [
                Ordinates::new(Some(seq(100.0)), None),
                Ordinates::new(None, Some(seq(200.0))),
                Ordinates::new(Some(seq(100.0)), Some(seq(200.0))),
            ] {
                let g = SridGeometry::with_ordinates(Srid::WGS84, g.clone(), ordinates).unwrap();
                let iso = geometry_to_wkb(&g).unwrap();
                assert_eq!(decode(&iso), Ok(g.clone()));
                let ewkb = geometry_to_ewkb(&g).unwrap();
                let back = geometry_from_wkb(&ewkb).unwrap();
                assert_eq!(back.srid, Some(4326));
                assert_eq!(back.geometry, g);
            }
        }
    }

    #[test]
    fn iso_point_zm_layout() {
        let g = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![3.0]), Some(vec![4.0])),
        )
        .unwrap();
        let wkb = geometry_to_wkb(&g).unwrap();
        assert_eq!(wkb.len(), 5 + 32);
        assert_eq!(u32::from_le_bytes(wkb[1..5].try_into().unwrap()), 3001);
    }

    #[test]
    fn empty_geometries_round_trip() {
        for kind in [
            GeometryKind::Point,
            GeometryKind::LineString,
            GeometryKind::Polygon,
            GeometryKind::MultiPoint,
            GeometryKind::MultiLineString,
            GeometryKind::MultiPolygon,
            GeometryKind::GeometryCollection,
        ] {
            let g = xy(Geometry::empty(kind));
            let back = decode(&geometry_to_wkb(&g).unwrap()).unwrap();
            assert!(back.is_empty(), "{kind:?}");
            assert_eq!(back.geometry().kind(), kind);
        }
    }

    #[test]
    fn member_dimension_must_match() {
        let mut wkb = Vec::new();
        header(&mut wkb, 1007); // GEOMETRYCOLLECTION Z
        wkb.extend_from_slice(&1u32.to_le_bytes());
        header(&mut wkb, 1); // POINT (XY)
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&2.0f64.to_le_bytes());
        assert!(matches!(
            decode(&wkb),
            Err(WkbError::MemberDimension { .. })
        ));
    }

    #[test]
    fn ewkb_flags_and_srid_are_read() {
        let mut wkb = Vec::new();
        header(&mut wkb, 1 | EWKB_Z_FLAG | EWKB_SRID_FLAG);
        wkb.extend_from_slice(&32648u32.to_le_bytes());
        for v in [1.0f64, 2.0, 3.0] {
            wkb.extend_from_slice(&v.to_le_bytes());
        }
        let w = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(w.srid, Some(32648));
        assert_eq!(w.geometry.ordinates().z(), Some(&[3.0][..]));
    }

    #[test]
    fn big_endian_is_read() {
        let mut wkb = vec![0u8];
        wkb.extend_from_slice(&1001u32.to_be_bytes());
        for v in [1.0f64, 2.0, 3.0] {
            wkb.extend_from_slice(&v.to_be_bytes());
        }
        assert_eq!(decode(&wkb).unwrap().ordinates().z(), Some(&[3.0][..]));
    }

    #[test]
    fn malformed_bytes_are_errors() {
        let wkb = geometry_to_wkb(&xy(Geometry::point(1.0, 2.0))).unwrap();
        assert!(matches!(decode(&wkb[..3]), Err(WkbError::Truncated { .. })));
        assert!(matches!(decode(&[]), Err(WkbError::Truncated { .. })));
        let mut bad = wkb.clone();
        bad[1] = 99;
        assert!(matches!(decode(&bad), Err(WkbError::UnknownType { .. })));
        let mut order = wkb.clone();
        order[0] = 7;
        assert!(matches!(decode(&order), Err(WkbError::ByteOrder { .. })));
        let mut trailing = wkb;
        trailing.push(0);
        assert_eq!(decode(&trailing), Err(WkbError::Trailing { count: 1 }));
    }

    #[test]
    fn oversized_and_truncated_counts_fail_before_allocation() {
        for code in [2u32, 3, 4, 5, 6, 7] {
            for count in [u32::MAX, 1] {
                let mut wkb = Vec::new();
                header(&mut wkb, code);
                wkb.extend_from_slice(&count.to_le_bytes());
                assert!(decode(&wkb).is_err(), "type={code} count={count}");
            }
        }
    }

    #[test]
    fn nested_counts_share_one_budget() {
        // The outer collection promises every remaining byte to its members,
        // so the first member cannot claim the same bytes again.
        let padding = 100usize;
        let mut wkb = Vec::new();
        header(&mut wkb, 7);
        let outer = (HEADER_BYTES + COUNT_BYTES + padding) / HEADER_BYTES;
        wkb.extend_from_slice(&(outer as u32).to_le_bytes());
        header(&mut wkb, 7);
        let inner_count_offset = wkb.len();
        wkb.extend_from_slice(&2u32.to_le_bytes());
        wkb.extend(std::iter::repeat_n(0u8, padding));
        assert_eq!(
            decode(&wkb),
            Err(WkbError::Truncated {
                offset: inner_count_offset
            })
        );
    }

    #[test]
    fn deep_nesting_reserves_capacity_once() {
        // Every level declares the largest count the input admits. Only the
        // outermost claim fits, so the reader rejects the second level.
        let mut wkb = Vec::new();
        let levels = 32usize;
        for _ in 0..levels {
            header(&mut wkb, 7);
            wkb.extend_from_slice(&0u32.to_le_bytes());
        }
        let total = wkb.len() + 4096;
        let mut cursor = 0usize;
        for _ in 0..levels {
            let count_at = cursor + HEADER_BYTES;
            let after = count_at + COUNT_BYTES;
            let count = (total - after) / HEADER_BYTES;
            wkb[count_at..after].copy_from_slice(&(count as u32).to_le_bytes());
            cursor = after;
        }
        wkb.resize(total, 0);
        assert_eq!(
            decode(&wkb),
            Err(WkbError::Truncated {
                offset: 2 * HEADER_BYTES + COUNT_BYTES
            })
        );
    }

    #[test]
    fn collection_depth_is_bounded() {
        let mut too_deep = Geometry::point(1.0, 2.0);
        for _ in 0..MAX_WKB_DEPTH {
            too_deep = Geometry::GeometryCollection {
                geometries: vec![too_deep],
            };
        }
        assert!(geometry_to_wkb(&xy(too_deep)).is_err());
    }

    #[test]
    fn nan_inside_a_line_is_refused() {
        let mut wkb = Vec::new();
        header(&mut wkb, 2);
        wkb.extend_from_slice(&1u32.to_le_bytes());
        wkb.extend_from_slice(&f64::NAN.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        assert!(matches!(decode(&wkb), Err(WkbError::NonFinite { .. })));
    }

    #[test]
    fn bbox_of_wkb() {
        let g = xy(Geometry::polygon(vec![vec![
            [-10.0, -5.0],
            [10.0, -5.0],
            [10.0, 5.0],
            [-10.0, 5.0],
            [-10.0, -5.0],
        ]]));
        let bb = wkb_bbox(&geometry_to_wkb(&g).unwrap()).unwrap();
        assert_eq!(
            (bb.min_lng, bb.max_lng, bb.min_lat, bb.max_lat),
            (-10.0, 10.0, -5.0, 5.0)
        );
    }
}
