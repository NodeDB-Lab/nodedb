// SPDX-License-Identifier: Apache-2.0

//! Region predicate over an array: a coordinate box and an optional
//! polygon zone on two numeric dims.
//!
//! A cell lies in an [`ArrayRegion`] when it passes the box ([`Slice`]) and,
//! when a zone is set, lies inside one member polygon of the zone and inside
//! none of that polygon's holes. Polygon membership uses the ray-casting rule
//! of [`nodedb_types::geometry::point_in_polygon`]: a point on a minimum
//! edge is inside, a point on a maximum edge is outside.
//!
//! A zone with a [`ZoneFrame`] is drawn in an SRID. Its cells map into
//! that SRID before the ray cast, and its frame's dim box bounds pruning.
//!
//! Two entry points:
//! * [`ArrayRegion::tile_may_contain`] — MBR-only check. A `false` proves
//!   the tile holds no live cell in the region, so no payload is decoded.
//! * [`filter_region_owned`] — per-cell filter of one tile, through a
//!   [`PreparedRegion`] built once per scan.

use nodedb_crs::Transformer;
use nodedb_types::geometry::point_in_polygon;

use super::slice::{Slice, cell_in_slice, filter_sparse, slice_sparse_owned, tile_overlaps_slice};
use super::zone_frame::ZoneFrame;
use crate::error::ArrayResult;
use crate::schema::ArraySchema;
use crate::tile::mbr::TileMBR;
use crate::tile::sparse_tile::SparseTile;
use crate::types::coord::value::CoordValue;
use crate::types::domain::DomainBound;

/// One polygon of a zone. Each ring is a closed list of `[x, y]` positions.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ZonePolygon {
    pub exterior: Vec<[f64; 2]>,
    pub holes: Vec<Vec<[f64; 2]>>,
}

impl ZonePolygon {
    fn contains(&self, x: f64, y: f64) -> bool {
        point_in_polygon(x, y, &self.exterior)
            && !self.holes.iter().any(|hole| point_in_polygon(x, y, hole))
    }

    /// `[min_x, min_y, max_x, max_y]` of the exterior ring, or `None` for an
    /// empty ring.
    fn bbox(&self) -> Option<[f64; 4]> {
        let first = self.exterior.first()?;
        let mut b = [first[0], first[1], first[0], first[1]];
        for p in &self.exterior {
            b[0] = b[0].min(p[0]);
            b[1] = b[1].min(p[1]);
            b[2] = b[2].max(p[0]);
            b[3] = b[3].max(p[1]);
        }
        Some(b)
    }
}

/// A polygon zone over two numeric dims. `x_dim` and `y_dim` are schema dim
/// positions. A cell is in the zone when it lies in any member polygon.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PolygonZone {
    pub x_dim: u32,
    pub y_dim: u32,
    pub polygons: Vec<ZonePolygon>,
    /// `None`: the polygons are drawn in dim coordinates. `Some`: they are
    /// drawn in the frame's zone SRID.
    pub frame: Option<ZoneFrame>,
}

impl PolygonZone {
    /// The zone ready for per-cell tests. Build it once per scan.
    pub fn prepare(&self) -> PreparedZone<'_> {
        PreparedZone {
            zone: self,
            frame: self.frame.as_ref().map(|f| (f, f.cell_transformer())),
        }
    }

    /// False only when the zone's dim extent misses the MBR's `x_dim` ×
    /// `y_dim` rectangle. A bound that is not numeric proves nothing, so the
    /// tile stays in.
    ///
    /// The dim extent is the frame's dim box, or each member polygon's
    /// bounding box when the zone has no frame.
    pub fn may_overlap(&self, mbr: &TileMBR) -> bool {
        let bound = |dims: &[DomainBound], dim: u32| dims.get(dim as usize).and_then(bound_to_f64);
        let (Some(tx0), Some(ty0), Some(tx1), Some(ty1)) = (
            bound(&mbr.dim_mins, self.x_dim),
            bound(&mbr.dim_mins, self.y_dim),
            bound(&mbr.dim_maxs, self.x_dim),
            bound(&mbr.dim_maxs, self.y_dim),
        ) else {
            return true;
        };
        let meets =
            |[px0, py0, px1, py1]: [f64; 4]| px0 <= tx1 && tx0 <= px1 && py0 <= ty1 && ty0 <= py1;
        match &self.frame {
            Some(frame) => meets(frame.dim_bbox),
            None => self
                .polygons
                .iter()
                .filter_map(ZonePolygon::bbox)
                .any(meets),
        }
    }
}

/// A [`PolygonZone`] with its cell-to-zone transformer built.
pub struct PreparedZone<'a> {
    zone: &'a PolygonZone,
    frame: Option<(&'a ZoneFrame, Transformer)>,
}

impl PreparedZone<'_> {
    /// True if the cell at `coord` lies in the zone.
    ///
    /// With a frame, the cell centre maps into the zone SRID first. A cell
    /// the projection cannot map is outside.
    pub fn contains(&self, coord: &[CoordValue]) -> bool {
        let (Some(x), Some(y)) = (
            coord.get(self.zone.x_dim as usize),
            coord.get(self.zone.y_dim as usize),
        ) else {
            return false;
        };
        let point = match &self.frame {
            Some((frame, transformer)) => frame.cell_to_zone(transformer, x, y),
            None => coord_to_f64(x).zip(coord_to_f64(y)).map(|(x, y)| [x, y]),
        };
        point.is_some_and(|[x, y]| self.zone.polygons.iter().any(|p| p.contains(x, y)))
    }
}

/// The cells an aggregate reads: a coordinate box and an optional zone.
/// `slice.dim_ranges` is empty or holds one entry per schema dim.
#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayRegion {
    pub slice: Slice,
    pub zone: Option<PolygonZone>,
}

impl ArrayRegion {
    pub fn new(slice: Slice, zone: Option<PolygonZone>) -> Self {
        Self { slice, zone }
    }

    /// False only when no live cell of a tile with this MBR lies in the
    /// region.
    pub fn tile_may_contain(&self, mbr: &TileMBR) -> bool {
        tile_overlaps_slice(&mbr.dim_mins, &mbr.dim_maxs, &self.slice)
            && self.zone.as_ref().is_none_or(|z| z.may_overlap(mbr))
    }

    /// The region ready for per-cell tests. Build it once per scan.
    pub fn prepare(&self) -> PreparedRegion<'_> {
        PreparedRegion {
            region: self,
            zone: self.zone.as_ref().map(PolygonZone::prepare),
        }
    }
}

/// An [`ArrayRegion`] with its zone prepared.
pub struct PreparedRegion<'a> {
    region: &'a ArrayRegion,
    zone: Option<PreparedZone<'a>>,
}

impl PreparedRegion<'_> {
    /// The region this was prepared from.
    pub fn region(&self) -> &ArrayRegion {
        self.region
    }

    /// True if the cell at `coord` lies in the region.
    pub fn contains(&self, coord: &[CoordValue]) -> bool {
        cell_in_slice(coord, &self.region.slice)
            && self.zone.as_ref().is_none_or(|z| z.contains(coord))
    }
}

/// The live cells of `tile` that lie in `region`. A tile whose MBR misses
/// the region is replaced by an empty tile with no per-cell work.
pub fn filter_region_owned(
    schema: &ArraySchema,
    tile: SparseTile,
    region: &PreparedRegion<'_>,
) -> ArrayResult<SparseTile> {
    if !region.region.tile_may_contain(&tile.mbr) {
        return Ok(SparseTile::empty(schema));
    }
    match &region.zone {
        None => slice_sparse_owned(schema, tile, &region.region.slice),
        Some(_) => filter_sparse(schema, &tile, |coord| region.contains(coord)),
    }
}

/// A numeric coordinate as `f64`. `None` for a timestamp or string.
pub(crate) fn coord_to_f64(c: &CoordValue) -> Option<f64> {
    match c {
        CoordValue::Int64(v) => Some(*v as f64),
        CoordValue::Float64(v) => Some(*v),
        CoordValue::TimestampMs(_) | CoordValue::String(_) => None,
    }
}

fn bound_to_f64(b: &DomainBound) -> Option<f64> {
    match b {
        DomainBound::Int64(v) => Some(*v as f64),
        DomainBound::Float64(v) => Some(*v),
        DomainBound::TimestampMs(_) | DomainBound::String(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::slice::DimRange;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::cell_value::value::CellValue;
    use crate::types::domain::Domain;
    use nodedb_types::geometry::Srid;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Float64,
                Domain::new(DomainBound::Float64(0.0), DomainBound::Float64(100.0)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Float64,
                Domain::new(DomainBound::Float64(0.0), DomainBound::Float64(100.0)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![10, 10])
            .build()
            .unwrap()
    }

    fn tile(cells: &[(f64, f64)]) -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for (x, y) in cells {
            b.push(
                &[CoordValue::Float64(*x), CoordValue::Float64(*y)],
                &[CellValue::Float64(1.0)],
            )
            .unwrap();
        }
        b.build()
    }

    fn square(lo: f64, hi: f64) -> Vec<[f64; 2]> {
        vec![[lo, lo], [hi, lo], [hi, hi], [lo, hi], [lo, lo]]
    }

    fn zone(polygons: Vec<ZonePolygon>) -> ArrayRegion {
        ArrayRegion::new(
            Slice::default(),
            Some(PolygonZone {
                x_dim: 0,
                y_dim: 1,
                polygons,
                frame: None,
            }),
        )
    }

    /// One-off membership for tests. A scan prepares the region once.
    trait Contains {
        fn contains(&self, coord: &[CoordValue]) -> bool;
    }

    impl Contains for ArrayRegion {
        fn contains(&self, coord: &[CoordValue]) -> bool {
            self.prepare().contains(coord)
        }
    }

    fn at(x: f64, y: f64) -> [CoordValue; 2] {
        [CoordValue::Float64(x), CoordValue::Float64(y)]
    }

    /// A zone drawn in `zone_srid` over a world-mode array in `array_srid`.
    fn framed(array_srid: Srid, zone_srid: Srid, polygons: Vec<ZonePolygon>) -> ArrayRegion {
        let ctx = nodedb_crs::CrsContext::builtin_only();
        ArrayRegion::new(
            Slice::default(),
            Some(PolygonZone {
                x_dim: 0,
                y_dim: 1,
                polygons,
                frame: Some(ZoneFrame {
                    transform: None,
                    array_crs: (*ctx.resolve(array_srid).unwrap()).clone(),
                    zone_crs: (*ctx.resolve(zone_srid).unwrap()).clone(),
                    dim_bbox: [-180.0, -90.0, 180.0, 90.0],
                }),
            }),
        )
    }

    #[test]
    fn a_framed_zone_keeps_the_edge_and_hole_rules() {
        let r = framed(
            Srid::WGS84,
            Srid::WGS84,
            vec![ZonePolygon {
                exterior: square(0.0, 10.0),
                holes: vec![square(4.0, 6.0)],
            }],
        );
        assert!(r.contains(&at(0.0, 2.0)), "min x edge");
        assert!(r.contains(&at(2.0, 0.0)), "min y edge");
        assert!(!r.contains(&at(10.0, 2.0)), "max x edge");
        assert!(!r.contains(&at(2.0, 10.0)), "max y edge");
        assert!(r.contains(&at(2.0, 2.0)));
        assert!(!r.contains(&at(5.0, 5.0)), "inside the hole");
        assert!(
            !r.contains(&at(4.0, 5.0)),
            "on the hole's min edge, so in the hole"
        );
        assert!(
            r.contains(&at(6.0, 5.0)),
            "on the hole's max edge, so outside it"
        );
    }

    #[test]
    fn a_cell_the_projection_cannot_map_is_outside() {
        let r = framed(
            Srid::WGS84,
            Srid::new(32647).unwrap(),
            vec![ZonePolygon {
                exterior: square(-1.0e7, 1.0e7),
                holes: Vec::new(),
            }],
        );
        assert!(r.contains(&at(99.5, 6.0)), "inside the UTM 47N domain");
        assert!(
            !r.contains(&at(150.0, 6.0)),
            "51° from the central meridian"
        );
        assert!(!r.contains(&[CoordValue::Float64(f64::NAN), CoordValue::Float64(6.0)]));
    }

    #[test]
    fn a_framed_zone_prunes_by_its_dim_box() {
        let mut r = framed(
            Srid::WGS84,
            Srid::WGS84,
            vec![ZonePolygon {
                exterior: square(0.0, 50.0),
                holes: Vec::new(),
            }],
        );
        if let Some(zone) = r.zone.as_mut()
            && let Some(frame) = zone.frame.as_mut()
        {
            frame.dim_bbox = [0.0, 0.0, 4.0, 4.0];
        }
        assert!(r.tile_may_contain(&tile(&[(1.0, 1.0)]).mbr));
        assert!(
            !r.tile_may_contain(&tile(&[(20.0, 20.0)]).mbr),
            "the dim box prunes, not the polygon's own box"
        );
    }

    #[test]
    fn a_minimum_edge_is_in_and_a_maximum_edge_is_out() {
        let r = zone(vec![ZonePolygon {
            exterior: square(0.0, 4.0),
            holes: Vec::new(),
        }]);
        assert!(r.contains(&at(0.0, 2.0)), "min x edge");
        assert!(r.contains(&at(2.0, 0.0)), "min y edge");
        assert!(!r.contains(&at(4.0, 2.0)), "max x edge");
        assert!(!r.contains(&at(2.0, 4.0)), "max y edge");
        assert!(r.contains(&at(2.0, 2.0)));
        assert!(!r.contains(&at(5.0, 2.0)));
    }

    #[test]
    fn a_hole_excludes_its_cells() {
        let r = zone(vec![ZonePolygon {
            exterior: square(0.0, 10.0),
            holes: vec![square(4.0, 6.0)],
        }]);
        assert!(r.contains(&at(2.0, 2.0)));
        assert!(!r.contains(&at(5.0, 5.0)));
    }

    #[test]
    fn any_member_polygon_admits_a_cell() {
        let r = zone(vec![
            ZonePolygon {
                exterior: square(0.0, 2.0),
                holes: Vec::new(),
            },
            ZonePolygon {
                exterior: square(8.0, 10.0),
                holes: Vec::new(),
            },
        ]);
        assert!(r.contains(&at(1.0, 1.0)));
        assert!(r.contains(&at(9.0, 9.0)));
        assert!(!r.contains(&at(5.0, 5.0)));
    }

    #[test]
    fn the_box_and_the_zone_both_bound_the_region() {
        let mut r = zone(vec![ZonePolygon {
            exterior: square(0.0, 10.0),
            holes: Vec::new(),
        }]);
        r.slice = Slice::new(vec![
            Some(DimRange::new(
                DomainBound::Float64(0.0),
                DomainBound::Float64(3.0),
            )),
            None,
        ]);
        assert!(r.contains(&at(2.0, 8.0)));
        assert!(!r.contains(&at(5.0, 8.0)), "outside the box");
    }

    #[test]
    fn a_tile_beside_every_polygon_is_pruned() {
        let r = zone(vec![ZonePolygon {
            exterior: square(0.0, 4.0),
            holes: Vec::new(),
        }]);
        assert!(r.tile_may_contain(&tile(&[(1.0, 1.0), (9.0, 9.0)]).mbr));
        assert!(!r.tile_may_contain(&tile(&[(6.0, 6.0), (9.0, 9.0)]).mbr));
        assert!(ArrayRegion::default().tile_may_contain(&tile(&[(9.0, 9.0)]).mbr));
    }

    #[test]
    fn the_filter_keeps_the_cells_inside() {
        let s = schema();
        let r = zone(vec![ZonePolygon {
            exterior: square(0.0, 4.0),
            holes: Vec::new(),
        }]);
        let r = r.prepare();
        let out = filter_region_owned(&s, tile(&[(1.0, 1.0), (0.0, 3.0), (4.0, 1.0)]), &r).unwrap();
        assert_eq!(out.nnz(), 2);
        let pruned = filter_region_owned(&s, tile(&[(6.0, 6.0)]), &r).unwrap();
        assert_eq!(pruned.nnz(), 0);
        let whole = ArrayRegion::default();
        let whole = filter_region_owned(&s, tile(&[(6.0, 6.0)]), &whole.prepare()).unwrap();
        assert_eq!(whole.nnz(), 1);
    }

    #[test]
    fn a_region_round_trips_through_msgpack() {
        let r = zone(vec![ZonePolygon {
            exterior: square(0.0, 4.0),
            holes: vec![square(1.0, 2.0)],
        }]);
        let bytes = zerompk::to_msgpack_vec(&r).unwrap();
        let back: ArrayRegion = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back.zone, r.zone);
    }
}
