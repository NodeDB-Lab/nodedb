// SPDX-License-Identifier: Apache-2.0

//! The frame of a zone drawn in an SRID: how a cell reaches the zone's SRID,
//! and the dim box that bounds the zone for tile pruning.
//!
//! Membership is exact per cell. The cell centre maps into the array CRS,
//! through the affine in pixel mode. It then maps into the zone SRID, and ray
//! casting runs there. A zone edge is straight in the zone SRID, so its image
//! in dim space is curved. The dim box densifies each exterior edge by
//! recursive bisection before it takes the bounds.
//!
//! The Control Plane resolves both CRS definitions when it builds the frame
//! and the frame carries them, so the Data Plane maps cells without a CRS
//! registry.

use std::sync::Arc;

use nodedb_crs::{CrsContext, CrsDef, Transformer, UnknownSrid};
use nodedb_types::geometry::Srid;
use serde::{Deserialize, Serialize};

use super::region::{ZonePolygon, coord_to_f64};
use crate::schema::{ArraySchema, DimType, GeoTransform};
use crate::types::coord::value::CoordValue;

/// Most positions one dim box may map, summed over every edge.
pub const MAX_FRAME_VERTICES: usize = 1 << 20;

/// Most bisection levels one edge may take.
pub const MAX_BISECTION_DEPTH: u32 = 24;

/// Chord tolerance in pixel mode, in pixels.
const PIXEL_TOLERANCE: f64 = 0.25;

/// World-mode tolerance is the smallest X/Y tile extent over this divisor.
const WORLD_TOLERANCE_DIVISOR: f64 = 64.0;

/// Offset from a pixel corner to its centre, in pixels.
const PIXEL_CENTRE: f64 = 0.5;

/// The CRS definitions and affine that carry a cell into the zone's SRID.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ZoneFrame {
    /// The georef affine. `None` means the dims hold world coordinates.
    pub transform: Option<GeoTransform>,
    /// The array's GEOREF CRS, resolved.
    pub array_crs: CrsDef,
    /// The zone's CRS, resolved.
    pub zone_crs: CrsDef,
    /// `[min_x, min_y, max_x, max_y]` in dim coordinates.
    ///
    /// Every cell the zone holds lies inside it. Pruning reads it. Membership
    /// never does.
    pub dim_bbox: [f64; 4],
}

impl ZoneFrame {
    /// The cell-to-zone transformer. Build one per scan, never one per cell.
    pub fn cell_transformer(&self) -> Transformer {
        Transformer::new(
            Arc::new(self.array_crs.clone()),
            Arc::new(self.zone_crs.clone()),
        )
    }

    /// The SRID of the zone's coordinates.
    pub fn zone_srid(&self) -> Srid {
        self.zone_crs.srid
    }

    /// Position in the zone SRID of the centre of the cell at dim
    /// coordinates `(x, y)`.
    ///
    /// `None` when a coordinate has the wrong type or a projection cannot
    /// map the centre.
    pub fn cell_to_zone(
        &self,
        transformer: &Transformer,
        x: &CoordValue,
        y: &CoordValue,
    ) -> Option<[f64; 2]> {
        let world = match &self.transform {
            Some(gt) => match (x, y) {
                (CoordValue::Int64(i), CoordValue::Int64(j)) => {
                    let (wx, wy) = gt.cell_centre(*i, *j);
                    [wx, wy]
                }
                _ => return None,
            },
            None => [coord_to_f64(x)?, coord_to_f64(y)?],
        };
        transformer
            .apply([world[0], world[1], 0.0])
            .ok()
            .map(|[zx, zy, _]| [zx, zy])
    }
}

/// Why a zone frame cannot be built.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ZoneFrameError {
    /// A zone with an SRID needs the array's GEOREF. SQLSTATE `55000`.
    #[error("array '{array}' has no GEOREF; a zone with an SRID needs one")]
    NoGeoref { array: String },

    /// Bounding the zone edges needs more positions than the budget.
    /// SQLSTATE `54000`.
    #[error(
        "zone on array '{array}' needs more than {limit} positions to bound its edges; \
         simplify the zone or split it"
    )]
    VertexBudget { array: String, limit: usize },

    /// A zone position cannot map into the array's dims. SQLSTATE `22000`.
    #[error(
        "zone position ({x}, {y}) in SRID {zone_srid} cannot map into the dims of \
         array '{array}': {detail}"
    )]
    OutOfDomain {
        array: String,
        x: f64,
        y: f64,
        zone_srid: Srid,
        detail: String,
    },

    /// The zone has no exterior position. SQLSTATE `22000`.
    #[error("zone on array '{array}' has no exterior position")]
    EmptyZone { array: String },

    /// The zone SRID or the array's GEOREF SRID names no defined CRS.
    /// SQLSTATE `22000`.
    #[error("zone on array '{array}': {source}")]
    Crs { array: String, source: UnknownSrid },
}

impl ZoneFrameError {
    /// The array the frame was built for.
    pub fn array(&self) -> &str {
        match self {
            ZoneFrameError::NoGeoref { array }
            | ZoneFrameError::VertexBudget { array, .. }
            | ZoneFrameError::OutOfDomain { array, .. }
            | ZoneFrameError::EmptyZone { array }
            | ZoneFrameError::Crs { array, .. } => array,
        }
    }
}

/// Build the frame of `polygons`, drawn in `zone_srid`, over `schema`.
/// Both SRIDs resolve through `ctx`.
///
/// The dim box is padded by twice the chord tolerance. An `INT64` dim rounds
/// it outward to whole coordinates.
pub fn build_zone_frame(
    schema: &ArraySchema,
    zone_srid: Srid,
    polygons: &[ZonePolygon],
    ctx: &CrsContext<'_>,
) -> Result<ZoneFrame, ZoneFrameError> {
    build_with_budget(schema, zone_srid, polygons, ctx, MAX_FRAME_VERTICES)
}

fn build_with_budget(
    schema: &ArraySchema,
    zone_srid: Srid,
    polygons: &[ZonePolygon],
    ctx: &CrsContext<'_>,
    budget: usize,
) -> Result<ZoneFrame, ZoneFrameError> {
    let georef = schema
        .georef
        .as_ref()
        .ok_or_else(|| ZoneFrameError::NoGeoref {
            array: schema.name.clone(),
        })?;
    let resolve = |srid: Srid| {
        ctx.resolve(srid).map_err(|source| ZoneFrameError::Crs {
            array: schema.name.clone(),
            source,
        })
    };
    let zone_crs = resolve(zone_srid)?;
    let array_crs = resolve(georef.srid)?;
    let tolerance = match georef.transform {
        Some(_) => PIXEL_TOLERANCE,
        None => world_tolerance(schema, georef.x_dim, georef.y_dim),
    };
    let mut walk = EdgeWalk {
        array: &schema.name,
        transformer: Transformer::new(Arc::clone(&zone_crs), Arc::clone(&array_crs)),
        transform: georef.transform,
        zone_srid,
        tolerance,
        budget,
        mapped: 0,
        bbox: None,
    };
    for polygon in polygons {
        walk.ring(&polygon.exterior)?;
    }
    let [x0, y0, x1, y1] = walk.bbox.ok_or_else(|| ZoneFrameError::EmptyZone {
        array: schema.name.clone(),
    })?;
    let pad = 2.0 * tolerance;
    let mut dim_bbox = [x0 - pad, y0 - pad, x1 + pad, y1 + pad];
    if is_int64(schema, georef.x_dim) {
        dim_bbox[0] = dim_bbox[0].floor();
        dim_bbox[2] = dim_bbox[2].ceil();
    }
    if is_int64(schema, georef.y_dim) {
        dim_bbox[1] = dim_bbox[1].floor();
        dim_bbox[3] = dim_bbox[3].ceil();
    }
    Ok(ZoneFrame {
        transform: georef.transform,
        array_crs: (*array_crs).clone(),
        zone_crs: (*zone_crs).clone(),
        dim_bbox,
    })
}

/// The smallest X/Y tile extent over [`WORLD_TOLERANCE_DIVISOR`].
fn world_tolerance(schema: &ArraySchema, x_dim: u32, y_dim: u32) -> f64 {
    let extent = [x_dim, y_dim]
        .iter()
        .filter_map(|&d| schema.tile_extents.get(d as usize).copied())
        .min()
        .unwrap_or(1)
        .max(1);
    extent as f64 / WORLD_TOLERANCE_DIVISOR
}

fn is_int64(schema: &ArraySchema, dim: u32) -> bool {
    schema
        .dims
        .get(dim as usize)
        .is_some_and(|d| d.dtype == DimType::Int64)
}

fn midpoint(a: [f64; 2], b: [f64; 2]) -> [f64; 2] {
    [(a[0] + b[0]) / 2.0, (a[1] + b[1]) / 2.0]
}

/// Maps zone positions into dim space and keeps their bounds.
struct EdgeWalk<'a> {
    array: &'a str,
    transformer: Transformer,
    transform: Option<GeoTransform>,
    zone_srid: Srid,
    tolerance: f64,
    budget: usize,
    mapped: usize,
    bbox: Option<[f64; 4]>,
}

impl EdgeWalk<'_> {
    /// Bound one ring: every vertex, then every edge, the closing edge too.
    fn ring(&mut self, ring: &[[f64; 2]]) -> Result<(), ZoneFrameError> {
        let Some((&first, rest)) = ring.split_first() else {
            return Ok(());
        };
        let first_dim = self.map(first)?;
        let mut prev = (first, first_dim);
        for &p in rest {
            let p_dim = if p == first { first_dim } else { self.map(p)? };
            self.edge(prev.0, prev.1, p, p_dim, 0)?;
            prev = (p, p_dim);
        }
        if prev.0 != first {
            self.edge(prev.0, prev.1, first, first_dim, 0)?;
        }
        Ok(())
    }

    /// Bisect edge `a → b` until the mapped midpoint lies within the
    /// tolerance of the mapped chord's midpoint, or the depth cap is reached.
    fn edge(
        &mut self,
        a: [f64; 2],
        a_dim: [f64; 2],
        b: [f64; 2],
        b_dim: [f64; 2],
        depth: u32,
    ) -> Result<(), ZoneFrameError> {
        if depth >= MAX_BISECTION_DEPTH {
            return Ok(());
        }
        let mid = midpoint(a, b);
        let mid_dim = self.map(mid)?;
        let chord = midpoint(a_dim, b_dim);
        if (mid_dim[0] - chord[0]).hypot(mid_dim[1] - chord[1]) <= self.tolerance {
            return Ok(());
        }
        self.edge(a, a_dim, mid, mid_dim, depth + 1)?;
        self.edge(mid, mid_dim, b, b_dim, depth + 1)
    }

    /// Map one zone position into dim coordinates and extend the bounds.
    fn map(&mut self, p: [f64; 2]) -> Result<[f64; 2], ZoneFrameError> {
        if self.mapped >= self.budget {
            return Err(ZoneFrameError::VertexBudget {
                array: self.array.to_string(),
                limit: self.budget,
            });
        }
        self.mapped += 1;
        let [wx, wy, _] = self
            .transformer
            .apply([p[0], p[1], 0.0])
            .map_err(|e| self.out_of_domain(p, e.to_string()))?;
        let world = [wx, wy];
        let dim = match &self.transform {
            None => world,
            Some(gt) => {
                let (u, v) = gt
                    .world_to_pixel(world[0], world[1])
                    .ok_or_else(|| self.out_of_domain(p, "the GEOREF has no inverse".into()))?;
                [u - PIXEL_CENTRE, v - PIXEL_CENTRE]
            }
        };
        if !(dim[0].is_finite() && dim[1].is_finite()) {
            return Err(self.out_of_domain(p, "the dim position is not finite".into()));
        }
        self.bbox = Some(match self.bbox {
            None => [dim[0], dim[1], dim[0], dim[1]],
            Some(b) => [
                b[0].min(dim[0]),
                b[1].min(dim[1]),
                b[2].max(dim[0]),
                b[3].max(dim[1]),
            ],
        });
        Ok(dim)
    }

    fn out_of_domain(&self, p: [f64; 2], detail: String) -> ZoneFrameError {
        ZoneFrameError::OutOfDomain {
            array: self.array.to_string(),
            x: p[0],
            y: p[1],
            zone_srid: self.zone_srid,
            detail,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::region::PolygonZone;
    use crate::schema::{ArrayGeoref, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec};
    use crate::types::domain::{Domain, DomainBound};

    /// Pixel size of the UTM grids, metres.
    const PIXEL_M: f64 = 30.0;

    fn ctx() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn utm47n() -> Srid {
        Srid::new(32647).unwrap()
    }

    fn transformer(from: Srid, to: Srid) -> Transformer {
        ctx().transformer(from, to).unwrap()
    }

    fn apply2(t: &Transformer, [x, y]: [f64; 2]) -> [f64; 2] {
        let [tx, ty, _] = t.apply([x, y, 0.0]).unwrap();
        [tx, ty]
    }

    fn utm(lon: f64, lat: f64) -> [f64; 2] {
        apply2(&transformer(Srid::WGS84, utm47n()), [lon, lat])
    }

    fn pixel_schema(gt: [f64; 6]) -> ArraySchema {
        let dim = |name: &str| {
            DimSpec::new(
                name,
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1 << 20)),
            )
        };
        ArraySchemaBuilder::new("scene")
            .dim(dim("row"))
            .dim(dim("col"))
            .attr(AttrSpec::new("b1", AttrType::Float64, true))
            .tile_extents(vec![256, 256])
            .build()
            .unwrap()
            .with_georef(Some(ArrayGeoref {
                srid: utm47n(),
                x_dim: 1,
                y_dim: 0,
                transform: Some(GeoTransform(gt)),
            }))
            .unwrap()
    }

    fn world_schema(srid: Srid, lo: f64, hi: f64, extent: u64) -> ArraySchema {
        let dim = |name: &str| {
            DimSpec::new(
                name,
                DimType::Float64,
                Domain::new(DomainBound::Float64(lo), DomainBound::Float64(hi)),
            )
        };
        ArraySchemaBuilder::new("world")
            .dim(dim("x"))
            .dim(dim("y"))
            .attr(AttrSpec::new("b1", AttrType::Float64, true))
            .tile_extents(vec![extent, extent])
            .build()
            .unwrap()
            .with_georef(Some(ArrayGeoref {
                srid,
                x_dim: 0,
                y_dim: 1,
                transform: None,
            }))
            .unwrap()
    }

    fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> ZonePolygon {
        ZonePolygon {
            exterior: vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]],
            holes: Vec::new(),
        }
    }

    /// The 30 m UTM 47N grid around the 6°N parallel between 99°E and 100°E.
    /// Returns the schema and its column and row counts.
    fn grid_at_six_north() -> (ArraySchema, i64, i64) {
        let west = utm(98.995, 6.0)[0].floor();
        let east = utm(100.005, 6.0)[0].ceil();
        let north = utm(100.005, 6.004)[1].ceil();
        let south = utm(99.0, 5.996)[1].floor();
        let cols = ((east - west) / PIXEL_M).ceil() as i64;
        let rows = ((north - south) / PIXEL_M).ceil() as i64;
        let schema = pixel_schema([west, PIXEL_M, 0.0, north, 0.0, -PIXEL_M]);
        (schema, cols, rows)
    }

    #[test]
    fn a_curved_edge_bounds_every_cell_inside_it() {
        let (schema, cols, rows) = grid_at_six_north();
        let polygons = vec![rect(99.0, 6.0, 100.0, 6.5)];
        let frame = build_zone_frame(&schema, Srid::WGS84, &polygons, &ctx()).unwrap();
        let zone = PolygonZone {
            x_dim: 1,
            y_dim: 0,
            polygons,
            frame: Some(frame.clone()),
        };
        let prepared = zone.prepare();
        let mut inside = 0usize;
        for row in 0..rows {
            for col in 0..cols {
                if prepared.contains(&[CoordValue::Int64(row), CoordValue::Int64(col)]) {
                    inside += 1;
                    let [x0, y0, x1, y1] = frame.dim_bbox;
                    let (c, r) = (col as f64, row as f64);
                    assert!(
                        x0 <= c && c <= x1 && y0 <= r && r <= y1,
                        "cell ({row}, {col}) is inside the zone but outside {:?}",
                        frame.dim_bbox
                    );
                }
            }
        }
        assert!(inside > 0, "the grid holds cells inside the zone");
    }

    /// The 6°N edge from 99°E to 100°E is straight in 4326. Its UTM image
    /// bows up to 25 m south of the chord between its projected vertices.
    #[test]
    fn exact_membership_follows_the_parallel_and_matches_per_cell_truth() {
        let (schema, cols, rows) = grid_at_six_north();
        let gt = schema.georef.as_ref().unwrap().transform.unwrap();
        let polygons = vec![rect(99.0, 6.0, 100.0, 6.5)];
        let frame = build_zone_frame(&schema, Srid::WGS84, &polygons, &ctx()).unwrap();
        let zone = PolygonZone {
            x_dim: 1,
            y_dim: 0,
            polygons: polygons.clone(),
            frame: Some(frame),
        };
        let prepared = zone.prepare();
        let straight: Vec<[f64; 2]> = polygons[0]
            .exterior
            .iter()
            .map(|p| utm(p[0], p[1]))
            .collect();
        let to_lon_lat = transformer(utm47n(), Srid::WGS84);
        let (mut differs, mut inside) = (0usize, 0usize);
        for row in 0..rows {
            for col in 0..cols {
                let exact = prepared.contains(&[CoordValue::Int64(row), CoordValue::Int64(col)]);
                let (x, y) = gt.cell_centre(col, row);
                let [lon, lat] = apply2(&to_lon_lat, [x, y]);
                let truth = (99.0..100.0).contains(&lon) && (6.0..6.5).contains(&lat);
                assert_eq!(exact, truth, "cell ({row}, {col}) at ({lon}, {lat})");
                let vertex_only = nodedb_types::geometry::point_in_polygon(x, y, &straight);
                differs += usize::from(exact != vertex_only);
                inside += usize::from(exact);
            }
        }
        assert!(inside > 0, "the grid holds cells inside the zone");
        assert!(
            differs > 100,
            "only {differs} cells differ from the straight UTM edges"
        );
    }

    #[test]
    fn the_dim_bbox_follows_the_parallel_not_the_chord() {
        let (schema, _, _) = grid_at_six_north();
        // The south edge straddles the central meridian at 99°E. In UTM the
        // parallel dips south at the meridian, between the two vertices.
        let frame = build_zone_frame(
            &schema,
            Srid::WGS84,
            &[rect(98.5, 6.0, 99.5, 6.001)],
            &ctx(),
        )
        .unwrap();
        let gt = schema.georef.as_ref().unwrap().transform.unwrap();
        let row_of = |lon: f64| {
            let p = utm(lon, 6.0);
            gt.world_to_pixel(p[0], p[1]).unwrap().1 - 0.5
        };
        let vertex_row = row_of(98.5).max(row_of(99.5));
        let bulge_row = row_of(99.0);
        assert!(
            bulge_row > vertex_row + 2.0 * PIXEL_TOLERANCE,
            "the bulge passes the padded vertex box: {bulge_row} vs {vertex_row}"
        );
        assert!(frame.dim_bbox[3] >= bulge_row, "{:?}", frame.dim_bbox);
        assert!(
            frame.dim_bbox.iter().all(|b| b.fract() == 0.0),
            "INT64 dims round"
        );
    }

    #[test]
    fn a_world_frame_pads_by_the_tile_tolerance() {
        let schema = world_schema(Srid::WGS84, -180.0, 180.0, 64);
        let frame = build_zone_frame(
            &schema,
            Srid::WGS84,
            &[rect(100.0, 2.0, 101.0, 3.0)],
            &ctx(),
        )
        .unwrap();
        // Same SRID: the edges map straight, and the pad is 2 · 64 / 64.
        assert_eq!(frame.dim_bbox, [98.0, 0.0, 103.0, 5.0]);
        assert_eq!(frame.transform, None);
    }

    #[test]
    fn a_zone_without_georef_is_refused() {
        let mut schema = world_schema(Srid::WGS84, 0.0, 10.0, 1);
        schema.georef = None;
        let err = build_zone_frame(&schema, Srid::WGS84, &[rect(1.0, 1.0, 2.0, 2.0)], &ctx())
            .expect_err("refused");
        assert!(matches!(err, ZoneFrameError::NoGeoref { .. }), "{err:?}");
    }

    #[test]
    fn a_vertex_outside_the_projection_is_refused() {
        let (schema, _, _) = grid_at_six_north();
        let err = build_zone_frame(
            &schema,
            Srid::WGS84,
            &[rect(150.0, 6.0, 151.0, 7.0)],
            &ctx(),
        )
        .expect_err("refused");
        assert!(matches!(err, ZoneFrameError::OutOfDomain { .. }), "{err:?}");
    }

    #[test]
    fn the_vertex_budget_is_enforced() {
        let (schema, _, _) = grid_at_six_north();
        let polygons = [rect(99.0, 6.0, 100.0, 6.5)];
        assert!(build_with_budget(&schema, Srid::WGS84, &polygons, &ctx(), 1 << 16).is_ok());
        let err =
            build_with_budget(&schema, Srid::WGS84, &polygons, &ctx(), 8).expect_err("over budget");
        assert_eq!(
            err,
            ZoneFrameError::VertexBudget {
                array: "scene".into(),
                limit: 8
            }
        );
    }

    #[test]
    fn a_sub_micron_pixel_exhausts_the_default_budget() {
        let origin = utm(99.0, 6.5);
        let schema = pixel_schema([origin[0], 1e-9, 0.0, origin[1], 0.0, -1e-9]);
        let ring = ZonePolygon {
            exterior: vec![
                [99.0, 6.0],
                [100.0, 6.0],
                [101.0, 6.0],
                [102.0, 6.0],
                [102.0, 6.5],
                [101.0, 6.5],
                [100.0, 6.5],
                [99.0, 6.5],
                [99.0, 6.0],
            ],
            holes: Vec::new(),
        };
        let err = build_zone_frame(&schema, Srid::WGS84, &[ring], &ctx()).expect_err("over budget");
        assert!(
            matches!(err, ZoneFrameError::VertexBudget { .. }),
            "{err:?}"
        );
    }

    /// Deterministic pseudo-random numbers in `[0, 1)`.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    #[test]
    fn every_inside_cell_lies_in_the_dim_bbox_for_random_zones() {
        let (schema, cols, rows) = grid_at_six_north();
        let gt = schema.georef.as_ref().unwrap().transform.unwrap();
        let to_lon_lat = transformer(utm47n(), Srid::WGS84);
        let mut rng = Lcg(0x5eed);
        for case in 0..24 {
            // A random quadrilateral whose corners fall inside the grid.
            let corner = |rng: &mut Lcg| {
                let (x, y) = gt.pixel_to_world(rng.next() * cols as f64, rng.next() * rows as f64);
                apply2(&to_lon_lat, [x, y])
            };
            let a = corner(&mut rng);
            let b = corner(&mut rng);
            let c = corner(&mut rng);
            let d = corner(&mut rng);
            let polygons = vec![ZonePolygon {
                exterior: vec![a, b, c, d, a],
                holes: Vec::new(),
            }];
            let frame = build_zone_frame(&schema, Srid::WGS84, &polygons, &ctx()).unwrap();
            let [x0, y0, x1, y1] = frame.dim_bbox;
            let zone = PolygonZone {
                x_dim: 1,
                y_dim: 0,
                polygons,
                frame: Some(frame),
            };
            let prepared = zone.prepare();
            for row in (0..rows).step_by(3) {
                for col in (0..cols).step_by(7) {
                    if prepared.contains(&[CoordValue::Int64(row), CoordValue::Int64(col)]) {
                        let (c, r) = (col as f64, row as f64);
                        assert!(
                            x0 <= c && c <= x1 && y0 <= r && r <= y1,
                            "case {case}: cell ({row}, {col}) outside [{x0}, {y0}, {x1}, {y1}]"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn a_frame_round_trips_through_msgpack() {
        let ctx = ctx();
        let frame = ZoneFrame {
            transform: Some(GeoTransform([1.0, 2.0, 0.0, 3.0, 0.0, -2.0])),
            array_crs: (*ctx.resolve(utm47n()).unwrap()).clone(),
            zone_crs: (*ctx.resolve(Srid::WGS84).unwrap()).clone(),
            dim_bbox: [0.0, 1.0, 2.0, 3.0],
        };
        let bytes = zerompk::to_msgpack_vec(&frame).unwrap();
        let back: ZoneFrame = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, frame);
    }

    #[test]
    fn an_undefined_zone_srid_is_refused() {
        let (schema, _, _) = grid_at_six_north();
        let err = build_zone_frame(
            &schema,
            Srid::new(999).unwrap(),
            &[rect(99.0, 6.0, 100.0, 6.5)],
            &ctx(),
        )
        .expect_err("refused");
        assert_eq!(
            err,
            ZoneFrameError::Crs {
                array: "scene".into(),
                source: UnknownSrid::Undefined { code: 999 }
            }
        );
    }
}
