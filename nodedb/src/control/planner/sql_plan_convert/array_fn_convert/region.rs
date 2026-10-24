// SPDX-License-Identifier: BUSL-1.1

//! The region an `ARRAY_AGG` or `ARRAY_SLICE` reads, lowered to the engine
//! [`ArrayRegion`] and [`PolygonZone`].
//!
//! The box comes from the slice argument. A zone also narrows the box on its
//! two dims, so tile pruning and the cluster fan-out see the zone's extent:
//!
//! - A zone in dim coordinates narrows to the bounding box of its polygons.
//! - A zone with an SRID narrows to its frame's dim box.
//!
//! An INT64 dim takes the ceiling of the low edge and the floor of the high
//! edge. A narrowed range can come out empty. The read then returns no cell.

use nodedb_array::query::region::{ArrayRegion, PolygonZone, ZonePolygon};
use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::query::zone_frame::{ZoneFrameError, build_zone_frame};
use nodedb_array::schema::{ArraySchema, DimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::types_array::{ArraySliceAst, ArrayZoneAst};
use nodedb_types::NodeDbError;

use super::helpers::resolve_slice;

/// The region of an `ARRAY_AGG` over array `name`, or `None` when it reads
/// the whole array.
pub(super) fn resolve_region(
    name: &str,
    slice: &ArraySliceAst,
    zone: Option<&ArrayZoneAst>,
    schema: &ArraySchema,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Option<ArrayRegion>> {
    if slice.dim_ranges.is_empty() && zone.is_none() {
        return Ok(None);
    }
    let mut engine_slice = resolve_slice("ARRAY_AGG", name, slice, schema)?;
    let zone = zone
        .map(|z| lower_zone("ARRAY_AGG", name, z, schema, &mut engine_slice, crs))
        .transpose()?;
    Ok(Some(ArrayRegion::new(engine_slice, zone)))
}

/// Lower `zone` of `func` over array `name` and narrow `slice` to its extent.
pub(super) fn lower_zone(
    func: &str,
    name: &str,
    zone: &ArrayZoneAst,
    schema: &ArraySchema,
    slice: &mut Slice,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<PolygonZone> {
    let x = zone_axis(func, name, &zone.x_dim, schema)?;
    let y = zone_axis(func, name, &zone.y_dim, schema)?;
    let polygons = lower_polygons(func, zone)?;
    let frame = match zone.srid {
        None => None,
        Some(code) => {
            let zone_srid = crs
                .resolve_u32(code)
                .map_err(|e| crate::Error::DataException {
                    detail: format!("{func}: zone SRID: {e}"),
                })?;
            check_georef_axes(func, name, schema, x.dim, y.dim)?;
            Some(build_zone_frame(schema, zone_srid, &polygons, crs).map_err(frame_error)?)
        }
    };
    let extent = match &frame {
        Some(frame) => Some(frame.dim_bbox),
        None => zone_bbox(&polygons),
    };
    if let Some([x0, y0, x1, y1]) = extent {
        for (axis, lo, hi) in [(x, x0, x1), (y, y0, y1)] {
            let (lo, hi) = match axis.kind {
                AxisKind::Int64 => (
                    DomainBound::Int64(int_bound(func, lo.ceil())?),
                    DomainBound::Int64(int_bound(func, hi.floor())?),
                ),
                AxisKind::Float64 => (DomainBound::Float64(lo), DomainBound::Float64(hi)),
            };
            narrow(&mut slice.dim_ranges[axis.dim as usize], lo, hi);
        }
    }
    Ok(PolygonZone {
        x_dim: x.dim,
        y_dim: y.dim,
        polygons,
        frame,
    })
}

/// The numeric type of a zone dim.
#[derive(Clone, Copy)]
enum AxisKind {
    Int64,
    Float64,
}

/// One zone dim: its schema position and its numeric type.
#[derive(Clone, Copy)]
struct ZoneAxis {
    dim: u32,
    kind: AxisKind,
}

fn zone_axis(func: &str, name: &str, dim: &str, schema: &ArraySchema) -> crate::Result<ZoneAxis> {
    let idx = schema
        .dims
        .iter()
        .position(|d| d.name == dim)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("{func}: array '{name}' has no dim '{dim}'"),
        })?;
    let kind = match schema.dims[idx].dtype {
        DimType::Int64 => AxisKind::Int64,
        DimType::Float64 => AxisKind::Float64,
        other @ (DimType::TimestampMs | DimType::String) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "{func}: zone dim '{dim}' is {other:?}; a zone needs an INT64 or FLOAT64 dim"
                ),
            });
        }
    };
    let dim = u32::try_from(idx).map_err(|_| crate::Error::PlanError {
        detail: format!("{func}: zone dim '{dim}' is past the dim index range"),
    })?;
    Ok(ZoneAxis { dim, kind })
}

/// A zone with an SRID is drawn over the GEOREF axes, x then y.
fn check_georef_axes(
    func: &str,
    name: &str,
    schema: &ArraySchema,
    x_dim: u32,
    y_dim: u32,
) -> crate::Result<()> {
    let prerequisite = |detail: String| crate::Error::ObjectNotInPrerequisiteState {
        object: format!("array '{name}'"),
        detail,
    };
    let georef = schema.georef.as_ref().ok_or_else(|| {
        prerequisite(format!(
            "{func}: array '{name}' has no GEOREF; a zone with an SRID prefix needs one"
        ))
    })?;
    if (georef.x_dim, georef.y_dim) != (x_dim, y_dim) {
        return Err(prerequisite(format!(
            "{func}: a zone with an SRID prefix is drawn over the GEOREF axes of array \
             '{name}', x then y"
        )));
    }
    Ok(())
}

fn lower_polygons(func: &str, zone: &ArrayZoneAst) -> crate::Result<Vec<ZonePolygon>> {
    zone.polygons
        .iter()
        .map(|rings| {
            let (exterior, holes) = rings.split_first().ok_or_else(|| crate::Error::PlanError {
                detail: format!("{func}: a zone polygon has no exterior ring"),
            })?;
            Ok(ZonePolygon {
                exterior: exterior.clone(),
                holes: holes.to_vec(),
            })
        })
        .collect()
}

/// The SQLSTATE each frame refusal reports.
fn frame_error(e: ZoneFrameError) -> crate::Error {
    match e {
        ZoneFrameError::NoGeoref { ref array } => crate::Error::ObjectNotInPrerequisiteState {
            object: format!("array '{array}'"),
            detail: e.to_string(),
        },
        ZoneFrameError::VertexBudget { .. } => {
            crate::Error::from(NodeDbError::program_limit_exceeded(e.to_string()))
        }
        ZoneFrameError::OutOfDomain { .. }
        | ZoneFrameError::EmptyZone { .. }
        | ZoneFrameError::Crs { .. } => crate::Error::DataException {
            detail: e.to_string(),
        },
    }
}

/// A rounded zone edge as an INT64 bound. A bound past `i64` clamps to it.
fn int_bound(func: &str, v: f64) -> crate::Result<i64> {
    if !v.is_finite() {
        return Err(crate::Error::DataException {
            detail: format!("{func}: zone extent {v} is not finite"),
        });
    }
    // `as` saturates a finite value at the `i64` limits.
    Ok(v as i64)
}

/// `[min_x, min_y, max_x, max_y]` over every exterior ring, or `None` when
/// no ring holds a position.
fn zone_bbox(polygons: &[ZonePolygon]) -> Option<[f64; 4]> {
    let mut points = polygons.iter().flat_map(|p| p.exterior.iter());
    let first = points.next()?;
    let mut b = [first[0], first[1], first[0], first[1]];
    for p in points {
        b[0] = b[0].min(p[0]);
        b[1] = b[1].min(p[1]);
        b[2] = b[2].max(p[0]);
        b[3] = b[3].max(p[1]);
    }
    Some(b)
}

/// Intersect `range` with `[lo, hi]`. The bounds share the dim's type.
fn narrow(range: &mut Option<DimRange>, lo: DomainBound, hi: DomainBound) {
    *range = Some(match range.take() {
        None => DimRange::new(lo, hi),
        Some(r) => DimRange::new(
            if lo > r.lo { lo } else { r.lo },
            if hi < r.hi { hi } else { r.hi },
        ),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{
        ArrayGeoref, ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, GeoTransform,
    };
    use nodedb_array::types::domain::Domain;
    use nodedb_sql::types_array::{ArrayCoordLiteral, NamedDimRange};
    use nodedb_types::geometry::Srid;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "lon".to_string(),
                DimType::Float64,
                Domain::new(DomainBound::Float64(0.0), DomainBound::Float64(100.0)),
            ))
            .dim(DimSpec::new(
                "row".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .dim(DimSpec::new(
                "ts".to_string(),
                DimType::TimestampMs,
                Domain::new(DomainBound::TimestampMs(0), DomainBound::TimestampMs(1_000)),
            ))
            .attr(AttrSpec::new("v".to_string(), AttrType::Float64, true))
            .tile_extents(vec![10, 10, 100])
            .build()
            .expect("schema build")
    }

    /// A 16x16 pixel raster of 1-degree WGS 84 pixels from (100°E, 8°N).
    fn pixel_schema() -> ArraySchema {
        let dim = |name: &str| {
            DimSpec::new(
                name.to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            )
        };
        ArraySchemaBuilder::new("img")
            .dim(dim("row"))
            .dim(dim("col"))
            .attr(AttrSpec::new("b1".to_string(), AttrType::Float64, true))
            .tile_extents(vec![4, 4])
            .build()
            .expect("schema build")
            .with_georef(Some(ArrayGeoref {
                srid: Srid::WGS84,
                x_dim: 1,
                y_dim: 0,
                transform: Some(GeoTransform([100.0, 1.0, 0.0, 8.0, 0.0, -1.0])),
            }))
            .expect("georef")
    }

    fn zone(x: &str, y: &str) -> ArrayZoneAst {
        ArrayZoneAst {
            polygons: vec![vec![vec![
                [1.5, 2.5],
                [7.25, 2.5],
                [7.25, 6.5],
                [1.5, 6.5],
                [1.5, 2.5],
            ]]],
            x_dim: x.into(),
            y_dim: y.into(),
            srid: None,
        }
    }

    fn lon_lat_zone(x0: f64, y0: f64, x1: f64, y1: f64) -> ArrayZoneAst {
        ArrayZoneAst {
            polygons: vec![vec![vec![[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]],
            x_dim: "col".into(),
            y_dim: "row".into(),
            srid: Some(4326),
        }
    }

    fn range(r: &ArrayRegion, dim: usize) -> Option<(DomainBound, DomainBound)> {
        r.slice.dim_ranges[dim]
            .as_ref()
            .map(|r| (r.lo.clone(), r.hi.clone()))
    }

    #[test]
    fn the_whole_array_has_no_region() {
        let region = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            None,
            &schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert!(region.is_none());
    }

    #[test]
    fn a_zone_narrows_the_box_to_its_bounding_box() {
        let region = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "row")),
            &schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap()
        .expect("region");
        assert_eq!(
            range(&region, 0),
            Some((DomainBound::Float64(1.5), DomainBound::Float64(7.25)))
        );
        assert_eq!(
            range(&region, 1),
            Some((DomainBound::Int64(3), DomainBound::Int64(6))),
            "an INT64 dim takes the ceiling of the low edge and the floor of the high edge"
        );
        assert!(range(&region, 2).is_none());
        let zone = region.zone.expect("zone");
        assert_eq!((zone.x_dim, zone.y_dim), (0, 1));
        assert_eq!(zone.polygons.len(), 1);
        assert!(zone.frame.is_none());
    }

    #[test]
    fn the_zone_box_intersects_the_slice_box() {
        let slice = ArraySliceAst {
            dim_ranges: vec![NamedDimRange {
                dim: "lon".into(),
                lo: ArrayCoordLiteral::Int64(5),
                hi: ArrayCoordLiteral::Int64(50),
            }],
        };
        let region = resolve_region(
            "grid",
            &slice,
            Some(&zone("lon", "row")),
            &schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap()
        .expect("region");
        assert_eq!(
            range(&region, 0),
            Some((DomainBound::Float64(5.0), DomainBound::Float64(7.25)))
        );
    }

    #[test]
    fn a_zone_on_a_timestamp_dim_is_refused() {
        let err = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "ts")),
            &schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("zone dim 'ts'")),
            "{err:?}"
        );
    }

    #[test]
    fn a_zone_on_an_unknown_dim_is_refused() {
        let err = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "lat")),
            &schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("no dim 'lat'")),
            "{err:?}"
        );
    }

    #[test]
    fn an_srid_zone_narrows_the_box_to_its_frame() {
        let region = resolve_region(
            "img",
            &ArraySliceAst::default(),
            Some(&lon_lat_zone(101.0, 5.0, 103.0, 7.0)),
            &pixel_schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap()
        .expect("region");
        let zone = region.zone.as_ref().expect("zone");
        let frame = zone.frame.as_ref().expect("frame");
        assert_eq!(
            (frame.array_crs.srid, frame.zone_srid()),
            (Srid::WGS84, Srid::WGS84)
        );
        // Cells 1..=2 on both axes hold the zone. The box pads by half a
        // pixel and rounds outward: 0..=3.
        assert_eq!(
            range(&region, 1),
            Some((DomainBound::Int64(0), DomainBound::Int64(3)))
        );
        assert_eq!(
            range(&region, 0),
            Some((DomainBound::Int64(0), DomainBound::Int64(3)))
        );
    }

    #[test]
    fn an_srid_zone_beside_the_raster_narrows_to_an_empty_range() {
        let mut slice = Slice::new(vec![None, None]);
        slice.dim_ranges[1] = Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(15)));
        let mut engine = slice.clone();
        lower_zone(
            "ARRAY_SLICE",
            "img",
            &lon_lat_zone(130.0, 5.0, 131.0, 6.0),
            &pixel_schema(),
            &mut engine,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("lowered");
        let col = engine.dim_ranges[1].as_ref().expect("col range");
        assert!(col.lo > col.hi, "an empty range, not an error: {col:?}");
    }

    #[test]
    fn an_srid_zone_on_an_array_without_georef_is_object_not_in_prerequisite_state() {
        let mut no_georef = pixel_schema();
        no_georef.georef = None;
        let err = resolve_region(
            "img",
            &ArraySliceAst::default(),
            Some(&lon_lat_zone(101.0, 5.0, 103.0, 7.0)),
            &no_georef,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("refused");
        assert!(
            matches!(err, crate::Error::ObjectNotInPrerequisiteState { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn an_srid_zone_over_other_dims_is_object_not_in_prerequisite_state() {
        let mut swapped = lon_lat_zone(101.0, 5.0, 103.0, 7.0);
        std::mem::swap(&mut swapped.x_dim, &mut swapped.y_dim);
        let err = resolve_region(
            "img",
            &ArraySliceAst::default(),
            Some(&swapped),
            &pixel_schema(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect_err("refused");
        assert!(
            matches!(err, crate::Error::ObjectNotInPrerequisiteState { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn frame_refusals_carry_their_sqlstate_class() {
        use nodedb_types::error::sqlstate;
        let budget = frame_error(ZoneFrameError::VertexBudget {
            array: "img".into(),
            limit: 8,
        });
        let (_, code, _) =
            crate::control::server::pgwire::types::error_map::error_to_sqlstate(&budget);
        assert_eq!(code, sqlstate::PROGRAM_LIMIT_EXCEEDED);
        let domain = frame_error(ZoneFrameError::OutOfDomain {
            array: "img".into(),
            x: 150.0,
            y: 6.0,
            zone_srid: Srid::WGS84,
            detail: "too far from the central meridian".into(),
        });
        assert!(
            matches!(domain, crate::Error::DataException { .. }),
            "{domain:?}"
        );
    }
}
