// SPDX-License-Identifier: Apache-2.0

//! The polygon zone argument pair of `ARRAY_AGG` and `ARRAY_SLICE`: an
//! (E)WKT `POLYGON` or `MULTIPOLYGON` and the two numeric dims it is drawn
//! over, x then y.
//!
//! - No `SRID=` prefix: the positions are dim coordinates, and `zone_dims`
//!   names the two dims.
//! - `SRID=<n>;` prefix: the positions are in SRID `n`. The array needs a
//!   GEOREF (`55000` otherwise). `zone_dims` NULL means the GEOREF axes. A
//!   non-NULL `zone_dims` must name exactly those axes (`55000` otherwise).

use sqlparser::ast;

use nodedb_crs::CrsContext;
use nodedb_types::geometry::Geometry;

use super::helpers::{expect_string_array, expect_string_literal, is_null_literal};
use crate::catalog::ArrayCatalogView;
use crate::error::{Result, SqlError};
use crate::types_array::{ArrayDimType, ArrayZoneAst};

/// The zone of `func` from `args[at]` (`zone_wkt`) and `args[at + 1]`
/// (`zone_dims`). Absent or NULL `zone_wkt` means no zone.
pub(super) fn parse_optional_zone(
    func: &str,
    args: &[ast::Expr],
    at: usize,
    name: &str,
    view: &ArrayCatalogView,
    crs: &CrsContext<'_>,
) -> Result<Option<ArrayZoneAst>> {
    match (args.get(at), args.get(at + 1)) {
        (Some(wkt), Some(dims)) if !is_null_literal(wkt) => {
            parse_zone_args(func, wkt, dims, name, view, crs).map(Some)
        }
        (Some(_), Some(dims)) if !is_null_literal(dims) => Err(SqlError::Unsupported {
            detail: format!("{func}: zone_dims needs a zone_wkt; pass both, or neither"),
        }),
        _ => Ok(None),
    }
}

/// Parse the `zone_wkt` and `zone_dims` arguments of `func` over array `name`.
fn parse_zone_args(
    func: &str,
    wkt_expr: &ast::Expr,
    dims_expr: &ast::Expr,
    name: &str,
    view: &ArrayCatalogView,
    crs: &CrsContext<'_>,
) -> Result<ArrayZoneAst> {
    let wkt = expect_string_literal(wkt_expr, &format!("{func} zone"))?;
    let ewkt = nodedb_spatial::geometry_from_ewkt(&wkt).map_err(|e| SqlError::InvalidFunction {
        detail: format!("{func}: zone is not valid WKT or EWKT: {e}"),
    })?;
    let ewkt_srid = ewkt.srid;
    // A zone is a 2D region: Z and M do not take part in membership.
    let (_, mut zone, _) = ewkt.geometry.into_parts();
    // Membership is an even-odd test, so ring direction never changes it.
    // Orienting first accepts a zone drawn clockwise.
    nodedb_spatial::orient_rings(&mut zone);
    let issues = nodedb_spatial::validate::validate_geometry(&zone);
    if !issues.is_empty() {
        let reasons: Vec<String> = issues.iter().map(ToString::to_string).collect();
        return Err(SqlError::InvalidFunction {
            detail: format!("{func}: invalid zone: {}", reasons.join("; ")),
        });
    }
    let polygons = match zone {
        Geometry::Polygon { coordinates } => vec![coordinates],
        Geometry::MultiPolygon { coordinates } => coordinates,
        other => {
            return Err(SqlError::InvalidFunction {
                detail: format!(
                    "{func}: zone must be a POLYGON or MULTIPOLYGON, got {}",
                    other.geometry_type()
                ),
            });
        }
    };
    let dims = if is_null_literal(dims_expr) {
        None
    } else {
        Some(expect_string_array(
            dims_expr,
            &format!("{func} zone dims"),
        )?)
    };
    let (x_dim, y_dim, srid) = match ewkt_srid {
        None => {
            let dims = dims.ok_or_else(|| SqlError::Unsupported {
                detail: format!(
                    "{func}: a zone without an SRID prefix needs zone_dims; name two dims, \
                     x then y"
                ),
            })?;
            let (x_dim, y_dim) = two_dims(func, dims)?;
            (x_dim, y_dim, None)
        }
        Some(code) => {
            let srid = crs
                .resolve_code(code)
                .map_err(|e| SqlError::DataException {
                    detail: format!("{func}: zone SRID: {e}"),
                })?;
            let (x_dim, y_dim) = georef_axes(func, name, view, dims)?;
            (x_dim, y_dim, Some(srid.code()))
        }
    };
    for dim in [&x_dim, &y_dim] {
        check_zone_dim(func, name, view, dim)?;
    }
    Ok(ArrayZoneAst {
        polygons,
        x_dim,
        y_dim,
        srid,
    })
}

/// The `[x, y]` pair `zone_dims` names.
fn two_dims(func: &str, dims: Vec<String>) -> Result<(String, String)> {
    let [x_dim, y_dim] = <[String; 2]>::try_from(dims).map_err(|dims| SqlError::Unsupported {
        detail: format!(
            "{func}: zone dims must name two dims, x then y; got {}",
            dims.len()
        ),
    })?;
    if x_dim == y_dim {
        return Err(SqlError::Unsupported {
            detail: format!("{func}: zone dims must be two different dims; got '{x_dim}' twice"),
        });
    }
    Ok((x_dim, y_dim))
}

/// The GEOREF axes of array `name`, checked against `dims` when given.
fn georef_axes(
    func: &str,
    name: &str,
    view: &ArrayCatalogView,
    dims: Option<Vec<String>>,
) -> Result<(String, String)> {
    let georef = view
        .georef
        .as_ref()
        .ok_or_else(|| SqlError::ObjectNotInPrerequisiteState {
            object: format!("array '{name}'"),
            detail: format!(
                "{func}: array '{name}' has no GEOREF; a zone with an SRID prefix needs one"
            ),
        })?;
    let axes = (georef.x_dim.clone(), georef.y_dim.clone());
    if let Some(dims) = dims
        && dims != [axes.0.as_str(), axes.1.as_str()]
    {
        return Err(SqlError::ObjectNotInPrerequisiteState {
            object: format!("array '{name}'"),
            detail: format!(
                "{func}: a zone with an SRID prefix is drawn over the GEOREF axes ['{}', '{}'] \
                 of array '{name}'; zone_dims {dims:?} names others. Pass NULL or those axes",
                axes.0, axes.1
            ),
        });
    }
    Ok(axes)
}

/// `dim` exists on array `name` and is numeric.
fn check_zone_dim(func: &str, name: &str, view: &ArrayCatalogView, dim: &str) -> Result<()> {
    let spec = view
        .dims
        .iter()
        .find(|d| d.name == dim)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("{func}: array '{name}' has no dim '{dim}'"),
        })?;
    match spec.dtype {
        ArrayDimType::Int64 | ArrayDimType::Float64 => Ok(()),
        ArrayDimType::TimestampMs | ArrayDimType::String => Err(SqlError::TypeMismatch {
            detail: format!(
                "{func}: zone dim '{dim}' is {:?}; a zone needs an INT64 or FLOAT64 dim",
                spec.dtype
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::error::{Result, SqlError};
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::*;
    use crate::types_array::{
        ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst,
        ArrayZoneAst,
    };

    /// `img` carries a pixel GEOREF over (col, row). `plain` has none.
    struct ZoneCatalog;

    impl SqlCatalog for ZoneCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            _: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(None)
        }

        fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
            let dim = |name: &str| ArrayDimAst {
                name: name.into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(255),
            };
            let georef = match name {
                "img" => Some(ArrayGeorefAst {
                    srid: 32647,
                    x_dim: "col".into(),
                    y_dim: "row".into(),
                    transform: Some([600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0]),
                }),
                "plain" => None,
                _ => return None,
            };
            Some(ArrayCatalogView {
                name: name.into(),
                dims: vec![dim("row"), dim("col")],
                attrs: vec![ArrayAttrAst {
                    name: "b1".into(),
                    dtype: ArrayAttrType::Float64,
                    nullable: true,
                    meta: Default::default(),
                }],
                tile_extents: vec![64, 64],
                georef,
            })
        }
    }

    const ZONE: &str = "'SRID=4326;POLYGON((99 6, 100 6, 100 6.5, 99 6.5, 99 6))'";

    fn plan(sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql)?;
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query: {sql}");
        };
        crate::planner::select::plan_query(
            q,
            &ZoneCatalog,
            &FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )
    }

    fn agg_zone(sql: &str) -> ArrayZoneAst {
        match plan(sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
            SqlPlan::Cte(CtePlan {
                mut definitions, ..
            }) => match definitions.remove(0).1 {
                SqlPlan::ArrayAgg(ArrayAggPlan { zone, .. }) => zone.expect("zone"),
                other => panic!("expected ArrayAgg, got {other:?}"),
            },
            other => panic!("expected Cte, got {other:?}"),
        }
    }

    fn slice_zone(sql: &str) -> Option<ArrayZoneAst> {
        match plan(sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
            SqlPlan::Cte(CtePlan {
                mut definitions, ..
            }) => match definitions.remove(0).1 {
                SqlPlan::ArraySlice(ArraySlicePlan { zone, .. }) => zone,
                other => panic!("expected ArraySlice, got {other:?}"),
            },
            other => panic!("expected Cte, got {other:?}"),
        }
    }

    #[test]
    fn a_null_zone_dims_takes_the_georef_axes() {
        let zone = agg_zone(&format!(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, {ZONE}, NULL)"
        ));
        assert_eq!((zone.x_dim.as_str(), zone.y_dim.as_str()), ("col", "row"));
        assert_eq!(zone.srid, Some(4326));
        assert_eq!(zone.polygons[0][0].len(), 5);
    }

    #[test]
    fn a_clockwise_zone_plans_with_its_rings_oriented() {
        let zone = agg_zone(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, \
             'SRID=4326;POLYGON((99 6, 99 6.5, 100 6.5, 100 6, 99 6))', NULL)",
        );
        let ccw = agg_zone(&format!(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, {ZONE}, NULL)"
        ));
        let mut planned = zone.polygons[0][0].clone();
        assert!(
            nodedb_spatial::validate::validate_geometry(
                &nodedb_types::geometry::Geometry::polygon(vec![planned.clone()])
            )
            .is_empty(),
            "{planned:?}"
        );
        // Same vertex set as the counter-clockwise spelling.
        let mut expected = ccw.polygons[0][0].clone();
        let key = |p: &[f64; 2]| (p[0].to_bits(), p[1].to_bits());
        planned.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(planned, expected);
    }

    #[test]
    fn zone_dims_equal_to_the_georef_axes_plan() {
        let zone = agg_zone(&format!(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, {ZONE}, ARRAY['col', 'row'])"
        ));
        assert_eq!(zone.srid, Some(4326));
    }

    #[test]
    fn zone_dims_other_than_the_georef_axes_are_55000() {
        let err = plan(&format!(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, {ZONE}, ARRAY['row', 'col'])"
        ))
        .expect_err("refused");
        assert!(
            matches!(err, SqlError::ObjectNotInPrerequisiteState { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn an_srid_zone_without_georef_is_55000() {
        for sql in [
            format!("SELECT * FROM ARRAY_AGG('plain', 'b1', 'sum', NULL, NULL, {ZONE}, NULL)"),
            format!("SELECT * FROM ARRAY_SLICE('plain', '{{row: [0, 9]}}', '*', 0, {ZONE}, NULL)"),
        ] {
            let err = plan(&sql).expect_err("refused");
            assert!(
                matches!(err, SqlError::ObjectNotInPrerequisiteState { .. }),
                "{sql}: {err:?}"
            );
        }
    }

    #[test]
    fn an_undefined_zone_srid_is_a_data_exception() {
        let err = plan(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, \
             'SRID=999;POLYGON((0 0, 1 0, 1 1, 0 0))', NULL)",
        )
        .expect_err("refused");
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
    }

    #[test]
    fn a_zone_without_srid_still_needs_its_dims() {
        let err = plan(
            "SELECT * FROM ARRAY_AGG('img', 'b1', 'sum', NULL, NULL, \
             'POLYGON((0 0, 1 0, 1 1, 0 0))', NULL)",
        )
        .expect_err("refused");
        assert!(format!("{err}").contains("zone_dims"), "{err}");
    }

    #[test]
    fn slice_takes_two_to_six_args_and_refuses_five() {
        assert!(slice_zone("SELECT * FROM ARRAY_SLICE('img', '{row: [0, 9]}')").is_none());
        assert!(
            slice_zone("SELECT * FROM ARRAY_SLICE('img', '{row: [0, 9]}', '*', 0, NULL, NULL)")
                .is_none()
        );
        let zone = slice_zone(&format!(
            "SELECT * FROM ARRAY_SLICE('img', '{{row: [0, 9]}}', '*', 0, {ZONE}, NULL)"
        ))
        .expect("zone");
        assert_eq!((zone.x_dim.as_str(), zone.y_dim.as_str()), ("col", "row"));
        let zone = slice_zone(
            "SELECT * FROM ARRAY_SLICE('img', '{row: [0, 9]}', NULL, NULL, \
             'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))', ARRAY['col', 'row'])",
        )
        .expect("zone");
        assert_eq!(zone.srid, None);

        let five = plan(&format!(
            "SELECT * FROM ARRAY_SLICE('img', '{{row: [0, 9]}}', '*', 0, {ZONE})"
        ))
        .expect_err("five args refused");
        assert!(format!("{five}").contains("zone_dims"), "{five}");
        let seven = plan(&format!(
            "SELECT * FROM ARRAY_SLICE('img', '{{row: [0, 9]}}', '*', 0, {ZONE}, NULL, 1)"
        ))
        .expect_err("seven args refused");
        assert!(format!("{seven}").contains("2..=6"), "{seven}");
    }

    #[test]
    fn zone_dims_without_a_zone_is_refused() {
        let err = plan(
            "SELECT * FROM ARRAY_SLICE('img', '{row: [0, 9]}', '*', 0, NULL, ARRAY['col', 'row'])",
        )
        .expect_err("refused");
        assert!(format!("{err}").contains("needs a zone_wkt"), "{err}");
    }
}
