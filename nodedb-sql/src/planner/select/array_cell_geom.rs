// SPDX-License-Identifier: Apache-2.0

//! `ARRAY_CELL_POINT()` and `ARRAY_CELL_FOOTPRINT()` in a SELECT over
//! `ARRAY_SLICE` / `ARRAY_PROJECT` of a georeferenced array.
//!
//! The planner rewrites each call into spatial functions over the cell's
//! GEOREF dim columns, with the GEOREF's SRID and coefficients as literals:
//!
//! - World mode, point: `ST_SetSRID(ST_Point(x, y), srid)`.
//! - Pixel mode, point: the affine of the pixel centre `(x + 0.5, y + 0.5)`.
//! - Pixel mode, footprint: `ST_SetSRID(ST_MakePolygon(..), srid)` over the
//!   affine of the four pixel corners.
//!
//! Refusals: a footprint in world mode is `0A000`. Either call on an array
//! without GEOREF is `55000`.

use std::ops::ControlFlow;

use sqlparser::ast::{self, Select};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::ArrayCatalogView;
use crate::error::{Result, SqlError};
use crate::planner::array_fn::{ArrayTableFn, try_plan_array_table_fn};
use crate::temporal::TemporalScope;
use crate::types::SqlCatalog;

/// The SELECT and ORDER BY of a cell read with every cell geometry call
/// rewritten.
pub(super) struct CellGeomRewrite {
    pub select: Select,
    pub order_by: Option<ast::OrderBy>,
}

/// Rewrite the cell geometry calls of a query whose FROM is one
/// `ARRAY_SLICE` / `ARRAY_PROJECT` call.
///
/// The query's ORDER BY and SELECT list are read again after the SELECT body
/// plans, so the rewrite runs on the query before any of it plans. Returns
/// `None` when the query holds no call or reads no cells.
pub(in crate::planner::select) fn rewrite_cell_read_query(
    select: &Select,
    order_by: Option<&ast::OrderBy>,
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<Option<CellGeomRewrite>> {
    if !mentions_cell_geometry(select, order_by) {
        return Ok(None);
    }
    let slice = match try_plan_array_table_fn(&select.from, catalog, temporal)? {
        Some(ArrayTableFn::Slice(slice) | ArrayTableFn::Project(slice)) => slice,
        Some(ArrayTableFn::Agg(_) | ArrayTableFn::Elementwise(_)) | None => return Ok(None),
    };
    let view = catalog
        .lookup_array(&slice.name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: slice.name.clone(),
        })?;
    let qualifier = match &select.from[0].relation {
        ast::TableFactor::Table {
            alias: Some(alias), ..
        } => alias.name.clone(),
        _ => ast::Ident::with_quote('"', view.name.as_str()),
    };
    rewrite_cell_geometry(select, order_by, &view, &qualifier)
}

/// Whether `select` or `order_by` holds a cell geometry call.
fn mentions_cell_geometry(select: &Select, order_by: Option<&ast::OrderBy>) -> bool {
    let has_call = |e: &ast::Expr| match cell_geom_kind(e) {
        Some(_) => ControlFlow::Break(()),
        None => ControlFlow::Continue(()),
    };
    ast::visit_expressions(select, has_call).is_break()
        || order_by.is_some_and(|o| ast::visit_expressions(o, has_call).is_break())
}

/// Rewrite the cell geometry calls in `select` and `order_by`.
///
/// `qualifier` names the cell relation in the SELECT. Returns `None` when
/// neither clause holds a call.
pub(super) fn rewrite_cell_geometry(
    select: &Select,
    order_by: Option<&ast::OrderBy>,
    view: &ArrayCatalogView,
    qualifier: &ast::Ident,
) -> Result<Option<CellGeomRewrite>> {
    if !mentions_cell_geometry(select, order_by) {
        return Ok(None);
    }
    let mut rewrite = |expr: &mut ast::Expr| -> ControlFlow<SqlError> {
        let Some(kind) = cell_geom_kind(expr) else {
            return ControlFlow::Continue(());
        };
        match check_call(expr, kind).and_then(|()| replacement(kind, view, qualifier)) {
            Ok(new) => {
                *expr = new;
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    };
    let mut select = select.clone();
    if let ControlFlow::Break(e) = ast::visit_expressions_mut(&mut select, &mut rewrite) {
        return Err(e);
    }
    let mut order_by = order_by.cloned();
    if let Some(order) = order_by.as_mut()
        && let ControlFlow::Break(e) = ast::visit_expressions_mut(order, &mut rewrite)
    {
        return Err(e);
    }
    Ok(Some(CellGeomRewrite { select, order_by }))
}

/// Which cell geometry a call asks for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CellGeom {
    Point,
    Footprint,
}

impl CellGeom {
    fn sql_name(self) -> &'static str {
        match self {
            CellGeom::Point => "ARRAY_CELL_POINT",
            CellGeom::Footprint => "ARRAY_CELL_FOOTPRINT",
        }
    }
}

fn cell_geom_kind(expr: &ast::Expr) -> Option<CellGeom> {
    let ast::Expr::Function(f) = expr else {
        return None;
    };
    let [ast::ObjectNamePart::Identifier(ident)] = f.name.0.as_slice() else {
        return None;
    };
    if ident.value.eq_ignore_ascii_case("array_cell_point") {
        Some(CellGeom::Point)
    } else if ident.value.eq_ignore_ascii_case("array_cell_footprint") {
        Some(CellGeom::Footprint)
    } else {
        None
    }
}

/// The call is a plain `NAME()`: no argument, no window, no filter.
fn check_call(expr: &ast::Expr, kind: CellGeom) -> Result<()> {
    let ast::Expr::Function(f) = expr else {
        return Ok(());
    };
    let no_args = match &f.args {
        ast::FunctionArguments::List(list) => list.args.is_empty() && list.clauses.is_empty(),
        ast::FunctionArguments::None | ast::FunctionArguments::Subquery(_) => false,
    };
    if !no_args || f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
        return Err(SqlError::Unsupported {
            detail: format!(
                "{}() takes no argument, window, or filter; it reads the cell of its row",
                kind.sql_name()
            ),
        });
    }
    Ok(())
}

/// The spatial expression a call rewrites to.
fn replacement(
    kind: CellGeom,
    view: &ArrayCatalogView,
    qualifier: &ast::Ident,
) -> Result<ast::Expr> {
    let georef = view
        .georef
        .as_ref()
        .ok_or_else(|| SqlError::ObjectNotInPrerequisiteState {
            object: format!("array '{}'", view.name),
            detail: format!(
                "{}(): array '{}' has no GEOREF; add one with ALTER ARRAY .. SET GEOREF",
                kind.sql_name(),
                view.name
            ),
        })?;
    let x = column(qualifier, &georef.x_dim);
    let y = column(qualifier, &georef.y_dim);
    let srid = georef.srid;
    let text = match (kind, georef.transform) {
        (CellGeom::Point, None) => format!("ST_SetSRID(ST_Point({x}, {y}), {srid})"),
        (CellGeom::Point, Some(gt)) => {
            let [wx, wy] = affine(&gt, &format!("({x} + 5e-1)"), &format!("({y} + 5e-1)"));
            format!("ST_SetSRID(ST_Point({wx}, {wy}), {srid})")
        }
        (CellGeom::Footprint, None) => {
            return Err(SqlError::FeatureNotSupported {
                detail: format!(
                    "ARRAY_CELL_FOOTPRINT() needs a GEOREF with TRANSFORM; array '{}' holds \
                     world coordinates with no pixel size. Use ARRAY_CELL_POINT()",
                    view.name
                ),
            });
        }
        (CellGeom::Footprint, Some(gt)) => {
            let (u0, u1) = (x.clone(), format!("({x} + 1)"));
            let (v0, v1) = (y.clone(), format!("({y} + 1)"));
            let ring: Vec<String> = [(&u0, &v0), (&u1, &v0), (&u1, &v1), (&u0, &v1), (&u0, &v0)]
                .iter()
                .map(|(u, v)| {
                    let [wx, wy] = affine(&gt, u, v);
                    format!("ARRAY[{wx}, {wy}]")
                })
                .collect();
            format!(
                "ST_SetSRID(ST_MakePolygon(ARRAY[{}]), {srid})",
                ring.join(", ")
            )
        }
    };
    let dialect = PostgreSqlDialect {};
    Ok(Parser::new(&dialect).try_with_sql(&text)?.parse_expr()?)
}

/// `qualifier."dim"`, with any `"` in the dim name doubled.
fn column(qualifier: &ast::Ident, dim: &str) -> String {
    format!("{qualifier}.\"{}\"", dim.replace('"', "\"\""))
}

/// `[X, Y]` of the GDAL affine at the pixel position `(u, v)`, as SQL text.
/// The evaluation order matches `GeoTransform::pixel_to_world`.
fn affine(gt: &[f64; 6], u: &str, v: &str) -> [String; 2] {
    let lit = |c: f64| format!("({c:e})");
    [
        format!(
            "({} + {u} * {} + {v} * {})",
            lit(gt[0]),
            lit(gt[1]),
            lit(gt[2])
        ),
        format!(
            "({} + {u} * {} + {v} * {})",
            lit(gt[3]),
            lit(gt[4]),
            lit(gt[5])
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::error::{Result, SqlError};
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::*;
    use crate::types_array::{
        ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst,
    };

    /// `img` is pixel mode, `world` is world mode, `plain` has no GEOREF.
    struct GeoCatalog;

    impl SqlCatalog for GeoCatalog {
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
                hi: ArrayDomainBound::Int64(15),
            };
            let georef = match name {
                "img" => Some(ArrayGeorefAst {
                    srid: 32647,
                    x_dim: "col".into(),
                    y_dim: "row".into(),
                    transform: Some([600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0]),
                }),
                "world" => Some(ArrayGeorefAst {
                    srid: 32647,
                    x_dim: "col".into(),
                    y_dim: "row".into(),
                    transform: None,
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
                tile_extents: vec![4, 4],
                georef,
            })
        }
    }

    fn plan(sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql)?;
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query: {sql}");
        };
        crate::planner::select::plan_query(
            q,
            &GeoCatalog,
            &FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )
    }

    fn rewritten_projection(sql: &str, array: &str) -> String {
        let stmts = parse_sql(sql).unwrap();
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query");
        };
        let ast::SetExpr::Select(select) = q.body.as_ref() else {
            panic!("not a select");
        };
        let view = GeoCatalog.lookup_array(array).unwrap();
        let out = rewrite_cell_geometry(select, None, &view, &ast::Ident::new("t"))
            .unwrap()
            .expect("rewritten");
        out.select.projection[0].to_string()
    }

    #[test]
    fn a_world_point_reads_the_dims() {
        let text = rewritten_projection(
            "SELECT ARRAY_CELL_POINT() FROM ARRAY_SLICE('world', '{row: [0, 3]}') AS t",
            "world",
        );
        assert_eq!(text, "ST_SetSRID(ST_Point(t.\"col\", t.\"row\"), 32647)");
    }

    #[test]
    fn a_pixel_point_is_the_affine_of_the_centre() {
        let text = rewritten_projection(
            "SELECT array_cell_point() FROM ARRAY_SLICE('img', '{row: [0, 3]}') AS t",
            "img",
        );
        assert!(text.contains("(t.\"col\" + 5e-1) * (3e1)"), "{text}");
        assert!(text.contains("(t.\"row\" + 5e-1) * (-3e1)"), "{text}");
        assert!(text.contains("(6e5)") && text.contains("(7e5)"), "{text}");
    }

    #[test]
    fn a_pixel_footprint_has_five_corners() {
        let text = rewritten_projection(
            "SELECT ARRAY_CELL_FOOTPRINT() FROM ARRAY_SLICE('img', '{row: [0, 3]}') AS t",
            "img",
        );
        assert!(
            text.starts_with("ST_SetSRID(ST_MakePolygon(ARRAY["),
            "{text}"
        );
        assert_eq!(text.matches("ARRAY[(").count(), 5, "{text}");
    }

    #[test]
    fn cell_geometry_plans_over_a_slice_and_a_project() {
        for sql in [
            "SELECT row, col, ST_AsText(ST_Transform(ARRAY_CELL_POINT(), 4326)) AS p \
             FROM ARRAY_SLICE('img', '{row: [0, 3]}')",
            "SELECT ARRAY_CELL_FOOTPRINT() AS f FROM ARRAY_PROJECT('img', ['b1'])",
            "SELECT ARRAY_CELL_POINT() AS p FROM ARRAY_SLICE('world', '{row: [0, 3]}') \
             WHERE ST_X(ARRAY_CELL_POINT()) > 0",
        ] {
            plan(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn order_by_calls_are_rewritten_too() {
        let stmts = parse_sql(
            "SELECT b1 FROM ARRAY_SLICE('img', '{row: [0, 3]}') AS t \
             ORDER BY ST_X(ARRAY_CELL_POINT())",
        )
        .unwrap();
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query");
        };
        let ast::SetExpr::Select(select) = q.body.as_ref() else {
            panic!("not a select");
        };
        let view = GeoCatalog.lookup_array("img").unwrap();
        let out = rewrite_cell_geometry(select, q.order_by.as_ref(), &view, &ast::Ident::new("t"))
            .unwrap()
            .expect("rewritten");
        let order = out.order_by.expect("order by").to_string();
        assert!(order.contains("ST_SetSRID(ST_Point("), "{order}");
        assert!(
            !order.to_ascii_lowercase().contains("array_cell_point"),
            "{order}"
        );
    }

    #[test]
    fn a_world_footprint_is_feature_not_supported() {
        let err = plan("SELECT ARRAY_CELL_FOOTPRINT() FROM ARRAY_SLICE('world', '{row: [0, 3]}')")
            .expect_err("refused");
        assert!(
            matches!(err, SqlError::FeatureNotSupported { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn cell_geometry_without_georef_is_object_not_in_prerequisite_state() {
        for call in ["ARRAY_CELL_POINT()", "ARRAY_CELL_FOOTPRINT()"] {
            let err = plan(&format!(
                "SELECT {call} FROM ARRAY_SLICE('plain', '{{row: [0, 3]}}')"
            ))
            .expect_err("refused");
            assert!(
                matches!(err, SqlError::ObjectNotInPrerequisiteState { .. }),
                "{call}: {err:?}"
            );
        }
    }

    #[test]
    fn an_argument_is_refused() {
        let err = plan("SELECT ARRAY_CELL_POINT(row) FROM ARRAY_SLICE('img', '{row: [0, 3]}')")
            .expect_err("refused");
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err:?}");
    }
}
