// SPDX-License-Identifier: Apache-2.0

//! `SELECT * FROM ARRAY_*(...)` table-valued function planning:
//! slice / project / aggregate / elementwise.

use crate::types::{ArrayAggPlan, ArrayElementwisePlan, ArraySlicePlan};
use sqlparser::ast;

use super::agg_args::{AGG_NAMED_ARGS, SLICE_NAMED_ARGS, parse_agg_options, parse_physical};
use super::helpers::{
    collect_named_args, expect_string_array, expect_string_literal, expect_u32, is_null_literal,
    parse_slice_arg, require_array_name, split_args,
};
use super::zone::parse_optional_zone;
use crate::error::{Result, SqlError};
use crate::resolver::array_tvf::check_agg_reducer;
use crate::temporal::TemporalScope;
use crate::types::{SqlCatalog, SqlPlan};
use crate::types_array::{ArrayBinaryOpAst, ArraySliceAst};

/// One planned array table-valued function call.
///
/// Each call's rows form a relation a SELECT reads:
///
/// - `Slice` and `Project` read cells. Their rows carry the array's dims and
///   the requested attrs.
/// - `Agg` rows carry `result`, after `group` when it groups by a dim.
/// - `Elementwise` rows carry the `coords` and `attrs` of one cell.
///
/// `ARRAY_PROJECT` is a slice over the whole domain with a required attr
/// list, so it plans to the slice read and keeps only its own name.
#[derive(Debug, Clone)]
pub enum ArrayTableFn {
    Slice(ArraySlicePlan),
    Project(ArraySlicePlan),
    Agg(ArrayAggPlan),
    Elementwise(ArrayElementwisePlan),
}

impl ArrayTableFn {
    /// The plan that reads this call's rows.
    pub fn into_plan(self) -> SqlPlan {
        match self {
            ArrayTableFn::Slice(plan) | ArrayTableFn::Project(plan) => SqlPlan::ArraySlice(plan),
            ArrayTableFn::Agg(plan) => SqlPlan::ArrayAgg(plan),
            ArrayTableFn::Elementwise(plan) => SqlPlan::ArrayElementwise(plan),
        }
    }

    /// The SQL name of the function, as an error message names it.
    pub fn sql_name(&self) -> &'static str {
        match self {
            ArrayTableFn::Slice(_) => "ARRAY_SLICE",
            ArrayTableFn::Project(_) => "ARRAY_PROJECT",
            ArrayTableFn::Agg(_) => "ARRAY_AGG",
            ArrayTableFn::Elementwise(_) => "ARRAY_ELEMENTWISE",
        }
    }

    /// The array the call names first: the one array of a slice, project or
    /// aggregate, or the left operand of an elementwise op. The relation the
    /// call's rows form takes its name and default qualifier from it.
    pub fn array_name(&self) -> &str {
        match self {
            ArrayTableFn::Slice(plan) | ArrayTableFn::Project(plan) => &plan.name,
            ArrayTableFn::Agg(plan) => &plan.name,
            ArrayTableFn::Elementwise(plan) => &plan.left,
        }
    }
}

/// Try to intercept a `SELECT * FROM array_xxx(...)` table-valued
/// function call. Returns `Ok(Some(call))` on a match, `Ok(None)` if
/// the FROM is not an array function (caller falls through to normal
/// catalog resolution).
///
/// `temporal` carries any `AS OF SYSTEM TIME` / `AS OF VALID TIME` qualifiers
/// extracted by the pre-processor. It is propagated verbatim into the slice
/// and aggregate plans so the planner-to-physical conversion can populate
/// the corresponding `ArrayOp` fields. When neither clause was present,
/// `temporal` is `TemporalScope::default()`, which maps to the live-state
/// fast path in the Data Plane handler.
pub fn try_plan_array_table_fn(
    from: &[ast::TableWithJoins],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<Option<ArrayTableFn>> {
    if from.len() != 1 {
        return Ok(None);
    }
    let twj = &from[0];
    if !twj.joins.is_empty() {
        return Ok(None);
    }
    let (name, args) = match &twj.relation {
        ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => {
            if let Some(alias) = alias {
                crate::reserved::check_ast_identifier(&alias.name)?;
            }
            (name, args)
        }
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name_checked(name)?;
    let (sql_name, allowed): (&str, &[&str]) = match fn_name.as_str() {
        "array_slice" => ("ARRAY_SLICE", SLICE_NAMED_ARGS),
        "array_project" => ("ARRAY_PROJECT", &[]),
        "array_agg" => ("ARRAY_AGG", AGG_NAMED_ARGS),
        "array_elementwise" => ("ARRAY_ELEMENTWISE", &[]),
        _ => return Ok(None),
    };
    let (arg_exprs, named) = split_args(&args.args, sql_name)?;
    let named = collect_named_args(named, sql_name, allowed)?;
    match fn_name.as_str() {
        "array_slice" => Ok(Some(ArrayTableFn::Slice(plan_slice(
            &arg_exprs, &named, catalog, temporal,
        )?))),
        "array_project" => Ok(Some(ArrayTableFn::Project(plan_project(
            &arg_exprs, catalog, temporal,
        )?))),
        "array_agg" => Ok(Some(ArrayTableFn::Agg(plan_agg(
            &arg_exprs, &named, catalog, temporal,
        )?))),
        _ => Ok(Some(ArrayTableFn::Elementwise(plan_elementwise(
            &arg_exprs, catalog, temporal,
        )?))),
    }
}

fn plan_slice(
    args: &[ast::Expr],
    named: &[(String, ast::Expr)],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<ArraySlicePlan> {
    if !(2..=6).contains(&args.len()) {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_SLICE expects 2..=6 args (name, slice_obj, [attrs], [limit], \
                 [zone_wkt, zone_dims]); got {}",
                args.len()
            ),
        });
    }
    if args.len() == 5 {
        return Err(SqlError::Unsupported {
            detail: "ARRAY_SLICE: zone_wkt needs zone_dims; pass both, or neither".into(),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_SLICE", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_SLICE: array '{name}' not found"),
        })?;

    let slice = parse_slice_arg(&args[1], "ARRAY_SLICE", &name, &view)?;

    let attr_projection = match args.get(2) {
        None => Vec::new(),
        Some(arg) if is_null_literal(arg) => Vec::new(),
        Some(ast::Expr::Value(v)) if matches!(v.value, ast::Value::SingleQuotedString(ref s) if s == "*") => {
            Vec::new()
        }
        Some(arg) => expect_string_array(arg, "ARRAY_SLICE attr projection")?,
    };
    // Validate attr names against the catalog.
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_SLICE: array '{name}' has no attr '{attr}'"),
            });
        }
    }

    let limit = match args.get(3) {
        Some(arg) if !is_null_literal(arg) => expect_u32(arg, "ARRAY_SLICE limit")?,
        _ => 0,
    };
    let zone = parse_optional_zone("ARRAY_SLICE", args, 4, &name, &view, &catalog.crs_context())?;

    Ok(ArraySlicePlan {
        name,
        slice,
        attr_projection,
        limit,
        zone,
        temporal,
        physical: parse_physical(named, "ARRAY_SLICE")?,
    })
}

fn plan_project(
    args: &[ast::Expr],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<ArraySlicePlan> {
    if args.len() != 2 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_PROJECT expects 2 args (name, [attrs]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_PROJECT", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_PROJECT: array '{name}' not found"),
        })?;
    let attr_projection = expect_string_array(&args[1], "ARRAY_PROJECT attrs")?;
    if attr_projection.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "ARRAY_PROJECT: attr list must not be empty".into(),
        });
    }
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_PROJECT: array '{name}' has no attr '{attr}'"),
            });
        }
    }
    Ok(ArraySlicePlan {
        name,
        slice: ArraySliceAst::default(),
        attr_projection,
        limit: 0,
        zone: None,
        temporal,
        physical: false,
    })
}

fn plan_agg(
    args: &[ast::Expr],
    named: &[(String, ast::Expr)],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<ArrayAggPlan> {
    if !(3..=7).contains(&args.len()) {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_AGG expects 3..=7 args (name, attr, reducer, [group_by_dim], [slice], \
                 [zone_wkt, zone_dims]); got {}",
                args.len()
            ),
        });
    }
    if args.len() == 6 {
        return Err(SqlError::Unsupported {
            detail: "ARRAY_AGG: zone_wkt needs zone_dims; pass both, or neither".into(),
        });
    }
    let name = require_array_name(args, 0, "ARRAY_AGG", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_AGG: array '{name}' not found"),
        })?;

    let attr = expect_string_literal(&args[1], "ARRAY_AGG attr")?;
    let attr_type = view
        .attrs
        .iter()
        .find(|a| a.name == attr)
        .map(|a| a.dtype)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_AGG: array '{name}' has no attr '{attr}'"),
        })?;

    let reducer_str = expect_string_literal(&args[2], "ARRAY_AGG reducer")?;
    let options = parse_agg_options(&reducer_str, named)?;
    let reducer = options.reducer;
    check_agg_reducer(reducer, &attr, attr_type)?;

    let group_by_dim = match args.get(3) {
        Some(arg) if !is_null_literal(arg) => {
            let dim = expect_string_literal(arg, "ARRAY_AGG group_by_dim")?;
            if !view.dims.iter().any(|d| d.name == dim) {
                return Err(SqlError::Unsupported {
                    detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
                });
            }
            Some(dim)
        }
        _ => None,
    };

    let slice = match args.get(4) {
        Some(arg) if !is_null_literal(arg) => parse_slice_arg(arg, "ARRAY_AGG", &name, &view)?,
        _ => ArraySliceAst::default(),
    };

    let zone = parse_optional_zone("ARRAY_AGG", args, 5, &name, &view, &catalog.crs_context())?;

    Ok(ArrayAggPlan {
        name,
        attr,
        reducer,
        group_by_dim,
        slice,
        zone,
        temporal,
        physical: options.physical,
    })
}

fn plan_elementwise(
    args: &[ast::Expr],
    catalog: &dyn SqlCatalog,
    temporal: TemporalScope,
) -> Result<ArrayElementwisePlan> {
    if args.len() != 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_ELEMENTWISE expects 4 args (left, right, op, attr); got {}",
                args.len()
            ),
        });
    }
    let left = require_array_name(args, 0, "ARRAY_ELEMENTWISE", catalog)?;
    let right = require_array_name(args, 1, "ARRAY_ELEMENTWISE", catalog)?;
    let lview = catalog
        .lookup_array(&left)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{left}' not found"),
        })?;
    let rview = catalog
        .lookup_array(&right)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{right}' not found"),
        })?;
    if lview.dims.len() != rview.dims.len() || lview.attrs.len() != rview.attrs.len() {
        return Err(SqlError::Unsupported {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left}' and '{right}' must share schema shape"
            ),
        });
    }
    // The schema hash excludes the georef, so this is the only check that
    // the two arrays place their cells in the same world frame.
    if lview.georef != rview.georef {
        return Err(SqlError::DataException {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left}' and '{right}' must have equal GEOREF \
                 (SRID, X and Y dims, and TRANSFORM)"
            ),
        });
    }
    let op_str = expect_string_literal(&args[2], "ARRAY_ELEMENTWISE op")?;
    let op = ArrayBinaryOpAst::parse(&op_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!("ARRAY_ELEMENTWISE: unknown op '{op_str}' (want add/sub/mul/div)"),
    })?;
    let attr = expect_string_literal(&args[3], "ARRAY_ELEMENTWISE attr")?;
    if !lview.attrs.iter().any(|a| a.name == attr) {
        return Err(SqlError::Unsupported {
            detail: format!("ARRAY_ELEMENTWISE: array '{left}' has no attr '{attr}'"),
        });
    }
    Ok(ArrayElementwisePlan {
        left,
        right,
        op,
        attr,
        temporal,
    })
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::error::{Result, SqlError};
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::{ArrayAggPlan, ArrayElementwisePlan, ArraySlicePlan};
    use crate::types::{CollectionInfo, SqlCatalog, SqlPlan};
    use crate::types_array::{
        ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst,
        ArrayReducerAst,
    };

    struct StubCatalog {
        view: Option<ArrayCatalogView>,
        right_view: Option<ArrayCatalogView>,
    }
    impl SqlCatalog for StubCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            _name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(None)
        }
        fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
            if name == "g" || name == "left" {
                self.view.clone()
            } else if name == "right" {
                self.right_view.clone()
            } else {
                None
            }
        }
    }

    fn view() -> ArrayCatalogView {
        ArrayCatalogView {
            name: "g".into(),
            dims: vec![
                ArrayDimAst {
                    name: "chrom".into(),
                    dtype: ArrayDimType::Int64,
                    lo: ArrayDomainBound::Int64(1),
                    hi: ArrayDomainBound::Int64(23),
                },
                ArrayDimAst {
                    name: "pos".into(),
                    dtype: ArrayDimType::Int64,
                    lo: ArrayDomainBound::Int64(0),
                    hi: ArrayDomainBound::Int64(1_000_000),
                },
            ],
            attrs: vec![
                ArrayAttrAst {
                    name: "variant".into(),
                    dtype: ArrayAttrType::String,
                    nullable: true,
                    meta: Default::default(),
                },
                ArrayAttrAst {
                    name: "qual".into(),
                    dtype: ArrayAttrType::Float64,
                    nullable: true,
                    meta: Default::default(),
                },
            ],
            tile_extents: vec![1, 1_000_000],
            georef: None,
        }
    }

    fn cat() -> StubCatalog {
        StubCatalog {
            view: Some(view()),
            right_view: Some(view()),
        }
    }

    fn plan_one(sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql)?;
        let q = match &stmts[0] {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("not a query"),
        };
        crate::planner::select::plan_query(
            q,
            &cat(),
            &FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )
    }

    /// The body a relational array read plans to: the one definition of the
    /// CTE its outer query reads.
    fn relation_body(plan: SqlPlan) -> SqlPlan {
        match plan {
            SqlPlan::Cte(crate::types::CtePlan {
                mut definitions, ..
            }) => {
                assert_eq!(definitions.len(), 1, "one array relation definition");
                definitions.remove(0).1
            }
            other => panic!("expected Cte over the array relation, got {other:?}"),
        }
    }

    #[test]
    fn slice_happy() {
        let p = plan_one(
            "SELECT * FROM ARRAY_SLICE('g', '{chrom: [1,1], pos: [0, 100]}', ['qual'], 50)",
        )
        .unwrap();
        match relation_body(p) {
            SqlPlan::ArraySlice(ArraySlicePlan {
                name,
                slice,
                attr_projection,
                limit,
                ..
            }) => {
                assert_eq!(name, "g");
                assert_eq!(slice.dim_ranges.len(), 2);
                assert_eq!(attr_projection, vec!["qual".to_string()]);
                assert_eq!(limit, 50);
            }
            other => panic!("expected ArraySlice, got {other:?}"),
        }
    }

    #[test]
    fn slice_unknown_dim_rejected() {
        let err = plan_one("SELECT * FROM ARRAY_SLICE('g', '{nope: [1, 2]}')")
            .err()
            .unwrap();
        assert!(format!("{err}").contains("no dim"));
    }

    #[test]
    fn project_happy() {
        let p = plan_one("SELECT * FROM ARRAY_PROJECT('g', ['qual', 'variant'])").unwrap();
        match relation_body(p) {
            SqlPlan::ArraySlice(ArraySlicePlan {
                name,
                slice,
                attr_projection,
                limit,
                ..
            }) => {
                assert_eq!(name, "g");
                assert!(
                    slice.dim_ranges.is_empty(),
                    "a project reads the whole domain"
                );
                assert_eq!(attr_projection, vec!["qual".to_string(), "variant".into()]);
                assert_eq!(limit, 0);
            }
            other => panic!("expected a whole-domain ArraySlice, got {other:?}"),
        }
    }

    /// A project reads the cells visible at its `AS OF` scope, as a slice
    /// does.
    #[test]
    fn project_carries_the_temporal_scope() {
        let stmts = parse_sql("SELECT * FROM ARRAY_PROJECT('g', ['qual'])").unwrap();
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query");
        };
        let temporal = crate::TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AsOf(1_234),
            valid_time: crate::temporal::ValidTime::At(99),
        };
        let p = crate::planner::select::plan_query(q, &cat(), &FunctionRegistry::new(), temporal)
            .unwrap();
        match relation_body(p) {
            SqlPlan::ArraySlice(ArraySlicePlan { temporal: got, .. }) => {
                assert_eq!(got, temporal);
            }
            other => panic!("expected a whole-domain ArraySlice, got {other:?}"),
        }
    }

    #[test]
    fn project_empty_rejected() {
        assert!(plan_one("SELECT * FROM ARRAY_PROJECT('g', ARRAY[])").is_err());
    }

    #[test]
    fn agg_scalar() {
        let p = plan_one("SELECT * FROM ARRAY_AGG('g', 'qual', 'sum')").unwrap();
        match relation_body(p) {
            SqlPlan::ArrayAgg(ArrayAggPlan {
                name,
                attr,
                reducer,
                group_by_dim,
                ..
            }) => {
                assert_eq!(name, "g");
                assert_eq!(attr, "qual");
                assert_eq!(reducer, ArrayReducerAst::Sum);
                assert!(group_by_dim.is_none());
            }
            other => panic!("expected ArrayAgg, got {other:?}"),
        }
    }

    #[test]
    fn agg_named_args_parse_and_unknown_ones_are_refused() {
        let p = plan_one(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'percentile', 'chrom', p => 0.9, physical => true)",
        )
        .unwrap();
        match relation_body(p) {
            SqlPlan::ArrayAgg(ArrayAggPlan {
                reducer, physical, ..
            }) => {
                assert_eq!(reducer, ArrayReducerAst::Percentile(0.9));
                assert!(physical);
            }
            other => panic!("expected ArrayAgg, got {other:?}"),
        }
        for sql in [
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', colour => 1)",
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', p => 0.5)",
            "SELECT * FROM ARRAY_SLICE('g', '{chrom: [1, 1]}', p => 0.5)",
            "SELECT * FROM ARRAY_PROJECT('g', ARRAY['qual'], physical => true)",
        ] {
            assert!(plan_one(sql).is_err(), "{sql}");
        }
        let p = plan_one("SELECT * FROM ARRAY_SLICE('g', '{chrom: [1, 1]}', physical => true)")
            .unwrap();
        match relation_body(p) {
            SqlPlan::ArraySlice(ArraySlicePlan { physical, .. }) => assert!(physical),
            other => panic!("expected ArraySlice, got {other:?}"),
        }
    }

    #[test]
    fn agg_grouped() {
        let p = plan_one("SELECT * FROM ARRAY_AGG('g', 'qual', 'mean', 'chrom')").unwrap();
        match relation_body(p) {
            SqlPlan::ArrayAgg(ArrayAggPlan {
                reducer,
                group_by_dim,
                ..
            }) => {
                assert_eq!(reducer, ArrayReducerAst::Mean);
                assert_eq!(group_by_dim, Some("chrom".into()));
            }
            other => panic!("expected ArrayAgg, got {other:?}"),
        }
    }

    #[test]
    fn agg_unknown_reducer_rejected() {
        assert!(plan_one("SELECT * FROM ARRAY_AGG('g', 'qual', 'bogus')").is_err());
    }

    #[test]
    fn agg_carries_its_box_and_zone() {
        let p = plan_one(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'mean', NULL, '{chrom: [1, 2]}', \
             'POLYGON((0 1, 100 1, 100 3, 0 3, 0 1))', ARRAY['pos', 'chrom'])",
        )
        .unwrap();
        let p = relation_body(p);
        let SqlPlan::ArrayAgg(ArrayAggPlan {
            group_by_dim,
            slice,
            zone,
            ..
        }) = p
        else {
            panic!("expected ArrayAgg, got {p:?}");
        };
        assert!(group_by_dim.is_none());
        assert_eq!(slice.dim_ranges.len(), 1);
        assert_eq!(slice.dim_ranges[0].dim, "chrom");
        let zone = zone.expect("zone");
        assert_eq!(zone.x_dim, "pos");
        assert_eq!(zone.y_dim, "chrom");
        assert_eq!(zone.polygons.len(), 1);
        assert_eq!(zone.polygons[0][0].len(), 5);
    }

    #[test]
    fn agg_null_box_reads_the_whole_array() {
        let p = plan_one(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', 'chrom', NULL, \
             'MULTIPOLYGON(((0 0, 9 0, 9 9, 0 9, 0 0)), ((20 20, 29 20, 29 29, 20 20)))', \
             ARRAY['pos', 'chrom'])",
        )
        .unwrap();
        let p = relation_body(p);
        let SqlPlan::ArrayAgg(ArrayAggPlan {
            group_by_dim,
            slice,
            zone,
            ..
        }) = p
        else {
            panic!("expected ArrayAgg, got {p:?}");
        };
        assert_eq!(group_by_dim.as_deref(), Some("chrom"));
        assert!(slice.dim_ranges.is_empty());
        assert_eq!(zone.expect("zone").polygons.len(), 2);
    }

    fn agg_error(sql: &str) -> String {
        format!("{}", plan_one(sql).expect_err("refused"))
    }

    #[test]
    fn agg_numeric_reducer_over_string_attr_is_refused() {
        for reducer in ["sum", "mean", "min", "max"] {
            let sql = format!("SELECT * FROM ARRAY_AGG('g', 'variant', '{reducer}')");
            match plan_one(&sql) {
                Err(SqlError::TypeMismatch { detail }) => {
                    assert!(detail.contains("STRING attr 'variant'"), "{detail}");
                }
                other => panic!("{sql} must be refused at plan time: {other:?}"),
            }
        }
        plan_one("SELECT * FROM ARRAY_AGG('g', 'variant', 'count')")
            .expect("COUNT over a STRING attr plans");
    }

    #[test]
    fn agg_zone_needs_its_dims() {
        let err = agg_error(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, \
             'POLYGON((0 0, 1 0, 1 1, 0 0))')",
        );
        assert!(err.contains("zone_dims"), "{err}");
    }

    #[test]
    fn agg_box_on_an_unknown_dim_is_refused() {
        let err = agg_error("SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, '{nope: [1, 2]}')");
        assert!(
            err.contains("ARRAY_AGG: array 'g' has no dim 'nope'"),
            "{err}"
        );
    }

    #[test]
    fn agg_zone_must_be_a_polygon() {
        let err = agg_error(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, 'POINT(1 2)', \
             ARRAY['pos', 'chrom'])",
        );
        assert!(err.contains("POLYGON or MULTIPOLYGON"), "{err}");
        let err = agg_error(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, 'POLYGON((0 0, 1', \
             ARRAY['pos', 'chrom'])",
        );
        assert!(err.contains("not valid WKT"), "{err}");
    }

    #[test]
    fn agg_zone_dims_name_two_distinct_dims() {
        let zone = "'POLYGON((0 0, 1 0, 1 1, 0 0))'";
        let err = agg_error(&format!(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, {zone}, ARRAY['pos'])"
        ));
        assert!(err.contains("two dims"), "{err}");
        let err = agg_error(&format!(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, {zone}, ARRAY['pos', 'pos'])"
        ));
        assert!(err.contains("two different dims"), "{err}");
        let err = agg_error(&format!(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, {zone}, ARRAY['pos', 'lat'])"
        ));
        assert!(err.contains("no dim 'lat'"), "{err}");
    }

    #[test]
    fn agg_more_than_seven_args_is_refused() {
        let err = agg_error(
            "SELECT * FROM ARRAY_AGG('g', 'qual', 'sum', NULL, NULL, \
             'POLYGON((0 0, 1 0, 1 1, 0 0))', ARRAY['pos', 'chrom'], 1)",
        );
        assert!(err.contains("3..=7 args"), "{err}");
    }

    #[test]
    fn elementwise_happy() {
        let p =
            plan_one("SELECT * FROM ARRAY_ELEMENTWISE('left', 'right', 'add', 'qual')").unwrap();
        assert!(matches!(relation_body(p), SqlPlan::ArrayElementwise { .. }));
    }

    /// An elementwise op reads both operands at its `AS OF` scope.
    #[test]
    fn elementwise_carries_the_temporal_scope() {
        let stmts =
            parse_sql("SELECT * FROM ARRAY_ELEMENTWISE('left', 'right', 'add', 'qual')").unwrap();
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query");
        };
        let temporal = crate::TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AsOf(5_000),
            valid_time: crate::temporal::ValidTime::At(7),
        };
        let p = crate::planner::select::plan_query(q, &cat(), &FunctionRegistry::new(), temporal)
            .unwrap();
        match relation_body(p) {
            SqlPlan::ArrayElementwise(ArrayElementwisePlan { temporal: got, .. }) => {
                assert_eq!(got, temporal);
            }
            other => panic!("expected ArrayElementwise, got {other:?}"),
        }
    }

    #[test]
    fn elementwise_unknown_op_rejected() {
        assert!(
            plan_one("SELECT * FROM ARRAY_ELEMENTWISE('left', 'right', 'wat', 'qual')").is_err()
        );
    }

    fn georef(srid: u32, transform: Option<[f64; 6]>) -> Option<ArrayGeorefAst> {
        Some(ArrayGeorefAst {
            srid,
            x_dim: "pos".into(),
            y_dim: "chrom".into(),
            transform,
        })
    }

    fn elementwise_with(
        left: Option<ArrayGeorefAst>,
        right: Option<ArrayGeorefAst>,
    ) -> Result<SqlPlan> {
        let catalog = StubCatalog {
            view: Some(ArrayCatalogView {
                georef: left,
                ..view()
            }),
            right_view: Some(ArrayCatalogView {
                georef: right,
                ..view()
            }),
        };
        let stmts = parse_sql("SELECT * FROM ARRAY_ELEMENTWISE('left', 'right', 'add', 'qual')")?;
        let sqlparser::ast::Statement::Query(q) = &stmts[0] else {
            panic!("not a query");
        };
        crate::planner::select::plan_query(
            q,
            &catalog,
            &FunctionRegistry::new(),
            crate::TemporalScope::default(),
        )
    }

    const GT: [f64; 6] = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];

    #[test]
    fn elementwise_equal_georefs_pass() {
        assert!(elementwise_with(georef(32647, Some(GT)), georef(32647, Some(GT))).is_ok());
        assert!(elementwise_with(georef(4326, None), georef(4326, None)).is_ok());
    }

    #[test]
    fn elementwise_different_georefs_refused() {
        let mut shifted = GT;
        shifted[0] += 10.0;
        for (left, right) in [
            (georef(32647, Some(GT)), georef(32647, Some(shifted))),
            (georef(32647, Some(GT)), georef(32648, Some(GT))),
            (georef(32647, Some(GT)), None),
            (None, georef(4326, None)),
        ] {
            let err = elementwise_with(left, right).unwrap_err();
            assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        }
    }
}
