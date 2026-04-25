//! Planner for `NDARRAY_*` table-valued / scalar functions.
//!
//! The functions live in two AST shapes:
//!
//! * **Read** (`NDARRAY_SLICE`, `NDARRAY_PROJECT`, `NDARRAY_AGG`,
//!   `NDARRAY_ELEMENTWISE`) — `SELECT * FROM ndarray_xxx(...)`. Parsed
//!   by sqlparser as `TableFactor::Table { name, args: Some(_), .. }`
//!   (Postgres-style table-valued function). [`try_plan_array_table_fn`]
//!   intercepts these before catalog resolution.
//! * **Maintenance** (`NDARRAY_FLUSH`, `NDARRAY_COMPACT`) — bare
//!   `SELECT ndarray_flush(name)` with no FROM clause.
//!   [`try_plan_array_maint_fn`] intercepts these from the constant-
//!   query path.
//!
//! Function arguments are validated against the array catalog: the
//! array must exist, attribute / dim names must resolve, and reducer /
//! op strings must match the AST enums. Errors surface as
//! `SqlError::Unsupported` with an actionable detail.
//!
//! The slice-predicate object literal `{chrom: [1, 1], pos: [12000,
//! 13000]}` arrives as a quoted string from the SQL surface (the
//! Postgres dialect does not accept brace-literals as expressions);
//! [`crate::parser::object_literal::parse_object_literal`] handles the
//! decode.

use sqlparser::ast;

use nodedb_types::Value;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::parser::object_literal::parse_object_literal;
use crate::types::{SqlCatalog, SqlPlan};
use crate::types_array::{
    ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst, ArraySliceAst, NamedDimRange,
};

/// Try to intercept a `SELECT * FROM ndarray_xxx(...)` table-valued
/// function call. Returns `Ok(Some(plan))` on a match, `Ok(None)` if
/// the FROM is not an array function (caller falls through to normal
/// catalog resolution).
pub fn try_plan_array_table_fn(
    from: &[ast::TableWithJoins],
    catalog: &dyn SqlCatalog,
) -> Result<Option<SqlPlan>> {
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
            ..
        } => (name, args),
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name(name);
    let arg_exprs = collect_args(&args.args);
    match fn_name.as_str() {
        "ndarray_slice" => Ok(Some(plan_slice(&arg_exprs, catalog)?)),
        "ndarray_project" => Ok(Some(plan_project(&arg_exprs, catalog)?)),
        "ndarray_agg" => Ok(Some(plan_agg(&arg_exprs, catalog)?)),
        "ndarray_elementwise" => Ok(Some(plan_elementwise(&arg_exprs, catalog)?)),
        _ => Ok(None),
    }
}

/// Try to intercept a no-FROM `SELECT ndarray_flush(name)` /
/// `SELECT ndarray_compact(name)`. The single projection item must be
/// a bare function call carrying one string-literal argument.
pub fn try_plan_array_maint_fn(
    items: &[ast::SelectItem],
    catalog: &dyn SqlCatalog,
) -> Result<Option<SqlPlan>> {
    if items.len() != 1 {
        return Ok(None);
    }
    let func = match &items[0] {
        ast::SelectItem::UnnamedExpr(ast::Expr::Function(f))
        | ast::SelectItem::ExprWithAlias {
            expr: ast::Expr::Function(f),
            ..
        } => f,
        _ => return Ok(None),
    };
    let fn_name = crate::parser::normalize::normalize_object_name(&func.name);
    let arg_exprs = match &func.args {
        ast::FunctionArguments::List(list) => collect_args(&list.args),
        _ => Vec::new(),
    };
    match fn_name.as_str() {
        "ndarray_flush" => {
            let name = require_array_name(&arg_exprs, 0, "NDARRAY_FLUSH", catalog)?;
            Ok(Some(SqlPlan::NdArrayFlush { name }))
        }
        "ndarray_compact" => {
            let name = require_array_name(&arg_exprs, 0, "NDARRAY_COMPACT", catalog)?;
            Ok(Some(SqlPlan::NdArrayCompact { name }))
        }
        _ => Ok(None),
    }
}

// ── Per-function planners ───────────────────────────────────────────

fn plan_slice(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() < 2 || args.len() > 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "NDARRAY_SLICE expects 2..=4 args (name, slice_obj, [attrs], [limit]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "NDARRAY_SLICE", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("NDARRAY_SLICE: array '{name}' not found"),
        })?;

    // Slice-predicate literal: encoded as a quoted string carrying the
    // brace-form object literal. The PostgreSQL dialect does not accept
    // bare `{...}` in expression position, so we decode the string
    // contents here.
    let slice_str = expect_string_literal(&args[1], "NDARRAY_SLICE slice predicate")?;
    let parsed = parse_object_literal(&slice_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!("NDARRAY_SLICE: slice predicate must be an object literal: {slice_str}"),
    })?;
    let map = parsed.map_err(|detail| SqlError::Unsupported {
        detail: format!("NDARRAY_SLICE: slice parse: {detail}"),
    })?;
    let mut dim_ranges: Vec<NamedDimRange> = Vec::with_capacity(map.len());
    for (dim, val) in map {
        // Verify the dim exists on the array.
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("NDARRAY_SLICE: array '{name}' has no dim '{dim}'"),
            });
        }
        let arr = match val {
            Value::Array(a) if a.len() == 2 => a,
            _ => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "NDARRAY_SLICE: dim '{dim}' range must be a 2-element array [lo, hi]"
                    ),
                });
            }
        };
        let lo = value_to_coord_literal(&arr[0], &dim)?;
        let hi = value_to_coord_literal(&arr[1], &dim)?;
        dim_ranges.push(NamedDimRange { dim, lo, hi });
    }

    let attr_projection = if args.len() >= 3 {
        expect_string_array(&args[2], "NDARRAY_SLICE attr projection")?
    } else {
        Vec::new()
    };
    // Validate attr names against the catalog.
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("NDARRAY_SLICE: array '{name}' has no attr '{attr}'"),
            });
        }
    }

    let limit = if args.len() >= 4 {
        expect_u32(&args[3], "NDARRAY_SLICE limit")?
    } else {
        0
    };

    Ok(SqlPlan::NdArraySlice {
        name,
        slice: ArraySliceAst { dim_ranges },
        attr_projection,
        limit,
    })
}

fn plan_project(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() != 2 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "NDARRAY_PROJECT expects 2 args (name, [attrs]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "NDARRAY_PROJECT", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("NDARRAY_PROJECT: array '{name}' not found"),
        })?;
    let attr_projection = expect_string_array(&args[1], "NDARRAY_PROJECT attrs")?;
    if attr_projection.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "NDARRAY_PROJECT: attr list must not be empty".into(),
        });
    }
    for attr in &attr_projection {
        if !view.attrs.iter().any(|a| &a.name == attr) {
            return Err(SqlError::Unsupported {
                detail: format!("NDARRAY_PROJECT: array '{name}' has no attr '{attr}'"),
            });
        }
    }
    Ok(SqlPlan::NdArrayProject {
        name,
        attr_projection,
    })
}

fn plan_agg(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() < 3 || args.len() > 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "NDARRAY_AGG expects 3..=4 args (name, attr, reducer, [group_by_dim]); got {}",
                args.len()
            ),
        });
    }
    let name = require_array_name(args, 0, "NDARRAY_AGG", catalog)?;
    let view = catalog
        .lookup_array(&name)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("NDARRAY_AGG: array '{name}' not found"),
        })?;

    let attr = expect_string_literal(&args[1], "NDARRAY_AGG attr")?;
    if !view.attrs.iter().any(|a| a.name == attr) {
        return Err(SqlError::Unsupported {
            detail: format!("NDARRAY_AGG: array '{name}' has no attr '{attr}'"),
        });
    }

    let reducer_str = expect_string_literal(&args[2], "NDARRAY_AGG reducer")?;
    let reducer = ArrayReducerAst::parse(&reducer_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!(
            "NDARRAY_AGG: unknown reducer '{reducer_str}' (want sum/count/min/max/mean)"
        ),
    })?;

    let group_by_dim = if args.len() == 4 && !is_null_literal(&args[3]) {
        let dim = expect_string_literal(&args[3], "NDARRAY_AGG group_by_dim")?;
        if !view.dims.iter().any(|d| d.name == dim) {
            return Err(SqlError::Unsupported {
                detail: format!("NDARRAY_AGG: array '{name}' has no dim '{dim}'"),
            });
        }
        Some(dim)
    } else {
        None
    };

    Ok(SqlPlan::NdArrayAgg {
        name,
        attr,
        reducer,
        group_by_dim,
    })
}

fn plan_elementwise(args: &[ast::Expr], catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if args.len() != 4 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "NDARRAY_ELEMENTWISE expects 4 args (left, right, op, attr); got {}",
                args.len()
            ),
        });
    }
    let left = require_array_name(args, 0, "NDARRAY_ELEMENTWISE", catalog)?;
    let right = require_array_name(args, 1, "NDARRAY_ELEMENTWISE", catalog)?;
    let lview = catalog
        .lookup_array(&left)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("NDARRAY_ELEMENTWISE: array '{left}' not found"),
        })?;
    let rview = catalog
        .lookup_array(&right)
        .ok_or_else(|| SqlError::Unsupported {
            detail: format!("NDARRAY_ELEMENTWISE: array '{right}' not found"),
        })?;
    if lview.dims.len() != rview.dims.len() || lview.attrs.len() != rview.attrs.len() {
        return Err(SqlError::Unsupported {
            detail: format!(
                "NDARRAY_ELEMENTWISE: arrays '{left}' and '{right}' must share schema shape"
            ),
        });
    }
    let op_str = expect_string_literal(&args[2], "NDARRAY_ELEMENTWISE op")?;
    let op = ArrayBinaryOpAst::parse(&op_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!("NDARRAY_ELEMENTWISE: unknown op '{op_str}' (want add/sub/mul/div)"),
    })?;
    let attr = expect_string_literal(&args[3], "NDARRAY_ELEMENTWISE attr")?;
    if !lview.attrs.iter().any(|a| a.name == attr) {
        return Err(SqlError::Unsupported {
            detail: format!("NDARRAY_ELEMENTWISE: array '{left}' has no attr '{attr}'"),
        });
    }
    Ok(SqlPlan::NdArrayElementwise {
        left,
        right,
        op,
        attr,
    })
}

// ── Helpers ─────────────────────────────────────────────────────────

fn collect_args(args: &[ast::FunctionArg]) -> Vec<ast::Expr> {
    args.iter()
        .filter_map(|a| match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect()
}

fn require_array_name(
    args: &[ast::Expr],
    idx: usize,
    fn_name: &str,
    catalog: &dyn SqlCatalog,
) -> Result<String> {
    let expr = args.get(idx).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array name at arg {idx}"),
    })?;
    let name = match expr {
        ast::Expr::Identifier(ident) => normalize_ident(ident),
        ast::Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join("."),
        _ => expect_string_literal(expr, &format!("{fn_name} array name"))?,
    };
    if !catalog.array_exists(&name) {
        return Err(SqlError::Unsupported {
            detail: format!("{fn_name}: array '{name}' not found"),
        });
    }
    Ok(name)
}

fn expect_string_literal(expr: &ast::Expr, ctx: &str) -> Result<String> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            other => Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected string literal, got {other}"),
            }),
        },
        ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        _ => Err(SqlError::Unsupported {
            detail: format!("{ctx}: expected string literal, got {expr}"),
        }),
    }
}

fn expect_u32(expr: &ast::Expr, ctx: &str) -> Result<u32> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<u32>().map_err(|_| SqlError::TypeMismatch {
                detail: format!("{ctx}: expected u32, got {n}"),
            }),
            other => Err(SqlError::TypeMismatch {
                detail: format!("{ctx}: expected number, got {other}"),
            }),
        },
        _ => Err(SqlError::TypeMismatch {
            detail: format!("{ctx}: expected number literal, got {expr}"),
        }),
    }
}

fn expect_string_array(expr: &ast::Expr, ctx: &str) -> Result<Vec<String>> {
    let elems = match expr {
        ast::Expr::Array(arr) => arr.elem.clone(),
        ast::Expr::Function(f)
            if matches!(
                crate::parser::normalize::normalize_object_name(&f.name).as_str(),
                "make_array" | "array"
            ) =>
        {
            match &f.args {
                ast::FunctionArguments::List(list) => collect_args(&list.args),
                _ => Vec::new(),
            }
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: format!("{ctx}: expected array literal, got {expr}"),
            });
        }
    };
    elems
        .iter()
        .map(|e| expect_string_literal(e, ctx))
        .collect()
}

fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Value(v) if matches!(v.value, ast::Value::Null)
    )
}

fn value_to_coord_literal(v: &Value, dim: &str) -> Result<ArrayCoordLiteral> {
    match v {
        Value::Integer(i) => Ok(ArrayCoordLiteral::Int64(*i)),
        Value::Float(f) => Ok(ArrayCoordLiteral::Float64(*f)),
        Value::String(s) => Ok(ArrayCoordLiteral::String(s.clone())),
        other => Err(SqlError::TypeMismatch {
            detail: format!("NDARRAY_SLICE dim '{dim}' bound: unsupported value kind {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::CollectionInfo;
    use crate::types_array::{
        ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound,
    };

    struct StubCatalog {
        view: Option<ArrayCatalogView>,
        right_view: Option<ArrayCatalogView>,
    }
    impl SqlCatalog for StubCatalog {
        fn get_collection(
            &self,
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
                },
                ArrayAttrAst {
                    name: "qual".into(),
                    dtype: ArrayAttrType::Float64,
                    nullable: true,
                },
            ],
            tile_extents: vec![1, 1_000_000],
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

    #[test]
    fn slice_happy() {
        let p = plan_one(
            "SELECT * FROM NDARRAY_SLICE('g', '{chrom: [1,1], pos: [0, 100]}', ['qual'], 50)",
        )
        .unwrap();
        match p {
            SqlPlan::NdArraySlice {
                name,
                slice,
                attr_projection,
                limit,
            } => {
                assert_eq!(name, "g");
                assert_eq!(slice.dim_ranges.len(), 2);
                assert_eq!(attr_projection, vec!["qual".to_string()]);
                assert_eq!(limit, 50);
            }
            other => panic!("expected NdArraySlice, got {other:?}"),
        }
    }

    #[test]
    fn slice_unknown_dim_rejected() {
        let err = plan_one("SELECT * FROM NDARRAY_SLICE('g', '{nope: [1, 2]}')")
            .err()
            .unwrap();
        assert!(format!("{err}").contains("no dim"));
    }

    #[test]
    fn project_happy() {
        let p = plan_one("SELECT * FROM NDARRAY_PROJECT('g', ['qual', 'variant'])").unwrap();
        match p {
            SqlPlan::NdArrayProject {
                name,
                attr_projection,
            } => {
                assert_eq!(name, "g");
                assert_eq!(attr_projection, vec!["qual".to_string(), "variant".into()]);
            }
            other => panic!("expected NdArrayProject, got {other:?}"),
        }
    }

    #[test]
    fn project_empty_rejected() {
        assert!(plan_one("SELECT * FROM NDARRAY_PROJECT('g', ARRAY[])").is_err());
    }

    #[test]
    fn agg_scalar() {
        let p = plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'sum')").unwrap();
        match p {
            SqlPlan::NdArrayAgg {
                name,
                attr,
                reducer,
                group_by_dim,
            } => {
                assert_eq!(name, "g");
                assert_eq!(attr, "qual");
                assert_eq!(reducer, ArrayReducerAst::Sum);
                assert!(group_by_dim.is_none());
            }
            other => panic!("expected NdArrayAgg, got {other:?}"),
        }
    }

    #[test]
    fn agg_grouped() {
        let p = plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'mean', 'chrom')").unwrap();
        match p {
            SqlPlan::NdArrayAgg {
                reducer,
                group_by_dim,
                ..
            } => {
                assert_eq!(reducer, ArrayReducerAst::Mean);
                assert_eq!(group_by_dim, Some("chrom".into()));
            }
            other => panic!("expected NdArrayAgg, got {other:?}"),
        }
    }

    #[test]
    fn agg_unknown_reducer_rejected() {
        assert!(plan_one("SELECT * FROM NDARRAY_AGG('g', 'qual', 'bogus')").is_err());
    }

    #[test]
    fn elementwise_happy() {
        let p =
            plan_one("SELECT * FROM NDARRAY_ELEMENTWISE('left', 'right', 'add', 'qual')").unwrap();
        assert!(matches!(p, SqlPlan::NdArrayElementwise { .. }));
    }

    #[test]
    fn elementwise_unknown_op_rejected() {
        assert!(
            plan_one("SELECT * FROM NDARRAY_ELEMENTWISE('left', 'right', 'wat', 'qual')").is_err()
        );
    }

    #[test]
    fn flush_happy() {
        let p = plan_one("SELECT NDARRAY_FLUSH('g')").unwrap();
        assert!(matches!(p, SqlPlan::NdArrayFlush { .. }));
    }

    #[test]
    fn compact_happy() {
        let p = plan_one("SELECT NDARRAY_COMPACT('g')").unwrap();
        assert!(matches!(p, SqlPlan::NdArrayCompact { .. }));
    }

    #[test]
    fn flush_unknown_array_rejected() {
        assert!(plan_one("SELECT NDARRAY_FLUSH('nope')").is_err());
    }
}
