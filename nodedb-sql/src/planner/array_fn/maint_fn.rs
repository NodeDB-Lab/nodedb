// SPDX-License-Identifier: Apache-2.0

//! Maintenance ARRAY_* functions: bare `SELECT ARRAY_FLUSH(name)` /
//! `SELECT ARRAY_COMPACT(name)` with no FROM clause.

use sqlparser::ast;

use super::helpers::{collect_args, require_array_name};
use crate::error::Result;
use crate::types::{SqlCatalog, SqlPlan};

/// Try to intercept a no-FROM `SELECT array_flush(name)` /
/// `SELECT array_compact(name)`. The single projection item must be
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
    let fn_name = crate::parser::normalize::normalize_object_name_checked(&func.name)?;
    let sql_name = match fn_name.as_str() {
        "array_flush" => "ARRAY_FLUSH",
        "array_compact" => "ARRAY_COMPACT",
        _ => return Ok(None),
    };
    let arg_exprs = match &func.args {
        ast::FunctionArguments::List(list) => collect_args(&list.args, sql_name)?,
        _ => Vec::new(),
    };
    let name = require_array_name(&arg_exprs, 0, sql_name, catalog)?;
    Ok(Some(match sql_name {
        "ARRAY_FLUSH" => SqlPlan::ArrayFlush { name },
        _ => SqlPlan::ArrayCompact { name },
    }))
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ArrayCatalogView, SqlCatalogError};
    use crate::error::Result;
    use crate::functions::registry::FunctionRegistry;
    use crate::parser::statement::parse_sql;
    use crate::types::{CollectionInfo, SqlCatalog, SqlPlan};
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

    #[test]
    fn flush_happy() {
        let p = plan_one("SELECT ARRAY_FLUSH('g')").unwrap();
        assert!(matches!(p, SqlPlan::ArrayFlush { .. }));
    }

    #[test]
    fn compact_happy() {
        let p = plan_one("SELECT ARRAY_COMPACT('g')").unwrap();
        assert!(matches!(p, SqlPlan::ArrayCompact { .. }));
    }

    #[test]
    fn flush_unknown_array_rejected() {
        assert!(plan_one("SELECT ARRAY_FLUSH('nope')").is_err());
    }
}
