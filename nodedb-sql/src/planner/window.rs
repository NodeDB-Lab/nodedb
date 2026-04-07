//! Window function extraction from SELECT projection.

use sqlparser::ast;

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::resolver::expr::convert_expr;
use crate::types::{SortKey, WindowSpec};

/// Extract window function specifications from SELECT items.
pub fn extract_window_functions(
    items: &[ast::SelectItem],
    _functions: &FunctionRegistry,
) -> Result<Vec<WindowSpec>> {
    let mut specs = Vec::new();
    for item in items {
        let (expr, alias) = match item {
            ast::SelectItem::UnnamedExpr(e) => (e, format!("{e}")),
            ast::SelectItem::ExprWithAlias { expr, alias } => (expr, normalize_ident(alias)),
            _ => continue,
        };
        if let ast::Expr::Function(func) = expr {
            if func.over.is_some() {
                specs.push(convert_window_spec(func, &alias)?);
            }
        }
    }
    Ok(specs)
}

fn convert_window_spec(func: &ast::Function, alias: &str) -> Result<WindowSpec> {
    let name = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join(".");

    let args = match &func.args {
        ast::FunctionArguments::List(args) => args
            .args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => convert_expr(e).ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    let (partition_by, order_by) = match &func.over {
        Some(ast::WindowType::WindowSpec(spec)) => {
            let pb = spec
                .partition_by
                .iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>>>()?;
            let ob = spec
                .order_by
                .iter()
                .map(|o| {
                    Ok(SortKey {
                        expr: convert_expr(&o.expr)?,
                        ascending: o.options.asc.unwrap_or(true),
                        nulls_first: o.options.nulls_first.unwrap_or(false),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (pb, ob)
        }
        _ => (Vec::new(), Vec::new()),
    };

    Ok(WindowSpec {
        function: name,
        args,
        partition_by,
        order_by,
        alias: alias.into(),
    })
}
