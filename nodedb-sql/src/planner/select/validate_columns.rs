// SPDX-License-Identifier: Apache-2.0

//! Plan-time existence validation for column references.
//!
//! A column reference that names nothing in scope used to plan as a field
//! reference and evaluate to `NULL` per row. On closed-schema collections
//! that is silent misbehavior — `WHERE` matches nothing, `ORDER BY` no-ops,
//! `UPDATE` touches zero rows — with no error anywhere. PostgreSQL raises
//! `42703` (undefined_column) at plan time for the same typo.
//!
//! Collections whose row shape is closed at plan time validate every column
//! reference before the plan is built:
//!
//! - `document_strict` and `kv` are closed by construction.
//! - `document` (schemaless) is closed exactly when the collection declares
//!   columns; a collection created without a column list stays open, so
//!   references to fields that appear only in the data keep resolving
//!   (the `NULL` fold remains the documented behavior there).
//!
//! Open engines (columnar/timeseries/spatial/array and column-less
//! schemaless) are untouched. The check covers the single-table SELECT
//! path (projection, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`) and the
//! `UPDATE`/`DELETE` target path (assignments and predicates).

use sqlparser::ast::{self, SelectItem};
use std::collections::BTreeSet;

use crate::error::{Result, SqlError};
use crate::resolver::columns::ResolvedTable;
use crate::types::{EngineType, SortKey};

/// Output-name allowlist from a SELECT list: explicit aliases plus bare
/// column projections. GROUP BY / HAVING / ORDER BY may reference these
/// (PostgreSQL output-name resolution); WHERE and projection cannot.
pub(crate) fn projection_alias_set(select: &ast::Select) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for item in &select.projection {
        match item {
            SelectItem::ExprWithAlias { alias, .. } => {
                set.insert(alias.value.to_lowercase());
            }
            SelectItem::UnnamedExpr(ast::Expr::Identifier(ident)) => {
                set.insert(ident.value.to_lowercase());
            }
            _ => {}
        }
    }
    set
}

/// Validate ORDER BY sort keys on a closed-schema single-table scan.
///
/// Allowed names are declared columns plus projection output names (aliases
/// and bare column projections); a sort key naming anything else is a typo
/// that would otherwise no-op silently.
pub(crate) fn validate_sort_keys(
    keys: &[SortKey],
    table: &ResolvedTable,
    output_names: &BTreeSet<String>,
) -> Result<()> {
    if !schema_is_closed(table) {
        return Ok(());
    }
    let mut allowed = declared_columns(table);
    allowed.extend(output_names.iter().cloned());
    for key in keys {
        validate_sql_expr_columns(&key.expr, table, &allowed)?;
    }
    Ok(())
}

/// Whether a collection's declared columns are a closed set at plan time.
///
/// Closed by construction: `document_strict`, `kv`, `columnar`, `timeseries`
/// and `spatial` all carry a DDL-declared row shape. A schemaless document
/// collection is open only when its planner column list holds nothing beyond
/// the synthesized primary-key column (the catalog adapter prepends one —
/// the declared PK name or the built-in `id` — before any declared fields).
/// One column means the user declared no fields, so dynamic fields that
/// appear only in the data keep resolving (the NULL fold stays); two or more
/// means the user declared fields, and that declared surface is the full
/// queryable surface — a reference outside it is a typo, not a field.
pub(crate) fn schema_is_closed(table: &ResolvedTable) -> bool {
    match table.info.engine {
        EngineType::DocumentSchemaless => table.info.columns.len() > 1,
        // Array scans go through engine rules that know their own shape.
        EngineType::Array => false,
        EngineType::DocumentStrict
        | EngineType::KeyValue
        | EngineType::Columnar
        | EngineType::Timeseries
        | EngineType::Spatial => true,
    }
}

/// Declared column names, lowercased.
fn declared_columns(table: &ResolvedTable) -> BTreeSet<String> {
    table
        .info
        .columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect()
}

/// Validate every column reference in a single-table SELECT.
///
/// `select` must already be qualifier-stripped (`strip_single_table_qualifiers`),
/// so references are bare identifiers. `GROUP BY` and `HAVING` may name
/// projection aliases (PostgreSQL allows output names there); `projection_aliases`
/// is the allowlist for those clauses.
pub(crate) fn validate_select(
    select: &ast::Select,
    table: &ResolvedTable,
    projection_aliases: &BTreeSet<String>,
) -> Result<()> {
    if !schema_is_closed(table) {
        return Ok(());
    }
    let columns = declared_columns(table);

    // Projection and WHERE never allow aliases: PostgreSQL resolves them
    // strictly against table columns.
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                check_ast_expr(expr, &columns, table)?;
            }
            // Wildcard / qualified wildcard expand at plan build; nothing to check.
            SelectItem::ExprWithAliases { .. }
            | SelectItem::Wildcard(_)
            | SelectItem::QualifiedWildcard(..) => {}
        }
    }
    if let Some(selection) = &select.selection {
        check_ast_expr(selection, &columns, table)?;
    }

    // GROUP BY and HAVING may reference output aliases; validate an
    // identifier only when it names neither a column nor an alias.
    let loose = columns.union(projection_aliases).cloned().collect();
    if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for expr in exprs {
            check_ast_expr(expr, &loose, table)?;
        }
    }
    if let Some(having) = &select.having {
        check_ast_expr(having, &loose, table)?;
    }
    Ok(())
}

/// Validate a WHERE clause against a single-table scan's columns.
pub(crate) fn validate_where(expr: &ast::Expr, table: &ResolvedTable) -> Result<()> {
    if !schema_is_closed(table) {
        return Ok(());
    }
    check_ast_expr(expr, &declared_columns(table), table)
}

/// Validate an UPDATE/DELETE SET target or row-shape reference.
pub(crate) fn validate_write_column(column: &str, table: &ResolvedTable) -> Result<()> {
    if !schema_is_closed(table) {
        return Ok(());
    }
    let col = column.to_lowercase();
    if !declared_columns(table).contains(&col) {
        return Err(unknown(table, &col));
    }
    Ok(())
}

/// Validate every column reference in an already-converted expression tree
/// (ORDER BY keys on the scan path, which are `SqlExpr` by then).
pub(crate) fn validate_sql_expr_columns(
    expr: &crate::types::SqlExpr,
    table: &ResolvedTable,
    allowed: &BTreeSet<String>,
) -> Result<()> {
    use crate::types::SqlExpr as S;
    if !schema_is_closed(table) {
        return Ok(());
    }
    match expr {
        S::Column { table: None, name } => {
            if !allowed.contains(&name.to_lowercase()) {
                return Err(unknown(table, name));
            }
            Ok(())
        }
        S::Column {
            table: Some(t),
            name,
        } => {
            // Qualified reference on a single-table scan: qualifier must be
            // this table (or its alias). Other qualifiers were already refused
            // during qualifier stripping; validate defensively.
            let qual = t.to_lowercase();
            let self_name = table.name.to_lowercase();
            let alias = table.alias.as_ref().map(|a| a.to_lowercase());
            if (qual == self_name || alias.as_deref() == Some(qual.as_str()))
                && !allowed.contains(&name.to_lowercase())
            {
                return Err(unknown(table, name));
            }
            Ok(())
        }
        S::BinaryOp { left, right, .. } => {
            validate_sql_expr_columns(left, table, allowed)?;
            validate_sql_expr_columns(right, table, allowed)
        }
        S::UnaryOp { expr, .. } => validate_sql_expr_columns(expr, table, allowed),
        S::Function { args, .. } => {
            for a in args {
                validate_sql_expr_columns(a, table, allowed)?;
            }
            Ok(())
        }
        S::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_sql_expr_columns(operand, table, allowed)?;
            }
            for (when, then) in when_then {
                validate_sql_expr_columns(when, table, allowed)?;
                validate_sql_expr_columns(then, table, allowed)?;
            }
            if let Some(else_expr) = else_expr {
                validate_sql_expr_columns(else_expr, table, allowed)?;
            }
            Ok(())
        }
        S::Cast { expr, .. } | S::IsNull { expr, .. } => {
            validate_sql_expr_columns(expr, table, allowed)
        }
        S::InList { expr, list, .. } => {
            validate_sql_expr_columns(expr, table, allowed)?;
            for item in list {
                validate_sql_expr_columns(item, table, allowed)?;
            }
            Ok(())
        }
        S::Between {
            expr, low, high, ..
        } => {
            validate_sql_expr_columns(expr, table, allowed)?;
            validate_sql_expr_columns(low, table, allowed)?;
            validate_sql_expr_columns(high, table, allowed)
        }
        S::Like { expr, pattern, .. } => {
            validate_sql_expr_columns(expr, table, allowed)?;
            validate_sql_expr_columns(pattern, table, allowed)
        }
        S::ArrayLiteral(items) => {
            for item in items {
                validate_sql_expr_columns(item, table, allowed)?;
            }
            Ok(())
        }
        // Literals, wildcard and subqueries carry no column reference here;
        // subqueries plan and validate on their own path.
        S::Literal(_) | S::Wildcard | S::Subquery(_) => Ok(()),
    }
}

/// Recursive walk over an AST expression, refusing subqueries (their inner
/// SELECT plans and validates separately) and never treating function names
/// as column references.
fn check_ast_expr(
    expr: &ast::Expr,
    allowed: &BTreeSet<String>,
    table: &ResolvedTable,
) -> Result<()> {
    use ast::Expr as E;
    match expr {
        E::Identifier(ident) => {
            let name = ident.value.to_lowercase();
            if !allowed.contains(&name) {
                return Err(unknown(table, &name));
            }
            Ok(())
        }
        E::CompoundIdentifier(parts) => {
            // Two-part `t.col` on a single-table scan: validate `col` against
            // this table; anything else was refused during qualifier stripping
            // (or belongs to a path this validator does not run on).
            if let [qual, col] = parts.as_slice() {
                let qual = qual.to_string().to_lowercase();
                let name = col.to_string().to_lowercase();
                let self_name = table.name.to_lowercase();
                let alias = table.alias.as_ref().map(|a| a.to_lowercase());
                if (qual == self_name || alias.as_deref() == Some(qual.as_str()))
                    && !allowed.contains(&name)
                {
                    return Err(unknown(table, &name));
                }
            }
            Ok(())
        }
        E::BinaryOp { left, right, .. } => {
            check_ast_expr(left, allowed, table)?;
            check_ast_expr(right, allowed, table)
        }
        E::UnaryOp { expr: inner, .. } => check_ast_expr(inner, allowed, table),
        E::IsFalse(inner)
        | E::IsNotFalse(inner)
        | E::IsTrue(inner)
        | E::IsNotTrue(inner)
        | E::IsNull(inner)
        | E::IsNotNull(inner)
        | E::Nested(inner)
        | E::Cast { expr: inner, .. }
        | E::AnyOp { left: inner, .. }
        | E::AllOp { left: inner, .. } => check_ast_expr(inner, allowed, table),
        E::Between {
            expr: b, low, high, ..
        } => {
            check_ast_expr(b, allowed, table)?;
            check_ast_expr(low, allowed, table)?;
            check_ast_expr(high, allowed, table)
        }
        E::InList {
            expr: item, list, ..
        } => {
            check_ast_expr(item, allowed, table)?;
            for l in list {
                check_ast_expr(l, allowed, table)?;
            }
            Ok(())
        }
        E::Like {
            expr: l, pattern, ..
        }
        | E::ILike {
            expr: l, pattern, ..
        }
        | E::SimilarTo {
            expr: l, pattern, ..
        }
        | E::RLike {
            expr: l, pattern, ..
        } => {
            check_ast_expr(l, allowed, table)?;
            check_ast_expr(pattern, allowed, table)
        }
        E::Function(f) => {
            if let ast::FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            check_ast_expr(e, allowed, table)?;
                        }
                        ast::FunctionArg::Named {
                            arg: ast::FunctionArgExpr::Expr(e),
                            ..
                        } => check_ast_expr(e, allowed, table)?,
                        _ => {}
                    }
                }
            }
            if let Some(filter) = &f.filter {
                check_ast_expr(filter, allowed, table)?;
            }
            Ok(())
        }
        E::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                check_ast_expr(operand, allowed, table)?;
            }
            for when in conditions {
                check_ast_expr(&when.condition, allowed, table)?;
                check_ast_expr(&when.result, allowed, table)?;
            }
            if let Some(else_result) = else_result {
                check_ast_expr(else_result, allowed, table)?;
            }
            Ok(())
        }
        // Subqueries and EXISTS plan and validate on their own path; the
        // remaining variants (literals, wildcards, tuples, JSON access,
        // typed strings, value lists) carry no bare column references.
        E::Subquery(_) | E::Exists { .. } => Ok(()),
        _ => Ok(()),
    }
}

fn unknown(table: &ResolvedTable, column: &str) -> SqlError {
    SqlError::UnknownColumn {
        table: table.name.clone(),
        column: column.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CollectionInfo, ColumnInfo, SortKey, SqlDataType, SqlExpr};
    use nodedb_types::PrimaryEngine;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn info(engine: EngineType, columns: Vec<&str>) -> CollectionInfo {
        CollectionInfo {
            name: "t".into(),
            engine,
            columns: columns
                .into_iter()
                .map(|c| ColumnInfo {
                    name: c.into(),
                    data_type: SqlDataType::String,
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                })
                .collect(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        }
    }

    fn tbl(engine: EngineType, columns: Vec<&str>) -> ResolvedTable {
        ResolvedTable {
            name: "t".into(),
            alias: None,
            info: info(engine, columns),
        }
    }

    fn parse_select(sql: &str) -> ast::Select {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse failed");
        match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                ast::SetExpr::Select(s) => (**s).clone(),
                other => panic!("expected plain SELECT body, got {other:?}"),
            },
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    // ── closed engines raise 42703 (SqlError::UnknownColumn) ──

    #[test]
    fn strict_projection_ghost_column_raises() {
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let select = parse_select("SELECT a, ghost FROM t");
        let err = validate_select(&select, &t, &BTreeSet::new()).unwrap_err();
        assert!(
            matches!(err, SqlError::UnknownColumn { ref column, .. } if column == "ghost"),
            "expected UnknownColumn(ghost), got {err:?}"
        );
    }

    #[test]
    fn strict_where_ghost_column_raises() {
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let select = parse_select("SELECT a FROM t WHERE ghost = 5");
        let err = validate_select(&select, &t, &BTreeSet::new()).unwrap_err();
        assert!(matches!(err, SqlError::UnknownColumn { ref column, .. } if column == "ghost"));
    }

    #[test]
    fn kv_where_and_projection_ghost_raise() {
        let t = tbl(EngineType::KeyValue, vec!["k", "v"]);
        let select = parse_select("SELECT v FROM t WHERE ghost IS NULL");
        assert!(matches!(
            validate_select(&select, &t, &BTreeSet::new()),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
        let select = parse_select("SELECT ghost FROM t");
        assert!(matches!(
            validate_select(&select, &t, &BTreeSet::new()),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
    }

    #[test]
    fn declared_document_is_closed_at_plan_time() {
        // A document collection with user-declared columns knows them at plan
        // time, exactly like strict. The open fold is reserved for collections
        // created without a column list (only the synthesized PK column exists).
        let t = tbl(EngineType::DocumentSchemaless, vec!["id", "x"]);
        let select = parse_select("SELECT nonexistent_col FROM t");
        assert!(matches!(
            validate_select(&select, &t, &BTreeSet::new()),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "nonexistent_col"
        ));
    }

    #[test]
    fn column_less_document_stays_open() {
        // Only the synthesized PK column (any name): any field resolves,
        // fold semantics kept. The PK name is whatever CREATE COLLECTION
        // declared (or the built-in `id`); the open/closed line is the
        // column COUNT, not the name.
        let t = tbl(EngineType::DocumentSchemaless, vec!["id"]);
        let select = parse_select("SELECT id, dynamic_field FROM t WHERE dynamic_field = 1");
        validate_select(&select, &t, &BTreeSet::new()).expect("open schemaless must not raise");

        let renamed = tbl(EngineType::DocumentSchemaless, vec!["sku"]);
        let select_renamed = parse_select("SELECT sku, dynamic_field FROM t");
        validate_select(&select_renamed, &renamed, &BTreeSet::new())
            .expect("PK-only schemaless stays open under a renamed key");

        // Declared columns close the set even when the catalog leaves
        // raw_type unset (it does: schemaless columns carry raw_type None).
        let t2 = tbl(EngineType::DocumentSchemaless, vec!["id", "x"]);
        let select2 = parse_select("SELECT ghost FROM t");
        assert!(matches!(
            validate_select(&select2, &t2, &BTreeSet::new()),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));

        // Fixed-shape engines are closed regardless of raw_type population.
        for engine in [
            EngineType::Columnar,
            EngineType::Timeseries,
            EngineType::Spatial,
        ] {
            let t3 = tbl(engine, vec!["id", "x"]);
            assert!(
                matches!(
                    validate_select(&select2, &t3, &BTreeSet::new()),
                    Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
                ),
                "engine {engine:?} must be closed"
            );
        }
    }

    #[test]
    fn valid_references_pass() {
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let select = parse_select("SELECT a, b FROM t WHERE a = 1 AND b LIKE 'x%' ORDER BY a");
        validate_select(&select, &t, &BTreeSet::new()).expect("valid refs must pass");
    }

    #[test]
    fn function_arguments_are_checked_but_names_are_not() {
        let t = tbl(EngineType::KeyValue, vec!["k", "v"]);
        // Function NAME identifiers are not column references.
        let select = parse_select("SELECT LENGTH(v) FROM t");
        validate_select(&select, &t, &BTreeSet::new()).expect("function name is not a column");
        // Function ARGUMENTS are.
        let select = parse_select("SELECT LENGTH(ghost) FROM t");
        assert!(matches!(
            validate_select(&select, &t, &BTreeSet::new()),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
    }

    #[test]
    fn group_by_alias_is_allowed_ghost_is_not() {
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let aliases = projection_alias_set(&parse_select("SELECT b AS bb FROM t GROUP BY bb"));
        assert!(aliases.contains("bb"));
        let select = parse_select("SELECT b AS bb FROM t GROUP BY bb");
        validate_select(&select, &t, &aliases).expect("group by output alias must pass");
        let select = parse_select("SELECT b AS bb FROM t GROUP BY ghost");
        assert!(matches!(
            validate_select(&select, &t, &aliases),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
    }

    #[test]
    fn update_set_target_must_exist_on_closed_engines() {
        let t = tbl(EngineType::KeyValue, vec!["k", "v"]);
        validate_write_column("v", &t).expect("declared target passes");
        assert!(matches!(
            validate_write_column("ghost", &t),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
        // Open document: any write target is a dynamic field.
        let open = tbl(EngineType::DocumentSchemaless, vec![]);
        validate_write_column("ghost", &open).expect("schemaless accepts any write target");
    }

    #[test]
    fn order_by_sort_key_must_exist_or_name_an_output() {
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let outputs: BTreeSet<String> = ["bb".into()].into_iter().collect();

        let key = SortKey {
            expr: SqlExpr::Column {
                table: None,
                name: "a".into(),
            },
            ascending: true,
            nulls_first: false,
        };
        validate_sort_keys(std::slice::from_ref(&key), &t, &outputs)
            .expect("declared column passes");

        let alias_key = SortKey {
            expr: SqlExpr::Column {
                table: None,
                name: "bb".into(),
            },
            ascending: true,
            nulls_first: false,
        };
        validate_sort_keys(&[alias_key], &t, &outputs).expect("output alias passes");

        let ghost = SortKey {
            expr: SqlExpr::Column {
                table: None,
                name: "ghost".into(),
            },
            ascending: true,
            nulls_first: false,
        };
        assert!(matches!(
            validate_sort_keys(&[ghost], &t, &outputs),
            Err(SqlError::UnknownColumn { ref column, .. }) if column == "ghost"
        ));
    }

    #[test]
    fn order_by_ordinal_and_expression_keys_pass() {
        // ORDER BY 1 (output ordinal) converts to a literal, and ORDER BY
        // a + b is an expression: neither is a bare column reference, so
        // neither may be rejected.
        let t = tbl(EngineType::DocumentStrict, vec!["a", "b"]);
        let outputs: BTreeSet<String> = ["bb".into()].into_iter().collect();
        for expr in [
            SqlExpr::Literal(crate::types::SqlValue::Int(1)),
            SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column {
                    table: None,
                    name: "a".into(),
                }),
                op: crate::types::BinaryOp::Add,
                right: Box::new(SqlExpr::Column {
                    table: None,
                    name: "b".into(),
                }),
            },
        ] {
            let key = SortKey {
                expr,
                ascending: true,
                nulls_first: false,
            };
            validate_sort_keys(&[key], &t, &outputs).expect("non-bare sort key must pass");
        }
    }
}
