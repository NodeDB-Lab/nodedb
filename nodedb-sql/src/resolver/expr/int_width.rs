// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL integer width typing for converted expressions.
//!
//! Every integer is an `i64` at run time, so the width lives in the plan. An
//! integer operator whose result is `smallint` or `integer` converts to a
//! cast of the operation to that width. The evaluator then checks the exact
//! `i64` result at the width and raises "smallint out of range" or "integer
//! out of range" past it, and the announced column type reads the cast. A
//! `bigint` result needs no cast: `i64` arithmetic is checked already.

use nodedb_query::CastType;
use nodedb_query::cast::IntWidth;
use sqlparser::ast::Expr;

use crate::resolver::ColumnScope;
use crate::types::{BinaryOp, SqlDataType, SqlExpr, SqlValue, UnaryOp};

/// The integer width `expr` evaluates at, as PostgreSQL types it. `None`
/// when `expr` is not certainly an integer.
///
/// - An integer literal is `integer` when `i32` holds it, else `bigint`.
/// - A column takes its declared width. An integer column declared with no
///   width is `bigint`.
/// - A cast to `smallint`, `integer` or `bigint` takes that width.
/// - `+ - * / %` over two integers, and `mod(a, b)`, take the wider operand
///   width.
/// - Unary minus and `abs(a)` keep the operand width.
pub fn int_width_of(expr: &SqlExpr, scope: &ColumnScope<'_>) -> Option<IntWidth> {
    match expr {
        SqlExpr::Literal(SqlValue::Int(i)) => Some(IntWidth::of_literal(*i)),
        SqlExpr::Column { table, name } => {
            let column = scope.declared_column(table.as_deref(), name)?;
            match column.data_type {
                SqlDataType::Int64 => Some(column.int_width.map_or(IntWidth::Int8, IntWidth::from)),
                _ => None,
            }
        }
        SqlExpr::Cast { to_type, .. } => {
            CastType::from_type_name(to_type).and_then(|cast| IntWidth::of_cast(&cast))
        }
        SqlExpr::BinaryOp { left, op, right } if is_arithmetic(*op) => {
            Some(int_width_of(left, scope)?.wider(int_width_of(right, scope)?))
        }
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg,
            expr,
        } => int_width_of(expr, scope),
        SqlExpr::Function { name, args, .. } => {
            match (name.to_ascii_lowercase().as_str(), args.as_slice()) {
                ("abs", [arg]) => int_width_of(arg, scope),
                ("mod", [a, b]) => Some(int_width_of(a, scope)?.wider(int_width_of(b, scope)?)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// `expr` checked at its integer width.
///
/// An integer `+ - * / %`, unary minus, `abs` or `mod` whose width is
/// `smallint` or `integer` becomes a cast of itself to that width. Every
/// other expression is returned unchanged.
pub(crate) fn check_at_int_width(expr: SqlExpr, scope: &ColumnScope<'_>) -> SqlExpr {
    if !is_int_operation(&expr) {
        return expr;
    }
    match int_width_of(&expr, scope) {
        Some(width @ (IntWidth::Int2 | IntWidth::Int4)) => SqlExpr::Cast {
            expr: Box::new(expr),
            to_type: width.type_name().to_ascii_uppercase(),
        },
        Some(IntWidth::Int8) | None => expr,
    }
}

/// The converted operand `converted` of an integer operation, typed
/// `bigint` when its source `ast` is an `INTERVAL` literal.
///
/// An interval converts to its microseconds as an integer literal. That
/// literal is not a PostgreSQL `integer`, so it is cast to `bigint`: an
/// interval scaled by an integer is never checked at `integer` width.
pub(crate) fn interval_operand_as_bigint(ast: &Expr, converted: SqlExpr) -> SqlExpr {
    if !is_interval(ast) {
        return converted;
    }
    SqlExpr::Cast {
        expr: Box::new(converted),
        to_type: IntWidth::Int8.type_name().to_ascii_uppercase(),
    }
}

/// Whether `ast` is an `INTERVAL` literal, parenthesized or not.
fn is_interval(ast: &Expr) -> bool {
    match ast {
        Expr::Interval(_) => true,
        Expr::Nested(inner) => is_interval(inner),
        _ => false,
    }
}

/// Whether `expr` is an operation whose integer result the evaluator
/// computes in `i64`.
fn is_int_operation(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::BinaryOp { op, .. } => is_arithmetic(*op),
        SqlExpr::UnaryOp {
            op: UnaryOp::Neg, ..
        } => true,
        SqlExpr::Function { name, args, .. } => matches!(
            (name.to_ascii_lowercase().as_str(), args.len()),
            ("abs", 1) | ("mod", 2)
        ),
        _ => false,
    }
}

fn is_arithmetic(op: BinaryOp) -> bool {
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => true,
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Concat => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::columns::{ResolvedTable, TableScope};
    use crate::resolver::expr::convert_expr;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType};
    use nodedb_types::columnar::IntWidth as Declared;

    fn table() -> TableScope {
        let col = |name: &str, data_type: SqlDataType, int_width: Option<Declared>| ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width,
            float_width: None,
        };
        let info = CollectionInfo {
            name: "t".to_string(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                col("s", SqlDataType::Int64, Some(Declared::I16)),
                col("i", SqlDataType::Int64, Some(Declared::I32)),
                col("b", SqlDataType::Int64, Some(Declared::I64)),
                col("u", SqlDataType::Int64, None),
                col("f", SqlDataType::Float64, None),
            ],
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        };
        TableScope::single(ResolvedTable {
            name: "t".to_string(),
            alias: None,
            info,
        })
        .expect("one relation")
    }

    /// The parsed expression `sql` spells.
    fn parsed(sql: &str) -> sqlparser::ast::Expr {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("tokenizes")
            .parse_expr()
            .expect("parses")
    }

    fn converted(sql: &str) -> SqlExpr {
        let scope = table();
        convert_expr(&parsed(sql), &ColumnScope::Relations(&scope)).expect("converts")
    }

    fn width(sql: &str) -> Option<IntWidth> {
        let scope = table();
        int_width_of(&converted(sql), &ColumnScope::Relations(&scope))
    }

    /// The cast an expression converts to, when it is one.
    fn cast_of(sql: &str) -> Option<String> {
        match converted(sql) {
            SqlExpr::Cast { to_type, .. } => Some(to_type),
            _ => None,
        }
    }

    #[test]
    fn each_width_pair_promotes_to_the_wider() {
        for (sql, want) in [
            ("s + s", IntWidth::Int2),
            ("s * i", IntWidth::Int4),
            ("i - s", IntWidth::Int4),
            ("i * i", IntWidth::Int4),
            ("i + b", IntWidth::Int8),
            ("s % b", IntWidth::Int8),
            ("b / b", IntWidth::Int8),
            ("u + i", IntWidth::Int8),
            ("i * 1", IntWidth::Int4),
            ("s + 1", IntWidth::Int4),
            ("i + 3000000000", IntWidth::Int8),
            ("-i", IntWidth::Int4),
            ("-s", IntWidth::Int2),
            ("abs(s)", IntWidth::Int2),
            ("mod(i, s)", IntWidth::Int4),
            ("1 + 1", IntWidth::Int4),
            ("CAST(1 AS SMALLINT) + CAST(1 AS SMALLINT)", IntWidth::Int2),
            ("1::INT8 * 2", IntWidth::Int8),
        ] {
            assert_eq!(width(sql), Some(want), "{sql}");
        }
        assert_eq!(width("i + f"), None);
        assert_eq!(width("i || 'x'"), None);
        assert_eq!(width("i > 1"), None);
    }

    #[test]
    fn a_narrow_result_converts_to_a_cast_to_its_width() {
        assert_eq!(cast_of("i * 1").as_deref(), Some("INTEGER"));
        assert_eq!(cast_of("s + s").as_deref(), Some("SMALLINT"));
        assert_eq!(cast_of("s + i").as_deref(), Some("INTEGER"));
        assert_eq!(cast_of("-i").as_deref(), Some("INTEGER"));
        assert_eq!(cast_of("i / 2").as_deref(), Some("INTEGER"));
        assert_eq!(cast_of("abs(i)").as_deref(), Some("INTEGER"));
        assert_eq!(cast_of("i + b"), None);
        assert_eq!(cast_of("b * 2"), None);
        assert_eq!(cast_of("i + f"), None);
        // A literal alone is not an operation.
        assert!(matches!(converted("5"), SqlExpr::Literal(SqlValue::Int(5))));
    }

    /// An interval is its microseconds, a `bigint`: scaling it by an
    /// integer is not checked at `integer` width.
    #[test]
    fn an_interval_operand_is_bigint() {
        assert_eq!(width("INTERVAL '1 second' * 3000"), Some(IntWidth::Int8));
        assert_eq!(cast_of("INTERVAL '1 second' * 3000"), None);
        assert_eq!(
            width("(INTERVAL '1 second') * 2 * 1000"),
            Some(IntWidth::Int8)
        );
        assert_eq!(width("-INTERVAL '1 second'"), Some(IntWidth::Int8));
        // An interval outside integer arithmetic stays a literal.
        assert!(matches!(
            converted("INTERVAL '1 second'"),
            SqlExpr::Literal(SqlValue::Int(1_000_000))
        ));
    }

    #[test]
    fn nested_operations_check_each_level() {
        let SqlExpr::Cast { expr, to_type } = converted("i * 2 + 1") else {
            panic!("an integer sum is cast to its width");
        };
        assert_eq!(to_type, "INTEGER");
        let SqlExpr::BinaryOp { left, .. } = *expr else {
            panic!("the cast wraps the sum");
        };
        assert!(
            matches!(*left, SqlExpr::Cast { ref to_type, .. } if to_type == "INTEGER"),
            "the inner product is checked at its own width: {left:?}"
        );
    }

    #[test]
    fn an_unchecked_scope_types_literals_only() {
        let converted = convert_expr(&parsed("i + 1"), &ColumnScope::Unchecked).expect("converts");
        assert!(
            matches!(converted, SqlExpr::BinaryOp { .. }),
            "{converted:?}"
        );
        let converted =
            convert_expr(&parsed("2147483647 + 1"), &ColumnScope::Unchecked).expect("converts");
        assert!(
            matches!(converted, SqlExpr::Cast { ref to_type, .. } if to_type == "INTEGER"),
            "{converted:?}"
        );
    }
}
