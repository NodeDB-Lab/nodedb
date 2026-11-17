// SPDX-License-Identifier: Apache-2.0

//! The forms whose type the SQL text names on its own, with no catalog
//! involved: `LIMIT` / `OFFSET` row counts and explicit casts.
//!
//! Implemented as a sqlparser [`Visitor`] so every nesting level — subqueries,
//! CTEs, set operations — is covered by the same three hooks.

use core::ops::ControlFlow;

use sqlparser::ast::{Expr, LimitClause, Query, Statement, Value, ValueWithSpan, Visitor};

use super::slots::{
    InferenceContext, InferredParamType, parse_placeholder_body, placeholder_index,
};
use crate::types_expr::SqlDataType;

impl InferenceContext {
    /// Record `$N` as an integer when `expr` is a bare placeholder.
    ///
    /// Used for every row-count position (`LIMIT`, `OFFSET`), all of which
    /// PostgreSQL types as `bigint`.
    fn record_row_count(&mut self, expr: &Expr) {
        self.record_if_placeholder(expr, InferredParamType::from_sql_type(SqlDataType::Int64));
    }

    fn record_limit_clause(&mut self, clause: &LimitClause) {
        match clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by: _,
            } => {
                // `LIMIT BY <expr>,...` is a ClickHouse grouping key, not a
                // row count — nothing to infer from it.
                if let Some(limit) = limit {
                    self.record_row_count(limit);
                }
                if let Some(offset) = offset {
                    self.record_row_count(&offset.value);
                }
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                self.record_row_count(offset);
                self.record_row_count(limit);
            }
        }
    }
}

impl Visitor for InferenceContext {
    type Break = ();

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        // Every placeholder position sqlparser defines reaches this hook —
        // the same coverage `params::ParamBinder` relies on for binding — so
        // the result is sized by the highest index that actually exists.
        if let Value::Placeholder(body) = &value.value
            && let Some(index) = parse_placeholder_body(body)
        {
            self.observe(index);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        // `$1::INT` and `CAST($1 AS INT)` differ only in `CastKind`; both
        // name the parameter's type outright.
        if let Expr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
            && let Some(index) = placeholder_index(inner)
        {
            let type_name = data_type.to_string();
            if let Some(ty) = type_name_to_sql_data_type(&type_name) {
                self.record(index, InferredParamType::from_cast(ty, &type_name));
            }
        }
        // Any other expression shape: not a form this pass infers. The walk
        // continues into it regardless — this hook only adds conclusions.
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(clause) = &query.limit_clause {
            self.record_limit_clause(clause);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        // `UPDATE ... LIMIT $N` / `DELETE ... LIMIT $N` carry their own limit
        // expression outside any `Query`, so `pre_visit_query` never sees it.
        match statement {
            Statement::Update(update) => {
                if let Some(limit) = &update.limit {
                    self.record_row_count(limit);
                }
            }
            Statement::Delete(delete) => {
                if let Some(limit) = &delete.limit {
                    self.record_row_count(limit);
                }
            }
            // Any other statement: no statement-level typed position. Nested
            // queries and expressions are still visited by the other hooks.
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Map a SQL type name to the planner's resolved type.
///
/// Takes the rendered type name (sqlparser's `DataType` `Display`, which is
/// how `resolver::expr::convert` and `planner::const_fold` already carry cast
/// targets) and normalises it the same way `const_fold::fold_cast` does:
/// upper-cased, with any `(precision, scale)` suffix stripped.
///
/// `None` for an unrecognised name — including types that have no faithful
/// wire representation on the caller's side yet. Adding a name here widens
/// what `Describe` promises, so only add one whose value a client can
/// actually round-trip.
fn type_name_to_sql_data_type(type_name: &str) -> Option<SqlDataType> {
    let upper = type_name.to_uppercase();
    let base = upper
        .split('(')
        .next()
        .map(str::trim)
        .unwrap_or(upper.as_str());

    match base {
        "INT" | "INTEGER" | "INT2" | "INT4" | "INT8" | "INT64" | "SMALLINT" | "BIGINT" => {
            Some(SqlDataType::Int64)
        }
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => {
            Some(SqlDataType::Float64)
        }
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" | "CHARACTER VARYING" | "BPCHAR" => {
            Some(SqlDataType::String)
        }
        "BOOL" | "BOOLEAN" => Some(SqlDataType::Bool),
        "BYTEA" | "BYTES" | "BLOB" => Some(SqlDataType::Bytes),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Some(SqlDataType::Timestamp),
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Some(SqlDataType::Timestamptz),
        // Unrecognised type name — the position stays unknown, which costs a
        // text-format round-trip and nothing else.
        _ => None,
    }
}
