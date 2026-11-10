// SPDX-License-Identifier: BUSL-1.1

//! Binds `NEW.field` references of a subquery CHECK to literal AST nodes.

use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, VisitMut, VisitorMut};

use crate::control::security::catalog::types::CheckConstraintDef;
use crate::control::server::shared::ddl::result::DdlError;

use super::simple::value_to_sql_literal;
use super::subquery::evaluation_error;

pub(super) fn bind_new_references(
    expression: &mut Expr,
    fields: &HashMap<String, nodedb_types::Value>,
    constraint: &CheckConstraintDef,
) -> Result<(), DdlError> {
    let mut binder = NewReferenceBinder {
        fields,
        error: None,
    };
    let _ = expression.visit(&mut binder);
    match binder.error {
        Some(detail) => Err(evaluation_error(constraint, &detail)),
        None => Ok(()),
    }
}

struct NewReferenceBinder<'a> {
    fields: &'a HashMap<String, nodedb_types::Value>,
    error: Option<String>,
}

impl VisitorMut for NewReferenceBinder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::CompoundIdentifier(parts) = expression else {
            return ControlFlow::Continue(());
        };
        if parts.len() != 2
            || parts.iter().any(|part| part.quote_style.is_some())
            || !parts[0].value.eq_ignore_ascii_case("NEW")
        {
            return ControlFlow::Continue(());
        }
        let field_name = parts[1].value.clone();
        let value = self
            .fields
            .iter()
            .find_map(|(name, value)| name.eq_ignore_ascii_case(&field_name).then_some(value))
            .cloned()
            .unwrap_or(nodedb_types::Value::Null);
        if !is_supported_bound_value(&value) {
            self.error = Some(format!(
                "CHECK subquery cannot bind non-scalar NEW field '{field_name}'"
            ));
            return ControlFlow::Break(());
        }
        match parse_literal_expression(&value) {
            Ok(literal) => *expression = literal,
            Err(detail) => {
                self.error = Some(detail);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

fn is_supported_bound_value(value: &nodedb_types::Value) -> bool {
    matches!(
        value,
        nodedb_types::Value::Null
            | nodedb_types::Value::Bool(_)
            | nodedb_types::Value::Integer(_)
            | nodedb_types::Value::Float(_)
            | nodedb_types::Value::String(_)
            | nodedb_types::Value::DateTime(_)
            | nodedb_types::Value::NaiveDateTime(_)
    )
}

fn parse_literal_expression(value: &nodedb_types::Value) -> Result<Expr, String> {
    let literal = value_to_sql_literal(value);
    // reconstructed-sql: parser-only converts one canonical Value literal into an Expr AST
    let statements = nodedb_sql::parser::statement::parse_sql(&format!("SELECT {literal}"))
        .map_err(|error| format!("failed to parse CHECK literal: {error}"))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err("CHECK literal did not produce exactly one query".to_string());
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("CHECK literal query is not a SELECT".to_string());
    };
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expression)] => Ok(expression.clone()),
        _ => Err("CHECK literal query did not produce exactly one expression".to_string()),
    }
}
