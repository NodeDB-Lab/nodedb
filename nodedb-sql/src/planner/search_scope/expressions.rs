// SPDX-License-Identifier: Apache-2.0

//! Row-expression search-function checks.

use super::lookup::first_search_function;
use super::scope::Scope;
use crate::error::{Result, SqlError};
use crate::types::query::{AggregateExpr, Projection, SortKey, WindowSpec};
use crate::types::{Filter, FilterExpr};
use crate::types_expr::SqlExpr;

impl Scope<'_> {
    pub(super) fn filters(&self, filters: &[Filter]) -> Result<()> {
        filters
            .iter()
            .try_for_each(|filter| self.filter(&filter.expr))
    }

    pub(super) fn filter(&self, expr: &FilterExpr) -> Result<()> {
        match expr {
            FilterExpr::Expr(expr) => self.expr(expr),
            FilterExpr::And(children) | FilterExpr::Or(children) => self.filters(children),
            FilterExpr::Not(child) => self.filter(&child.expr),
            FilterExpr::Comparison { .. }
            | FilterExpr::InList { .. }
            | FilterExpr::Between { .. }
            | FilterExpr::IsNull { .. }
            | FilterExpr::IsNotNull { .. } => Ok(()),
        }
    }

    pub(super) fn projection(&self, projection: &[Projection]) -> Result<()> {
        for item in projection {
            match item {
                Projection::Computed { expr, .. } | Projection::CpComputed { expr, .. } => {
                    self.expr(expr)?
                }
                Projection::Column(_) | Projection::Star | Projection::QualifiedStar(_) => {}
            }
        }
        Ok(())
    }

    pub(super) fn sort_keys(&self, sort_keys: &[SortKey]) -> Result<()> {
        sort_keys.iter().try_for_each(|key| self.expr(&key.expr))
    }

    pub(super) fn windows(&self, windows: &[WindowSpec]) -> Result<()> {
        for window in windows {
            self.exprs(&window.args)?;
            self.exprs(&window.partition_by)?;
            self.sort_keys(&window.order_by)?;
        }
        Ok(())
    }

    pub(super) fn aggregates(&self, aggregates: &[AggregateExpr]) -> Result<()> {
        aggregates
            .iter()
            .try_for_each(|aggregate| self.exprs(&aggregate.args))
    }

    pub(super) fn assignments(&self, assignments: &[(String, SqlExpr)]) -> Result<()> {
        assignments.iter().try_for_each(|(_, expr)| self.expr(expr))
    }

    pub(super) fn exprs(&self, exprs: &[SqlExpr]) -> Result<()> {
        exprs.iter().try_for_each(|expr| self.expr(expr))
    }

    pub(super) fn expr(&self, expr: &SqlExpr) -> Result<()> {
        match first_search_function(expr, self.functions) {
            Some(name) => Err(SqlError::SearchFunctionOutsideSearch {
                name: name.to_owned(),
            }),
            None => Ok(()),
        }
    }
}
