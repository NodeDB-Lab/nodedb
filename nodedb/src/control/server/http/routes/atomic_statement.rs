// SPDX-License-Identifier: BUSL-1.1

//! The shared atomic routes for the HTTP query routes: `/v1/query`,
//! `/v1/query/stream` and WebSocket RPC.
//!
//! Each route calls [`route_http_statement`] after admission. A statement
//! the shared router takes runs there, through the same implicit
//! transaction, implicit-edge gate and Calvin commit native and pgwire use,
//! and answers as JSON rows. Every other statement comes back for the
//! route's own per-task loop.

use crate::control::server::response_shape::cell::row_to_wire_json;
use crate::control::server::response_shape::types::FoldedTag;
use crate::control::server::shared::session::DetachedTxnScope;
use crate::control::server::shared::statement_exec::{
    AtomicRoute, AtomicStatement, StatementAnswer, StatementExec, calvin_answer,
    route_atomic_statement,
};

/// What the shared router did with a statement.
pub(super) enum RoutedStatement {
    /// An atomic route ran the statement. Its answer as JSON rows.
    Answered(Vec<serde_json::Value>),
    /// No atomic route applies. The route runs its per-task loop.
    PerTask(AtomicStatement),
}

/// Route `statement` through the shared atomic routes.
///
/// These routes carry no session: no transaction block is open, and the
/// cross-shard mode is the default, `Strict`. An implicit transaction runs
/// on a private session of its own.
pub(super) async fn route_http_statement(
    exec: &StatementExec<'_>,
    statement: AtomicStatement,
) -> crate::Result<RoutedStatement> {
    let detached = DetachedTxnScope::new();
    let session = detached.ctx();
    let answer = match route_atomic_statement(exec, statement, &session).await {
        Ok(AtomicRoute::Implicit(answer)) => Ok(answer),
        Ok(AtomicRoute::Calvin(applied)) => calvin_answer(exec, applied),
        Ok(AtomicRoute::PerTask(statement)) => return Ok(RoutedStatement::PerTask(statement)),
        Err(error) => Err(error),
    };
    let answer = answer.map_err(|error| error.into_error(exec.tenant_id()))?;
    Ok(RoutedStatement::Answered(answer_json_rows(answer)))
}

/// A statement's answer as JSON rows, each keyed by its cell keys as
/// `shape_http_payload` keys a row. A write that answers no rows answers one
/// `{"affected": n}` row: the shape a single write's count takes on these
/// routes. A derived task (a balance move, an implicit edge) adds no count.
fn answer_json_rows(answer: StatementAnswer) -> Vec<serde_json::Value> {
    let mut rows: Vec<serde_json::Value> = answer
        .rows
        .iter()
        .flat_map(|shaped| {
            shaped
                .rows
                .iter()
                .map(|row| serde_json::Value::Object(row_to_wire_json(row)))
        })
        .collect();
    if rows.is_empty()
        && let Some(FoldedTag::Dml(outcome)) = answer.tag
    {
        rows.push(serde_json::json!({ "affected": outcome.affected }));
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::types::DmlOutcome;

    fn answer(tag: Option<FoldedTag>) -> StatementAnswer {
        StatementAnswer {
            rows: Vec::new(),
            warnings: Vec::new(),
            tag,
            last_lsn: 0,
        }
    }

    #[test]
    fn a_counted_write_answers_one_affected_row() {
        let rows = answer_json_rows(answer(Some(FoldedTag::Dml(DmlOutcome {
            verb: "INSERT",
            affected: 2,
        }))));
        assert_eq!(rows, vec![serde_json::json!({ "affected": 2 })]);
    }

    #[test]
    fn an_opaque_statement_answers_no_rows() {
        assert!(answer_json_rows(answer(Some(FoldedTag::Opaque))).is_empty());
        assert!(answer_json_rows(answer(None)).is_empty());
    }
}
