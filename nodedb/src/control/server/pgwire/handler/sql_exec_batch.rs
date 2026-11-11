// SPDX-License-Identifier: BUSL-1.1

//! Statement loop over a SQL string, and the simple-query answer to a failed statement.
//!
//! A simple query of several statements stops at the first error. The client reads
//! the responses of the earlier statements, then the ErrorResponse of the failed one,
//! as PostgreSQL does. Statements after the failed one do not run.

use pgwire::api::portal::Format;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::core::NodeDbPgHandler;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::control::server::shared::session::audit_context::AuditScope;
use crate::control::server::shared::sql_split::split_sql_statements;

impl NodeDbPgHandler {
    /// Execute a SQL query: session state → identity → DDL check → quota → plan → perms → dispatch.
    ///
    /// Handles multi-statement queries (e.g. psql heredoc sends all statements in one message).
    /// Splits at top-level semicolons before dispatching so that `parts[2]` is never polluted
    /// with trailing `;` characters.
    ///
    /// `requested` is the client's result-format request: `TEXT_RESULTS`
    /// for the simple-query protocol, the portal's formats for an
    /// extended-query Execute. Typed DDL and planned results honour it.
    ///
    /// An error returns as `Err` and drops the responses of earlier statements.
    /// The extended-query Execute and the `EXECUTE name` paths answer this way.
    pub(super) async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
        requested: &Format,
    ) -> PgWireResult<Vec<Response>> {
        let (all, outcome) = self
            .execute_statements(identity, session_id, sql, requested)
            .await;
        outcome.map(|()| all)
    }

    /// [`Self::execute_sql`] for a simple-query message.
    ///
    /// An error at statement k answers the responses of statements 1 to k-1, then the error.
    /// See [`answer_after_error`] for the cases that return the error alone.
    pub(super) async fn execute_simple_query(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
        requested: &Format,
    ) -> PgWireResult<Vec<Response>> {
        let (all, outcome) = self
            .execute_statements(identity, session_id, sql, requested)
            .await;
        answer_after_error(all, outcome)
    }

    /// Run each statement of `sql` in order and stop at the first error.
    ///
    /// Returns the responses of the statements that ran, and the error of the
    /// statement that stopped the loop.
    async fn execute_statements(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql: &str,
        requested: &Format,
    ) -> (Vec<Response>, PgWireResult<()>) {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return (vec![Response::EmptyQuery], Ok(()));
        }
        let mut all = Vec::new();
        for stmt in statements {
            // Each statement's DDL proposals carry that statement's own text.
            let _audit = AuditScope::statement(identity, &stmt);
            if let Some(binding) = self.sessions.binding(session_id) {
                binding.set_statement_text(&stmt);
            }
            match self
                .execute_single_sql(identity, session_id, &stmt, requested)
                .await
            {
                Ok(mut resp) => all.append(&mut resp),
                Err(error) => return (all, Err(error)),
            }
        }
        (all, Ok(()))
    }
}

/// Shape the simple-query answer from the responses that ran and the loop outcome.
///
/// The error returns alone as `Err` in these cases:
/// - no earlier response: a one-statement query, or a failed first statement.
/// - an earlier response starts COPY FROM STDIN: pgwire waits for copy data after it.
/// - the error ends the connection: see [`ends_connection`].
///
/// Otherwise the error follows the earlier responses as `Response::Error`.
fn answer_after_error(
    mut all: Vec<Response>,
    outcome: PgWireResult<()>,
) -> PgWireResult<Vec<Response>> {
    let error = match outcome {
        Ok(()) => return Ok(all),
        Err(error) => error,
    };
    if all.is_empty() || all.iter().any(starts_copy_in) {
        return Err(error);
    }
    // pgwire converts a returned `Err` with this same `From`.
    // The ErrorResponse carries the same severity, SQLSTATE and message on both paths.
    let info = ErrorInfo::from(error);
    if ends_connection(&info) {
        return Err(PgWireError::UserError(Box::new(info)));
    }
    all.push(Response::Error(Box::new(info)));
    Ok(all)
}

/// A response after which pgwire waits for copy data instead of `ReadyForQuery`.
fn starts_copy_in(response: &Response) -> bool {
    matches!(response, Response::CopyIn(_) | Response::CopyBoth(_))
}

/// An error that ends the connection: FATAL or PANIC severity, class 57P, or class 08.
fn ends_connection(info: &ErrorInfo) -> bool {
    matches!(info.severity.as_str(), "FATAL" | "PANIC")
        || info.code.starts_with("57P")
        || info.code.starts_with("08")
}

#[cfg(test)]
mod tests {
    use pgwire::api::results::Tag;

    use super::*;

    fn user_error(severity: &str, code: &str) -> PgWireError {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            severity.to_owned(),
            code.to_owned(),
            "statement error".to_owned(),
        )))
    }

    fn insert_tag() -> Response {
        Response::Execution(Tag::new("INSERT").with_oid(0).with_rows(1))
    }

    fn error_code(result: PgWireResult<Vec<Response>>) -> String {
        match result {
            Err(PgWireError::UserError(info)) => info.code,
            Err(other) => panic!("expected a user error, got {other}"),
            Ok(_) => panic!("expected an error, got responses"),
        }
    }

    #[test]
    fn success_returns_every_response() {
        let answered = answer_after_error(vec![insert_tag(), insert_tag()], Ok(()));
        assert!(matches!(answered.as_deref(), Ok([_, _])));
    }

    #[test]
    fn error_with_no_earlier_response_returns_the_error() {
        let answered = answer_after_error(Vec::new(), Err(user_error("ERROR", "42P01")));
        assert_eq!(error_code(answered), "42P01");
    }

    #[test]
    fn error_after_a_response_follows_that_response() {
        let answered = answer_after_error(vec![insert_tag()], Err(user_error("ERROR", "42P01")));
        let Ok(responses) = answered else {
            panic!("expected the earlier response and the error");
        };
        assert_eq!(responses.len(), 2);
        assert!(matches!(responses[0], Response::Execution(_)));
        let Response::Error(info) = &responses[1] else {
            panic!("the last response must be the error");
        };
        assert_eq!(
            (
                info.severity.as_str(),
                info.code.as_str(),
                info.message.as_str()
            ),
            ("ERROR", "42P01", "statement error")
        );
    }

    #[test]
    fn connection_error_after_a_response_returns_the_error() {
        for (severity, code) in [("FATAL", "28000"), ("ERROR", "57P01"), ("ERROR", "08003")] {
            let answered = answer_after_error(vec![insert_tag()], Err(user_error(severity, code)));
            assert_eq!(error_code(answered), code);
        }
    }

    #[test]
    fn query_canceled_after_a_response_follows_that_response() {
        let answered = answer_after_error(vec![insert_tag()], Err(PgWireError::QueryCanceled));
        let Ok(responses) = answered else {
            panic!("a canceled statement is a statement error");
        };
        let Some(Response::Error(info)) = responses.last() else {
            panic!("the last response must be the error");
        };
        assert_eq!(info.code, "57014");
    }
}
