// SPDX-License-Identifier: BUSL-1.1

//! The statement's tail after every task ran: the folded `RETURNING` rows as
//! one result set, then the statement's one command tag.

use pgwire::api::portal::Format;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::server::response_shape::types::{ShapedRows, StatementTag};
use crate::control::server::shared::session::SessionId;

use super::super::super::super::command_tag::push_folded_tag;
use super::super::super::core::NodeDbPgHandler;
use super::super::super::shape_encode;

/// What the dispatch loop accumulated for the statement's tail.
pub(super) struct StatementTail<'a> {
    /// The statement's `RETURNING` rows, folded across every task.
    pub(super) returning_rows: Option<ShapedRows>,
    /// The statement's command tag, folded across every write task.
    pub(super) statement_tag: StatementTag,
    pub(super) result_formats: &'a Format,
    pub(super) session_id: SessionId,
}

impl NodeDbPgHandler {
    /// Emit the statement's tail onto `responses`.
    pub(super) fn finish_statement(
        &self,
        responses: &mut Vec<Response>,
        tail: StatementTail<'_>,
    ) -> PgWireResult<()> {
        let StatementTail {
            returning_rows,
            statement_tag,
            result_formats,
            session_id,
        } = tail;

        // The statement's RETURNING rows, as one result set.
        if let Some(shaped) = returning_rows {
            let (response, notice) = shape_encode::shaped_query_response(shaped, result_formats);
            if let Some(n) = notice {
                self.sessions.push_notice(session_id, n);
            }
            responses.push(response);
        }

        // The statement's one command tag, after its rows. A statement never
        // carries both RETURNING rows and a count-bearing tag: `RETURNING`
        // classifies as `ReturningRows`, which folds rows and no count.
        push_folded_tag(responses, statement_tag.finish());

        Ok(())
    }
}
