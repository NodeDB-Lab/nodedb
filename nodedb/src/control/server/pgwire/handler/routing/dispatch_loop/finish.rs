// SPDX-License-Identifier: BUSL-1.1

//! The statement's tail after every task ran: the folded `RETURNING` rows as
//! one result set, then the set-operation merge of the deferred payloads,
//! then the statement's one command tag.

use std::sync::Arc;

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::PgWireResult;

use nodedb_physical::physical_task::PostSetOp;

use crate::control::sequence::{SequenceAccess, SessionSequenceAccess, SessionSequenceValues};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::{ShapedRows, StatementTag};
use crate::control::server::shared::session::SessionId;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::command_tag::push_folded_tag;
use super::super::super::core::NodeDbPgHandler;
use super::super::super::shape_encode;
use super::super::set_ops;

/// What the dispatch loop accumulated for the statement's tail.
pub(super) struct StatementTail<'a> {
    /// The statement's `RETURNING` rows, folded across every task.
    pub(super) returning_rows: Option<ShapedRows>,
    /// The statement's command tag, folded across every write task.
    pub(super) statement_tag: StatementTag,
    /// Per-branch payloads deferred for a set-operation merge.
    pub(super) dedup_payloads: Vec<Vec<u8>>,
    pub(super) dedup_set_op: PostSetOp,
    pub(super) projection: Option<&'a OutputSchema>,
    pub(super) result_formats: &'a [FieldFormat],
    /// Redaction over the union of the branches' sources, present only when
    /// the statement carries a set operation.
    pub(super) set_op_redaction: Option<QueryRedaction>,
    /// This connection's `currval` map, for the projection's Control-Plane
    /// computed columns.
    pub(super) session_sequences: Option<Arc<SessionSequenceValues>>,
    /// The statement's database, from its first task; `None` for an empty
    /// task list, which also defers no payload.
    pub(super) statement_database_id: Option<DatabaseId>,
    pub(super) tenant_id: TenantId,
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
            dedup_payloads,
            dedup_set_op,
            projection,
            result_formats,
            set_op_redaction,
            session_sequences,
            statement_database_id,
            tenant_id,
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

        // Set operations: merge sub-query payloads.
        if !dedup_payloads.is_empty() {
            let sequences = statement_database_id.map(|database_id| {
                SessionSequenceAccess::for_session(
                    &self.state,
                    session_sequences,
                    database_id,
                    tenant_id,
                )
            });
            let (response, notice) = set_ops::apply_set_ops(
                &dedup_payloads,
                dedup_set_op,
                projection,
                result_formats,
                set_op_redaction
                    .as_ref()
                    .map(|r| r.ctx(&self.state.redaction)),
                sequences.as_ref().map(|s| s as &dyn SequenceAccess),
            )?;
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
