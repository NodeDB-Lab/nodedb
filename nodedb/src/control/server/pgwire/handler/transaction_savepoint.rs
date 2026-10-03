// SPDX-License-Identifier: BUSL-1.1

//! Savepoint and deferred-offset adapters for `NodeDbPgHandler`.
//!
//! Thin pgwire shims over the protocol-neutral savepoint orchestrator
//! (`control/server/shared/session/savepoint_ops.rs`): they parse the wire
//! statement, drive the neutral op, and shape the tag / SQLSTATE. The overlay
//! marker capture/decode and the COMMIT OFFSET parsing live in the neutral
//! core.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::savepoint_ops::{
    self, DeferredOffsetCmd, SavepointError,
};
use crate::control::server::shared::session::{PendingOffsetCommit, SessionId, TransactionState};

use super::core::NodeDbPgHandler;
use super::transaction_cmds::PgwireTxnDp;

/// Map a neutral savepoint error to its pgwire error: `25P01` outside a
/// transaction, `25P02` in an aborted block, `3B001` for an unknown
/// savepoint, `XX000` for a failed overlay mark or rewind.
fn savepoint_error_to_pgerror(e: &SavepointError) -> PgWireError {
    let (code, message) = match e {
        SavepointError::NoActiveTransaction => (
            "25P01",
            "SAVEPOINT can only be used in transaction blocks".to_owned(),
        ),
        SavepointError::TransactionAborted => (
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block"
                .to_owned(),
        ),
        SavepointError::NotFound { message } => ("3B001", message.clone()),
        SavepointError::OverlayDispatch { message } => ("XX000", message.clone()),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

impl NodeDbPgHandler {
    /// Handle deferred COMMIT OFFSET inside a transaction block.
    ///
    /// Returns `Some(response)` if handled, `None` if not a deferred offset commit.
    pub(super) fn try_handle_deferred_offset(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql_trimmed: &str,
        upper: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        let cmd = match savepoint_ops::parse_deferred_offset(sql_trimmed, upper) {
            Ok(Some(cmd)) => cmd,
            Ok(None) => return None,
            Err(error) => {
                return Some(Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    error.to_string(),
                )))));
            }
        };
        if self.sessions.transaction_state(session_id) != TransactionState::InBlock {
            return None;
        }
        let tenant_id = identity.tenant_id.as_u64();
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .or(identity.default_database)?;

        match cmd {
            DeferredOffsetCmd::Single {
                stream,
                group,
                partition_id,
                offset,
            } => {
                let stream = crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name(
                    &self.state,
                    database_id,
                    tenant_id,
                    &stream,
                );
                self.sessions.defer_offset_commit(
                    session_id,
                    PendingOffsetCommit {
                        database_id,
                        tenant_id,
                        stream,
                        group,
                        partition_id,
                        offset,
                    },
                );
                Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]))
            }
            DeferredOffsetCmd::Batch { stream, group } => {
                let stream = crate::control::server::shared::ddl::neutral::consumer_group::identity::canonical_stream_name(
                    &self.state,
                    database_id,
                    tenant_id,
                    &stream,
                );
                if let Some(buffer) =
                    self.state
                        .cdc_router
                        .get_buffer(database_id, tenant_id, &stream)
                {
                    for (pid, offset) in buffer.partition_tails() {
                        self.sessions.defer_offset_commit(
                            session_id,
                            PendingOffsetCommit {
                                database_id,
                                tenant_id,
                                stream: stream.clone(),
                                group: group.clone(),
                                partition_id: pid,
                                offset,
                            },
                        );
                    }
                }
                Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]))
            }
        }
    }

    /// Handle SAVEPOINT <name>.
    pub(super) async fn handle_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().nth(1).unwrap_or("sp");
        let dp = PgwireTxnDp { handler: self };
        match savepoint_ops::run_savepoint(
            &self.sessions,
            session_id,
            identity.tenant_id,
            &dp,
            sp_name,
        )
        .await
        {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }

    /// Handle RELEASE SAVEPOINT <name>.
    pub(super) fn handle_release_savepoint(
        &self,
        session_id: SessionId,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
        match savepoint_ops::run_release_savepoint(&self.sessions, session_id, sp_name) {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("RELEASE"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }

    /// Handle ROLLBACK TO SAVEPOINT <name>.
    pub(super) async fn handle_rollback_to_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
        let dp = PgwireTxnDp { handler: self };
        match savepoint_ops::run_rollback_to_savepoint(
            &self.sessions,
            session_id,
            identity.tenant_id,
            &dp,
            sp_name,
        )
        .await
        {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }
}
