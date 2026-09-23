// SPDX-License-Identifier: BUSL-1.1

//! COMMIT / END / END TRANSACTION adapter.
//!
//! Thin pgwire shim over the protocol-neutral commit orchestrator in
//! `control/server/shared/session/commit.rs`: builds the pgwire Data-Plane
//! dispatch seam (keeping the materialize-freeze gate), drives `run_commit`,
//! and shapes the neutral [`CommitOutcome`] into a pgwire tag or error.

use std::future::Future;
use std::pin::Pin;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::bridge::envelope::{ErrorCode, Response as DpResponse};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    AbortReason, CommitOutcome, SessionId, TxnDataPlane, commit,
};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::errors::calvin_cancelled_error;

/// pgwire Data-Plane dispatch seam for the neutral transaction orchestrator.
///
/// Wraps `dispatch_task_no_wal`, preserving its materialize-freeze gate so a
/// transaction that began before a clone freeze cannot COMMIT writes during the
/// freeze window.
pub(in crate::control::server::pgwire::handler) struct PgwireTxnDp<'a> {
    pub(in crate::control::server::pgwire::handler) handler: &'a NodeDbPgHandler,
}

impl TxnDataPlane for PgwireTxnDp<'_> {
    fn dispatch_no_wal<'a>(
        &'a self,
        task: PhysicalTask,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> Pin<Box<dyn Future<Output = crate::Result<DpResponse>> + Send + 'a>> {
        Box::pin(self.handler.dispatch_task_no_wal(task, None, wal_lsn))
    }

    fn event_source(&self) -> crate::event::EventSource {
        crate::event::EventSource::User
    }
}

impl NodeDbPgHandler {
    /// Handle COMMIT / END / END TRANSACTION.
    pub(in crate::control::server::pgwire::handler) async fn handle_commit(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
    ) -> PgWireResult<Vec<Response>> {
        let dp = PgwireTxnDp { handler: self };
        match commit::run_commit(&self.sessions, session_id, identity, &self.state, &dp).await {
            CommitOutcome::Committed => Ok(vec![Response::Execution(Tag::new("COMMIT"))]),
            CommitOutcome::Aborted { reason } => Err(commit_abort_to_pgerror(&reason)),
        }
    }
}

/// Map a neutral commit abort reason to the pgwire error the pre-extraction
/// COMMIT path emitted.
fn commit_abort_to_pgerror(reason: &AbortReason) -> PgWireError {
    let (severity, code, message): (&'static str, &'static str, String) = match reason {
        AbortReason::Serialization => (
            "ERROR",
            "40001",
            "could not serialize access due to concurrent update".to_owned(),
        ),
        AbortReason::NoTransaction => (
            "ERROR",
            "25000",
            "current transaction is aborted, commands ignored until end of transaction block"
                .to_owned(),
        ),
        AbortReason::BatchRejected { code } => {
            let code = code.clone().unwrap_or(ErrorCode::RejectedPrevalidation {
                reason: "transaction commit failed".to_owned(),
            });
            error_code_to_sqlstate(&code)
        }
        AbortReason::CalvinCancelled => return calvin_cancelled_error(),
        AbortReason::CalvinTimeout => (
            "ERROR",
            "57014",
            "timed out waiting for Calvin sequencer".to_owned(),
        ),
        AbortReason::SchemaChanged { detail } => (
            "ERROR",
            "40001",
            format!("could not serialize access due to concurrent schema change: {detail}"),
        ),
        AbortReason::Dispatch(e) | AbortReason::DdlPropose(e) => error_to_sqlstate(e),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_owned(),
        code.to_owned(),
        message,
    )))
}
