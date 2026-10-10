// SPDX-License-Identifier: BUSL-1.1

//! What one dispatched task leaves on its sessions: the reads a transaction
//! validates at COMMIT, and the session's own committed write version.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::DistributedReadCapture;
use crate::control::server::shared::session::{
    DmlTxnCtx, ResponseReads, SessionId, record_reads_for_response,
};
use crate::types::DatabaseId;

use super::super::super::core::NodeDbPgHandler;

/// One task the loop dispatched, and the sessions it ran for.
pub(super) struct DispatchedTask<'a> {
    pub identity: &'a AuthenticatedIdentity,
    /// The client's session, which notes its own committed writes.
    pub client_session: SessionId,
    /// The statement's transaction, which validates the task's reads at
    /// COMMIT. `None` for an autocommit statement.
    pub txn: Option<&'a DmlTxnCtx<'a>>,
    pub plan: &'a PhysicalPlan,
    pub database_id: DatabaseId,
}

impl NodeDbPgHandler {
    /// Record what one dispatched task observed and wrote.
    ///
    /// A read in a transaction joins the transaction's read set, for commit
    /// conflict checks. An absent-key point read (a `NotFound` from the Data
    /// Plane) joins it too: a "not found" is a validatable phantom
    /// observation. A genuine dispatch failure records nothing.
    ///
    /// A successful write records the versions it stamped on the client's
    /// session, so a later transaction's read-set capture is floored at them
    /// (the read-your-writes floor). An autocommit write floors a later
    /// transaction's read too, so this records outside a transaction as well.
    pub(super) async fn track_dispatched_task(
        &self,
        task: DispatchedTask<'_>,
        resp: &Response,
        distributed_reads: &[DistributedReadCapture],
    ) {
        let DispatchedTask {
            identity,
            client_session,
            txn,
            plan,
            database_id,
        } = task;
        let records_read =
            resp.status == Status::Ok || resp.error_code.as_deref() == Some(&ErrorCode::NotFound);
        if records_read && let Some(txn) = txn {
            record_reads_for_response(
                &self.state,
                txn.sessions,
                txn.session_id,
                identity.tenant_id,
                ResponseReads {
                    plan,
                    read_versions: &resp.read_versions,
                    found: resp.status == Status::Ok,
                    distributed_reads,
                },
            )
            .await;
        }

        self.sessions.note_own_write_response(
            client_session,
            database_id,
            identity.tenant_id,
            plan,
            resp,
        );
    }
}
