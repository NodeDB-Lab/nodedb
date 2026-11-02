// SPDX-License-Identifier: BUSL-1.1

//! What one dispatched task leaves on its session: the reads a transaction
//! validates at COMMIT.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::DistributedReadCapture;
use crate::control::server::shared::session::{
    DmlTxnCtx, ResponseReads, record_reads_for_response,
};

use super::super::super::core::NodeDbPgHandler;

/// One task the loop dispatched, and the transaction it ran in.
pub(super) struct DispatchedTask<'a> {
    pub identity: &'a AuthenticatedIdentity,
    /// The statement's transaction, which validates the task's reads at
    /// COMMIT. `None` for an autocommit statement.
    pub txn: Option<&'a DmlTxnCtx<'a>>,
    pub plan: &'a PhysicalPlan,
}

impl NodeDbPgHandler {
    /// Record what one dispatched task observed.
    ///
    /// A read in a transaction joins the transaction's read set, for commit
    /// conflict checks. An absent-key point read (a `NotFound` from the Data
    /// Plane) joins it too: a "not found" is a validatable phantom
    /// observation. A genuine dispatch failure records nothing.
    pub(super) async fn track_dispatched_task(
        &self,
        task: DispatchedTask<'_>,
        resp: &Response,
        distributed_reads: &[DistributedReadCapture],
    ) {
        let DispatchedTask {
            identity,
            txn,
            plan,
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
                    read_vshards: &resp.read_vshards,
                    found: resp.status == Status::Ok,
                    distributed_reads,
                },
            )
            .await;
        }
    }
}
