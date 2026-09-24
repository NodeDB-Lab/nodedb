// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane dispatch seam for a system-initiated transaction.

use std::future::Future;
use std::pin::Pin;

use crate::bridge::envelope::Response;
use crate::control::server::dispatch_utils;
use crate::control::server::shared::session::TxnDataPlane;
use crate::control::state::SharedState;
use crate::types::TraceId;
use nodedb_physical::physical_task::PhysicalTask;

/// Dispatches a system transaction's commit-time tasks straight to the core
/// that owns their vShard.
///
/// The gateway must NOT be used here: commit-time tasks carry `MetaOp` plans
/// (`ResolveTxn`, `TransactionBatch`) with no named collection, so the
/// gateway's router cannot derive a route for them and falls back to vShard 0,
/// durably applying the commit batch on the wrong core.
pub(super) struct SystemTxnDataPlane<'a> {
    pub(super) state: &'a SharedState,
    /// Provenance stamped on the writes this transaction applies.
    ///
    /// A trigger or event action is itself a write source, and its writes
    /// re-enter the Event Plane. Stamping them as `Trigger` is what stops a
    /// trigger's own output from firing that same trigger again.
    pub(super) event_source: crate::event::EventSource,
}

impl TxnDataPlane for SystemTxnDataPlane<'_> {
    fn dispatch_no_wal<'a>(
        &'a self,
        task: PhysicalTask,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
        let state = self.state;
        let event_source = self.event_source;
        Box::pin(async move {
            dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
                state,
                dispatch_utils::WriteDispatch {
                    tenant_id: task.tenant_id,
                    database_id: task.database_id,
                    vshard_id: task.vshard_id,
                    plan: task.plan,
                    trace_id: TraceId::ZERO,
                    event_source,
                    txn_id: None,
                    wal_lsn: None,
                    resolved_now_ms: None,
                    minted: None,
                },
            )
            .await
        })
    }

    fn event_source(&self) -> crate::event::EventSource {
        self.event_source
    }
}
