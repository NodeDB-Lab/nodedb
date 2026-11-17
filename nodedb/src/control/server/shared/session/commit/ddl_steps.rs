// SPDX-License-Identifier: BUSL-1.1

//! The buffered-DDL steps of a COMMIT: finalize the DDL before any buffered
//! DML dispatches, and reverse it when that dispatch fails.

use nodedb_cluster::PendingDdlObject;

use crate::control::state::SharedState;

use super::super::connection::SessionId;
use super::super::ddl_compensate;
use super::super::ddl_flush::{self, DdlCommitPlan};
use super::super::store::SessionStore;

/// Reverse whatever `begin_commit` already finalized to the catalog before a
/// dispatch failure that follows it. Called on every abort path reachable
/// after `finalize_pending` — the caller still aborts either way, since the
/// durable batch never dispatched; a compensation failure is surfaced in the
/// trace alongside the original failure rather than swallowed.
pub(super) async fn compensate_finalized_ddl(
    state: &SharedState,
    compensation: &Option<Vec<PendingDdlObject>>,
) {
    let Some(objects) = compensation else {
        return;
    };
    if let Err(error) = ddl_compensate::compensate_finalized(state, objects).await {
        tracing::error!(
            %error,
            "commit compensation failed: this transaction's finalized DDL may still be \
             cataloged despite the DML dispatch that used it having failed"
        );
    }
}

/// Reserve and finalize this connection's buffered DDL through the metadata
/// group. Called once no matter which dispatch shape follows, always after
/// the fence check appropriate to that shape and always before any buffered
/// DML dispatches — see `ddl_flush`'s module doc for why.
///
/// Returns the reserved objects to compensate if dispatch later fails, or
/// `None` when nothing was buffered. `Err` aborts the whole COMMIT before
/// anything durable happens.
pub(super) async fn begin_ddl(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
) -> crate::Result<Option<Vec<PendingDdlObject>>> {
    match ddl_flush::begin_commit(state, sessions, session_id).await? {
        DdlCommitPlan::None => Ok(None),
        DdlCommitPlan::Pending(handle) => {
            let objects = handle.objects().to_vec();
            ddl_flush::finalize_pending(state, handle).await?;
            Ok(Some(objects))
        }
    }
}
