// SPDX-License-Identifier: BUSL-1.1

//! Cleanup after a failed reserve in `ddl_flush`.

use nodedb_cluster::{DescriptorId, DrainOwner, MetadataEntry};

use crate::control::metadata_proposer::MetadataRaftHandle;
use crate::control::state::SharedState;

use super::ddl_flush::propose_and_await;

/// Undo what a failed reserve left behind. Cancel the pending record of
/// `token`, then end each drain in `started_drains`.
///
/// Always propose the cancel. After an apply wait timeout the record can still
/// be pending locally. The cancel is idempotent for a token with no record and
/// needs no lease ownership. An error is logged and the rest of the cleanup
/// still runs. The caller releases the lease afterwards.
pub(super) async fn cleanup_failed_reserve(
    state: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
    started_drains: Vec<DescriptorId>,
) {
    let cancel = MetadataEntry::DdlPendingCancel { token };
    if let Err(error) = propose_and_await(state, handle, &cancel).await {
        tracing::warn!(
            token,
            %error,
            "pending DDL cancel failed after a failed reserve"
        );
    }
    end_started_drains(state, started_drains).await;
}

/// End each drain a failed reserve started. The drain has no expiry, so only
/// this propose clears it. The propose reaches the log even when its apply
/// wait passes the statement deadline. An error is logged and the next drain
/// still ends.
async fn end_started_drains(state: &SharedState, started: Vec<DescriptorId>) {
    for id in started {
        if let Err(error) =
            crate::control::lease::end_drain_async(state, id.clone(), DrainOwner::Ddl).await
        {
            tracing::warn!(
                descriptor = ?id,
                %error,
                "DDL drain end failed after a failed reserve"
            );
        }
    }
}
