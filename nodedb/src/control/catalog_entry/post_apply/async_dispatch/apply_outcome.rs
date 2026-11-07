// SPDX-License-Identifier: BUSL-1.1

//! How the post-apply dispatcher treats the result of an awaited effect.

use tracing::warn;

use super::collection::ReclaimFailure;

/// Log a Register that a Data Plane core did not acknowledge. The metadata
/// applier runs the post-apply lane, and no client waits on it. The batch
/// advances: the catalog row is durable, and boot seeds every core's config
/// from it.
pub(super) fn register_outcome(result: crate::Result<()>, collection: &str) {
    if let Err(error) = result {
        tracing::error!(
            collection,
            error = %error,
            "catalog_entry: Register barrier failed — one or more Data Plane cores \
             did not acknowledge the schema update; this node may serve stale schema"
        );
    }
}

/// Map a reclaim result onto the post-apply contract.
///
/// A queued durable retry is owned by the pending-reclaim worker and the boot
/// drain, so it counts as done. Anything else is `Err`, which stops the apply
/// batch at this entry.
pub(super) fn reclaim_outcome(result: Result<(), ReclaimFailure>, what: &str) -> crate::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(failure) if failure.retry_queued => {
            warn!(
                error = %failure.error,
                "{what} post-apply: reclaim failed on this node; the pending-reclaim worker \
                 owns the retry"
            );
            Ok(())
        }
        Err(failure) => Err(failure.error),
    }
}
