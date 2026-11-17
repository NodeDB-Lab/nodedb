// SPDX-License-Identifier: BUSL-1.1

//! How the post-apply dispatcher treats the result of an awaited effect.

use tracing::warn;

use super::collection::ReclaimFailure;

/// Map a Register result onto the post-apply contract.
///
/// A core that did not acknowledge the Register holds a stale config: a write
/// it applies can index no text field or decode a strict row wrongly. So the
/// error stops the apply batch at this entry. The metadata applier classifies
/// it and files a report that names the collection and carries the error.
/// The applier's durable owed marker makes the re-delivered entry run the
/// Register again before the watermark passes it, after a restart too.
pub(super) fn register_outcome(result: crate::Result<()>, collection: &str) -> crate::Result<()> {
    result.map_err(|error| {
        tracing::error!(
            collection,
            error = %error,
            "catalog_entry: Register barrier failed — one or more Data Plane cores \
             did not acknowledge the schema update; the apply batch stops at this entry"
        );
        error
    })
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
