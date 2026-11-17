// SPDX-License-Identifier: BUSL-1.1

//! The commit HLC of WAL records a metadata entry's apply appends.
//!
//! The metadata applier applies each entry inside one task, and awaits every
//! effect the entry has (a collection purge's tombstone, for one) inside that
//! task. While it applies a stamped entry, every record the task appends
//! without a commit HLC of its own carries the entry's stamp. A cluster
//! restore then keeps such a record exactly when it keeps the entry.
//!
//! The stamp is task-local, so it follows the apply across await points and
//! worker threads. Code outside any task reads no stamp.

use std::future::Future;

tokio::task_local! {
    static EFFECT_STAMP: Option<u64>;
}

/// Run `apply` with `stamp` as the commit HLC of every record its task
/// appends without one. A nested scope holds its own stamp until it ends.
pub async fn with_effect_stamp<F: Future>(stamp: Option<u64>, apply: F) -> F::Output {
    EFFECT_STAMP.scope(stamp, apply).await
}

/// The stamp of the metadata entry the current task applies, if any.
pub(crate) fn effect_stamp() -> Option<u64> {
    EFFECT_STAMP.try_with(|stamp| *stamp).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_stamp_holds_inside_the_scope_only() {
        assert_eq!(effect_stamp(), None);
        let inner = with_effect_stamp(Some(9), async {
            with_effect_stamp(Some(4), async { effect_stamp() }).await;
            tokio::task::yield_now().await;
            effect_stamp()
        })
        .await;
        assert_eq!(inner, Some(9), "a nested scope restores the outer stamp");
        assert_eq!(effect_stamp(), None);
    }
}
