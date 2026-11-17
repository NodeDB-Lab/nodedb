// SPDX-License-Identifier: BUSL-1.1

//! The connection a descriptor lease hold belongs to.
//!
//! A statement that waits out a descriptor drain blocks its connection. A hold
//! the same connection keeps outside that statement cannot end during the
//! wait. Examples are an open transaction's buffered writes and an open
//! cursor. The drain counts such a hold, so the drain and the wait each wait
//! on the other. The statement retry reads this id to find those holds and
//! ends at once instead of waiting.
//!
//! Every connection scope gets a fresh id. A system transaction scope keeps
//! the id of the connection it runs inside, because that connection is
//! blocked on it too.

use std::sync::atomic::{AtomicU64, Ordering};

use super::conn_scope::with_scope;

/// The next id a connection scope takes. `0` is never handed out.
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

/// A fresh owner id for a new connection scope.
pub(super) fn fresh() -> u64 {
    NEXT_OWNER.fetch_add(1, Ordering::Relaxed)
}

/// The owner id of the connection running on this task, or `None` outside a
/// connection scope.
pub fn current() -> Option<u64> {
    with_scope(None, |scope| Some(scope.hold_owner))
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope::{scoped, scoped_system_txn};
    use super::*;

    #[tokio::test]
    async fn each_connection_gets_its_own_owner() {
        let first = scoped(async { current() }).await;
        let second = scoped(async { current() }).await;
        assert!(first.is_some());
        assert!(second.is_some());
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn a_system_txn_keeps_the_connection_owner() {
        scoped(async {
            let outer = current();
            let inner = scoped_system_txn(async { current() }).await;
            assert_eq!(outer, inner);
        })
        .await;
    }

    #[tokio::test]
    async fn no_owner_outside_a_connection() {
        assert_eq!(current(), None);
    }
}
