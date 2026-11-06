// SPDX-License-Identifier: BUSL-1.1

//! The snapshot a transaction block reads at.
//!
//! A block takes its snapshot at its first statement, as PostgreSQL takes a
//! REPEATABLE READ or SERIALIZABLE snapshot, not at `BEGIN`. The snapshot is
//! one read cut ([`crate::control::read_cut::statement_cap`]): an ordinal
//! every write committed before the statement sits at or below, and the
//! Calvin cut marker that fixes it. Every read the block plans afterwards
//! reads at that ordinal, with the block's own staged writes merged on top
//! (`SystemTimeScope::TxnSnapshot`). COMMIT, ROLLBACK, and a session reset
//! clear it.

use nodedb_types::SystemTimeScope;

use crate::control::read_cut::{
    SnapshotGuard, StatementCap, check_snapshot_age, open_txn_snapshot,
};
use crate::control::state::SharedState;

use super::connection::SessionId;
use super::state::TransactionState;
use super::store::SessionStore;

/// The read cut of one transaction block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnSnapshotCut {
    /// The ordinal every read of the block reads at.
    pub ordinal: i64,
    /// The Calvin cut marker whose cut caps `ordinal`, `0` on a node without
    /// cluster routing. Every serving node waits for it before it reads.
    pub cut_marker: u64,
}

impl TxnSnapshotCut {
    /// The scope a read of the block that names no system time reads at.
    pub fn scope(self) -> SystemTimeScope {
        SystemTimeScope::TxnSnapshot {
            ordinal: self.ordinal,
            cut_marker: self.cut_marker,
        }
    }

    /// The cap an explicit `AS OF SYSTEM TIME` inside the block pins under:
    /// a time past the snapshot reads at the snapshot.
    pub(crate) fn statement_cap(self) -> StatementCap {
        StatementCap {
            cap: self.ordinal,
            marker: self.cut_marker,
        }
    }

    fn of_cap(cap: StatementCap) -> Self {
        Self {
            ordinal: cap.cap,
            cut_marker: cap.marker,
        }
    }
}

/// The snapshot a block holds, with the registration that keeps it in the
/// node's active-snapshot registry until the block ends.
#[derive(Debug)]
pub struct HeldTxnSnapshot {
    pub cut: TxnSnapshotCut,
    /// `None` only for a cut a test installs by hand.
    pub registration: Option<SnapshotGuard>,
}

/// The snapshot the statement about to plan on `session_id` reads at.
///
/// Outside an open transaction block this is `None`: the statement reads the
/// current state. Inside one, the block's first statement takes the read cut
/// and installs it, and every later statement reads the installed one. A
/// statement whose block snapshot passed the age limit fails with `40001`.
pub async fn ensure_txn_snapshot(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
) -> crate::Result<Option<TxnSnapshotCut>> {
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        return Ok(None);
    }
    if let Some(snapshot) = sessions.tx_snapshot(session_id) {
        check_snapshot_age(state, snapshot.ordinal)?;
        return Ok(Some(snapshot));
    }
    let database = sessions
        .get_current_database(session_id)
        .unwrap_or(nodedb_types::DatabaseId::DEFAULT);
    let (cap, registration) = open_txn_snapshot(state, database).await?;
    Ok(sessions.install_tx_snapshot(session_id, TxnSnapshotCut::of_cap(cap), Some(registration)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared_state(dir: &tempfile::TempDir) -> std::sync::Arc<SharedState> {
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("state")
    }

    /// The block takes its snapshot at its first statement, keeps it for
    /// every later statement, and drops it at COMMIT and at ROLLBACK.
    #[tokio::test]
    async fn the_snapshot_is_taken_at_the_first_statement_and_cleared_at_the_end() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = shared_state(&dir);
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5200".parse().expect("addr");
        store.ensure_session(addr);
        let id = SessionId::from(&addr);

        assert_eq!(
            ensure_txn_snapshot(&store, id, &state)
                .await
                .expect("autocommit"),
            None,
            "an autocommit statement takes no snapshot"
        );

        store.begin(addr).expect("begin");
        assert_eq!(store.tx_snapshot(addr), None, "BEGIN takes no snapshot");
        let first = ensure_txn_snapshot(&store, id, &state)
            .await
            .expect("first statement")
            .expect("a block statement reads a snapshot");
        assert_eq!(store.tx_snapshot(addr), Some(first));
        // A write after the snapshot stamps above it.
        let later_write = state.wal.ordinal_clock().next_ordinal();
        assert!(later_write > first.ordinal);
        let second = ensure_txn_snapshot(&store, id, &state)
            .await
            .expect("second statement");
        assert_eq!(second, Some(first), "every statement reads one snapshot");
        assert_eq!(
            first.scope(),
            SystemTimeScope::TxnSnapshot {
                ordinal: first.ordinal,
                cut_marker: first.cut_marker,
            }
        );

        store.commit(addr).expect("commit");
        assert_eq!(store.tx_snapshot(addr), None, "COMMIT drops the snapshot");

        store.begin(addr).expect("begin");
        let next = ensure_txn_snapshot(&store, id, &state)
            .await
            .expect("next block")
            .expect("a new block reads a snapshot");
        assert!(
            next.ordinal > first.ordinal,
            "a new block takes a new snapshot"
        );
        store.rollback(addr).expect("rollback");
        assert_eq!(store.tx_snapshot(addr), None, "ROLLBACK drops the snapshot");
    }

    /// A snapshot installs only inside an open block, and the first install
    /// wins.
    #[test]
    fn only_an_open_block_installs_a_snapshot_once() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5201".parse().expect("addr");
        store.ensure_session(addr);
        let cut = TxnSnapshotCut {
            ordinal: 7,
            cut_marker: 0,
        };
        assert_eq!(store.install_tx_snapshot(addr, cut, None), None);
        store.begin(addr).expect("begin");
        assert_eq!(store.install_tx_snapshot(addr, cut, None), Some(cut));
        let other = TxnSnapshotCut {
            ordinal: 9,
            cut_marker: 0,
        };
        assert_eq!(store.install_tx_snapshot(addr, other, None), Some(cut));
        assert_eq!(cut.statement_cap(), StatementCap { cap: 7, marker: 0 });
    }
}
