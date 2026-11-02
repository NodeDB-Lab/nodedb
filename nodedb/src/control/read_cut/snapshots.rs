// SPDX-License-Identifier: BUSL-1.1

//! The transaction snapshots open on this node.
//!
//! A transaction block opens a snapshot before it takes its cut
//! ([`ActiveSnapshots::open`]): the open holds the wall clock at that
//! instant, so a report sent between the open and the cut never passes the
//! cut the block then takes. [`SnapshotGuard::pin`] sets the cut. Dropping
//! the guard closes the snapshot.
//!
//! Every change publishes the oldest open snapshot to the node's
//! [`SnapshotBoard`], whose report every health ping carries, so the cleanup
//! horizon is the minimum over the live nodes
//! (`SnapshotBoard::horizon`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use nodedb_cluster::SnapshotBoard;

#[derive(Default)]
struct Inner {
    next_id: u64,
    /// Open snapshots by `(cut, id)`.
    open: BTreeMap<(i64, u64), ()>,
}

/// Every transaction snapshot open on this node.
pub struct ActiveSnapshots {
    inner: Mutex<Inner>,
    board: Arc<SnapshotBoard>,
}

/// One open snapshot. Dropping it closes the snapshot.
#[must_use = "dropping the guard closes the snapshot"]
pub struct SnapshotGuard {
    registry: Arc<ActiveSnapshots>,
    key: (i64, u64),
}

impl std::fmt::Debug for SnapshotGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotGuard")
            .field("cut", &self.key.0)
            .finish()
    }
}

impl ActiveSnapshots {
    /// A registry that publishes to `board`.
    pub fn new(board: Arc<SnapshotBoard>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            board,
        })
    }

    /// The board this registry publishes to.
    pub fn board(&self) -> &Arc<SnapshotBoard> {
        &self.board
    }

    /// Open a snapshot that holds the wall clock now, before its cut is
    /// taken. Pin the cut with [`SnapshotGuard::pin`].
    pub fn open(self: &Arc<Self>) -> SnapshotGuard {
        self.register(nodedb_types::wall_now_ns_i64())
    }

    /// Open a snapshot at `cut`.
    pub fn register(self: &Arc<Self>, cut: i64) -> SnapshotGuard {
        let key = {
            let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let id = inner.next_id;
            inner.next_id = inner.next_id.wrapping_add(1);
            inner.open.insert((cut, id), ());
            self.publish(&inner);
            (cut, id)
        };
        SnapshotGuard {
            registry: Arc::clone(self),
            key,
        }
    }

    /// The oldest open snapshot on this node, `None` when none is open.
    pub fn oldest(&self) -> Option<i64> {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.open.keys().next().map(|(cut, _)| *cut)
    }

    fn publish(&self, inner: &Inner) {
        self.board
            .set_local_oldest(inner.open.keys().next().map(|(cut, _)| *cut));
    }

    fn repin(&self, from: (i64, u64), cut: i64) -> (i64, u64) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.open.remove(&from);
        let key = (cut, from.1);
        inner.open.insert(key, ());
        self.publish(&inner);
        key
    }

    fn close(&self, key: (i64, u64)) {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.open.remove(&key);
        self.publish(&inner);
    }
}

impl SnapshotGuard {
    /// Pin the snapshot at the cut the block took.
    pub fn pin(&mut self, cut: i64) {
        self.key = self.registry.repin(self.key, cut);
    }

    /// The cut the snapshot holds.
    pub fn cut(&self) -> i64 {
        self.key.0
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.registry.close(self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_oldest_open_snapshot_holds_until_it_closes() {
        let board = Arc::new(SnapshotBoard::new());
        let registry = ActiveSnapshots::new(Arc::clone(&board));
        assert_eq!(registry.oldest(), None);
        let older = registry.register(100);
        let newer = registry.register(200);
        assert_eq!(registry.oldest(), Some(100));
        assert_eq!(board.local_oldest(), Some(100), "the board sees it");
        drop(older);
        assert_eq!(registry.oldest(), Some(200));
        drop(newer);
        assert_eq!(registry.oldest(), None);
        assert_eq!(board.local_oldest(), None);
    }

    #[test]
    fn two_snapshots_at_one_cut_close_apart() {
        let registry = ActiveSnapshots::new(Arc::new(SnapshotBoard::new()));
        let first = registry.register(7);
        let second = registry.register(7);
        drop(first);
        assert_eq!(registry.oldest(), Some(7));
        drop(second);
        assert_eq!(registry.oldest(), None);
    }

    /// An open snapshot holds the wall clock until its cut is pinned.
    #[test]
    fn an_open_snapshot_holds_its_open_instant_until_pinned() {
        let registry = ActiveSnapshots::new(Arc::new(SnapshotBoard::new()));
        let before = nodedb_types::wall_now_ns_i64();
        let mut guard = registry.open();
        let held = registry.oldest().expect("open");
        assert!(held >= before);
        guard.pin(held + 1_000);
        assert_eq!(registry.oldest(), Some(held + 1_000));
        assert_eq!(guard.cut(), held + 1_000);
    }
}
