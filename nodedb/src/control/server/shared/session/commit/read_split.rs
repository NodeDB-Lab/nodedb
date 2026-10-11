// SPDX-License-Identifier: BUSL-1.1

//! Which reads of a COMMIT enlist Calvin participants, and which this node
//! checks itself before dispatch.
//!
//! A homed read is one vShard of a cross-shard graph or array read. A
//! whole-graph read homes on every vShard. On a node with no cluster routing,
//! every vShard is local, so this node checks such reads against its own
//! cores and they enlist no participant. A single-node transaction that makes
//! a cross-shard read and writes one vShard then keeps its single-vShard
//! commit. In a cluster, every read enlists its vShard, so a participant
//! checks it inside the Calvin barrier.

use crate::control::state::SharedState;

use super::super::connection::SessionId;
use super::super::outcome::CommitOutcome;
use super::super::read_set::ReadSetEntry;
use super::super::store::SessionStore;
use super::read_validation::stale_read_abort;

/// A COMMIT's read set, split by who validates each read.
pub(super) struct SplitReads<'a> {
    all: &'a [ReadSetEntry],
    /// `(enlisted, checked_here)` when this node checks some reads itself.
    local: Option<(Vec<ReadSetEntry>, Vec<ReadSetEntry>)>,
}

impl<'a> SplitReads<'a> {
    pub(super) fn of(state: &SharedState, all: &'a [ReadSetEntry]) -> Self {
        let local = (state.cluster_routing.is_none() && all.iter().any(|read| read.home.is_some()))
            .then(|| {
                let (here, enlisted): (Vec<_>, Vec<_>) =
                    all.iter().cloned().partition(|read| read.home.is_some());
                (enlisted, here)
            });
        Self { all, local }
    }

    /// The reads that enlist their vShard as a participant.
    pub(super) fn enlisted(&self) -> &[ReadSetEntry] {
        match &self.local {
            Some((enlisted, _)) => enlisted,
            None => self.all,
        }
    }

    /// The reads this node checks before dispatch.
    fn checked_here(&self) -> &[ReadSetEntry] {
        match &self.local {
            Some((_, here)) => here,
            None => &[],
        }
    }

    /// Abort the transaction when a read this node checks itself is no
    /// longer current. Releases the reservations and rolls the session back
    /// before returning the outcome. `None` when every such read holds, or
    /// there is none.
    pub(super) async fn stale_here(
        &self,
        state: &SharedState,
        sessions: &SessionStore,
        session_id: SessionId,
        written_collections: &std::collections::HashSet<String>,
    ) -> Option<CommitOutcome> {
        let here = self.checked_here();
        if here.is_empty() {
            return None;
        }
        stale_read_abort(state, sessions, session_id, here, written_collections).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadOrigin};
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn read(collection: &str, home: Option<u32>) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Graph,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: collection.to_owned(),
            key: ReadKey::Predicate,
            read_version: nodedb_types::WriteVersion::ZERO,
            origin: ReadOrigin::Session,
            home: home.map(VShardId::new),
        }
    }

    fn single_node() -> (std::sync::Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, dir)
    }

    #[test]
    fn a_single_node_checks_homed_reads_itself() {
        let (state, _dir) = single_node();
        let reads = [read("docs", None), read("", Some(3)), read("", Some(9))];
        let split = SplitReads::of(&state, &reads);
        assert_eq!(split.enlisted(), &reads[..1]);
        assert_eq!(split.checked_here(), &reads[1..]);
    }

    #[test]
    fn a_read_set_with_no_homed_read_enlists_every_read() {
        let (state, _dir) = single_node();
        let reads = [read("docs", None)];
        let split = SplitReads::of(&state, &reads);
        assert_eq!(split.enlisted(), &reads[..]);
        assert!(split.checked_here().is_empty());
    }
}
