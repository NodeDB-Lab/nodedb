// SPDX-License-Identifier: BUSL-1.1

//! The session streams whose abandon proposal failed.
//!
//! The session proposes an abandon once. When that proposal fails, the
//! stream joins this queue, and the abandoner retries it while the stream is
//! open on this node. The stream closes once its proposer's term ends, so
//! the term bounds the retries.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::Notify;

use crate::control::wal_replication::encode::RedoEntryTarget;
use crate::wal::RedoStreamId;

use super::store::RedoChunkStore;

/// One queued abandon whose stream is open on this node.
#[derive(Debug, Clone, Copy)]
pub struct DueAbandon {
    pub stream: RedoStreamId,
    /// Where the abandon entry goes.
    pub target: RedoEntryTarget,
    /// The data group whose log carries the stream.
    pub group_id: u64,
    /// The Raft term of the stream's first chunk entry.
    pub term: u64,
    /// The stream's declared byte length.
    pub len: u64,
}

impl RedoChunkStore {
    /// Queue the abandon of `stream` for the abandoner, and wake it.
    pub fn queue_abandon(&self, stream: RedoStreamId, target: RedoEntryTarget) {
        self.lock().abandons.insert(stream, target);
        self.abandon_wake.notify_one();
    }

    /// The queued abandons whose stream is open. Drops every queued abandon
    /// whose stream closed.
    pub fn due_abandons(&self) -> Vec<DueAbandon> {
        let mut state = self.lock();
        let state = &mut *state;
        let streams = &state.streams;
        state
            .abandons
            .retain(|stream, _| streams.contains_key(stream));
        state
            .abandons
            .iter()
            .filter_map(|(stream, target)| {
                streams.get(stream).map(|open| DueAbandon {
                    stream: *stream,
                    target: *target,
                    group_id: open.group_id,
                    term: open.term,
                    len: open.len,
                })
            })
            .collect()
    }

    /// Remove `stream` from the queue: its abandon applied on this node.
    pub fn settle_abandon(&self, stream: &RedoStreamId) {
        self.lock().abandons.remove(stream);
    }

    /// Wakes the abandoner when an abandon is queued.
    pub fn abandon_wake(&self) -> Arc<Notify> {
        Arc::clone(&self.abandon_wake)
    }

    /// Claim the one abandoner task of this store. `false` when one runs.
    pub fn claim_abandoner(&self) -> bool {
        !self.abandoner.swap(true, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{chunk, session, wal};
    use super::super::store::{RedoChunkLimits, RedoChunkStore};
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn target() -> RedoEntryTarget {
        RedoEntryTarget {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(2),
        }
    }

    #[tokio::test]
    async fn a_queued_abandon_is_due_while_its_stream_is_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(
            wal(&dir),
            RedoChunkLimits {
                max_entry_bytes: 64 * 1024,
                max_open_bytes: 1 << 20,
            },
        );
        store
            .apply_chunk(chunk(session(1), 0, 4, b"ab"))
            .await
            .expect("chunk");
        store.queue_abandon(session(1), target());
        // A stream that never opened here is not due.
        store.queue_abandon(session(2), target());
        let due = store.due_abandons();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].stream, session(1));
        assert_eq!((due[0].group_id, due[0].term, due[0].len), (1, 3, 4));
        // A later term of the group closes the stream, and its abandon with it.
        store.end_terms_before(1, 4);
        assert!(store.due_abandons().is_empty());
        assert!(store.lock().abandons.is_empty());
        assert!(store.claim_abandoner());
        assert!(!store.claim_abandoner());
    }
}
