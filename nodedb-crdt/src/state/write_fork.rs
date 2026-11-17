// SPDX-License-Identifier: Apache-2.0

//! Isolated native state for preparing writes under the canonical author.

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::document_cell::DocumentCell;

impl CrdtState {
    /// Create an isolated write candidate with the same peer and existing container identities.
    ///
    /// The caller must prevent concurrent source writes until it discards or publishes the candidate.
    /// Two divergent writers under one peer ID produce conflicting operation identities.
    /// Reserve the candidate's memory budget before calling this O(document) operation.
    /// Pending source operations return an error without committing them.
    pub fn fork_for_write(&self) -> Result<Self> {
        let operations = self.doc.get_pending_txn_len();
        if operations != 0 {
            return Err(CrdtError::SourceTransactionPending { operations });
        }

        let fork = self.doc.fork();
        fork.set_peer_id(self.peer_id).map_err(|error| {
            CrdtError::Loro(format!(
                "write preparation peer_id {}: {error}",
                self.peer_id
            ))
        })?;
        Ok(Self {
            doc: DocumentCell::new(fork),
            peer_id: self.peer_id,
            _single_owner: std::marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use loro::{Container, ContainerTrait, LoroMap, LoroMovableList, LoroValue, ValueOrContainer};

    use crate::error::CrdtError;

    use super::CrdtState;

    fn source() -> CrdtState {
        let state = CrdtState::new(11).unwrap();
        state
            .upsert("pages", "p1", &[("title", "Draft".into())])
            .unwrap();
        state
            .list_insert_fields("pages", "p1", "blocks", 0, &[("id".into(), "b1".into())])
            .unwrap();
        state
    }

    fn row(state: &CrdtState) -> LoroMap {
        let Some(ValueOrContainer::Container(Container::Map(row))) =
            state.doc.get_map("pages").get("p1")
        else {
            panic!("expected a row map");
        };
        row
    }

    fn blocks(state: &CrdtState) -> LoroMovableList {
        let Some(ValueOrContainer::Container(Container::MovableList(blocks))) =
            row(state).get("blocks")
        else {
            panic!("expected a movable block list");
        };
        blocks
    }

    fn block(state: &CrdtState) -> LoroMap {
        let Some(ValueOrContainer::Container(Container::Map(block))) = blocks(state).get(0) else {
            panic!("expected a block map");
        };
        block
    }

    #[test]
    fn write_fork_preserves_author_counters_and_container_identities() {
        let source = source();
        let version = source.oplog_version_vector();
        let candidate = source.fork_for_write().unwrap();
        assert_eq!(candidate.peer_id(), source.peer_id());
        assert_eq!(candidate.doc.peer_id(), source.peer_id());
        assert_eq!(candidate.oplog_version_vector(), version);
        assert_eq!(
            candidate.doc.state_frontiers(),
            source.doc.state_frontiers()
        );
        assert_eq!(
            candidate.doc.get_map("pages").id(),
            source.doc.get_map("pages").id()
        );
        assert_eq!(row(&candidate).id(), row(&source).id());
        assert_eq!(blocks(&candidate).id(), blocks(&source).id());
        assert_eq!(block(&candidate).id(), block(&source).id());

        let next = source.local_op_counter();
        candidate
            .set_fields("pages", "p1", &[("title", "Prepared".into())])
            .unwrap();
        assert_eq!(candidate.local_op_counter(), next + 1);
        let after = candidate.oplog_version_vector();
        assert_eq!(after.len(), version.len());
        assert_eq!(after.get(&source.peer_id()), Some(&(next + 1)));
    }

    #[test]
    fn discarded_candidate_preserves_source_state_and_next_operation() {
        let source = source();
        let version = source.oplog_version_vector();
        let state_frontier = source.doc.state_frontiers();
        let oplog_frontier = source.doc.oplog_frontiers();
        let before = source.read_row_deep("pages", "p1");
        let delta = source
            .export_updates_since(&loro::VersionVector::default())
            .unwrap();
        let pending = source.doc.get_pending_txn_len();
        let next = source.local_op_counter();
        {
            let candidate = source.fork_for_write().unwrap();
            candidate
                .set_fields("pages", "p1", &[("title", "Discarded".into())])
                .unwrap();
            candidate
                .list_insert_fields("pages", "p1", "blocks", 1, &[("id".into(), "b2".into())])
                .unwrap();
            assert_ne!(candidate.read_row_deep("pages", "p1"), before);
        }
        assert_eq!(source.read_row_deep("pages", "p1"), before);
        assert_eq!(source.oplog_version_vector(), version);
        assert_eq!(source.doc.state_frontiers(), state_frontier);
        assert_eq!(source.doc.oplog_frontiers(), oplog_frontier);
        assert_eq!(source.doc.get_pending_txn_len(), pending);
        assert_eq!(source.local_op_counter(), next);
        assert_eq!(
            source
                .export_updates_since(&loro::VersionVector::default())
                .unwrap(),
            delta
        );

        source
            .set_fields("pages", "p1", &[("title", "Accepted".into())])
            .unwrap();
        assert_eq!(source.local_op_counter(), next + 1);
        assert_eq!(
            source.read_field("pages", "p1", "title"),
            Some("Accepted".into())
        );
    }

    #[test]
    fn delayed_peer_edits_merge_after_discard_or_candidate_publication() {
        let source = source();
        let version = source.oplog_version_vector();
        let mut peer = source.fork_for_write().unwrap();
        peer.set_peer_id(22).unwrap();
        block(&peer).insert("remote", true).unwrap();
        peer.doc.commit();
        let remote_delta = peer.export_updates_since(&version).unwrap();

        let discarded = source.fork_for_write().unwrap();
        discarded
            .set_fields("pages", "p1", &[("title", "Discarded".into())])
            .unwrap();
        drop(discarded);

        let candidate = source.fork_for_write().unwrap();
        let replay = source.fork_for_write().unwrap();
        let start = candidate.local_op_counter();
        candidate
            .set_fields("pages", "p1", &[("title", "Published".into())])
            .unwrap();
        candidate
            .list_insert_fields("pages", "p1", "blocks", 1, &[("id".into(), "b2".into())])
            .unwrap();
        let delta = candidate
            .export_local_range(start, candidate.local_op_counter())
            .unwrap();

        source.import(&remote_delta).unwrap();
        assert_eq!(
            block(&source).get("remote").unwrap().get_deep_value(),
            LoroValue::Bool(true)
        );
        assert_eq!(source.list_length("pages", "p1", "blocks").unwrap(), 1);
        let published = candidate;
        published.import(&remote_delta).unwrap();
        assert_eq!(block(&published).id(), block(&source).id());
        assert_eq!(
            block(&published).get("remote").unwrap().get_deep_value(),
            LoroValue::Bool(true)
        );
        assert_eq!(published.list_length("pages", "p1", "blocks").unwrap(), 2);
        assert_eq!(
            published.read_field("pages", "p1", "title"),
            Some("Published".into())
        );

        replay.import(&delta).unwrap();
        replay.import(&remote_delta).unwrap();
        assert_eq!(
            replay.read_row_deep("pages", "p1"),
            published.read_row_deep("pages", "p1")
        );
        assert_eq!(
            replay.oplog_version_vector(),
            published.oplog_version_vector()
        );
        assert_eq!(
            replay.doc.state_frontiers(),
            published.doc.state_frontiers()
        );
    }

    #[test]
    fn write_fork_preserves_pending_source_transactions_on_error() {
        let source = source();
        let committed_frontier = source.doc.oplog_frontiers();
        row(&source).insert("pending", "Uncommitted").unwrap();
        // Loro advances both frontiers when writing, before transaction commit.
        let oplog_frontier = source.doc.oplog_frontiers();
        let state_frontier = source.doc.state_frontiers();
        assert_ne!(oplog_frontier, committed_frontier);
        let pending = source.doc.get_pending_txn_len();
        let version = source.oplog_version_vector();
        let value = source.read_row_deep("pages", "p1");
        assert_ne!(pending, 0);

        assert!(matches!(
            source.fork_for_write(),
            Err(CrdtError::SourceTransactionPending { operations }) if operations == pending
        ));
        assert_eq!(source.doc.get_pending_txn_len(), pending);
        assert_eq!(source.oplog_version_vector(), version);
        assert_eq!(source.read_row_deep("pages", "p1"), value);
        assert_eq!(source.doc.oplog_frontiers(), oplog_frontier);
        assert_eq!(source.doc.state_frontiers(), state_frontier);
    }
}
