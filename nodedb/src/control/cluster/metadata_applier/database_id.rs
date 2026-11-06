// SPDX-License-Identifier: BUSL-1.1

//! `DatabaseIdReserve` host-side effect.

use tracing::debug;

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// Carve the next database id at `raft_index` and persist the new hwm
    /// with the index before the watermark advances.
    ///
    /// Every node runs this in log order against the same hwm and the
    /// entry's id space, so every node issues the same id. A persist error
    /// returns `Err`: the watermark stays on this entry and Raft re-delivers it.
    pub(super) fn apply_database_id_reserve(
        &self,
        node_id: u64,
        request_id: u64,
        id_space: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let registry = &shared.database_registry;
        let Some(id) =
            registry.reserve_at_index(raft_index, id_space, self.credentials.catalog())?
        else {
            debug!(raft_index, "database_id_reserve: already folded into hwm");
            return Ok(());
        };
        if node_id == shared.node_id {
            registry.complete_request(request_id, id);
        }
        debug!(
            node_id,
            request_id,
            raft_index,
            database_id = id.as_u64(),
            "database id reserved via raft"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{MetadataApplier, MetadataEntry, encode_entry};
    use nodedb_types::DatabaseId;

    use crate::control::database::USER_DB_START;
    use crate::control::state::SharedState;

    use super::super::test_fixture::applier_with_shared_at;
    use super::MetadataCommitApplier;

    fn applier_with_shared() -> (MetadataCommitApplier, Arc<SharedState>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let (applier, state) = applier_with_shared_at(tmp.path(), "test.wal");
        (applier, state, tmp)
    }

    fn reserve(node_id: u64, request_id: u64) -> Vec<u8> {
        encode_entry(&MetadataEntry::DatabaseIdReserve {
            node_id,
            request_id,
            id_space: 0,
        })
        .expect("encode")
    }

    /// The requesting node receives the carved id, the hwm and cursor are
    /// durable, and a re-delivered entry issues nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_issues_persists_and_ignores_redelivery() {
        let (applier, state, _tmp) = applier_with_shared();
        let registry = &state.database_registry;
        let catalog = state.credentials.catalog();

        let request = registry.begin_request();
        assert_eq!(
            applier.apply(&[(4, reserve(state.node_id, request))]).await,
            4
        );
        assert_eq!(
            registry.finish_request(request),
            Some(DatabaseId::new(USER_DB_START))
        );
        assert_eq!(catalog.get_database_hwm().unwrap(), USER_DB_START);
        assert_eq!(catalog.get_database_reserve_index().unwrap(), 4);

        let again = registry.begin_request();
        assert_eq!(
            applier.apply(&[(4, reserve(state.node_id, again))]).await,
            4
        );
        assert_eq!(registry.finish_request(again), None);
        assert_eq!(catalog.get_database_hwm().unwrap(), USER_DB_START);
    }

    /// The id comes from the entry's space, with no input from this node.
    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_issues_from_the_space_the_entry_carries() {
        let (applier, state, _tmp) = applier_with_shared();
        let registry = &state.database_registry;
        let request = registry.begin_request();
        let space = crate::control::database::database_id_space(5);
        let entry = encode_entry(&MetadataEntry::DatabaseIdReserve {
            node_id: state.node_id,
            request_id: request,
            id_space: space,
        })
        .expect("encode");
        assert_eq!(applier.apply(&[(1, entry)]).await, 1);
        let id = registry.finish_request(request).expect("issued id");
        assert_eq!(id.as_u64() >> 30, space);
    }

    /// Another node's reservation advances this node's hwm, so the next
    /// local request never receives the id the other node owns.
    #[tokio::test(flavor = "multi_thread")]
    async fn peer_reservation_advances_hwm_without_completing_local_request() {
        let (applier, state, _tmp) = applier_with_shared();
        let registry = &state.database_registry;
        let peer = state.node_id + 1;

        let local = registry.begin_request();
        assert_eq!(applier.apply(&[(1, reserve(peer, local))]).await, 1);
        assert_eq!(registry.finish_request(local), None);

        let local = registry.begin_request();
        assert_eq!(
            applier.apply(&[(2, reserve(state.node_id, local))]).await,
            2
        );
        assert_eq!(
            registry.finish_request(local),
            Some(DatabaseId::new(USER_DB_START + 1))
        );
    }
}
