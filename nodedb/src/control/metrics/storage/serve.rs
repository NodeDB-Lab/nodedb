// SPDX-License-Identifier: BUSL-1.1

//! Answer a peer's storage share request from this node's latest sample.
//!
//! The answer reads the published share and never reads the cores, so a
//! request costs one encode.

use std::time::Instant;

use nodedb_cluster::error::{ClusterError, Result as ClusterResult};
use nodedb_cluster::storage_share::{decode_request, encode_response};
use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};

use crate::control::state::SharedState;

use super::share_wire::share_to_wire;

/// Answer the `StorageShareRequest` envelope `envelope` with a serialized
/// `StorageShareResponse` envelope. Any other message type is refused.
pub fn answer_storage_share(
    state: &SharedState,
    envelope: &VShardEnvelope,
) -> ClusterResult<Vec<u8>> {
    match envelope.msg_type {
        VShardMessageType::StorageShareRequest => {}
        other => {
            return Err(ClusterError::Transport {
                detail: format!(
                    "storage share: a {other:?} envelope arrived as a request; only \
                     StorageShareRequest is served"
                ),
            });
        }
    }
    decode_request(&envelope.payload)?;
    let latest = state.storage_shares.latest();
    let response = share_to_wire(state.node_id, latest.as_deref(), Instant::now());
    let reply = VShardEnvelope::new(
        VShardMessageType::StorageShareResponse,
        envelope.target_node,
        envelope.source_node,
        envelope.vshard_id,
        encode_response(&response)?,
    );
    Ok(reply.to_bytes())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use nodedb_cluster::storage_share::{StorageShareRequest, decode_response, encode_request};
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::metrics::storage::shares::{GroupTotals, StorageShare, TenantTotals};
    use crate::wal::WalManager;

    fn state(dir: &std::path::Path) -> Arc<SharedState> {
        let wal = Arc::new(WalManager::open_for_testing(&dir.join("serve.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    fn request() -> VShardEnvelope {
        VShardEnvelope::new(
            VShardMessageType::StorageShareRequest,
            2,
            1,
            0,
            encode_request(&StorageShareRequest {}).expect("encode"),
        )
    }

    fn answer(state: &SharedState) -> nodedb_cluster::storage_share::StorageShareResponse {
        let bytes = answer_storage_share(state, &request()).expect("answer");
        let reply = VShardEnvelope::from_bytes(&bytes).expect("reply envelope");
        assert_eq!(reply.msg_type, VShardMessageType::StorageShareResponse);
        assert_eq!((reply.source_node, reply.target_node), (1, 2));
        decode_response(&reply.payload).expect("decode")
    }

    /// Before the first sample the node answers no sample, then it answers
    /// its latest share.
    #[test]
    fn the_answer_is_the_latest_share() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(dir.path());
        assert!(answer(&state).sample.is_none());

        let mut totals = TenantTotals::new();
        totals.insert((DatabaseId::new(1), TenantId::new(1)), 64);
        let mut groups = BTreeMap::new();
        groups.insert(2, GroupTotals { term: 3, totals });
        state.storage_shares.publish(Arc::new(StorageShare {
            groups,
            led_vshards: Vec::new(),
            sampled_at: Instant::now(),
        }));
        let sample = answer(&state).sample.expect("a sample");
        assert_eq!(sample.groups.len(), 1);
        assert_eq!((sample.groups[0].group_id, sample.groups[0].term), (2, 3));
        assert_eq!(sample.groups[0].totals[0].bytes, 64);
    }

    #[test]
    fn a_response_envelope_is_refused_as_a_request() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = state(dir.path());
        let envelope =
            VShardEnvelope::new(VShardMessageType::StorageShareResponse, 2, 1, 0, vec![]);
        assert!(answer_storage_share(&state, &envelope).is_err());
    }
}
