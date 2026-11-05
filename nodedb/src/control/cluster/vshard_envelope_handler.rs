// SPDX-License-Identifier: BUSL-1.1

//! The `VShardEnvelopeHandler` `RaftLoop` dispatches vShard envelopes to.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use nodedb_cluster::distributed_array::{ArrayLocalExecutor, handle_array_shard_rpc};
use nodedb_cluster::vshard_handler::{DispatchTarget, dispatch_by_type};
use nodedb_cluster::wire::VShardEnvelope;

use crate::control::metrics::storage::answer_storage_share;
use crate::control::state::SharedState;
use crate::event::cross_shard::CrossShardReceiver;

/// Build the `VShardEnvelopeHandler` closure used by `RaftLoop`.
///
/// The closure receives raw envelope bytes from the QUIC transport layer,
/// dispatches based on `msg_type`, and returns a serialized response.
///
/// `shared` answers storage share requests. It is weak, so the handler the
/// Raft loop holds never keeps the node's state alive.
pub(crate) fn build_vshard_handler(
    array_executor: Arc<dyn ArrayLocalExecutor>,
    cross_shard_receiver: Arc<CrossShardReceiver>,
    shared: Weak<SharedState>,
) -> nodedb_cluster::VShardEnvelopeHandler {
    Arc::new(move |bytes: Vec<u8>| {
        let executor = array_executor.clone();
        let receiver = Arc::clone(&cross_shard_receiver);
        let shared = Weak::clone(&shared);
        let fut: Pin<
            Box<dyn std::future::Future<Output = nodedb_cluster::error::Result<Vec<u8>>> + Send>,
        > = Box::pin(async move {
            let envelope = VShardEnvelope::from_bytes(&bytes).ok_or_else(|| {
                nodedb_cluster::error::ClusterError::Codec {
                    detail: "vshard_handler: failed to deserialize VShardEnvelope".into(),
                }
            })?;

            let target = dispatch_by_type(&envelope);
            match target {
                DispatchTarget::ArrayShard => {
                    let opcode = envelope.msg_type as u32;
                    let resp_payload = handle_array_shard_rpc(
                        opcode,
                        envelope.vshard_id,
                        &envelope.payload,
                        &executor,
                    )
                    .await?;

                    // Response opcode = request opcode + 1 for all array shard RPCs.
                    // Resolve the msg_type variant via a minimal scratch envelope parse
                    // (avoids any unsafe transmute — the `from_bytes` mapping in wire.rs
                    // is the canonical source of truth for the opcode→variant table).
                    let resp_opcode = opcode + 1;
                    let resp_msg_type = resolve_vshard_msg_type(resp_opcode)?;
                    let resp_envelope = VShardEnvelope::new(
                        resp_msg_type,
                        envelope.target_node,
                        envelope.source_node,
                        envelope.vshard_id,
                        resp_payload,
                    );
                    Ok(resp_envelope.to_bytes())
                }

                // `CrossShardEvent` (remote trigger DML) and `NotifyBroadcast`
                // (cluster-wide CDC fan-out) both land here. `handle_envelope`
                // re-parses the raw bytes and returns a fully-formed response
                // envelope — including the error-shaped one for a message type
                // that must not arrive as a REQUEST. The `*Ack` variants are such
                // a case: every sender reads its Ack as the RESPONSE on the same
                // QUIC stream, so an inbound Ack request is a protocol violation,
                // not a case to handle. Unlike the ArrayShard arm there is no
                // opcode+1 convention to apply: the receiver picks the response
                // msg_type per request type itself.
                DispatchTarget::EventPlane => Ok(receiver.handle_envelope(bytes).await),

                // A peer's storage round asks for this node's latest share.
                DispatchTarget::StorageShare => {
                    let state = shared.upgrade().ok_or_else(|| {
                        nodedb_cluster::error::ClusterError::Transport {
                            detail: "vshard_handler: the node is shutting down; no storage share"
                                .into(),
                        }
                    })?;
                    answer_storage_share(&state, &envelope)
                }

                other => Err(nodedb_cluster::error::ClusterError::Transport {
                    detail: format!(
                        "vshard_handler: no handler registered for dispatch target {other:?}"
                    ),
                }),
            }
        });
        fut
    })
}

/// Resolve a raw opcode `u32` to a `VShardMessageType` variant through
/// `VShardMessageType::from_raw`, the wire format's one opcode table.
pub(crate) fn resolve_vshard_msg_type(
    opcode: u32,
) -> nodedb_cluster::error::Result<nodedb_cluster::wire::VShardMessageType> {
    u16::try_from(opcode)
        .ok()
        .and_then(nodedb_cluster::wire::VShardMessageType::from_raw)
        .ok_or_else(|| nodedb_cluster::error::ClusterError::Codec {
            detail: format!("resolve_vshard_msg_type: unknown opcode {opcode}"),
        })
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::wire::VShardMessageType;

    use super::resolve_vshard_msg_type;

    /// Every array shard response opcode resolves, so a shard can answer
    /// each array request it serves.
    #[test]
    fn every_array_response_opcode_resolves() {
        let responses = [
            (81, VShardMessageType::ArrayShardSliceResp),
            (83, VShardMessageType::ArrayShardAggResp),
            (85, VShardMessageType::ArrayShardPutResp),
            (87, VShardMessageType::ArrayShardDeleteResp),
            (89, VShardMessageType::ArrayShardSurrogateBitmapResp),
            (91, VShardMessageType::ArrayShardElementwiseResp),
            (93, VShardMessageType::ArrayShardMaintResp),
        ];
        for (opcode, expected) in responses {
            let resolved = resolve_vshard_msg_type(opcode)
                .unwrap_or_else(|e| panic!("opcode {opcode} must resolve: {e}"));
            assert_eq!(resolved, expected);
        }
    }

    #[test]
    fn an_unknown_opcode_is_a_codec_error() {
        assert!(resolve_vshard_msg_type(94).is_err());
        assert!(resolve_vshard_msg_type(u32::from(u16::MAX) + 1).is_err());
    }
}
