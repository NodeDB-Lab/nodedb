// SPDX-License-Identifier: BUSL-1.1

//! Storage share wire types: one node's stored bytes, per Raft group it leads.
//!
//! The payloads travel inside a [`crate::wire::VShardEnvelope`]. A
//! `StorageShareRequest` envelope asks a node for its latest sampled share.
//! The node answers a `StorageShareResponse` envelope from that sample and
//! never reads its cores per request.
//!
//! Each group carries the Raft term its leader sampled it at. A group whose
//! leader moved can appear in two answers, and the higher term wins.

use crate::error::{ClusterError, Result};

/// Ask a node for its latest stored-byte share.
#[derive(
    Debug, Clone, Default, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct StorageShareRequest {}

/// A node's latest stored-byte share.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct StorageShareResponse {
    /// The answering node.
    pub node_id: u64,
    /// The latest sample, `None` before the node's first sample.
    pub sample: Option<ShareSample>,
}

/// One sample of a node's stored bytes.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ShareSample {
    /// Age of the sample when the node answered, in milliseconds. The
    /// receiver subtracts it from its own clock, so node clocks never compare.
    pub sample_age_ms: u64,
    /// One entry per Raft group the node led at the sample.
    pub groups: Vec<GroupShare>,
}

/// The stored bytes of one Raft group, as its leader sampled them.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct GroupShare {
    pub group_id: u64,
    /// The Raft term the node led the group at.
    pub term: u64,
    /// Stored bytes per `(database, tenant)` on the group's vShards.
    pub totals: Vec<TenantBytes>,
}

/// Stored bytes of one `(database, tenant)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TenantBytes {
    pub database_id: u64,
    pub tenant_id: u64,
    pub bytes: u64,
}

macro_rules! to_bytes {
    ($msg:expr) => {
        rkyv::to_bytes::<rkyv::rancor::Error>($msg)
            .map(|b| b.to_vec())
            .map_err(|e| ClusterError::Codec {
                detail: format!("rkyv serialize: {e}"),
            })
    };
}

macro_rules! from_bytes {
    ($payload:expr, $T:ty, $name:expr) => {{
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity($payload.len());
        aligned.extend_from_slice($payload);
        rkyv::from_bytes::<$T, rkyv::rancor::Error>(&aligned).map_err(|e| ClusterError::Codec {
            detail: format!("rkyv deserialize {}: {e}", $name),
        })
    }};
}

/// Encode a request as an envelope payload.
pub fn encode_request(msg: &StorageShareRequest) -> Result<Vec<u8>> {
    to_bytes!(msg)
}

/// Decode a request envelope payload.
pub fn decode_request(payload: &[u8]) -> Result<StorageShareRequest> {
    from_bytes!(payload, StorageShareRequest, "StorageShareRequest")
}

/// Encode a response as an envelope payload.
pub fn encode_response(msg: &StorageShareResponse) -> Result<Vec<u8>> {
    to_bytes!(msg)
}

/// Decode a response envelope payload.
pub fn decode_response(payload: &[u8]) -> Result<StorageShareResponse> {
    from_bytes!(payload, StorageShareResponse, "StorageShareResponse")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{VShardEnvelope, VShardMessageType};

    fn through_envelope(msg_type: VShardMessageType, payload: Vec<u8>) -> Vec<u8> {
        let bytes = VShardEnvelope::new(msg_type, 1, 2, 0, payload).to_bytes();
        let envelope = VShardEnvelope::from_bytes(&bytes).expect("envelope decodes");
        assert_eq!(envelope.msg_type, msg_type);
        envelope.payload
    }

    #[test]
    fn a_request_survives_the_wire() {
        let payload = encode_request(&StorageShareRequest {}).expect("encode");
        let payload = through_envelope(VShardMessageType::StorageShareRequest, payload);
        assert_eq!(
            decode_request(&payload).expect("decode"),
            StorageShareRequest {}
        );
    }

    #[test]
    fn a_response_survives_the_wire() {
        let responses = [
            StorageShareResponse {
                node_id: 3,
                sample: None,
            },
            StorageShareResponse {
                node_id: 3,
                sample: Some(ShareSample {
                    sample_age_ms: 1_250,
                    groups: vec![
                        GroupShare {
                            group_id: 1,
                            term: 7,
                            totals: vec![
                                TenantBytes {
                                    database_id: 0,
                                    tenant_id: 1,
                                    bytes: 4_096,
                                },
                                TenantBytes {
                                    database_id: 1024,
                                    tenant_id: 2,
                                    bytes: u64::MAX,
                                },
                            ],
                        },
                        GroupShare {
                            group_id: 2,
                            term: 1,
                            totals: Vec::new(),
                        },
                    ],
                }),
            },
        ];
        for response in responses {
            let payload = encode_response(&response).expect("encode");
            let payload = through_envelope(VShardMessageType::StorageShareResponse, payload);
            assert_eq!(decode_response(&payload).expect("decode"), response);
        }
    }

    #[test]
    fn a_truncated_response_is_a_codec_error() {
        let payload = encode_response(&StorageShareResponse {
            node_id: 9,
            sample: Some(ShareSample {
                sample_age_ms: 1,
                groups: vec![GroupShare {
                    group_id: 1,
                    term: 1,
                    totals: Vec::new(),
                }],
            }),
        })
        .expect("encode");
        let err = decode_response(&payload[..payload.len() / 2]).expect_err("truncated");
        assert!(matches!(err, ClusterError::Codec { .. }), "{err:?}");
    }
}
