// SPDX-License-Identifier: BUSL-1.1

//! Transport-agnostic vShard envelope.
//!
//! "The on-wire format for vShard migration (segment files, WAL tail,
//! routing metadata) MUST be identical regardless of whether the transport
//! is RDMA or QUIC/TCP. The transport layer is a dumb pipe; serialization
//! logic MUST NOT branch on transport type."
//!
//! This module defines the canonical wire format for all vShard-related
//! messages. Both RDMA and QUIC paths serialize/deserialize using this
//! format — no transport-specific branches.

/// Envelope wrapping all vShard wire messages.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VShardEnvelope {
    /// Protocol version for forward compatibility.
    pub version: u16,
    /// Message type discriminant.
    pub msg_type: VShardMessageType,
    /// Source node ID.
    pub source_node: u64,
    /// Target node ID.
    pub target_node: u64,
    /// vShard being referenced.
    pub vshard_id: u32,
    /// Opaque payload (type-dependent).
    pub payload: Vec<u8>,
}

/// Message types for vShard wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u16)]
pub enum VShardMessageType {
    /// Phase 1: Segment file chunk during base copy.
    SegmentChunk = 1,
    /// Phase 1: Segment transfer complete marker.
    SegmentComplete = 2,
    /// Phase 2: WAL tail entries for catch-up.
    WalTail = 3,
    /// Phase 3: Routing table update (atomic cut-over).
    RoutingUpdate = 4,
    /// Routing table acknowledgement.
    RoutingAck = 5,
    /// Ghost stub creation notification.
    GhostCreate = 10,
    /// Ghost stub deletion notification.
    GhostDelete = 11,
    /// Anti-entropy sweep query.
    GhostVerifyRequest = 12,
    /// Anti-entropy sweep response.
    GhostVerifyResponse = 13,
    /// Migration base-copy segment data.
    MigrationBaseCopy = 20,
    /// GSI forward entry.
    GsiForward = 22,
    /// Edge validation request.
    EdgeValidation = 23,

    // ── Timeseries Distributed Aggregation ──
    /// Scatter: coordinator sends aggregation query to a shard.
    TsScatterRequest = 40,
    /// Gather: shard responds with partial aggregates.
    TsScatterResponse = 41,
    /// Coordinator broadcasts retention command to all shards.
    TsRetentionCommand = 42,
    /// Shard acknowledges retention execution.
    TsRetentionAck = 43,
    /// S3 archival command: coordinator tells shard to archive partitions.
    TsArchiveCommand = 44,
    /// S3 archival acknowledgement.
    TsArchiveAck = 45,

    // ── Distributed Vector Search ──
    /// Scatter: coordinator sends k-NN query to a shard.
    VectorScatterRequest = 50,
    /// Gather: shard responds with local top-K hits.
    VectorScatterResponse = 51,

    // ── Compass: coarse-code routing ──
    /// Phase 1 request: coordinator asks a shard for its coarse routing
    /// descriptor (learned coarse codes, centroid summary, or equivalent).
    /// The shard responds with `VectorCoarseRouteResponse` before the
    /// coordinator selects the shard subset for the fine search phase.
    VectorCoarseRouteRequest = 52,
    /// Phase 1 response: shard returns its coarse routing descriptor so
    /// the coordinator can decide whether to include it in phase 2.
    VectorCoarseRouteResponse = 53,

    // ── SPIRE: build-time centroid exchange ──
    /// Build-time request: a shard sends its IVF centroid table to a peer
    /// so the peer can build cross-shard centroid knowledge.
    /// Sent shard-to-shard without coordinator involvement.
    VectorBuildExchangeRequest = 54,
    /// Build-time response: receiving shard acknowledges and optionally
    /// echoes its own centroid summary back to the sender.
    VectorBuildExchangeResponse = 55,

    // ── CoTra-RDMA: one-sided read support ──
    /// Registration request: a shard asks a peer to expose a named memory
    /// region (e.g. a pinned HNSW graph segment) for one-sided reads.
    /// The peer responds with `VectorMemRegionInfo` containing the address
    /// and rkey, or indicates the region is unavailable.
    VectorMemRegionRequest = 56,
    /// Registration response: peer returns address/rkey for the requested
    /// memory region, or `available = false` when not supported.
    VectorMemRegionResponse = 57,

    // ── Distributed Spatial Queries ──
    /// Scatter: coordinator sends spatial predicate query to a shard.
    SpatialScatterRequest = 60,
    /// Gather: shard responds with matching document IDs.
    SpatialScatterResponse = 61,

    // ── Event Plane Cross-Shard Delivery ──
    /// Cross-shard event write request (trigger DML, CDC, etc.).
    CrossShardEvent = 70,
    /// Acknowledgement for a cross-shard event write.
    CrossShardEventAck = 71,
    /// Broadcast NOTIFY message to all peers (LISTEN/NOTIFY cluster-wide).
    NotifyBroadcast = 72,
    /// Acknowledgement for a NOTIFY broadcast.
    NotifyBroadcastAck = 73,

    // ── Distributed Array (Hilbert-sharded sparse arrays) ──
    /// Scatter: coordinator sends a coord-range slice query to a shard.
    ArrayShardSliceReq = 80,
    /// Gather: shard responds with matching row bytes.
    ArrayShardSliceResp = 81,
    /// Scatter: coordinator sends an aggregate query to a shard.
    ArrayShardAggReq = 82,
    /// Gather: shard responds with partial aggregate(s).
    ArrayShardAggResp = 83,
    /// Coordinator forwards a cell write batch to the owning shard.
    ArrayShardPutReq = 84,
    /// Shard acknowledges a cell write batch.
    ArrayShardPutResp = 85,
    /// Coordinator forwards a coord-based delete to the owning shard.
    ArrayShardDeleteReq = 86,
    /// Shard acknowledges a coord-based delete.
    ArrayShardDeleteResp = 87,
    /// Scatter: coordinator requests a surrogate bitmap scan from a shard.
    ArrayShardSurrogateBitmapReq = 88,
    /// Gather: shard returns the surrogate bitmap for matching cells.
    ArrayShardSurrogateBitmapResp = 89,
}

/// Current wire protocol version.
///
/// v2 widens `vshard_id` u16→u32 in the binary frame, increasing min header
/// size from 26 to 28 bytes.
///
/// v3 adds the `txn_id` field to `ExecuteRequest` (cross-node in-transaction
/// read-your-own-writes). `ExecuteRequest` is an rkyv fixed-layout message, so
/// the added field changes its archived layout: a v2 node cannot decode a v3
/// frame and vice versa. The matching `WireVersion::CURRENT` bump makes a v2
/// node reject a v3-stamped envelope outright rather than silently misdecode —
/// so a mixed v2/v3 cluster fails loudly and nodes must be upgraded together.
pub const WIRE_VERSION: u16 = 3;

impl VShardEnvelope {
    pub fn new(
        msg_type: VShardMessageType,
        source_node: u64,
        target_node: u64,
        vshard_id: u32,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            version: WIRE_VERSION,
            msg_type,
            source_node,
            target_node,
            vshard_id,
            payload,
        }
    }

    /// Serialize to bytes (transport-agnostic).
    ///
    /// Binary format (v2, all little-endian):
    ///   version(2) + msg_type(2) + source(8) + target(8) + vshard(4) + payload_len(4) + payload
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(28 + self.payload.len());
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&(self.msg_type as u16).to_le_bytes());
        buf.extend_from_slice(&self.source_node.to_le_bytes());
        buf.extend_from_slice(&self.target_node.to_le_bytes());
        buf.extend_from_slice(&self.vshard_id.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 28 {
            return None;
        }
        let version = u16::from_le_bytes(buf[0..2].try_into().ok()?);
        let msg_type_raw = u16::from_le_bytes(buf[2..4].try_into().ok()?);
        let source_node = u64::from_le_bytes(buf[4..12].try_into().ok()?);
        let target_node = u64::from_le_bytes(buf[12..20].try_into().ok()?);
        let vshard_id = u32::from_le_bytes(buf[20..24].try_into().ok()?);
        let payload_len = u32::from_le_bytes(buf[24..28].try_into().ok()?) as usize;

        if buf.len() < 28 + payload_len {
            return None;
        }
        let payload = buf[28..28 + payload_len].to_vec();

        let msg_type = match msg_type_raw {
            1 => VShardMessageType::SegmentChunk,
            2 => VShardMessageType::SegmentComplete,
            3 => VShardMessageType::WalTail,
            4 => VShardMessageType::RoutingUpdate,
            5 => VShardMessageType::RoutingAck,
            10 => VShardMessageType::GhostCreate,
            11 => VShardMessageType::GhostDelete,
            12 => VShardMessageType::GhostVerifyRequest,
            13 => VShardMessageType::GhostVerifyResponse,
            20 => VShardMessageType::MigrationBaseCopy,
            22 => VShardMessageType::GsiForward,
            23 => VShardMessageType::EdgeValidation,
            // 30-33 retired (GraphAlgoSuperstep/Contributions/SuperstepAck/Complete)
            40 => VShardMessageType::TsScatterRequest,
            41 => VShardMessageType::TsScatterResponse,
            42 => VShardMessageType::TsRetentionCommand,
            43 => VShardMessageType::TsRetentionAck,
            44 => VShardMessageType::TsArchiveCommand,
            45 => VShardMessageType::TsArchiveAck,
            50 => VShardMessageType::VectorScatterRequest,
            51 => VShardMessageType::VectorScatterResponse,
            52 => VShardMessageType::VectorCoarseRouteRequest,
            53 => VShardMessageType::VectorCoarseRouteResponse,
            54 => VShardMessageType::VectorBuildExchangeRequest,
            55 => VShardMessageType::VectorBuildExchangeResponse,
            56 => VShardMessageType::VectorMemRegionRequest,
            57 => VShardMessageType::VectorMemRegionResponse,
            60 => VShardMessageType::SpatialScatterRequest,
            61 => VShardMessageType::SpatialScatterResponse,
            70 => VShardMessageType::CrossShardEvent,
            71 => VShardMessageType::CrossShardEventAck,
            72 => VShardMessageType::NotifyBroadcast,
            73 => VShardMessageType::NotifyBroadcastAck,
            80 => VShardMessageType::ArrayShardSliceReq,
            81 => VShardMessageType::ArrayShardSliceResp,
            82 => VShardMessageType::ArrayShardAggReq,
            83 => VShardMessageType::ArrayShardAggResp,
            84 => VShardMessageType::ArrayShardPutReq,
            85 => VShardMessageType::ArrayShardPutResp,
            86 => VShardMessageType::ArrayShardDeleteReq,
            87 => VShardMessageType::ArrayShardDeleteResp,
            88 => VShardMessageType::ArrayShardSurrogateBitmapReq,
            89 => VShardMessageType::ArrayShardSurrogateBitmapResp,
            _ => return None,
        };

        Some(Self {
            version,
            msg_type,
            source_node,
            target_node,
            vshard_id,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_version_is_3() {
        assert_eq!(
            WIRE_VERSION, 3,
            "v3 added ExecuteRequest.txn_id for cross-node in-transaction RYOW"
        );
    }

    #[test]
    fn envelope_roundtrip() {
        let env = VShardEnvelope::new(
            VShardMessageType::SegmentChunk,
            1,
            2,
            42,
            b"segment data".to_vec(),
        );
        let bytes = env.to_bytes();
        let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn all_message_types_roundtrip() {
        let types = [
            VShardMessageType::SegmentChunk,
            VShardMessageType::SegmentComplete,
            VShardMessageType::WalTail,
            VShardMessageType::RoutingUpdate,
            VShardMessageType::RoutingAck,
            VShardMessageType::GhostCreate,
            VShardMessageType::GhostDelete,
            VShardMessageType::GhostVerifyRequest,
            VShardMessageType::GhostVerifyResponse,
            VShardMessageType::TsScatterRequest,
            VShardMessageType::TsScatterResponse,
            VShardMessageType::TsRetentionCommand,
            VShardMessageType::TsRetentionAck,
            VShardMessageType::TsArchiveCommand,
            VShardMessageType::TsArchiveAck,
            VShardMessageType::VectorScatterRequest,
            VShardMessageType::VectorScatterResponse,
            VShardMessageType::VectorCoarseRouteRequest,
            VShardMessageType::VectorCoarseRouteResponse,
            VShardMessageType::VectorBuildExchangeRequest,
            VShardMessageType::VectorBuildExchangeResponse,
            VShardMessageType::VectorMemRegionRequest,
            VShardMessageType::VectorMemRegionResponse,
            VShardMessageType::SpatialScatterRequest,
            VShardMessageType::SpatialScatterResponse,
            VShardMessageType::CrossShardEvent,
            VShardMessageType::CrossShardEventAck,
            VShardMessageType::NotifyBroadcast,
            VShardMessageType::NotifyBroadcastAck,
            VShardMessageType::ArrayShardSliceReq,
            VShardMessageType::ArrayShardSliceResp,
            VShardMessageType::ArrayShardAggReq,
            VShardMessageType::ArrayShardAggResp,
            VShardMessageType::ArrayShardPutReq,
            VShardMessageType::ArrayShardPutResp,
            VShardMessageType::ArrayShardDeleteReq,
            VShardMessageType::ArrayShardDeleteResp,
            VShardMessageType::ArrayShardSurrogateBitmapReq,
            VShardMessageType::ArrayShardSurrogateBitmapResp,
        ];

        for msg_type in types {
            let env = VShardEnvelope::new(msg_type, 10, 20, 100, vec![1, 2, 3]);
            let bytes = env.to_bytes();
            let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.msg_type, msg_type);
        }
    }

    #[test]
    fn truncated_buffer_returns_none() {
        let env = VShardEnvelope::new(VShardMessageType::WalTail, 1, 2, 0, vec![0; 100]);
        let bytes = env.to_bytes();
        // Truncate payload.
        assert!(VShardEnvelope::from_bytes(&bytes[..50]).is_none());
        // Truncate header.
        assert!(VShardEnvelope::from_bytes(&bytes[..10]).is_none());
    }

    #[test]
    fn empty_payload() {
        let env = VShardEnvelope::new(VShardMessageType::RoutingAck, 5, 6, 999, vec![]);
        let bytes = env.to_bytes();
        assert_eq!(bytes.len(), 28); // header only (v2: vshard_id widened u16→u32)
        let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn unknown_message_type_returns_none() {
        let mut env =
            VShardEnvelope::new(VShardMessageType::SegmentChunk, 1, 2, 0, vec![]).to_bytes();
        // Corrupt msg_type to unknown value.
        env[2] = 0xFF;
        env[3] = 0xFF;
        assert!(VShardEnvelope::from_bytes(&env).is_none());
    }

    #[test]
    fn wire_format_is_transport_agnostic() {
        // The same bytes work whether sent over RDMA or QUIC.
        // This test documents the invariant: no transport-specific branching.
        let env = VShardEnvelope::new(VShardMessageType::SegmentChunk, 1, 2, 42, b"data".to_vec());

        let rdma_bytes = env.to_bytes();
        let quic_bytes = env.to_bytes();
        assert_eq!(
            rdma_bytes, quic_bytes,
            "wire format must be transport-agnostic"
        );
    }

    #[test]
    fn large_vshard_id_roundtrip() {
        // vshard_id is now u32; ensure values above old u16::MAX round-trip
        // without truncation.
        let env = VShardEnvelope::new(
            VShardMessageType::WalTail,
            10,
            20,
            0x0001_FFFF,
            b"payload".to_vec(),
        );
        let bytes = env.to_bytes();
        let decoded = VShardEnvelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.vshard_id, 0x0001_FFFFu32);
    }

    #[test]
    fn truncated_below_28_returns_none() {
        // With v2 header = 28 bytes, a 27-byte buffer must be rejected.
        let env = VShardEnvelope::new(VShardMessageType::RoutingAck, 1, 2, 0, vec![]);
        let bytes = env.to_bytes();
        assert_eq!(bytes.len(), 28);
        assert!(VShardEnvelope::from_bytes(&bytes[..27]).is_none());
    }
}
