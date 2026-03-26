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
    pub vshard_id: u16,
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
    /// Cross-shard transaction forward.
    CrossShardForward = 21,
    /// GSI forward entry.
    GsiForward = 22,
    /// Edge validation request.
    EdgeValidation = 23,

    // ── Graph Algorithm BSP (Bulk Synchronous Parallel) ──
    /// Superstep barrier: coordinator tells all shards to begin iteration N.
    GraphAlgoSuperstep = 30,
    /// Boundary vertex contributions: shard sends rank contributions for
    /// vertices owned by the target shard (scatter phase).
    GraphAlgoContributions = 31,
    /// Superstep complete: shard reports local convergence delta and
    /// vertex count to coordinator (gather phase).
    GraphAlgoSuperstepAck = 32,
    /// Algorithm complete: coordinator broadcasts final signal with
    /// global convergence status.
    GraphAlgoComplete = 33,

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
}

/// Current wire protocol version.
pub const WIRE_VERSION: u16 = 1;

impl VShardEnvelope {
    pub fn new(
        msg_type: VShardMessageType,
        source_node: u64,
        target_node: u64,
        vshard_id: u16,
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
    pub fn to_bytes(&self) -> Vec<u8> {
        // Simple binary format: version(2) + msg_type(2) + source(8) + target(8)
        // + vshard(2) + payload_len(4) + payload
        let mut buf = Vec::with_capacity(26 + self.payload.len());
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
        if buf.len() < 26 {
            return None;
        }
        let version = u16::from_le_bytes(buf[0..2].try_into().ok()?);
        let msg_type_raw = u16::from_le_bytes(buf[2..4].try_into().ok()?);
        let source_node = u64::from_le_bytes(buf[4..12].try_into().ok()?);
        let target_node = u64::from_le_bytes(buf[12..20].try_into().ok()?);
        let vshard_id = u16::from_le_bytes(buf[20..22].try_into().ok()?);
        let payload_len = u32::from_le_bytes(buf[22..26].try_into().ok()?) as usize;

        if buf.len() < 26 + payload_len {
            return None;
        }
        let payload = buf[26..26 + payload_len].to_vec();

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
            21 => VShardMessageType::CrossShardForward,
            22 => VShardMessageType::GsiForward,
            23 => VShardMessageType::EdgeValidation,
            30 => VShardMessageType::GraphAlgoSuperstep,
            31 => VShardMessageType::GraphAlgoContributions,
            32 => VShardMessageType::GraphAlgoSuperstepAck,
            33 => VShardMessageType::GraphAlgoComplete,
            40 => VShardMessageType::TsScatterRequest,
            41 => VShardMessageType::TsScatterResponse,
            42 => VShardMessageType::TsRetentionCommand,
            43 => VShardMessageType::TsRetentionAck,
            44 => VShardMessageType::TsArchiveCommand,
            45 => VShardMessageType::TsArchiveAck,
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
            VShardMessageType::GraphAlgoSuperstep,
            VShardMessageType::GraphAlgoContributions,
            VShardMessageType::GraphAlgoSuperstepAck,
            VShardMessageType::GraphAlgoComplete,
            VShardMessageType::TsScatterRequest,
            VShardMessageType::TsScatterResponse,
            VShardMessageType::TsRetentionCommand,
            VShardMessageType::TsRetentionAck,
            VShardMessageType::TsArchiveCommand,
            VShardMessageType::TsArchiveAck,
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
        assert_eq!(bytes.len(), 26); // header only
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
}
