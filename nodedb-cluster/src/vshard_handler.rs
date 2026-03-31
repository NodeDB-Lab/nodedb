//! Shard-side handler for incoming VShardEnvelope messages.
//!
//! When a remote node sends a VShardEnvelope via QUIC (wrapped in
//! `RaftRpc::VShardEnvelope`), the transport server routes it here.
//! This handler dispatches based on `VShardMessageType` to the
//! appropriate engine handler:
//!
//! - Graph BSP messages → graph algorithm shard handler
//! - Timeseries scatter → local scan + partial aggregate response
//! - Retention command → local retention enforcement
//! - S3 archive command → local archive execution

use crate::wire::{VShardEnvelope, VShardMessageType};

/// Result of handling a VShardEnvelope.
pub enum HandleResult {
    /// Send a response envelope back to the coordinator.
    Response(VShardEnvelope),
    /// No response needed (fire-and-forget message).
    NoResponse,
    /// Handler error.
    Error(String),
}

/// Trait for handling incoming VShardEnvelope messages on a shard.
///
/// Implemented by the main binary, which has access to the Data Plane
/// engines (timeseries scan, graph traversal, etc.).
pub trait VShardHandler: Send + Sync + 'static {
    /// Handle an incoming VShardEnvelope and optionally produce a response.
    fn handle_vshard_envelope(
        &self,
        envelope: VShardEnvelope,
    ) -> impl std::future::Future<Output = HandleResult> + Send;
}

/// Default handler that dispatches based on message type.
///
/// The main binary implements `VShardHandler` and uses this function
/// as the dispatch core, delegating to engine-specific handlers.
pub fn dispatch_by_type(envelope: &VShardEnvelope) -> DispatchTarget {
    match envelope.msg_type {
        // Graph BSP
        VShardMessageType::GraphAlgoSuperstep => DispatchTarget::GraphBsp,
        VShardMessageType::GraphAlgoContributions => DispatchTarget::GraphBsp,
        VShardMessageType::GraphAlgoSuperstepAck => DispatchTarget::GraphBsp,
        VShardMessageType::GraphAlgoComplete => DispatchTarget::GraphBsp,

        // Timeseries distributed
        VShardMessageType::TsScatterRequest => DispatchTarget::TimeseriesScan,
        VShardMessageType::TsScatterResponse => DispatchTarget::TimeseriesCoordinator,
        VShardMessageType::TsRetentionCommand => DispatchTarget::TimeseriesRetention,
        VShardMessageType::TsRetentionAck => DispatchTarget::TimeseriesCoordinator,
        VShardMessageType::TsArchiveCommand => DispatchTarget::TimeseriesArchive,
        VShardMessageType::TsArchiveAck => DispatchTarget::TimeseriesCoordinator,

        // Migration / infrastructure
        VShardMessageType::SegmentChunk
        | VShardMessageType::SegmentComplete
        | VShardMessageType::WalTail
        | VShardMessageType::RoutingUpdate
        | VShardMessageType::RoutingAck => DispatchTarget::Migration,

        VShardMessageType::GhostCreate
        | VShardMessageType::GhostDelete
        | VShardMessageType::GhostVerifyRequest
        | VShardMessageType::GhostVerifyResponse => DispatchTarget::Ghost,

        VShardMessageType::MigrationBaseCopy => DispatchTarget::Migration,
        VShardMessageType::CrossShardForward => DispatchTarget::Forward,
        VShardMessageType::GsiForward => DispatchTarget::Forward,
        VShardMessageType::EdgeValidation => DispatchTarget::GraphValidation,

        // Vector distributed search
        VShardMessageType::VectorScatterRequest => DispatchTarget::VectorSearch,
        VShardMessageType::VectorScatterResponse => DispatchTarget::VectorCoordinator,

        // Spatial distributed queries
        VShardMessageType::SpatialScatterRequest => DispatchTarget::SpatialSearch,
        VShardMessageType::SpatialScatterResponse => DispatchTarget::SpatialCoordinator,

        // Event Plane cross-shard delivery
        VShardMessageType::CrossShardEvent => DispatchTarget::EventPlane,
        VShardMessageType::CrossShardEventAck => DispatchTarget::EventPlane,
        VShardMessageType::NotifyBroadcast => DispatchTarget::EventPlane,
        VShardMessageType::NotifyBroadcastAck => DispatchTarget::EventPlane,
    }
}

/// Which engine subsystem should handle this envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchTarget {
    /// Graph BSP algorithm handler.
    GraphBsp,
    /// Graph edge validation.
    GraphValidation,
    /// Timeseries local scan (shard executes scan, returns partials).
    TimeseriesScan,
    /// Timeseries coordinator (receives shard responses).
    TimeseriesCoordinator,
    /// Timeseries retention enforcement on local shard.
    TimeseriesRetention,
    /// Timeseries S3 archive execution on local shard.
    TimeseriesArchive,
    /// Vector local k-NN search (shard executes search, returns top-K hits).
    VectorSearch,
    /// Vector coordinator (receives shard search responses).
    VectorCoordinator,
    /// Migration infrastructure.
    Migration,
    /// Ghost stub management.
    Ghost,
    /// Query/transaction forwarding.
    Forward,
    /// Spatial local R-tree search (shard executes predicate, returns matching docs).
    SpatialSearch,
    /// Spatial coordinator (receives shard search responses).
    SpatialCoordinator,
    /// Event Plane cross-shard delivery (trigger DML, CDC events).
    EventPlane,
}

/// Build a response envelope for a timeseries scatter response.
pub fn build_ts_scatter_response(
    source_node: u64,
    target_node: u64,
    vshard_id: u16,
    partials_json: &[u8],
) -> VShardEnvelope {
    VShardEnvelope::new(
        VShardMessageType::TsScatterResponse,
        source_node,
        target_node,
        vshard_id,
        partials_json.to_vec(),
    )
}

/// Build a response envelope for a retention acknowledgement.
pub fn build_ts_retention_ack(
    source_node: u64,
    target_node: u64,
    vshard_id: u16,
    result_json: &[u8],
) -> VShardEnvelope {
    VShardEnvelope::new(
        VShardMessageType::TsRetentionAck,
        source_node,
        target_node,
        vshard_id,
        result_json.to_vec(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_graph_bsp() {
        let env = VShardEnvelope::new(VShardMessageType::GraphAlgoSuperstep, 1, 2, 42, vec![]);
        assert_eq!(dispatch_by_type(&env), DispatchTarget::GraphBsp);
    }

    #[test]
    fn dispatch_ts_scatter() {
        let env = VShardEnvelope::new(VShardMessageType::TsScatterRequest, 1, 2, 42, vec![]);
        assert_eq!(dispatch_by_type(&env), DispatchTarget::TimeseriesScan);
    }

    #[test]
    fn dispatch_ts_retention() {
        let env = VShardEnvelope::new(VShardMessageType::TsRetentionCommand, 1, 2, 42, vec![]);
        assert_eq!(dispatch_by_type(&env), DispatchTarget::TimeseriesRetention);
    }

    #[test]
    fn dispatch_migration() {
        let env = VShardEnvelope::new(VShardMessageType::SegmentChunk, 1, 2, 42, vec![]);
        assert_eq!(dispatch_by_type(&env), DispatchTarget::Migration);
    }

    #[test]
    fn all_message_types_dispatched() {
        // Every VShardMessageType must map to a DispatchTarget —
        // this test ensures no new types are added without a handler.
        let all_types = [
            VShardMessageType::SegmentChunk,
            VShardMessageType::SegmentComplete,
            VShardMessageType::WalTail,
            VShardMessageType::RoutingUpdate,
            VShardMessageType::RoutingAck,
            VShardMessageType::GhostCreate,
            VShardMessageType::GhostDelete,
            VShardMessageType::GhostVerifyRequest,
            VShardMessageType::GhostVerifyResponse,
            VShardMessageType::MigrationBaseCopy,
            VShardMessageType::CrossShardForward,
            VShardMessageType::GsiForward,
            VShardMessageType::EdgeValidation,
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
            VShardMessageType::VectorScatterRequest,
            VShardMessageType::VectorScatterResponse,
            VShardMessageType::SpatialScatterRequest,
            VShardMessageType::SpatialScatterResponse,
            VShardMessageType::CrossShardEvent,
            VShardMessageType::CrossShardEventAck,
            VShardMessageType::NotifyBroadcast,
            VShardMessageType::NotifyBroadcastAck,
        ];
        for msg_type in all_types {
            let env = VShardEnvelope::new(msg_type, 1, 2, 0, vec![]);
            let _ = dispatch_by_type(&env); // Must not panic.
        }
    }
}
