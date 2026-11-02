// SPDX-License-Identifier: BUSL-1.1

//! Errors from stream consumption.

use crate::event::cdc::offset::CdcOffset;

/// Errors from stream consumption.
#[derive(Debug)]
pub enum ConsumeError {
    StreamNotFound(String),
    GroupNotFound(String, String),
    /// Stream exists but buffer is empty (no events yet).
    BufferEmpty(String),
    /// This node holds no replica of the partition. The caller forwards the
    /// consume to `leader_node` with `consume_remote()`.
    RemotePartition {
        partition_id: u32,
        leader_node: u64,
    },
    /// Remote consume failed.
    RemoteError(String),
    /// Gateway not available (cluster transport not ready).
    NoClusterTransport,
    /// The node's routing table is not wired yet, so no partition's replicas
    /// are known.
    NoClusterRouting,
    /// Requested LIMIT cannot be represented by the SQL integer type.
    InvalidLimit(usize),
    /// Caller-provided cluster cursor vector is malformed or exceeds its bound.
    InvalidRemoteOffsets(&'static str),
    /// The consumer's cursor on `partition_id` lies below the first event the
    /// serving node holds, and no replica that holds the events answered. A
    /// snapshot install or retention dropped them. The consumer resets to
    /// `available_from`.
    OffsetOutOfRange {
        partition_id: u32,
        available_from: CdcOffset,
    },
}

impl std::fmt::Display for ConsumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamNotFound(s) => write!(f, "change stream '{s}' does not exist"),
            Self::GroupNotFound(g, s) => {
                write!(f, "consumer group '{g}' does not exist on stream '{s}'")
            }
            Self::BufferEmpty(s) => write!(f, "stream '{s}' has no buffered events"),
            Self::RemotePartition {
                partition_id,
                leader_node,
            } => {
                write!(
                    f,
                    "partition {partition_id} is on remote node {leader_node}"
                )
            }
            Self::RemoteError(e) => write!(f, "remote consume error: {e}"),
            Self::NoClusterTransport => {
                write!(f, "cluster transport not available for remote stream read")
            }
            Self::NoClusterRouting => write!(
                f,
                "cluster routing not available for a stream read: this node's cluster is not wired"
            ),
            Self::InvalidLimit(limit) => {
                write!(f, "stream LIMIT {limit} exceeds cluster wire range")
            }
            Self::InvalidRemoteOffsets(reason) => {
                write!(f, "invalid remote CDC committed offsets: {reason}")
            }
            Self::OffsetOutOfRange {
                partition_id,
                available_from,
            } => write!(
                f,
                "reset_required: partition {partition_id} holds no events before offset \
                 {available_from}; commit offset {available_from} or later for this consumer group"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_error_display() {
        let e = ConsumeError::StreamNotFound("orders".into());
        assert!(e.to_string().contains("orders"));
    }

    #[test]
    fn remote_partition_error_display() {
        let e = ConsumeError::RemotePartition {
            partition_id: 5,
            leader_node: 3,
        };
        assert!(e.to_string().contains("partition 5"));
        assert!(e.to_string().contains("node 3"));
    }

    #[test]
    fn out_of_range_display_names_the_reset_offset() {
        let e = ConsumeError::OffsetOutOfRange {
            partition_id: 4,
            available_from: CdcOffset::at(0, 101, 0),
        };
        let text = e.to_string();
        assert!(text.starts_with("reset_required"));
        assert!(text.contains("0:101:0"));
    }
}
