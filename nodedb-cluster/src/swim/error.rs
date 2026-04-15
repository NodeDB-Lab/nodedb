//! Typed error variants for the SWIM subsystem.
//!
//! `SwimError` is the single error type returned by every public function
//! in `nodedb_cluster::swim`. It is wired into the cluster-wide
//! [`ClusterError`] enum via a `From` impl in `crate::error`, which in turn
//! bridges to `nodedb_types::NodeDbError` at the public API boundary.

use thiserror::Error;

use nodedb_types::NodeId;

use super::incarnation::Incarnation;
use super::member::MemberState;

/// Errors produced by the SWIM failure detector and membership layer.
#[derive(Debug, Error)]
pub enum SwimError {
    /// A message or update referenced a node id not present in the
    /// membership list. This is non-fatal — the detector will request a
    /// full sync from the sender.
    #[error("swim: unknown member {node_id}")]
    UnknownMember { node_id: NodeId },

    /// Received update carries an incarnation strictly older than the
    /// locally recorded value, so the update is refuted.
    #[error("swim: stale incarnation for {node_id}: received {received:?} <= local {local:?}")]
    StaleIncarnation {
        node_id: NodeId,
        received: Incarnation,
        local: Incarnation,
    },

    /// Received a `Suspect` update targeting the local node. The failure
    /// detector must bump its own incarnation and broadcast an `Alive`
    /// refutation. Callers treat this as a signal, not a fatal error.
    #[error("swim: local node suspected at incarnation {incarnation:?}")]
    SelfSuspected { incarnation: Incarnation },

    /// A state transition violated the SWIM state machine (e.g. attempting
    /// to move a `Left` member back to `Alive`). Always a bug.
    #[error("swim: invalid state transition {from:?} -> {to:?}")]
    InvalidTransition { from: MemberState, to: MemberState },

    /// Configuration validation failed. Returned by [`super::SwimConfig::validate`].
    #[error("swim: invalid config field {field}: {reason}")]
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },

    /// zerompk failed to serialize a `SwimMessage`. In practice this is
    /// infallible for the current message schema — the variant exists so
    /// future additions to the wire format cannot silently panic.
    #[error("swim: encode failure: {detail}")]
    Encode { detail: String },

    /// zerompk failed to parse incoming bytes as a `SwimMessage`. Common
    /// causes: truncated datagram, version skew, random UDP noise.
    #[error("swim: decode failure: {detail}")]
    Decode { detail: String },
}

impl From<SwimError> for crate::error::ClusterError {
    fn from(err: SwimError) -> Self {
        crate::error::ClusterError::Transport {
            detail: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_contains_context() {
        let err = SwimError::StaleIncarnation {
            node_id: NodeId::new("n1"),
            received: Incarnation::new(3),
            local: Incarnation::new(5),
        };
        let msg = err.to_string();
        assert!(msg.contains("n1"));
        assert!(msg.contains('3'));
        assert!(msg.contains('5'));
    }

    #[test]
    fn invalid_config_display() {
        let err = SwimError::InvalidConfig {
            field: "probe_timeout",
            reason: "must be strictly less than probe_interval",
        };
        assert!(err.to_string().contains("probe_timeout"));
    }

    #[test]
    fn bridges_to_cluster_error() {
        let err: crate::error::ClusterError = SwimError::UnknownMember {
            node_id: NodeId::new("n42"),
        }
        .into();
        assert!(matches!(err, crate::error::ClusterError::Transport { .. }));
    }
}
