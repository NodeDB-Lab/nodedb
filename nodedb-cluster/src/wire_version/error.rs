//! Wire-version error types.

use thiserror::Error;

use super::types::WireVersion;

/// Errors arising from wire-version negotiation or versioned decode.
#[derive(Debug, Error)]
pub enum WireVersionError {
    /// A peer sent a message whose version field is outside the range this
    /// node supports. The connection must be closed; continuing would risk
    /// silent misinterpretation of the payload.
    #[error(
        "unsupported wire version {peer_version} from peer \
         (supported: {supported_min}..={supported_max})"
    )]
    UnsupportedVersion {
        peer_version: WireVersion,
        supported_min: WireVersion,
        supported_max: WireVersion,
    },

    /// The raw bytes could not be decoded as either a versioned envelope or
    /// a raw v1 inner type.
    #[error("wire decode failure: {0}")]
    DecodeFailure(String),

    /// `negotiate` was called with disjoint `VersionRange` sets — no
    /// protocol version is mutually acceptable.
    #[error(
        "version negotiation failed: local range {local_min}..={local_max} \
         does not overlap remote range {remote_min}..={remote_max}"
    )]
    NegotiationFailed {
        local_min: WireVersion,
        local_max: WireVersion,
        remote_min: WireVersion,
        remote_max: WireVersion,
    },
}

impl From<WireVersionError> for crate::error::ClusterError {
    fn from(e: WireVersionError) -> Self {
        crate::error::ClusterError::Codec {
            detail: e.to_string(),
        }
    }
}
