// SPDX-License-Identifier: Apache-2.0

//! Typed errors for the array engine.
//!
//! [`ArrayError`] is the crate-internal error enum. Every variant maps
//! into the public [`NodeDbError::array`] constructor at the API
//! boundary via the `From` impl at the bottom of this file.

use nodedb_types::NodeDbError;
use thiserror::Error;

use crate::types::cell_value::NarrowError;

/// Crate-internal result alias.
pub type ArrayResult<T> = std::result::Result<T, ArrayError>;

/// Crate-internal error enum.
///
/// Domain errors carry the offending array name where it is known; the
/// `array` field becomes the `array` slot on the public
/// [`NodeDbError::array`] details.
#[derive(Debug, Error)]
pub enum ArrayError {
    #[error("schema validation failed for '{array}': {detail}")]
    InvalidSchema { array: String, detail: String },

    #[error("dimension '{dim}' rejected on '{array}': {detail}")]
    InvalidDim {
        array: String,
        dim: String,
        detail: String,
    },

    #[error("attribute '{attr}' rejected on '{array}': {detail}")]
    InvalidAttr {
        array: String,
        attr: String,
        detail: String,
    },

    #[error("tile-extent vector rejected on '{array}': {detail}")]
    InvalidTileExtents { array: String, detail: String },

    /// An attr's nodata, scale, offset or units does not fit its type.
    #[error("attribute '{attr}' metadata rejected on '{array}': {detail}")]
    InvalidAttrMeta {
        array: String,
        attr: String,
        detail: String,
    },

    /// The schema's content fields failed to encode.
    #[error("schema of '{array}' failed to encode: {detail}")]
    SchemaEncode { array: String, detail: String },

    /// An exact aggregate holds more distinct values than its memory budget
    /// allows. SQLSTATE `54000`.
    #[error("aggregate exceeds its memory budget: {detail}")]
    AggregateBudgetExceeded { detail: String },

    /// A georef names a missing or ill-typed dim, or carries a degenerate
    /// transform.
    #[error("georef rejected on '{array}': {detail}")]
    InvalidGeoref { array: String, detail: String },

    /// A zone with an SRID cannot be framed over the array's dims.
    #[error(transparent)]
    ZoneFrame(#[from] crate::query::zone_frame::ZoneFrameError),

    #[error("coordinate arity {got} does not match schema arity {expected} on '{array}'")]
    CoordArityMismatch {
        array: String,
        expected: usize,
        got: usize,
    },

    #[error("coordinate out of domain on '{array}' dim '{dim}': {detail}")]
    CoordOutOfDomain {
        array: String,
        dim: String,
        detail: String,
    },

    #[error("cell-value type mismatch on '{array}' attr '{attr}': {detail}")]
    CellTypeMismatch {
        array: String,
        attr: String,
        detail: String,
    },

    /// A literal does not fit its attribute's declared type.
    #[error("attribute '{attr}' on '{array}': {source}")]
    Narrow {
        array: String,
        attr: String,
        source: NarrowError,
    },

    /// Integer arithmetic over cells left the `i64` range, such as an
    /// exact `SUM` or an elementwise product. SQLSTATE `22003`.
    #[error("numeric value out of range: {detail}")]
    NumericOverflow { detail: String },

    #[error("segment corruption: {detail}")]
    SegmentCorruption { detail: String },

    #[error("unsupported WAL format version: {version}")]
    UnsupportedFormat { version: u8 },

    #[error("unsupported segment format version: {version}")]
    UnsupportedSegmentFormat { version: u16 },

    /// A replica-id string could not be parsed from hex.
    #[error("invalid replica_id: {detail}")]
    InvalidReplicaId { detail: String },

    /// An HLC value is invalid (physical_ms overflow, logical overflow, etc.).
    #[error("invalid HLC: {detail}")]
    InvalidHlc { detail: String },

    /// The HLC generator's internal mutex was poisoned.
    #[error("HLC generator lock poisoned")]
    HlcLockPoisoned,

    /// An `ArrayOp` violates the shape contract (e.g. `Put` without attrs).
    #[error("invalid array op: {detail}")]
    InvalidOp { detail: String },

    /// An error returned by the Loro CRDT library.
    #[error("loro error: {detail}")]
    LoroError { detail: String },

    /// An encrypted (`SEGA`) segment was found but no KEK was provided.
    #[error("encrypted segment requires a KEK but none was configured")]
    MissingKek,

    /// A plaintext (`NDAS`) segment was found but a KEK was configured,
    /// refusing to load unencrypted data when encryption is enforced.
    #[error("plaintext segment rejected — encryption is enforced by the configured KEK")]
    KekRequired,

    /// AES-256-GCM encryption of a segment failed.
    #[error("segment encryption failed: {detail}")]
    EncryptionFailed { detail: String },

    /// AES-256-GCM decryption or authentication of a segment failed.
    #[error("segment decryption failed: {detail}")]
    DecryptionFailed { detail: String },

    /// The Loro snapshot envelope version does not match `LORO_FORMAT_VERSION`.
    ///
    /// This is a hard rejection: loading a snapshot produced by a different
    /// (incompatible) Loro format version would silently corrupt state.
    #[error("loro snapshot version mismatch: expected {expected}, got {got}")]
    LoroSnapshotVersionMismatch { expected: u8, got: u8 },

    /// An encoded Loro schema snapshot exceeds its pre-import byte budget.
    #[error("loro schema snapshot exceeds byte limit: {actual} > {limit}")]
    LoroSnapshotTooLarge { limit: usize, actual: usize },

    /// Loro metadata could not be decoded before schema snapshot import.
    #[error("loro schema snapshot metadata is malformed: {detail}")]
    LoroSnapshotMalformed { detail: String },

    /// An encoded Loro schema snapshot carries too many operations.
    #[error("loro schema snapshot has too many operations: {actual} > {limit}")]
    LoroSnapshotOperationLimitExceeded { limit: usize, actual: usize },
}

impl ArrayError {
    /// The array name carried by this error (for the public details slot).
    pub fn array_name(&self) -> &str {
        match self {
            ArrayError::InvalidSchema { array, .. }
            | ArrayError::InvalidDim { array, .. }
            | ArrayError::InvalidAttr { array, .. }
            | ArrayError::InvalidTileExtents { array, .. }
            | ArrayError::InvalidGeoref { array, .. }
            | ArrayError::InvalidAttrMeta { array, .. }
            | ArrayError::SchemaEncode { array, .. }
            | ArrayError::CoordArityMismatch { array, .. }
            | ArrayError::CoordOutOfDomain { array, .. }
            | ArrayError::CellTypeMismatch { array, .. }
            | ArrayError::Narrow { array, .. } => array,
            ArrayError::ZoneFrame(e) => e.array(),
            ArrayError::NumericOverflow { .. }
            | ArrayError::AggregateBudgetExceeded { .. }
            | ArrayError::SegmentCorruption { .. }
            | ArrayError::UnsupportedFormat { .. }
            | ArrayError::UnsupportedSegmentFormat { .. }
            | ArrayError::InvalidReplicaId { .. }
            | ArrayError::InvalidHlc { .. }
            | ArrayError::HlcLockPoisoned
            | ArrayError::InvalidOp { .. }
            | ArrayError::LoroError { .. }
            | ArrayError::LoroSnapshotVersionMismatch { .. }
            | ArrayError::LoroSnapshotTooLarge { .. }
            | ArrayError::LoroSnapshotMalformed { .. }
            | ArrayError::LoroSnapshotOperationLimitExceeded { .. }
            | ArrayError::MissingKek
            | ArrayError::KekRequired
            | ArrayError::EncryptionFailed { .. }
            | ArrayError::DecryptionFailed { .. } => "",
        }
    }
}

impl From<ArrayError> for NodeDbError {
    fn from(e: ArrayError) -> Self {
        let array = e.array_name().to_string();
        NodeDbError::array(array, e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_error_carries_name() {
        let e = ArrayError::CoordArityMismatch {
            array: "genome".into(),
            expected: 4,
            got: 3,
        };
        assert_eq!(e.array_name(), "genome");
    }

    #[test]
    fn array_error_converts_to_nodedb_error() {
        let e = ArrayError::InvalidSchema {
            array: "vcf".into(),
            detail: "no dimensions".into(),
        };
        let n: NodeDbError = e.into();
        assert!(n.to_string().contains("NDB-1300"));
        assert!(n.to_string().contains("vcf"));
    }

    #[test]
    fn array_error_round_trips_through_details() {
        let e = ArrayError::InvalidDim {
            array: "raster".into(),
            dim: "lat".into(),
            detail: "lo > hi".into(),
        };
        let n: NodeDbError = e.into();
        let json = serde_json::to_value(&n).unwrap();
        assert_eq!(json["details"]["kind"], "array");
        assert_eq!(json["details"]["array"], "raster");
    }
}
