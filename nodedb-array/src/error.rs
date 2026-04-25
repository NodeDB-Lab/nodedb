//! Typed errors for the array engine.
//!
//! [`ArrayError`] is the crate-internal error enum. Every variant maps
//! into the public [`NodeDbError::array`] constructor at the API
//! boundary via the `From` impl at the bottom of this file.

use nodedb_types::NodeDbError;
use thiserror::Error;

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

    #[error("segment corruption: {detail}")]
    SegmentCorruption { detail: String },
}

impl ArrayError {
    /// The array name carried by this error (for the public details slot).
    pub fn array_name(&self) -> &str {
        match self {
            ArrayError::InvalidSchema { array, .. }
            | ArrayError::InvalidDim { array, .. }
            | ArrayError::InvalidAttr { array, .. }
            | ArrayError::InvalidTileExtents { array, .. }
            | ArrayError::CoordArityMismatch { array, .. }
            | ArrayError::CoordOutOfDomain { array, .. }
            | ArrayError::CellTypeMismatch { array, .. } => array,
            ArrayError::SegmentCorruption { .. } => "",
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
