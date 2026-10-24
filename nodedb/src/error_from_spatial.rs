// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_spatial` errors into the crate error.

use crate::Error;

/// An undefined SRID keeps its own variant. Every other geometry fault — a
/// parse error, an operation the input does not support, a coordinate
/// outside a projection — is a data exception, SQLSTATE `22000`.
impl From<nodedb_spatial::SpatialError> for Error {
    fn from(e: nodedb_spatial::SpatialError) -> Self {
        match e {
            nodedb_spatial::SpatialError::Srid(source) => Self::UnknownSrid { source },
            other => Self::DataException {
                detail: other.to_string(),
            },
        }
    }
}

impl From<nodedb_spatial::RTreeCheckpointError> for Error {
    /// Spatial checkpoint failures fail-stop as segment corruption.
    fn from(e: nodedb_spatial::RTreeCheckpointError) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_faults_are_data_exceptions() {
        let e = nodedb_spatial::geometry_from_text("POINT(", nodedb_spatial::SridCheck::Syntax)
            .expect_err("malformed");
        assert!(matches!(Error::from(e), Error::DataException { .. }));
        let undefined =
            nodedb_spatial::SpatialError::Srid(nodedb_crs::UnknownSrid::Undefined { code: 999 });
        assert!(matches!(Error::from(undefined), Error::UnknownSrid { .. }));
    }
}
