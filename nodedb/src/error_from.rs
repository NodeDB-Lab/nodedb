// SPDX-License-Identifier: BUSL-1.1

//! Internal and public error conversions.

use nodedb_types::error::NodeDbError;

use crate::types::TenantId;

use super::Error;

impl From<nodedb_query::expr_parse::ExprParseError> for Error {
    fn from(e: nodedb_query::expr_parse::ExprParseError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

/// A non-scalar offered where a raw KV body is required is a request
/// shape error, not an internal fault.
impl From<nodedb_types::sync::wire::SyncWireVersionMismatch> for Error {
    fn from(e: nodedb_types::sync::wire::SyncWireVersionMismatch) -> Self {
        Error::VersionCompat {
            detail: e.to_string(),
        }
    }
}

impl From<nodedb_types::NotScalar> for Error {
    fn from(e: nodedb_types::NotScalar) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

/// A KV read-modify-write that cannot re-encode into the stored shape.
/// Shape mismatches name the offending keys or kind for the caller; a body
/// that fails to decode or encode is a serialization fault.
impl From<nodedb_query::msgpack_scan::KvBodyError> for Error {
    fn from(e: nodedb_query::msgpack_scan::KvBodyError) -> Self {
        use nodedb_query::msgpack_scan::KvBodyError as Ke;
        match e {
            Ke::RowNotObject { .. }
            | Ke::RawMissingValue
            | Ke::RawExtraKeys { .. }
            | Ke::RawNotScalar(_) => Self::BadRequest {
                detail: e.to_string(),
            },
            Ke::Decode(_) | Ke::Encode(_) => Self::Serialization {
                format: "msgpack".into(),
                detail: e.to_string(),
            },
        }
    }
}

impl From<crate::engine::timeseries::ilp::IlpError> for Error {
    fn from(e: crate::engine::timeseries::ilp::IlpError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::merge::MergeIncompatibility> for Error {
    fn from(e: crate::engine::timeseries::merge::MergeIncompatibility) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::server::sync::kv_session::KvPushRowError> for Error {
    fn from(e: crate::control::server::sync::kv_session::KvPushRowError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::columnar_segment::SegmentError> for Error {
    fn from(e: crate::engine::timeseries::columnar_segment::SegmentError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::grouped_filter::UnsupportedPredicate> for Error {
    fn from(e: crate::engine::timeseries::grouped_filter::UnsupportedPredicate) -> Self {
        Self::FeatureNotSupported {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::query::QueryError> for Error {
    fn from(e: crate::engine::timeseries::query::QueryError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::crl::CrlError> for Error {
    fn from(e: crate::control::security::crl::CrlError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::jwt::JwtError> for Error {
    fn from(e: crate::control::security::jwt::JwtError) -> Self {
        Self::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: e.to_string(),
        }
    }
}

impl From<crate::storage::quarantine::engines::FtsOrQuarantine> for Error {
    fn from(e: crate::storage::quarantine::engines::FtsOrQuarantine) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

impl From<nodedb_vector::error::VectorError> for Error {
    /// An input vector of the wrong dimension, or index input the engine
    /// cannot use, is the caller's data error: SQLSTATE `22000`. Checkpoint
    /// and stored-data failures fail-stop because replay history may be
    /// truncated.
    fn from(e: nodedb_vector::error::VectorError) -> Self {
        use nodedb_vector::error::VectorError as Ve;
        let detail = e.to_string();
        match e {
            Ve::DimensionMismatch { .. } | Ve::InvalidInput { .. } => {
                Self::DataException { detail }
            }
            // A codec that refuses to train or encode fails one build or
            // search. The stored data is intact, so the core keeps serving.
            Ve::InvalidFilterBitmap { .. } | Ve::Codec(_) | Ve::RerankCodec { .. } => {
                Self::Internal { detail }
            }
            Ve::BudgetExhausted(_) => Self::MemoryExhausted {
                engine: "vector".to_string(),
            },
            Ve::SegmentIo(_) => Self::Storage {
                engine: "vector".to_string(),
                detail,
            },
            Ve::StoredDimensionMismatch { .. }
            | Ve::UnsupportedVersion { .. }
            | Ve::InvalidMagic
            | Ve::DeserializationFailed(_)
            | Ve::CheckpointEncryptedNoKey
            | Ve::CheckpointPlaintextKeyRequired
            | Ve::CheckpointEncryptionError { .. }
            | Ve::CheckpointSerializationError { .. }
            | Ve::CheckpointDeserializationError { .. }
            | Ve::VectorUnavailable { .. }
            | Ve::VectorDecodeFailed { .. } => Self::SegmentCorrupted { detail },
            // `VectorError` is `#[non_exhaustive]` and lives in another crate,
            // so the compiler requires this arm. A variant this build cannot
            // name fails stop.
            _ => Self::SegmentCorrupted { detail },
        }
    }
}

/// `WrongBackupKek` and `TenantMismatch` are caller-actionable and get their
/// own `Error` variant + SQLSTATE; every other envelope failure means the
/// presented bytes are not a recoverable backup, so it stays generic —
/// distinguishing corruption from truncation from a bad magic byte gives an
/// attacker probing the format free feedback without helping a real operator.
impl From<nodedb_types::backup_envelope::EnvelopeError> for Error {
    fn from(e: nodedb_types::backup_envelope::EnvelopeError) -> Self {
        use nodedb_types::backup_envelope::EnvelopeError as Ee;
        match e {
            Ee::TenantMismatch { expected, actual } => {
                Self::BackupTenantMismatch { expected, actual }
            }
            Ee::WrongBackupKek => Self::BackupKeyMismatch,
            Ee::UnsupportedVersion(v) => Self::Internal {
                detail: format!("unsupported backup version: {v}"),
            },
            Ee::OverSizeTotal { cap } => Self::Internal {
                detail: format!("backup exceeds size cap of {cap} bytes"),
            },
            Ee::OverSizeSection { cap } => Self::Internal {
                detail: format!("backup section exceeds size cap of {cap} bytes"),
            },
            Ee::BadMagic
            | Ee::HeaderCrcMismatch
            | Ee::BodyCrcMismatch
            | Ee::TrailerCrcMismatch
            | Ee::Truncated
            | Ee::TooManySections(_)
            | Ee::DecryptionFailed
            | Ee::EncryptionFailed
            | Ee::RandomFailure(_) => Self::Internal {
                detail: "invalid backup format".to_string(),
            },
            // `EnvelopeError` is `#[non_exhaustive]`. An envelope this build
            // cannot name is still an envelope it cannot verify, so refuse it
            // the same way rather than letting a future variant fall through.
            _ => Self::Internal {
                detail: "invalid backup format".to_string(),
            },
        }
    }
}

/// The mapping table itself lives in [`crate::error_classify`] and borrows,
/// so the native protocol's error frames — which only ever hold a `&Error` —
/// classify through the same table instead of a second one that would drift.
impl From<Error> for NodeDbError {
    fn from(e: Error) -> Self {
        crate::error_classify::classify(&e)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    /// An input vector of the wrong width is the caller's data error; stored
    /// data of the wrong width is corruption.
    #[test]
    fn vector_dimension_errors_classify_by_source() {
        use nodedb_vector::error::VectorError;
        let input: Error = VectorError::DimensionMismatch {
            expected: 3,
            got: 2,
        }
        .into();
        match input {
            Error::DataException { detail } => {
                assert_eq!(detail, "vector dimension mismatch: expected 3, got 2");
            }
            other => panic!("expected Error::DataException, got {other:?}"),
        }
        let stored: Error = VectorError::StoredDimensionMismatch {
            expected: 3,
            got: 2,
        }
        .into();
        assert!(
            matches!(stored, Error::SegmentCorrupted { .. }),
            "{stored:?}"
        );
    }
}
