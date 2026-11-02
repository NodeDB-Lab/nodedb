// SPDX-License-Identifier: BUSL-1.1

//! `From` impls that build a [`super::Error`] from `nodedb-physical` error
//! types (wire decoding, physical-plan conversion) and from the
//! `nodedb-types` collection-key and LSN-time errors. Kept apart from the enum
//! definition in `types.rs` so a new physical-layer error source has one
//! obvious home instead of growing the enum file further.

use super::Error;

impl From<nodedb_physical::physical_plan::wire::WireError> for Error {
    fn from(e: nodedb_physical::physical_plan::wire::WireError) -> Self {
        Error::Internal {
            detail: e.to_string(),
        }
    }
}

impl From<nodedb_physical::ConvertError> for Error {
    fn from(e: nodedb_physical::ConvertError) -> Self {
        use nodedb_physical::ConvertError;
        match e {
            ConvertError::PlanError(detail) => Error::PlanError { detail },
            ConvertError::BadRequest(detail) => Error::BadRequest { detail },
            ConvertError::LimitExceeded {
                limit_name,
                value,
                max,
            } => Error::LimitExceeded {
                limit_name,
                value,
                max,
            },
            ConvertError::Serialization(detail) => Error::Serialization {
                format: "msgpack".into(),
                detail,
            },
            ConvertError::Other(detail) => Error::Internal { detail },
        }
    }
}

/// A CRDT delta record that cannot be written. Every apply reaching the WAL
/// writer carries its row's bound surrogate, so an unbound or partial target
/// is an internal invariant break.
impl From<crate::wal::CrdtDeltaWalError> for Error {
    fn from(e: crate::wal::CrdtDeltaWalError) -> Self {
        match e {
            crate::wal::CrdtDeltaWalError::Encode { source } => Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal crdt delta: {source}"),
            },
            other => Error::Internal {
                detail: other.to_string(),
            },
        }
    }
}

/// A KV write that reached the engine without a bound surrogate. Every write
/// path binds its rows first, so this is an internal invariant break.
impl From<crate::engine::kv::UnboundKvWrite> for Error {
    fn from(e: crate::engine::kv::UnboundKvWrite) -> Self {
        Error::Internal {
            detail: e.to_string(),
        }
    }
}

/// A KV restore whose ended versions overlap the versions the key holds. The
/// restore is refused as a whole, and nothing of it is written.
impl From<crate::engine::kv::KvHistoryOverlap> for Error {
    fn from(e: crate::engine::kv::KvHistoryOverlap) -> Self {
        Error::RejectedPrevalidation {
            constraint: "kv_history_overlap".into(),
            reason: e.to_string(),
        }
    }
}

/// A qualified collection name that does not carry its database's qualifier
/// reached a placement or surrogate path. Every qualified name is built by
/// `QualifiedCollection::new`, so this is an internal invariant break.
impl From<nodedb_types::CollectionKeyError> for Error {
    fn from(e: nodedb_types::CollectionKeyError) -> Self {
        Error::Internal {
            detail: e.to_string(),
        }
    }
}

/// An `AS OF SYSTEM TIME` target no retained WAL time anchor can resolve.
/// Rendered as SQLSTATE `22000` (data_exception): the value is out of range.
impl From<nodedb_types::LsnTimeError> for Error {
    fn from(e: nodedb_types::LsnTimeError) -> Self {
        Error::DataException {
            detail: format!("AS OF SYSTEM TIME: {e}"),
        }
    }
}

/// An array-engine error. A value outside its attr type's range or an
/// integer overflow renders as SQLSTATE `22003`. A cell of another type than
/// its attr declares renders as `42804`. Every other error is engine
/// storage, so it renders as a storage error of the `array` engine.
impl From<crate::engine::array::engine::ArrayEngineError> for Error {
    fn from(e: crate::engine::array::engine::ArrayEngineError) -> Self {
        use crate::engine::array::engine::ArrayEngineError;
        use nodedb_array::ArrayError;
        match &e {
            ArrayEngineError::Array(ArrayError::NumericOverflow { .. }) => {
                Error::NumericValueOutOfRange {
                    detail: e.to_string(),
                }
            }
            ArrayEngineError::Array(ArrayError::Narrow { source, .. })
                if source.is_out_of_range() =>
            {
                Error::NumericValueOutOfRange {
                    detail: e.to_string(),
                }
            }
            ArrayEngineError::Array(
                ArrayError::Narrow { .. } | ArrayError::CellTypeMismatch { .. },
            ) => Error::DatatypeMismatch {
                detail: e.to_string(),
            },
            _ => Error::Storage {
                engine: "array".to_string(),
                detail: e.to_string(),
            },
        }
    }
}
