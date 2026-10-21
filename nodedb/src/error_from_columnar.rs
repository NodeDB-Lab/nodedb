// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_columnar::ColumnarError` into the crate error.
//!
//! A row the caller sent that the memtable cannot hold is the caller's
//! error: `BadRequest`, or the constraint it breaks. Every other columnar
//! error is a storage fault of the engine.

use nodedb_columnar::error::ColumnarError;

use crate::Error;

impl From<ColumnarError> for Error {
    fn from(e: ColumnarError) -> Self {
        match e {
            ColumnarError::TypeMismatch { .. }
            | ColumnarError::JsonParse { .. }
            | ColumnarError::RangeParse { .. }
            | ColumnarError::MsgpackSerialize { .. }
            | ColumnarError::MsgpackDeserialize { .. } => Self::BadRequest {
                detail: e.to_string(),
            },
            // A stored cell or segment that does not decode is corruption,
            // not a fault the caller can retry around.
            ColumnarError::Corruption { .. }
            | ColumnarError::MemtableCellCorrupt { .. }
            | ColumnarError::WalRowCorrupt { .. }
            | ColumnarError::StringCellNotUtf8 { .. }
            | ColumnarError::FooterCrcMismatch { .. }
            | ColumnarError::TruncatedSegment { .. }
            | ColumnarError::InvalidMagic(_) => Self::SegmentCorrupted {
                detail: e.to_string(),
            },
            // A geometry value that does not parse is the caller's data
            // fault, SQLSTATE `22000`, like the evaluator's parse errors.
            ColumnarError::Geometry { .. } => Self::DataException {
                detail: e.to_string(),
            },
            ColumnarError::NullViolation(ref column) => Self::RejectedConstraint {
                collection: String::new(),
                constraint: "not_null".into(),
                detail: format!("column '{column}' is NOT NULL"),
            },
            ColumnarError::DuplicatePrimaryKey => Self::RejectedConstraint {
                collection: String::new(),
                constraint: "unique".into(),
                detail: e.to_string(),
            },
            // `ColumnarError` is `#[non_exhaustive]`: any other variant is a
            // fault of the engine or its stored segments.
            _ => Self::Storage {
                engine: "columnar".into(),
                detail: e.to_string(),
            },
        }
    }
}

impl From<crate::storage::quarantine::engines::ColumnarOrQuarantine> for Error {
    /// A quarantined segment is corrupt by definition. Any other open error
    /// keeps the class `ColumnarError` maps to.
    fn from(e: crate::storage::quarantine::engines::ColumnarOrQuarantine) -> Self {
        use crate::storage::quarantine::engines::ColumnarOrQuarantine as Coq;
        match e {
            Coq::Columnar(inner) => Self::from(inner),
            Coq::Quarantined(q) => Self::SegmentCorrupted {
                detail: q.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_the_column_cannot_hold_is_a_bad_request() {
        let e = Error::from(ColumnarError::TypeMismatch {
            column: "v".into(),
            expected: "Decimal".into(),
        });
        assert!(matches!(e, Error::BadRequest { ref detail } if detail.contains("'v'")));
    }

    #[test]
    fn constraint_breaks_keep_their_constraint() {
        let e = Error::from(ColumnarError::NullViolation("v".into()));
        assert!(
            matches!(e, Error::RejectedConstraint { ref constraint, .. } if constraint == "not_null")
        );
        let e = Error::from(ColumnarError::DuplicatePrimaryKey);
        assert!(
            matches!(e, Error::RejectedConstraint { ref constraint, .. } if constraint == "unique")
        );
    }

    #[test]
    fn a_corrupt_cell_is_segment_corruption() {
        let e = Error::from(ColumnarError::MemtableCellCorrupt {
            column: "v".into(),
            row: 0,
            reason: "JSON cell is not MessagePack".into(),
        });
        assert!(matches!(e, Error::SegmentCorrupted { ref detail } if detail.contains("'v'")));
        let e = Error::from(ColumnarError::Corruption {
            segment_id: None,
            reason: "bad cell".into(),
            offset: None,
        });
        assert!(matches!(e, Error::SegmentCorrupted { .. }));
    }

    #[test]
    fn an_engine_fault_is_a_storage_error() {
        let e = Error::from(ColumnarError::EmptyMemtable);
        assert!(matches!(e, Error::Storage { ref engine, .. } if engine == "columnar"));
    }
}
