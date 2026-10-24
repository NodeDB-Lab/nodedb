// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_query::EvalError` into the crate error.

use crate::Error;

/// The `match` is exhaustive rather than a `_ =>` fallback, so a new
/// evaluator error picks its own `crate::Error` mapping instead of silently
/// inheriting one.
impl From<nodedb_query::EvalError> for Error {
    fn from(e: nodedb_query::EvalError) -> Self {
        match e {
            nodedb_query::EvalError::DivisionByZero => Self::DivisionByZero,
            nodedb_query::EvalError::UnknownFunction { name } => Self::UndefinedFunction { name },
            e @ (nodedb_query::EvalError::VectorDimensionMismatch { .. }
            | nodedb_query::EvalError::ArgumentType { .. }
            | nodedb_query::EvalError::InvalidJsonPath { .. }
            | nodedb_query::EvalError::Crs { .. }
            | nodedb_query::EvalError::MixedSrid { .. }
            | nodedb_query::EvalError::Transform { .. }
            | nodedb_query::EvalError::Geometry { .. }
            | nodedb_query::EvalError::ArgumentValue { .. }) => Self::DataException {
                detail: e.to_string(),
            },
            e @ nodedb_query::EvalError::NumericOverflow { .. } => Self::NumericValueOutOfRange {
                detail: e.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An SRID fault fails the statement with SQLSTATE `22000`.
    #[test]
    fn srid_faults_are_data_exceptions() {
        for e in [
            nodedb_query::EvalError::Crs {
                function: "st_transform",
                source: nodedb_crs::UnknownSrid::Undefined { code: 999 },
            },
            nodedb_query::EvalError::MixedSrid {
                function: "st_intersects",
                left: 4326,
                right: 32648,
            },
            nodedb_query::EvalError::Transform {
                function: "st_transform",
                source: nodedb_crs::CrsError::OutOfDomain {
                    srid: nodedb_crs::Srid::new(32648).expect("srid"),
                    x: f64::NAN,
                    y: 0.0,
                    reason: nodedb_crs::DomainReason::NonFinite,
                },
            },
        ] {
            let text = e.to_string();
            match Error::from(e) {
                Error::DataException { detail } => assert_eq!(detail, text),
                other => panic!("expected Error::DataException, got {other:?}"),
            }
        }
    }
}
