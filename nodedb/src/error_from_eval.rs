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
            nodedb_query::EvalError::MathDomain(fault) => match fault.class() {
                nodedb_query::expr::MathDomainClass::Logarithm => {
                    Self::InvalidArgumentForLogarithm {
                        detail: fault.to_string(),
                    }
                }
                nodedb_query::expr::MathDomainClass::PowerFunction => {
                    Self::InvalidArgumentForPowerFunction {
                        detail: fault.to_string(),
                    }
                }
            },
            e @ (nodedb_query::EvalError::NumericOverflow { .. }
            | nodedb_query::EvalError::NumericFieldOverflow { .. }
            | nodedb_query::EvalError::OutOfRange(_)
            | nodedb_query::EvalError::NumericSpecialValue { .. }) => {
                Self::NumericValueOutOfRange {
                    detail: e.to_string(),
                }
            }
            e @ nodedb_query::EvalError::InvalidTextRepresentation { .. } => {
                Self::InvalidTextRepresentation {
                    detail: e.to_string(),
                }
            }
            e @ nodedb_query::EvalError::InvalidDatetimeFormat { .. } => {
                Self::InvalidDatetimeFormat {
                    detail: e.to_string(),
                }
            }
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

    /// A logarithm fault keeps `2201E`, and the others `2201F`.
    #[test]
    fn math_domain_faults_keep_their_subclass() {
        use nodedb_query::expr::MathDomain;
        match Error::from(nodedb_query::EvalError::MathDomain(MathDomain::LogOfZero)) {
            Error::InvalidArgumentForLogarithm { detail } => {
                assert_eq!(detail, "cannot take logarithm of zero")
            }
            other => panic!("expected Error::InvalidArgumentForLogarithm, got {other:?}"),
        }
        match Error::from(nodedb_query::EvalError::MathDomain(
            MathDomain::SqrtOfNegative,
        )) {
            Error::InvalidArgumentForPowerFunction { detail } => {
                assert_eq!(detail, "cannot take square root of a negative number")
            }
            other => panic!("expected Error::InvalidArgumentForPowerFunction, got {other:?}"),
        }
    }

    /// An out-of-range result, and NaN or an infinity cast to numeric, are
    /// `22003`.
    #[test]
    fn range_faults_keep_their_class() {
        let e = nodedb_query::EvalError::OutOfRange(nodedb_query::expr::OutOfRange::Bigint);
        match Error::from(e) {
            Error::NumericValueOutOfRange { detail } => assert_eq!(detail, "bigint out of range"),
            other => panic!("expected Error::NumericValueOutOfRange, got {other:?}"),
        }
        let e = nodedb_query::EvalError::NumericSpecialValue { value: "NaN" };
        match Error::from(e) {
            Error::NumericValueOutOfRange { detail } => {
                assert_eq!(detail, "cannot convert NaN to numeric")
            }
            other => panic!("expected Error::NumericValueOutOfRange, got {other:?}"),
        }
    }
}
