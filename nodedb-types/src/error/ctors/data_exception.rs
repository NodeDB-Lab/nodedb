// SPDX-License-Identifier: Apache-2.0

//! Data-exception constructors (1200s): arithmetic, text, date/time and
//! type errors raised while evaluating an expression.

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

/// Defines a constructor whose details variant carries only the full message.
macro_rules! detail_ctor {
    ($(#[$doc:meta])* $name:ident, $code:ident, $variant:ident) => {
        $(#[$doc])*
        pub fn $name(detail: impl Into<String>) -> Self {
            let detail = detail.into();
            Self {
                code: ErrorCode::$code,
                message: detail.clone(),
                details: ErrorDetails::$variant { detail },
                cause: None,
            }
        }
    };
}

impl NodeDbError {
    /// Expression evaluation divided or took a modulus by zero. Distinct
    /// from `plan_error` so clients can match on the specific code
    /// (SQLSTATE `22012`, `division_by_zero`) rather than parsing the
    /// message.
    pub fn division_by_zero() -> Self {
        Self {
            code: ErrorCode::DIVISION_BY_ZERO,
            message: "division by zero".to_string(),
            details: ErrorDetails::DivisionByZero,
            cause: None,
        }
    }

    /// A function received a value it cannot compute on: a vector of the
    /// wrong dimension, an argument of the wrong shape, a malformed path.
    /// SQLSTATE `22000` (`data_exception`). `detail` is the full message.
    pub fn data_exception(detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self {
            code: ErrorCode::DATA_EXCEPTION,
            message: detail.clone(),
            details: ErrorDetails::DataException { detail },
            cause: None,
        }
    }

    /// A value does not fit its numeric type: an integer past a column's
    /// declared width, or arithmetic that overflows. SQLSTATE `22003`
    /// (`numeric_value_out_of_range`). The error names no collection.
    /// `detail` is the full message.
    pub fn numeric_value_out_of_range(detail: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::OVERFLOW,
            message: detail.into(),
            details: ErrorDetails::Overflow {
                collection: String::new(),
            },
            cause: None,
        }
    }

    /// A statement exceeded a server limit on its own size or depth: a
    /// recursion depth, a per-transaction staging budget. SQLSTATE `54000`
    /// (`program_limit_exceeded`). `detail` is the full message.
    pub fn program_limit_exceeded(detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self {
            code: ErrorCode::PROGRAM_LIMIT_EXCEEDED,
            message: detail.clone(),
            details: ErrorDetails::ProgramLimitExceeded { detail },
            cause: None,
        }
    }

    detail_ctor! {
        /// `ln` or `log` of zero or a negative number. SQLSTATE `2201E`
        /// (`invalid_argument_for_logarithm`). `detail` is the full message.
        invalid_argument_for_logarithm, INVALID_ARGUMENT_FOR_LOGARITHM, InvalidArgumentForLogarithm
    }

    detail_ctor! {
        /// `sqrt` of a negative number, or `power` outside its domain. SQLSTATE
        /// `2201F` (`invalid_argument_for_power_function`). `detail` is the full
        /// message.
        invalid_argument_for_power_function, INVALID_ARGUMENT_FOR_POWER_FUNCTION, InvalidArgumentForPowerFunction
    }

    detail_ctor! {
        /// Text that spells no value of the target type. SQLSTATE `22P02`.
        /// `detail` is the full message.
        invalid_text_representation, INVALID_TEXT_REPRESENTATION, InvalidTextRepresentation
    }

    detail_ctor! {
        /// Text that spells no value of a date or time type. SQLSTATE `22007`.
        /// `detail` is the full message.
        invalid_datetime_format, INVALID_DATETIME_FORMAT, InvalidDatetimeFormat
    }

    detail_ctor! {
        /// A date or time field outside its range. SQLSTATE `22008`. `detail`
        /// is the full message.
        datetime_field_overflow, DATETIME_FIELD_OVERFLOW, DatetimeFieldOverflow
    }

    detail_ctor! {
        /// A value whose type the target type cannot hold. SQLSTATE `42804`.
        /// `detail` is the full message.
        datatype_mismatch, DATATYPE_MISMATCH, DatatypeMismatch
    }

    /// A LIMIT/OFFSET/FETCH bound did not resolve to `[0, usize::MAX]`.
    /// Distinct from `plan_error` so clients match the code, SQLSTATE
    /// `2201W`, instead of parsing the message.
    pub fn invalid_limit_value(clause: impl Into<String>, value: impl Into<String>) -> Self {
        let clause = clause.into();
        let value = value.into();
        Self {
            code: ErrorCode::INVALID_LIMIT_VALUE,
            message: format!("invalid {clause} value: {value}"),
            details: ErrorDetails::InvalidLimitValue { clause, value },
            cause: None,
        }
    }
}
