// SPDX-License-Identifier: BUSL-1.1

//! Why a KV counter atomic (`INCR`, `INCRBY`, `DECR`, `INCRBYFLOAT`)
//! computed no value.

/// Why a KV counter atomic computed no value.
///
/// Each fault has one client message, the text Redis answers with for the
/// same condition. RESP sends it after `ERR`. The SQL surfaces send it with
/// the SQLSTATE [`CounterFault::sqlstate`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterFault {
    /// The stored value is not a decimal integer in the `i64` range.
    NotAnInteger,
    /// The stored value is not a decimal float.
    NotAFloat,
    /// The integer result is outside the `i64` range.
    IntegerOverflow,
    /// The float result is NaN or infinite.
    NonFinite,
}

impl CounterFault {
    /// The client message, without the RESP `ERR` prefix.
    pub fn message(self) -> &'static str {
        match self {
            Self::NotAnInteger => "value is not an integer or out of range",
            Self::NotAFloat => "value is not a valid float",
            Self::IntegerOverflow => "increment or decrement would overflow",
            Self::NonFinite => "increment would produce NaN or Infinity",
        }
    }

    /// The SQLSTATE for the fault: `22P02` for a stored value that does not
    /// parse, `22003` for a result out of range.
    pub fn sqlstate(self) -> &'static str {
        use nodedb_types::error::sqlstate;
        match self {
            Self::NotAnInteger | Self::NotAFloat => sqlstate::INVALID_TEXT_REPRESENTATION,
            Self::IntegerOverflow | Self::NonFinite => sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_faults_answer_invalid_text_representation() {
        assert_eq!(CounterFault::NotAnInteger.sqlstate(), "22P02");
        assert_eq!(CounterFault::NotAFloat.sqlstate(), "22P02");
    }

    #[test]
    fn range_faults_answer_numeric_value_out_of_range() {
        assert_eq!(CounterFault::IntegerOverflow.sqlstate(), "22003");
        assert_eq!(CounterFault::NonFinite.sqlstate(), "22003");
    }
}
