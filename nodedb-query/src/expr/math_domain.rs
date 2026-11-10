// SPDX-License-Identifier: Apache-2.0

//! Arguments outside a math function's domain.

use nodedb_types::error::sqlstate;

/// An argument outside the domain of `sqrt`, a logarithm or `power`, with
/// PostgreSQL's message for each case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MathDomain {
    #[error("cannot take square root of a negative number")]
    SqrtOfNegative,
    #[error("cannot take logarithm of zero")]
    LogOfZero,
    #[error("cannot take logarithm of a negative number")]
    LogOfNegative,
    #[error("zero raised to a negative power is undefined")]
    ZeroToNegativePower,
    #[error("a negative number raised to a non-integer power yields a complex result")]
    NegativeToFractionalPower,
}

/// The SQLSTATE subclass of a [`MathDomain`] fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MathDomainClass {
    /// `2201E` (invalid_argument_for_logarithm).
    Logarithm,
    /// `2201F` (invalid_argument_for_power_function). PostgreSQL gives the
    /// square root of a negative number this code too.
    PowerFunction,
}

impl MathDomain {
    /// The SQLSTATE subclass PostgreSQL raises for this fault.
    pub fn class(self) -> MathDomainClass {
        match self {
            MathDomain::LogOfZero | MathDomain::LogOfNegative => MathDomainClass::Logarithm,
            MathDomain::SqrtOfNegative
            | MathDomain::ZeroToNegativePower
            | MathDomain::NegativeToFractionalPower => MathDomainClass::PowerFunction,
        }
    }

    /// The SQLSTATE PostgreSQL raises for this fault.
    pub fn sqlstate(self) -> &'static str {
        self.class().sqlstate()
    }
}

impl MathDomainClass {
    /// The SQLSTATE of this subclass.
    pub fn sqlstate(self) -> &'static str {
        match self {
            MathDomainClass::Logarithm => sqlstate::INVALID_ARGUMENT_FOR_LOGARITHM,
            MathDomainClass::PowerFunction => sqlstate::INVALID_ARGUMENT_FOR_POWER_FUNCTION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_fault_carries_postgres_sqlstate() {
        assert_eq!(MathDomain::LogOfZero.sqlstate(), "2201E");
        assert_eq!(MathDomain::LogOfNegative.sqlstate(), "2201E");
        assert_eq!(MathDomain::SqrtOfNegative.sqlstate(), "2201F");
        assert_eq!(MathDomain::ZeroToNegativePower.sqlstate(), "2201F");
        assert_eq!(MathDomain::NegativeToFractionalPower.sqlstate(), "2201F");
    }
}
