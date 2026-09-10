// SPDX-License-Identifier: Apache-2.0

//! Function volatility: how far a call's result can be reused.
//!
//! Volatility is orthogonal to a function's category. A function is
//! `Scalar` and `Volatile` at the same time.

/// How far a function call's result can be reused.
///
/// Two states, because the engine has two reuse boundaries: a value folded
/// into a cached plan, and a value produced fresh for one execution. A
/// per-statement memoization boundary would need a third state; no such
/// boundary exists, so no third state does either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Volatility {
    /// Same arguments always give the same result. Foldable at plan time.
    #[default]
    Immutable,
    /// Can change per call, or has side effects.
    Volatile,
}

impl Volatility {
    /// Whether a call can be folded to a literal at plan time.
    pub fn is_foldable(self) -> bool {
        matches!(self, Self::Immutable)
    }

    /// Whether a call must re-evaluate on every execution.
    pub fn is_volatile(self) -> bool {
        matches!(self, Self::Volatile)
    }
}
