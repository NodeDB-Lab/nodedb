// SPDX-License-Identifier: Apache-2.0

//! Why a KV atomic computed no stored value.

use super::counter_fault::CounterFault;

/// Why [`super::compute`] computed no stored value for a KV atomic.
///
/// Each executor maps it into its own error. Origin maps it into the Data
/// Plane `ErrorCode`, and Lite maps it into `LiteError`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AtomicComputeError {
    /// A typed row has no column of the type the atomic reads.
    #[error("{detail}")]
    TypeMismatch { detail: String },
    /// A counter atomic read a stored value it cannot parse as a number, or
    /// computed a result out of range.
    #[error("{0}")]
    Counter(CounterFault),
    /// The computed new value failed to re-encode as MessagePack.
    #[error("{detail}")]
    Encode { detail: String },
}
