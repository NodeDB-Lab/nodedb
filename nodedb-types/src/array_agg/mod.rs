// SPDX-License-Identifier: Apache-2.0

//! Exact, mergeable partial states of the array reducers that need more
//! than a running number: median, percentile and mode (a value
//! distribution), variance and standard deviation (moments), and
//! histogram (fixed-width bin counts).
//!
//! A Data Plane core builds a partial, a shard ships it, and the
//! coordinator merges and finalizes it. Every merge is exact.

pub mod dist;
pub mod dist_acc;
pub mod histogram;
pub mod moments;

pub use dist::{DistBudgetExceeded, DistValue, FloatRun, IntRun, MAX_DIST_ENTRIES, ValueDist};
pub use dist_acc::DistAcc;
pub use histogram::{HistogramBin, HistogramSpec, HistogramSpecError, MAX_HISTOGRAM_BINS};
pub use moments::Moments;
