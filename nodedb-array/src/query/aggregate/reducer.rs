// SPDX-License-Identifier: Apache-2.0

//! The reducers an array aggregate runs.

use nodedb_types::array_agg::HistogramSpec;

/// One array reducer.
///
/// - `Sum`, `Min`, `Max`: exact for an integer attr. `Min` / `Max` sort NaN
///   above every other float.
/// - `Count`: the non-null, non-nodata cells of any attr type.
/// - `Mean`: carries `(sum, count)`, so merges are exact.
/// - `Median`, `Percentile(p)`: exact, by linear interpolation between the
///   closest ranks (PostgreSQL `percentile_cont`). `Median` is `p = 0.5`.
/// - `Mode`: the value the most cells hold, the smallest among ties.
/// - `StdDevPop`, `StdDevSamp`, `VarPop`, `VarSamp`: population and sample
///   forms, as PostgreSQL names them.
/// - `Histogram`: fixed-width bin counts over `[lo, hi]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reducer {
    Sum,
    Count,
    Min,
    Max,
    Mean,
    Median,
    /// `0 <= p <= 1`.
    Percentile(f64),
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
    Mode,
    Histogram(HistogramSpec),
}
