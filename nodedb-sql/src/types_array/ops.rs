// SPDX-License-Identifier: Apache-2.0

//! Reducers and pairwise ops carried on array plans.

/// Reducer carried on `SqlPlan::ArrayAgg`. Mirrors
/// `nodedb_physical::physical_plan::ArrayReducer` without the dependency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ArrayReducerAst {
    Sum,
    Count,
    Min,
    Max,
    Mean,
    Median,
    /// Exact percentile, `0 <= p <= 1`.
    Percentile(f64),
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
    Mode,
    /// Fixed-width bins over `[lo, hi]`.
    Histogram {
        bins: u32,
        lo: f64,
        hi: f64,
    },
}

/// A reducer name of the SQL surface, before its named arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayReducerName {
    Sum,
    Count,
    Min,
    Max,
    Mean,
    Median,
    Percentile,
    StdDevPop,
    StdDevSamp,
    VarPop,
    VarSamp,
    Mode,
    Histogram,
}

impl ArrayReducerName {
    /// Every reducer name the SQL surface accepts, for error messages.
    pub const ACCEPTED: &'static str = "sum, count, min, max, mean (avg), median, percentile, \
         stddev (stddev_samp), stddev_pop, variance (var_samp), var_pop, mode, histogram";

    /// Parse the reducer string (`'sum'`, `'stddev_pop'`, ...). `stddev`
    /// and `variance` are the sample forms, as in PostgreSQL. `None` for
    /// any other value.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "sum" => ArrayReducerName::Sum,
            "count" => ArrayReducerName::Count,
            "min" => ArrayReducerName::Min,
            "max" => ArrayReducerName::Max,
            "mean" | "avg" => ArrayReducerName::Mean,
            "median" => ArrayReducerName::Median,
            "percentile" | "percentile_cont" => ArrayReducerName::Percentile,
            "stddev" | "stddev_samp" => ArrayReducerName::StdDevSamp,
            "stddev_pop" => ArrayReducerName::StdDevPop,
            "variance" | "var_samp" => ArrayReducerName::VarSamp,
            "var_pop" => ArrayReducerName::VarPop,
            "mode" => ArrayReducerName::Mode,
            "histogram" => ArrayReducerName::Histogram,
            _ => return None,
        })
    }
}

impl ArrayReducerAst {
    /// The uppercase SQL name of the reducer.
    pub fn sql_name(self) -> &'static str {
        match self {
            ArrayReducerAst::Sum => "SUM",
            ArrayReducerAst::Count => "COUNT",
            ArrayReducerAst::Min => "MIN",
            ArrayReducerAst::Max => "MAX",
            ArrayReducerAst::Mean => "MEAN",
            ArrayReducerAst::Median => "MEDIAN",
            ArrayReducerAst::Percentile(_) => "PERCENTILE",
            ArrayReducerAst::StdDevPop => "STDDEV_POP",
            ArrayReducerAst::StdDevSamp => "STDDEV_SAMP",
            ArrayReducerAst::VarPop => "VAR_POP",
            ArrayReducerAst::VarSamp => "VAR_SAMP",
            ArrayReducerAst::Mode => "MODE",
            ArrayReducerAst::Histogram { .. } => "HISTOGRAM",
        }
    }
}

/// Pairwise op carried on `SqlPlan::ArrayElementwise`. Mirrors
/// `crate::bridge::physical_plan::ArrayBinaryOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayBinaryOpAst {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArrayBinaryOpAst {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "add" | "+" => Some(ArrayBinaryOpAst::Add),
            "sub" | "-" => Some(ArrayBinaryOpAst::Sub),
            "mul" | "*" => Some(ArrayBinaryOpAst::Mul),
            "div" | "/" => Some(ArrayBinaryOpAst::Div),
            _ => None,
        }
    }
}
