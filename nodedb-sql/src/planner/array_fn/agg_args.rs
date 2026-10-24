// SPDX-License-Identifier: Apache-2.0

//! The reducer string and named arguments of `ARRAY_AGG`.
//!
//! - `'percentile'` takes `p => <0..1>`.
//! - `'histogram'` takes `bins => <1..65536>, lo => <n>, hi => <n>`, with
//!   `lo < hi`.
//! - Every reducer takes `physical => true | false`.
//!
//! A parameter given to a reducer that takes none is refused, so a typo
//! never changes the answer silently.

use sqlparser::ast;

use super::helpers::{expect_bool, expect_f64, expect_u32};
use crate::error::{Result, SqlError};
use crate::types_array::{ArrayReducerAst, ArrayReducerName};

/// The named arguments `ARRAY_AGG` accepts.
pub(super) const AGG_NAMED_ARGS: &[&str] = &["physical", "p", "bins", "lo", "hi"];

/// The named arguments `ARRAY_SLICE` accepts.
pub(super) const SLICE_NAMED_ARGS: &[&str] = &["physical"];

/// The most bins a histogram takes. Mirrors
/// `nodedb_types::array_agg::MAX_HISTOGRAM_BINS`.
const MAX_HISTOGRAM_BINS: u32 = 65_536;

/// The reducer and read mode of one `ARRAY_AGG` call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct AggOptions {
    pub reducer: ArrayReducerAst,
    pub physical: bool,
}

/// The value of named argument `name`, if given.
fn named<'a>(args: &'a [(String, ast::Expr)], name: &str) -> Option<&'a ast::Expr> {
    args.iter().find(|(n, _)| n == name).map(|(_, e)| e)
}

/// `physical => <bool>`, false when absent.
pub(super) fn parse_physical(args: &[(String, ast::Expr)], fn_name: &str) -> Result<bool> {
    named(args, "physical")
        .map(|e| expect_bool(e, &format!("{fn_name} physical")))
        .transpose()
        .map(|v| v.unwrap_or(false))
}

/// The reducer `reducer_str` names, with its named parameters.
pub(super) fn parse_agg_options(
    reducer_str: &str,
    args: &[(String, ast::Expr)],
) -> Result<AggOptions> {
    let name = ArrayReducerName::parse(reducer_str).ok_or_else(|| SqlError::Unsupported {
        detail: format!(
            "ARRAY_AGG: unknown reducer '{reducer_str}' (want {})",
            ArrayReducerName::ACCEPTED
        ),
    })?;
    let takes: &[&str] = match name {
        ArrayReducerName::Percentile => &["p"],
        ArrayReducerName::Histogram => &["bins", "lo", "hi"],
        ArrayReducerName::Sum
        | ArrayReducerName::Count
        | ArrayReducerName::Min
        | ArrayReducerName::Max
        | ArrayReducerName::Mean
        | ArrayReducerName::Median
        | ArrayReducerName::StdDevPop
        | ArrayReducerName::StdDevSamp
        | ArrayReducerName::VarPop
        | ArrayReducerName::VarSamp
        | ArrayReducerName::Mode => &[],
    };
    for param in ["p", "bins", "lo", "hi"] {
        let given = named(args, param).is_some();
        let wanted = takes.contains(&param);
        if given && !wanted {
            return Err(SqlError::Unsupported {
                detail: format!("ARRAY_AGG: reducer '{reducer_str}' takes no '{param}' argument"),
            });
        }
        if wanted && !given {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "ARRAY_AGG: reducer '{reducer_str}' needs {}",
                    takes
                        .iter()
                        .map(|t| format!("{t} => <value>"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }
    let number = |param: &str| -> Result<f64> {
        let expr = named(args, param).ok_or_else(|| SqlError::Unsupported {
            detail: format!("ARRAY_AGG: missing '{param}' argument"),
        })?;
        expect_f64(expr, &format!("ARRAY_AGG {param}"))
    };
    let reducer = match name {
        ArrayReducerName::Sum => ArrayReducerAst::Sum,
        ArrayReducerName::Count => ArrayReducerAst::Count,
        ArrayReducerName::Min => ArrayReducerAst::Min,
        ArrayReducerName::Max => ArrayReducerAst::Max,
        ArrayReducerName::Mean => ArrayReducerAst::Mean,
        ArrayReducerName::Median => ArrayReducerAst::Median,
        ArrayReducerName::StdDevPop => ArrayReducerAst::StdDevPop,
        ArrayReducerName::StdDevSamp => ArrayReducerAst::StdDevSamp,
        ArrayReducerName::VarPop => ArrayReducerAst::VarPop,
        ArrayReducerName::VarSamp => ArrayReducerAst::VarSamp,
        ArrayReducerName::Mode => ArrayReducerAst::Mode,
        ArrayReducerName::Percentile => {
            let p = number("p")?;
            if !(0.0..=1.0).contains(&p) {
                return Err(SqlError::DataException {
                    detail: format!("ARRAY_AGG: percentile p {p} must be between 0 and 1"),
                });
            }
            ArrayReducerAst::Percentile(p)
        }
        ArrayReducerName::Histogram => {
            let bins_expr = named(args, "bins").ok_or_else(|| SqlError::Unsupported {
                detail: "ARRAY_AGG: missing 'bins' argument".into(),
            })?;
            let bins = expect_u32(bins_expr, "ARRAY_AGG bins")?;
            if bins == 0 || bins > MAX_HISTOGRAM_BINS {
                return Err(SqlError::DataException {
                    detail: format!(
                        "ARRAY_AGG: histogram bins {bins} must be between 1 and \
                         {MAX_HISTOGRAM_BINS}"
                    ),
                });
            }
            let (lo, hi) = (number("lo")?, number("hi")?);
            if !lo.is_finite() || !hi.is_finite() || lo >= hi {
                return Err(SqlError::DataException {
                    detail: format!(
                        "ARRAY_AGG: histogram range [{lo}, {hi}] must be finite with lo < hi"
                    ),
                });
            }
            ArrayReducerAst::Histogram { bins, lo, hi }
        }
    };
    Ok(AggOptions {
        reducer,
        physical: parse_physical(args, "ARRAY_AGG")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(v: &str) -> ast::Expr {
        ast::Expr::Value(ast::Value::Number(v.into(), false).into())
    }

    fn args(pairs: &[(&str, ast::Expr)]) -> Vec<(String, ast::Expr)> {
        pairs
            .iter()
            .map(|(n, e)| ((*n).to_string(), e.clone()))
            .collect()
    }

    #[test]
    fn reducer_names_parse_with_their_parameters() {
        let ok = |s: &str, a: &[(&str, ast::Expr)]| parse_agg_options(s, &args(a)).expect(s);
        assert_eq!(ok("median", &[]).reducer, ArrayReducerAst::Median);
        assert_eq!(ok("stddev", &[]).reducer, ArrayReducerAst::StdDevSamp);
        assert_eq!(ok("variance", &[]).reducer, ArrayReducerAst::VarSamp);
        assert_eq!(ok("VAR_POP", &[]).reducer, ArrayReducerAst::VarPop);
        assert_eq!(
            ok("percentile", &[("p", num("0.9"))]).reducer,
            ArrayReducerAst::Percentile(0.9)
        );
        let neg = ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: Box::new(num("1")),
        };
        assert_eq!(
            ok(
                "histogram",
                &[("bins", num("4")), ("lo", neg), ("hi", num("1"))]
            )
            .reducer,
            ArrayReducerAst::Histogram {
                bins: 4,
                lo: -1.0,
                hi: 1.0
            }
        );
        let physical = ast::Expr::Value(ast::Value::Boolean(true).into());
        assert!(ok("sum", &[("physical", physical)]).physical);
    }

    #[test]
    fn wrong_or_missing_parameters_are_refused() {
        let err = |s: &str, a: &[(&str, ast::Expr)]| parse_agg_options(s, &args(a)).is_err();
        assert!(err("percentile", &[]));
        assert!(err("percentile", &[("p", num("1.5"))]));
        assert!(err("median", &[("p", num("0.5"))]));
        assert!(err("histogram", &[("bins", num("4")), ("lo", num("0"))]));
        assert!(err(
            "histogram",
            &[("bins", num("0")), ("lo", num("0")), ("hi", num("1"))]
        ));
        assert!(err(
            "histogram",
            &[("bins", num("2")), ("lo", num("1")), ("hi", num("1"))]
        ));
        assert!(err("geomean", &[]));
        assert!(err("sum", &[("physical", num("1"))]));
    }
}
