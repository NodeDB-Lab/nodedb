// SPDX-License-Identifier: Apache-2.0

//! Offset window functions: lag, lead, nth_value.

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use super::arg::{arg_at, const_default_arg, const_usize_arg, eval_arg_values};
use super::helpers::set_window_col;
use super::spec::WindowFuncSpec;

pub(super) fn apply_lag(
    rows: &mut [(String, Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let arg_values = eval_arg_values(rows, indices, spec, 0, crs)?;
    let offset = const_usize_arg(spec, 1, 1);
    let default = const_default_arg(spec, 2);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos >= offset {
            arg_at(&arg_values, pos - offset)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
    Ok(())
}

pub(super) fn apply_lead(
    rows: &mut [(String, Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let arg_values = eval_arg_values(rows, indices, spec, 0, crs)?;
    let offset = const_usize_arg(spec, 1, 1);
    let default = const_default_arg(spec, 2);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + offset < indices.len() {
            arg_at(&arg_values, pos + offset)
        } else {
            default.clone()
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
    Ok(())
}

/// PostgreSQL `nth_value(expr, n)` — value of `expr` at the n'th row of the
/// window frame, NULL if the frame doesn't yet contain n rows. Default frame
/// is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so the first n-1
/// rows of each partition return NULL and rows from the n'th onward return
/// the value of `expr` at the n'th row.
pub(super) fn apply_nth_value(
    rows: &mut [(String, Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    crs: &CrsContext<'_>,
) -> Result<(), crate::expr::EvalError> {
    let arg_values = eval_arg_values(rows, indices, spec, 0, crs)?;
    let n = const_usize_arg(spec, 1, 1).max(1);

    for (pos, &i) in indices.iter().enumerate() {
        let val = if pos + 1 >= n {
            arg_at(&arg_values, n - 1)
        } else {
            Value::Null
        };
        set_window_col(&mut rows[i].1, &spec.alias, val);
    }
    Ok(())
}
