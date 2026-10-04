// SPDX-License-Identifier: Apache-2.0

//! Peer-aware running-aggregate for RANGE BETWEEN UNBOUNDED PRECEDING AND
//! CURRENT ROW.
//!
//! This is the specialised fast path for the most common ordered-window
//! pattern (the PostgreSQL default frame for windows with ORDER BY).
//!
//! Peer semantics: two rows with equal ORDER BY values are in the same peer
//! group. All peers in a group see the same aggregate result: the value
//! computed at the *last* peer in the group (i.e., they all include each
//! other). This matches PostgreSQL behaviour for RANGE CURRENT ROW.

use super::arg::{ArgValues, arg_at};
use super::extremum::{extremum_direction, json_replaces};
use super::helpers::{order_keys_equal, set_window_col};
use super::spec::WindowFuncSpec;
use crate::numeric_sum::ExactSum;

/// Apply a peer-aware running aggregate over a sorted partition.
///
/// `indices` is the sorted slice of row indices within the partition.
/// `arg_values` holds the function argument already evaluated once per
/// partition position; `None` is the no-argument form (`COUNT(*)`).
pub(super) fn running_aggregate(
    rows: &mut [(String, serde_json::Value)],
    indices: &[usize],
    spec: &WindowFuncSpec,
    arg_values: &ArgValues,
) -> Result<(), crate::expr::EvalError> {
    let len = indices.len();
    if len == 0 {
        return Ok(());
    }

    // Accumulate state incrementally row-by-row, but defer writing results
    // until the end of each peer group (so all peers see the group's final
    // value). We track where the current peer group started.
    // SUM / AVG total exactly per `ExactSum`.
    let mut running_sum = ExactSum::new();
    let mut running_count = 0u64;
    // MIN / MAX hold the original argument value, compared exactly.
    let extremum_dir = extremum_direction(&spec.func_name);
    let mut running_extreme: Option<serde_json::Value> = None;

    // Indices of rows belonging to the *current* peer group (deferred write).
    let mut peer_start = 0usize;

    for pos in 0..len {
        let i = indices[pos];
        let val = arg_at(arg_values, pos);
        if running_sum.add_json(&val) {
            running_count += 1;
            if let Some(want_max) = extremum_dir
                && json_replaces(&val, running_extreme.as_ref(), want_max)
            {
                running_extreme = Some(val);
            }
        } else if spec.func_name == "count" && (arg_values.is_none() || !val.is_null()) {
            // `COUNT(*)` counts every row; `COUNT(expr)` counts rows whose
            // argument is non-NULL, including non-numeric values.
            running_count += 1;
        }

        // Check if the *next* row starts a new peer group (or we're at the end).
        let is_last_in_group =
            pos + 1 == len || !order_keys_equal(rows, i, indices[pos + 1], &spec.order_by)?;

        if is_last_in_group {
            // Compute the result at the end of this peer group.
            let result = match spec.func_name.as_str() {
                "sum" => serde_json::Value::from(running_sum.sum()?),
                "count" => serde_json::json!(running_count),
                "avg" => serde_json::Value::from(running_sum.avg()?),
                "min" | "max" => running_extreme.clone().unwrap_or(serde_json::Value::Null),
                "first_value" => arg_at(arg_values, 0),
                "last_value" => arg_at(arg_values, pos),
                _ => serde_json::Value::Null,
            };

            // Write the *same* result to every row in the peer group.
            for &peer_idx in &indices[peer_start..=pos] {
                set_window_col(&mut rows[peer_idx].1, &spec.alias, result.clone());
            }

            peer_start = pos + 1;
        }
    }
    Ok(())
}
