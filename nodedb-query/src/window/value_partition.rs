// SPDX-License-Identifier: Apache-2.0

//! Partition building and per-partition ORDER BY sort for the Value-native
//! window evaluator.

use std::collections::HashMap;

use nodedb_types::Value;

use super::helpers::null_order;
use super::spec::WindowFuncSpec;
use super::value_eval::{WindowError, cmp_values, eval_arg_for_row};
use crate::expr::types::SqlExpr;

/// Group row indices by partition key, preserving first-seen partition
/// order, then sort each partition's indices by the spec's ORDER BY.
///
/// The returned index lists are ordered by `spec.order_by`; `rows` itself
/// keeps its input order — only the per-partition index lists move.
pub(super) fn build_value_partitions(
    rows: &[Vec<Value>],
    column_index: &HashMap<String, usize>,
    spec: &WindowFuncSpec,
) -> Result<Vec<Vec<usize>>, WindowError> {
    let mut partitions = if spec.partition_by.is_empty() {
        vec![(0..rows.len()).collect::<Vec<usize>>()]
    } else {
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for (i, row) in rows.iter().enumerate() {
            let key = partition_key(row, column_index, &spec.partition_by)?;
            let entry = groups.entry(key.clone()).or_default();
            if entry.is_empty() {
                order.push(key);
            }
            entry.push(i);
        }

        order.iter().filter_map(|k| groups.remove(k)).collect()
    };

    if !spec.order_by.is_empty() {
        let mut keys: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            keys.push(
                spec.order_by
                    .iter()
                    .map(|(expr, _)| eval_arg_for_row(expr, row, column_index))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }

        for partition in &mut partitions {
            partition.sort_by(|&a, &b| compare_order_keys(&keys[a], &keys[b], &spec.order_by));
        }
    }

    Ok(partitions)
}

fn partition_key(
    row: &[Value],
    column_index: &HashMap<String, usize>,
    partition_by: &[SqlExpr],
) -> Result<String, WindowError> {
    Ok(partition_by
        .iter()
        .map(|expr| {
            let v = eval_arg_for_row(expr, row, column_index)?;
            Ok(format!("{v:?}"))
        })
        .collect::<Result<Vec<_>, WindowError>>()?
        .join("\x00"))
}

/// Compare two rows' pre-evaluated ORDER BY keys.
fn compare_order_keys(
    a: &[Value],
    b: &[Value],
    order_by: &[(SqlExpr, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (idx, (_, ascending)) in order_by.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(idx), b.get(idx)) else {
            continue;
        };
        let ord = null_order(
            matches!(va, Value::Null),
            matches!(vb, Value::Null),
            *ascending,
        )
        .unwrap_or_else(|| {
            let c = cmp_values(va, vb);
            if *ascending { c } else { c.reverse() }
        });
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}
