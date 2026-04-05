//! WHEN clause batch filtering.
//!
//! Evaluates a trigger's WHEN predicate against each row in a batch,
//! returning a boolean mask indicating which rows should fire the trigger.
//!
//! Fast path: simple `NEW.field OP value` conditions are parsed into a
//! `ScanFilter` and evaluated directly on raw MessagePack bytes via
//! `matches_binary`, avoiding HashMap decode for non-matching rows.

use super::collector::TriggerBatchRow;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::trigger::fire_common::evaluate_simple_condition;
use crate::control::trigger::when_parse::WhenTarget;

/// Filter a batch of rows by a WHEN clause predicate.
///
/// Returns a boolean mask: `true` = this row should fire the trigger.
/// Rows where the WHEN condition evaluates to false are skipped.
///
/// If `when_condition` is `None`, all rows pass (trigger always fires).
///
/// ## Fast-reject path
///
/// If the WHEN condition is a simple `NEW.field OP value` or `OLD.field OP
/// value` expression, it is parsed at call time into a `ScanFilter` and
/// evaluated via `matches_binary` on the raw MessagePack bytes. This avoids
/// full HashMap decode for rows that do not match the predicate.
///
/// If raw bytes are unavailable (rows created via `from_decoded`, i.e. in
/// tests) the function falls through to the existing decode + substitute path.
pub fn filter_batch_by_when(
    rows: &[TriggerBatchRow],
    collection: &str,
    operation: &str,
    when_condition: Option<&str>,
) -> Vec<bool> {
    let when_cond = match when_condition {
        Some(cond) => cond,
        None => return vec![true; rows.len()],
    };

    // Attempt to parse as binary-evaluable filter(s) — supports AND-joined predicates.
    if let Some((target, filters)) =
        crate::control::trigger::when_parse::try_parse_when_to_filters(when_cond)
    {
        return rows
            .iter()
            .map(|row| {
                let raw = match target {
                    WhenTarget::New => row.new_raw(),
                    WhenTarget::Old => row.old_raw(),
                };
                match raw {
                    Some(bytes) => filters.iter().all(|f| f.matches_binary(bytes)),
                    // No raw bytes (test rows from from_decoded) — fall back to
                    // the decode + substitute path for this row.
                    None => {
                        let bindings = build_row_bindings(row, collection, operation);
                        let bound_cond = bindings.substitute(when_cond);
                        evaluate_simple_condition(&bound_cond)
                    }
                }
            })
            .collect();
    }

    // General path: substitute bindings and evaluate.
    rows.iter()
        .map(|row| {
            let bindings = build_row_bindings(row, collection, operation);
            let bound_cond = bindings.substitute(when_cond);
            evaluate_simple_condition(&bound_cond)
        })
        .collect()
}

/// Build [`RowBindings`] for a single batch row.
///
/// Used both for WHEN clause substitution and for trigger body dispatch.
pub fn build_row_bindings(row: &TriggerBatchRow, collection: &str, operation: &str) -> RowBindings {
    let new_row = row
        .new_fields()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let old_row = row
        .old_fields()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

    match operation {
        "INSERT" => RowBindings::after_insert(collection, new_row),
        "UPDATE" => RowBindings::after_update(collection, old_row.unwrap_or_default(), new_row),
        "DELETE" => RowBindings::after_delete(collection, old_row.unwrap_or_default()),
        _ => RowBindings::after_insert(collection, new_row),
    }
}

/// Count how many rows pass the filter.
pub fn count_passing(mask: &[bool]) -> usize {
    mask.iter().filter(|&&b| b).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_with_field(key: &str, val: nodedb_types::Value) -> TriggerBatchRow {
        let mut map = std::collections::HashMap::new();
        map.insert(key.to_string(), val);
        TriggerBatchRow::from_decoded(Some(map), None, "r1".into())
    }

    #[test]
    fn no_when_all_pass() {
        let rows = vec![
            row_with_field("x", nodedb_types::Value::Integer(1)),
            row_with_field("x", nodedb_types::Value::Integer(2)),
        ];
        let mask = filter_batch_by_when(&rows, "c", "INSERT", None);
        assert_eq!(mask, vec![true, true]);
    }

    #[test]
    fn when_true_all_pass() {
        let rows = vec![row_with_field("x", nodedb_types::Value::Integer(1))];
        let mask = filter_batch_by_when(&rows, "c", "INSERT", Some("TRUE"));
        assert_eq!(mask, vec![true]);
    }

    #[test]
    fn when_false_none_pass() {
        let rows = vec![
            row_with_field("x", nodedb_types::Value::Integer(1)),
            row_with_field("x", nodedb_types::Value::Integer(2)),
        ];
        let mask = filter_batch_by_when(&rows, "c", "INSERT", Some("FALSE"));
        assert_eq!(mask, vec![false, false]);
    }

    #[test]
    fn count_passing_works() {
        assert_eq!(count_passing(&[true, false, true, true, false]), 3);
        assert_eq!(count_passing(&[false, false]), 0);
        assert_eq!(count_passing(&[]), 0);
    }
}
