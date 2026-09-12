// SPDX-License-Identifier: BUSL-1.1

//! The pre-/post-image arithmetic one materialized-sum binding performs, and
//! the post-image a statement's assignments produce.
//!
//! Both planes need the SAME answer from the SAME inputs, exactly as they do
//! for the join keys in [`materialized_sum_keys`](super::materialized_sum_keys)
//! and the amounts in [`materialized_sum_delta`](super::materialized_sum_delta).
//!
//! * the **Data Plane** folds a write's real images inside the source write's
//!   transaction, for every target that shares the source's core;
//! * the **Control Plane** folds the images it can read at plan time — the
//!   stored row a point write rewrites, the rows a predicate matches — so it can
//!   settle a CROSS-SHARD target's delta and ship it on a task of its own.
//!
//! A second implementation of the difference rule would be free to drift from
//! the first, and a drift here is a stored total that disagrees with the
//! `SUM(...)` over the source rows. So the rule lives here once and both planes
//! call it.
//!
//! Everything is pure: documents in, decimals out. No storage handle, no
//! transaction, no plane state.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::{MaterializedSumBinding, UpdateValue};

use super::materialized_sum_delta::{binding_amount, binding_join_value};

/// One signed contribution a write makes to one target row's running total.
///
/// `join_value` is the JOIN-KEY VALUE the source row carries, not a storage
/// key: it selects which resolved target-row surrogate addresses the row to
/// write.
#[derive(Debug, Clone, PartialEq)]
pub struct BindingDelta {
    /// Value of the source row's join column.
    pub join_value: String,
    /// Signed amount to add to the target's balance column. Negative on the
    /// losing side of a DELETE or a join-key move.
    pub delta: Decimal,
}

/// Fold one binding over one write's pre-/post-image pair into the signed
/// deltas it owes.
///
/// `old` is `None` for an INSERT-shaped write and `new` is `None` for a
/// DELETE-shaped one; both `None` is a write that touched no row and owes
/// nothing.
///
/// # The join-key move
///
/// A pair whose join key CHANGED yields TWO deltas against TWO target rows: the
/// old one loses the row's old value, the new one gains its new one. The old
/// side comes first, which is the order both planes' callers rely on when they
/// pair a delta with the task that carries it.
///
/// A row that carries no join value contributes nothing: it does not
/// participate in the binding at all, so there is no target for it to move
/// value onto.
pub fn binding_image_deltas(
    binding: &MaterializedSumBinding,
    old: Option<&serde_json::Value>,
    new: Option<&serde_json::Value>,
) -> crate::Result<Vec<BindingDelta>> {
    match (old, new) {
        (None, None) => Ok(Vec::new()),
        (None, Some(new_doc)) => Ok(single(binding, new_doc, Sign::Plus)?.into_iter().collect()),
        (Some(old_doc), None) => Ok(single(binding, old_doc, Sign::Minus)?.into_iter().collect()),
        (Some(old_doc), Some(new_doc)) => {
            let old_target = binding_join_value(binding, old_doc);
            let new_target = binding_join_value(binding, new_doc);
            match (old_target, new_target) {
                // Same target: only the DIFFERENCE moves. Re-adding the new
                // value in full would double-count the row's old contribution.
                (Some(old_target), Some(new_target)) if old_target == new_target => {
                    let delta =
                        binding_amount(binding, new_doc)? - binding_amount(binding, old_doc)?;
                    Ok(vec![BindingDelta {
                        join_value: new_target,
                        delta,
                    }])
                }
                // The join-key move: two targets, two writes, opposite signs.
                (Some(old_target), Some(new_target)) => Ok(vec![
                    BindingDelta {
                        join_value: old_target,
                        delta: -binding_amount(binding, old_doc)?,
                    },
                    BindingDelta {
                        join_value: new_target,
                        delta: binding_amount(binding, new_doc)?,
                    },
                ]),
                // The row left the binding (its join column was cleared) or
                // joined it (the column was set). One side only.
                (Some(old_target), None) => Ok(vec![BindingDelta {
                    join_value: old_target,
                    delta: -binding_amount(binding, old_doc)?,
                }]),
                (None, Some(new_target)) => Ok(vec![BindingDelta {
                    join_value: new_target,
                    delta: binding_amount(binding, new_doc)?,
                }]),
                (None, None) => Ok(Vec::new()),
            }
        }
    }
}

/// Sum a set of image pairs' deltas per join value, preserving first-seen
/// order.
///
/// Two rows moving value onto the same target settle as ONE balance write: the
/// target row is read and written once, whichever plane performs the write.
pub fn coalesce_binding_deltas(deltas: Vec<BindingDelta>) -> Vec<(String, Decimal)> {
    let mut totals: Vec<(String, Decimal)> = Vec::with_capacity(deltas.len());
    for entry in deltas {
        match totals
            .iter()
            .position(|(join_value, _)| *join_value == entry.join_value)
        {
            Some(index) => totals[index].1 += entry.delta,
            None => totals.push((entry.join_value, entry.delta)),
        }
    }
    totals
}

/// The document a statement's `SET` assignments produce from one pre-image.
///
/// Assignments are evaluated against the PRE-image snapshot, so a later
/// assignment observing a column an earlier one wrote still sees the pre-update
/// value — the rule the write path applies, and the one
/// [`binding_join_keys`](super::materialized_sum_keys::binding_join_keys)
/// already applies to the join column alone.
///
/// A literal that will not decode is the field the write path silently leaves
/// unassigned, so it is left unassigned here too. An expression that will not
/// evaluate fails the statement, exactly as it does on the write path.
///
/// Generated columns are NOT recomputed here: their definitions are catalog
/// state and the write path recomputes them from the stored row. A binding
/// whose value or join column is itself a generated column therefore folds from
/// the assigned columns only — the same approximation
/// [`binding_join_keys`](super::materialized_sum_keys::binding_join_keys)
/// makes.
pub fn apply_update_assignments(
    doc: &serde_json::Value,
    updates: &[(String, UpdateValue)],
) -> crate::Result<serde_json::Value> {
    assign(doc, updates, None)
}

/// The document an UPSERT's CONFLICT branch produces from the stored row and
/// the body the statement submitted.
///
/// The conflict branch has two forms and they produce different post-images, so
/// both are spelled out here rather than approximated by one of them:
///
/// * with no `ON CONFLICT DO UPDATE SET` assignments, the submitted body is
///   OVERLAID onto the stored row — the columns it carries win, the ones it
///   omits are kept;
/// * with assignments, the stored row is rewritten by them, and `EXCLUDED.col`
///   resolves against the SUBMITTED body.
///
/// Both are what the write path does. Evaluating the assignments without the
/// submitted body would resolve every `EXCLUDED.col` to NULL — and
/// `SET amount = EXCLUDED.amount`, the ordinary way to write this statement,
/// would then fold as if the row's value had been cleared: the target loses the
/// row's whole old contribution instead of gaining the difference.
pub fn apply_conflict_assignments(
    stored: &serde_json::Value,
    submitted: &serde_json::Value,
    updates: &[(String, UpdateValue)],
) -> crate::Result<serde_json::Value> {
    if updates.is_empty() {
        return Ok(overlay(stored, submitted));
    }
    assign(stored, updates, Some(submitted))
}

/// Apply a statement's assignments to one pre-image, with `excluded` standing in
/// for the `EXCLUDED.*` row when the statement has one.
///
/// `None` leaves `EXCLUDED.col` resolving to NULL, which is what the evaluator
/// does for a statement that carries no such row at all.
fn assign(
    doc: &serde_json::Value,
    updates: &[(String, UpdateValue)],
    excluded: Option<&serde_json::Value>,
) -> crate::Result<serde_json::Value> {
    if updates.is_empty() {
        return Ok(doc.clone());
    }
    let snapshot = nodedb_types::Value::from(doc.clone());
    let excluded_row = excluded.map(|row| nodedb_types::Value::from(row.clone()));
    let mut post = doc.clone();
    let Some(object) = post.as_object_mut() else {
        return Ok(post);
    };
    for (field, value) in updates {
        match value {
            UpdateValue::Literal(bytes) => {
                if let Ok(decoded) = nodedb_types::json_from_msgpack(bytes) {
                    object.insert(field.clone(), decoded);
                }
            }
            UpdateValue::Expr(expr) => {
                let evaluated = match &excluded_row {
                    Some(row) => expr.eval_with_excluded(&snapshot, row),
                    None => expr.eval(&snapshot),
                }
                .map_err(crate::Error::from)?;
                object.insert(field.clone(), serde_json::Value::from(evaluated));
            }
        }
    }
    Ok(post)
}

/// Overlay `submitted`'s fields onto `stored`, the merge an UPSERT with no
/// conflict assignments performs. Two images that are not both objects resolve
/// to the submitted one entirely, exactly as the write path's merge does.
fn overlay(stored: &serde_json::Value, submitted: &serde_json::Value) -> serde_json::Value {
    match (stored.as_object(), submitted.as_object()) {
        (Some(base), Some(incoming)) => {
            let mut merged = base.clone();
            for (field, value) in incoming {
                merged.insert(field.clone(), value.clone());
            }
            serde_json::Value::Object(merged)
        }
        _ => submitted.clone(),
    }
}

/// Which way a single-image contribution points.
enum Sign {
    Plus,
    Minus,
}

/// The one delta a single row image contributes, or `None` when the row does
/// not participate in the binding.
fn single(
    binding: &MaterializedSumBinding,
    doc: &serde_json::Value,
    sign: Sign,
) -> crate::Result<Option<BindingDelta>> {
    let Some(join_value) = binding_join_value(binding, doc) else {
        return Ok(None);
    };
    let amount = binding_amount(binding, doc)?;
    Ok(Some(BindingDelta {
        join_value,
        delta: match sign {
            Sign::Plus => amount,
            Sign::Minus => -amount,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("decimal literal")
    }

    fn binding() -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: "accounts".to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }
    }

    fn row(account: &str, amount: i64) -> serde_json::Value {
        serde_json::json!({"account_id": account, "amount": amount})
    }

    #[test]
    fn an_insert_shaped_pair_adds_the_whole_value() {
        let new_doc = row("a1", 25);
        assert_eq!(
            binding_image_deltas(&binding(), None, Some(&new_doc)).expect("fold"),
            vec![BindingDelta {
                join_value: "a1".to_string(),
                delta: d("25"),
            }]
        );
    }

    #[test]
    fn a_delete_shaped_pair_subtracts_the_whole_value() {
        let old_doc = row("a1", 25);
        assert_eq!(
            binding_image_deltas(&binding(), Some(&old_doc), None).expect("fold"),
            vec![BindingDelta {
                join_value: "a1".to_string(),
                delta: d("-25"),
            }]
        );
    }

    #[test]
    fn an_in_place_update_contributes_only_the_difference() {
        let old_doc = row("a1", 25);
        let new_doc = row("a1", 40);
        assert_eq!(
            binding_image_deltas(&binding(), Some(&old_doc), Some(&new_doc)).expect("fold"),
            vec![BindingDelta {
                join_value: "a1".to_string(),
                delta: d("15"),
            }]
        );
    }

    /// The join-key MOVE: the abandoned target first, losing the OLD value; the
    /// joined target second, gaining the NEW one. Both planes ship one balance
    /// write per entry, so the order is part of the contract.
    #[test]
    fn a_join_key_move_yields_the_old_side_first() {
        let old_doc = row("a1", 25);
        let new_doc = row("a2", 40);
        let deltas =
            binding_image_deltas(&binding(), Some(&old_doc), Some(&new_doc)).expect("fold");
        assert_eq!(
            deltas,
            vec![
                BindingDelta {
                    join_value: "a1".to_string(),
                    delta: d("-25"),
                },
                BindingDelta {
                    join_value: "a2".to_string(),
                    delta: d("40"),
                },
            ]
        );
    }

    #[test]
    fn a_row_outside_the_binding_owes_nothing() {
        let new_doc = serde_json::json!({"amount": 25});
        assert!(
            binding_image_deltas(&binding(), None, Some(&new_doc))
                .expect("fold")
                .is_empty()
        );
    }

    #[test]
    fn deltas_against_one_target_coalesce() {
        let totals = coalesce_binding_deltas(vec![
            BindingDelta {
                join_value: "a1".to_string(),
                delta: d("25"),
            },
            BindingDelta {
                join_value: "a1".to_string(),
                delta: d("-5"),
            },
            BindingDelta {
                join_value: "a2".to_string(),
                delta: d("7"),
            },
        ]);
        assert_eq!(
            totals,
            vec![("a1".to_string(), d("20")), ("a2".to_string(), d("7"))]
        );
    }

    /// A literal assignment produces the post-image the write path stores.
    #[test]
    fn a_literal_assignment_lands_on_the_post_image() {
        let literal = nodedb_types::json_to_msgpack(&serde_json::json!("a9")).expect("encode");
        let updates = vec![("account_id".to_string(), UpdateValue::Literal(literal))];
        let post = apply_update_assignments(&row("a1", 25), &updates).expect("assign");
        assert_eq!(post.get("account_id").and_then(|v| v.as_str()), Some("a9"));
        assert_eq!(post.get("amount").and_then(|v| v.as_i64()), Some(25));
    }

    /// Assignments are evaluated against the PRE-image, so two assignments in
    /// one statement do not observe each other.
    #[test]
    fn expression_assignments_read_the_pre_image() {
        let updates = vec![
            (
                "amount".to_string(),
                UpdateValue::Expr(nodedb_query::expr::SqlExpr::Column(
                    "account_id".to_string(),
                )),
            ),
            (
                "account_id".to_string(),
                UpdateValue::Expr(nodedb_query::expr::SqlExpr::Column("amount".to_string())),
            ),
        ];
        let post = apply_update_assignments(&row("a1", 25), &updates).expect("assign");
        assert_eq!(post.get("amount").and_then(|v| v.as_str()), Some("a1"));
        assert_eq!(post.get("account_id").and_then(|v| v.as_i64()), Some(25));
    }

    /// A statement with no assignments rewrites nothing — the shape a whole-row
    /// PUT and a DELETE both take.
    #[test]
    fn no_assignments_leaves_the_document_alone() {
        let doc = row("a1", 25);
        assert_eq!(apply_update_assignments(&doc, &[]).expect("assign"), doc);
    }

    /// `SET col = EXCLUDED.col` resolves against the SUBMITTED body.
    ///
    /// Without it the post-image's value column is NULL, which folds to zero:
    /// the target loses the row's whole old contribution instead of gaining the
    /// difference between the stored row and the merged one.
    #[test]
    fn a_conflict_assignment_reads_the_submitted_body() {
        let updates = vec![(
            "amount".to_string(),
            UpdateValue::Expr(nodedb_query::expr::SqlExpr::ExcludedColumn(
                "amount".to_string(),
            )),
        )];
        let post = apply_conflict_assignments(&row("a1", 25), &row("a1", 60), &updates)
            .expect("conflict assign");
        assert_eq!(post.get("amount").and_then(|v| v.as_i64()), Some(60));
        assert_eq!(
            binding_image_deltas(&binding(), Some(&row("a1", 25)), Some(&post)).expect("fold"),
            vec![BindingDelta {
                join_value: "a1".to_string(),
                delta: d("35"),
            }]
        );
    }

    /// A conflict assignment still reads the STORED row for a plain column
    /// reference, so `SET amount = amount + EXCLUDED.amount` accumulates.
    #[test]
    fn a_conflict_assignment_still_reads_the_stored_row() {
        let updates = vec![(
            "amount".to_string(),
            UpdateValue::Expr(nodedb_query::expr::SqlExpr::BinaryOp {
                left: Box::new(nodedb_query::expr::SqlExpr::Column("amount".to_string())),
                op: nodedb_query::expr::BinaryOp::Add,
                right: Box::new(nodedb_query::expr::SqlExpr::ExcludedColumn(
                    "amount".to_string(),
                )),
            }),
        )];
        let post = apply_conflict_assignments(&row("a1", 25), &row("a1", 60), &updates)
            .expect("conflict assign");
        assert_eq!(post.get("amount").and_then(|v| v.as_i64()), Some(85));
    }

    /// An UPSERT with NO conflict assignments merges the submitted body over the
    /// stored row: the columns it carries win, the ones it omits are kept.
    ///
    /// Treating it as "no assignments, so nothing changed" would settle a zero
    /// delta for a statement that did move the row's value.
    #[test]
    fn a_conflict_branch_without_assignments_overlays_the_body() {
        let stored = serde_json::json!({"account_id": "a1", "amount": 25, "memo": "kept"});
        let submitted = serde_json::json!({"account_id": "a1", "amount": 60});
        let post = apply_conflict_assignments(&stored, &submitted, &[]).expect("conflict merge");
        assert_eq!(post.get("amount").and_then(|v| v.as_i64()), Some(60));
        assert_eq!(
            post.get("memo").and_then(|v| v.as_str()),
            Some("kept"),
            "a column the body omits survives the merge"
        );
    }

    /// A conflict branch that rewrites the join column moves the row between two
    /// targets, and the value it moves comes off the submitted body.
    #[test]
    fn a_conflict_assignment_can_move_the_join_key() {
        let updates = vec![
            (
                "account_id".to_string(),
                UpdateValue::Expr(nodedb_query::expr::SqlExpr::ExcludedColumn(
                    "account_id".to_string(),
                )),
            ),
            (
                "amount".to_string(),
                UpdateValue::Expr(nodedb_query::expr::SqlExpr::ExcludedColumn(
                    "amount".to_string(),
                )),
            ),
        ];
        let stored = row("a1", 25);
        let post =
            apply_conflict_assignments(&stored, &row("a2", 60), &updates).expect("conflict assign");
        assert_eq!(
            binding_image_deltas(&binding(), Some(&stored), Some(&post)).expect("fold"),
            vec![
                BindingDelta {
                    join_value: "a1".to_string(),
                    delta: d("-25"),
                },
                BindingDelta {
                    join_value: "a2".to_string(),
                    delta: d("60"),
                },
            ]
        );
    }
}
