// SPDX-License-Identifier: BUSL-1.1

//! The join-key values a set of row images needs resolved before a
//! materialized-sum binding can be folded onto its target rows.
//!
//! Both planes need the SAME answer from the SAME inputs. The Control Plane
//! computes it over the rows a reconnaissance scan returned, so it knows which
//! join values to resolve into target-row surrogates; the Data-Plane leader
//! recomputes it over the rows the write ACTUALLY matched, so it can refuse to
//! write when the two disagree. A second implementation of this fold would be
//! free to drift from the first, and a drift here is a stored total that
//! disagrees with the `SUM(...)` over the source rows — so the fold lives in one
//! plane-neutral place.
//!
//! Everything here is pure: documents in, join values out. No storage handle, no
//! transaction, no plane state.

use nodedb_physical::physical_plan::{MaterializedSumBinding, ResolvedSumTarget, UpdateValue};

/// Sorted, distinct join values `binding` needs resolved for `rows`, given the
/// statement's `SET` assignments.
///
/// Each row contributes its CURRENT join value and — when the statement assigns
/// the binding's join column — the value that column will hold afterwards. Both
/// sides are needed: an update that rewrites the join key debits the target it
/// leaves and credits the one it joins, so a resolution covering one side only
/// leaves the other's total wrong.
///
/// A row whose join column is absent or is not a string does not participate in
/// the binding at all, exactly as the delta fold treats it — there is no target
/// for it to move value onto.
///
/// `updates` is empty for a statement that writes no assignments (a delete, a
/// truncate) and for a caller that hands in the POST-images itself rather than
/// asking for them to be derived.
pub fn binding_join_keys(
    binding: &MaterializedSumBinding,
    updates: &[(String, UpdateValue)],
    rows: &[serde_json::Value],
) -> crate::Result<Vec<String>> {
    let assignment = updates
        .iter()
        .find(|(field, _)| *field == binding.join_column)
        .map(|(_, value)| value);

    let mut keys: Vec<String> = Vec::new();
    for row in rows {
        if let Some(value) = string_value(row.get(&binding.join_column)) {
            keys.push(value);
        }
    }

    match assignment {
        // A literal assignment is a plan constant — the same post-image join
        // value for every matched row — so it is decoded once rather than per
        // row. A literal that will not decode is the field the write path
        // silently leaves unassigned, so it contributes no new join value here
        // either.
        Some(UpdateValue::Literal(bytes)) => {
            if !rows.is_empty()
                && let Ok(decoded) = nodedb_types::json_from_msgpack(bytes)
                && let Some(value) = string_value(Some(&decoded))
            {
                keys.push(value);
            }
        }
        // An expression assignment is evaluated against the row's PRE-image,
        // which is what the write path evaluates it against: assignments in one
        // statement do not observe each other.
        Some(UpdateValue::Expr(expr)) => {
            for row in rows {
                let evaluated = expr
                    .eval(&nodedb_types::Value::from(row.clone()))
                    .map_err(crate::Error::from)?;
                if let Some(value) = string_value(Some(&serde_json::Value::from(evaluated))) {
                    keys.push(value);
                }
            }
        }
        None => {}
    }

    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// The first value in `required` that `resolved` does not bind a surrogate to
/// FOR `target_collection`.
///
/// The target collection is part of the question, not context: a resolution
/// entry for some other binding's target happens to carry the same join value
/// and answers nothing here. Ignoring it would report coverage the write does
/// not have, and the fold would then address the wrong row.
///
/// `None` means the resolution covers every row this binding will address.
pub fn missing_join_key<'a>(
    target_collection: &str,
    required: &'a [String],
    resolved: &[ResolvedSumTarget],
) -> Option<&'a str> {
    required.iter().map(String::as_str).find(|key| {
        !resolved
            .iter()
            .any(|entry| entry.addresses(target_collection, key))
    })
}

/// A join value is a STRING or it is nothing — the same rule the delta fold
/// applies, so the two cannot disagree about which rows participate.
fn string_value(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| v.as_str()).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: "accounts".to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }
    }

    fn row(account: &str) -> serde_json::Value {
        serde_json::json!({"account_id": account, "amount": 10})
    }

    #[test]
    fn rows_contribute_their_current_join_value_once() {
        let keys = binding_join_keys(&binding(), &[], &[row("a1"), row("a1"), row("a2")])
            .expect("fold succeeds");
        assert_eq!(keys, vec!["a1".to_string(), "a2".to_string()]);
    }

    /// A row that does not carry the join column is not in the binding, so it
    /// needs no target resolved.
    #[test]
    fn a_row_without_the_join_column_needs_nothing() {
        let keys = binding_join_keys(&binding(), &[], &[serde_json::json!({"amount": 10})])
            .expect("fold succeeds");
        assert!(keys.is_empty());
    }

    /// An assignment that rewrites the join key needs BOTH targets resolved:
    /// the one the row leaves and the one it joins. Resolving only the old side
    /// leaves the new target's total short by the row's whole value.
    #[test]
    fn rewriting_the_join_key_needs_both_sides() {
        let literal = nodedb_types::json_to_msgpack(&serde_json::json!("a9")).expect("encode");
        let updates = vec![("account_id".to_string(), UpdateValue::Literal(literal))];
        let keys = binding_join_keys(&binding(), &updates, &[row("a1")]).expect("fold succeeds");
        assert_eq!(keys, vec!["a1".to_string(), "a9".to_string()]);
    }

    /// A statement that assigns the join column but matches no row needs no
    /// target: there is nothing to move.
    #[test]
    fn an_assignment_with_no_matched_rows_needs_nothing() {
        let literal = nodedb_types::json_to_msgpack(&serde_json::json!("a9")).expect("encode");
        let updates = vec![("account_id".to_string(), UpdateValue::Literal(literal))];
        let keys = binding_join_keys(&binding(), &updates, &[]).expect("fold succeeds");
        assert!(keys.is_empty());
    }

    /// An expression assignment is evaluated per row against its PRE-image.
    #[test]
    fn an_expression_assignment_contributes_its_evaluated_value() {
        let updates = vec![(
            "account_id".to_string(),
            UpdateValue::Expr(nodedb_query::expr::SqlExpr::Column("owner".to_string())),
        )];
        let rows = vec![serde_json::json!({"account_id": "a1", "owner": "a7"})];
        let keys = binding_join_keys(&binding(), &updates, &rows).expect("fold succeeds");
        assert_eq!(keys, vec!["a1".to_string(), "a7".to_string()]);
    }

    #[test]
    fn a_covered_resolution_reports_nothing_missing() {
        let resolved = vec![ResolvedSumTarget::new(
            "accounts",
            "a1",
            nodedb_types::Surrogate::new(4),
        )];
        assert_eq!(
            missing_join_key("accounts", &["a1".to_string()], &resolved),
            None
        );
    }

    #[test]
    fn an_uncovered_join_value_is_reported() {
        let resolved = vec![ResolvedSumTarget::new(
            "accounts",
            "a1",
            nodedb_types::Surrogate::new(4),
        )];
        assert_eq!(
            missing_join_key("accounts", &["a1".to_string(), "a2".to_string()], &resolved),
            Some("a2")
        );
    }

    /// A resolution entry for ANOTHER target that carries the same join value
    /// covers nothing here. Reported as missing, the statement retries against a
    /// fresh recon rather than folding this binding's delta into the other
    /// binding's row.
    #[test]
    fn a_sibling_targets_entry_does_not_cover_this_binding() {
        let resolved = vec![ResolvedSumTarget::new(
            "accounts",
            "a1",
            nodedb_types::Surrogate::new(4),
        )];
        assert_eq!(
            missing_join_key("audit_totals", &["a1".to_string()], &resolved),
            Some("a1")
        );
    }
}
