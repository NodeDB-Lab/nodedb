// SPDX-License-Identifier: BUSL-1.1

//! The arithmetic one materialized-sum binding performs on one row image.
//!
//! Both planes need the SAME answer from the SAME inputs, exactly as they do
//! for the join keys next door in
//! [`materialized_sum_keys`](super::materialized_sum_keys). The Data Plane folds
//! a write's images into signed deltas inside the source write's transaction;
//! the Control Plane folds the row bodies a plan carries so it can settle a
//! CROSS-SHARD target's delta at plan time and ship it on its own task. A second
//! implementation of the amount rule would be free to drift from the first, and
//! a drift here is a stored total that disagrees with the `SUM(...)` over the
//! source rows.
//!
//! Everything here is pure: a binding and a document in, a decimal out. No
//! storage handle, no transaction, no plane state.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::MaterializedSumBinding;

/// The row's join-key value, or `None` when the row does not participate in the
/// binding at all.
///
/// A join value is a STRING or it is nothing — the same rule
/// [`binding_join_keys`](super::materialized_sum_keys::binding_join_keys)
/// applies, so the two cannot disagree about which rows participate.
pub fn binding_join_value(
    binding: &MaterializedSumBinding,
    doc: &serde_json::Value,
) -> Option<String> {
    doc.get(&binding.join_column)
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Evaluate the binding's value expression against one row image.
///
/// A materialized-sum binding fires on the write path, so a division or modulus
/// by zero fails the write rather than silently skipping the balance update. An
/// expression that evaluates to NULL or to something non-numeric contributes
/// zero — the row is in the binding but has nothing to add.
pub fn binding_amount(
    binding: &MaterializedSumBinding,
    doc: &serde_json::Value,
) -> crate::Result<Decimal> {
    let row = nodedb_types::Value::from(doc.clone());
    let evaluated = binding
        .value_expr
        .eval(&row)
        .map_err(|_e| crate::Error::DivisionByZero)?;
    Ok(json_to_decimal(&serde_json::Value::from(evaluated)).unwrap_or(Decimal::ZERO))
}

/// Convert a JSON value to `rust_decimal::Decimal`.
///
/// Strings parse exactly, which is how a balance survives more than 15
/// significant digits: the stored total is a string for the same reason.
pub fn json_to_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Decimal::from(i))
            } else {
                n.as_f64().and_then(|f| Decimal::try_from(f).ok())
            }
        }
        serde_json::Value::String(s) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

/// Sum the amounts a set of INSERT-shaped row images contributes, per join
/// value, preserving first-seen order.
///
/// Insert-shaped only, and deliberately so: a plan carries the body a row will
/// HAVE, never the body it had, so an update's difference and a delete's
/// reversal cannot be derived from the plan alone. Those shapes are folded on
/// the Data Plane, from the real pre- and post-images, where the answer is
/// exact.
pub fn binding_insert_deltas(
    binding: &MaterializedSumBinding,
    docs: &[serde_json::Value],
) -> crate::Result<Vec<(String, Decimal)>> {
    let mut totals: Vec<(String, Decimal)> = Vec::new();
    for doc in docs {
        let Some(join_value) = binding_join_value(binding, doc) else {
            continue;
        };
        let amount = binding_amount(binding, doc)?;
        match totals.iter().position(|(value, _)| *value == join_value) {
            Some(index) => totals[index].1 += amount,
            None => totals.push((join_value, amount)),
        }
    }
    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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

    /// A page of rows against one account settles as ONE delta: the target row
    /// is read and written once, whichever plane performs the write.
    #[test]
    fn rows_against_one_target_coalesce() {
        let deltas = binding_insert_deltas(&binding(), &[row("a1", 25), row("a1", 75)])
            .expect("fold succeeds");
        assert_eq!(
            deltas,
            vec![("a1".to_string(), Decimal::from_str("100").expect("decimal"))]
        );
    }

    /// A row that carries no join value is not in the binding, so it owes no
    /// target anything — the same conclusion the Data-Plane fold reaches.
    #[test]
    fn a_row_without_the_join_column_owes_nothing() {
        let deltas = binding_insert_deltas(&binding(), &[serde_json::json!({"amount": 25})])
            .expect("fold succeeds");
        assert!(deltas.is_empty());
    }

    #[test]
    fn a_string_amount_parses_exactly() {
        let doc = serde_json::json!({"account_id": "a1", "amount": "1500.75"});
        assert_eq!(
            binding_amount(&binding(), &doc).expect("evaluate"),
            Decimal::from_str("1500.75").expect("decimal")
        );
    }
}
