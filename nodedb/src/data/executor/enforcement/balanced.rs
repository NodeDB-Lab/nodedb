// SPDX-License-Identifier: BUSL-1.1

//! BALANCED constraint: at the end of a write boundary, for each distinct
//! group_key value, `SUM(amount WHERE entry_type = debit_value)` must equal
//! `SUM(amount WHERE entry_type = credit_value)`.
//!
//! The boundary is a single transaction (cross-transaction balance is an
//! application concern), and an autocommit statement IS a transaction — so a
//! statement that writes one leg of a journal on its own is unbalanced by the
//! definition and is refused.
//!
//! Every mutation a boundary performs contributes, with the sign of its effect
//! on the stored set: an INSERT adds its post-image, a DELETE subtracts its
//! pre-image, and an UPDATE does both. A boundary that only ever added would
//! let a transaction remove one leg of a balanced journal and still pass, which
//! is the same "unbalanced ledger" state the constraint exists to refuse.

use rust_decimal::Decimal;
use std::collections::BTreeMap;

use crate::data::executor::enforcement::images::RowImages;
use nodedb_physical::physical_plan::BalancedDef;

/// One row image's signed contribution to a boundary's balance.
pub struct BalancedEntry {
    /// Value of the group_key column (e.g. journal_id).
    pub group_key: String,
    /// Value of the entry_type column (e.g. "DEBIT" or "CREDIT").
    pub entry_type: String,
    /// Monetary amount, NEGATIVE for a pre-image the boundary removed.
    pub amount: Decimal,
}

impl BalancedEntry {
    /// The same entry as the removal of the row it describes.
    fn negated(self) -> Self {
        Self {
            group_key: self.group_key,
            entry_type: self.entry_type,
            amount: -self.amount,
        }
    }
}

/// The signed entries one write contributes to its boundary's balance.
///
/// The mutation shape decides the signs, which is why this takes [`RowImages`]
/// rather than a document plus a flag: a caller cannot describe a delete as an
/// insert, and an update always carries both legs — the old contribution out,
/// the new one in.
pub(in crate::data::executor) fn entries_for(
    def: &BalancedDef,
    images: &RowImages<'_>,
) -> Vec<BalancedEntry> {
    match images {
        RowImages::Insert { new_doc } => extract_entry(def, new_doc).into_iter().collect(),
        RowImages::Delete { old_doc } => extract_entry(def, old_doc)
            .map(BalancedEntry::negated)
            .into_iter()
            .collect(),
        RowImages::Update { old_doc, new_doc } => extract_entry(def, old_doc)
            .map(BalancedEntry::negated)
            .into_iter()
            .chain(extract_entry(def, new_doc))
            .collect(),
    }
}

/// Extract a balanced entry from a JSON document using the constraint definition.
///
/// Returns `None` if any required field is missing (the row is not part of
/// the balanced group and is ignored by the constraint).
pub fn extract_entry(def: &BalancedDef, doc: &serde_json::Value) -> Option<BalancedEntry> {
    let obj = doc.as_object()?;

    let group_key = obj.get(&def.group_key_column)?.as_str().map(String::from)?;

    let entry_type = obj
        .get(&def.entry_type_column)?
        .as_str()
        .map(String::from)?;

    let amount = extract_decimal(obj.get(&def.amount_column)?)?;

    Some(BalancedEntry {
        group_key,
        entry_type,
        amount,
    })
}

/// Validate the balanced constraint across every entry a boundary produced.
///
/// Groups entries by `group_key`, then for each group checks that
/// `SUM(debit amounts) == SUM(credit amounts)`. Returns the violation of the
/// first unbalanced group in group-key byte order, or `Ok(())` if all groups
/// are balanced. The order is fixed, so every replica names the same group.
pub fn check_balanced(
    collection: &str,
    def: &BalancedDef,
    entries: &[BalancedEntry],
) -> crate::Result<()> {
    // Group by group_key → (debit_sum, credit_sum), in key byte order.
    let mut groups: BTreeMap<&str, (Decimal, Decimal)> = BTreeMap::new();

    for entry in entries {
        let (debit_sum, credit_sum) = groups.entry(&entry.group_key).or_default();
        if entry.entry_type == def.debit_value {
            *debit_sum += entry.amount;
        } else if entry.entry_type == def.credit_value {
            *credit_sum += entry.amount;
        }
        // Unknown entry_type values are ignored (not debits or credits).
    }

    for (group_key, (debit_sum, credit_sum)) in groups {
        if let Some(violation) = group_violation(collection, group_key, debit_sum, credit_sum) {
            return Err(violation);
        }
    }

    Ok(())
}

/// The refusal of group `group_key` of `collection` when its summed debits
/// differ from its summed credits. `None` when they match.
pub(in crate::data::executor) fn group_violation(
    collection: &str,
    group_key: &str,
    debit_sum: Decimal,
    credit_sum: Decimal,
) -> Option<crate::Error> {
    (debit_sum != credit_sum).then(|| crate::Error::BalanceViolation {
        collection: collection.to_string(),
        detail: format!("group '{group_key}': debits {debit_sum} != credits {credit_sum}"),
    })
}

/// Extract a Decimal from a JSON value (number or string).
fn extract_decimal(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::Number(n) => {
            // Try i64 first (exact), then f64 (lossy but common in JSON).
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn test_def() -> BalancedDef {
        BalancedDef {
            group_key_column: "journal_id".into(),
            entry_type_column: "entry_type".into(),
            debit_value: "DEBIT".into(),
            credit_value: "CREDIT".into(),
            amount_column: "amount".into(),
        }
    }

    #[test]
    fn balanced_passes() {
        let entries = vec![
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("100.00"),
            },
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("100.00"),
            },
        ];
        assert!(check_balanced("ledger", &test_def(), &entries).is_ok());
    }

    #[test]
    fn unbalanced_fails() {
        let entries = vec![
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("100.00"),
            },
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("99.99"),
            },
        ];
        let result = check_balanced("ledger", &test_def(), &entries);
        assert!(result.is_err());
    }

    #[test]
    fn multiple_groups_independent() {
        let entries = vec![
            // Group j-001: balanced
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("50.00"),
            },
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("50.00"),
            },
            // Group j-002: unbalanced
            BalancedEntry {
                group_key: "j-002".into(),
                entry_type: "DEBIT".into(),
                amount: d("200.00"),
            },
            BalancedEntry {
                group_key: "j-002".into(),
                entry_type: "CREDIT".into(),
                amount: d("150.00"),
            },
        ];
        assert!(check_balanced("ledger", &test_def(), &entries).is_err());
    }

    /// With several unbalanced groups, the refusal names the group whose key
    /// sorts first by bytes, whatever order the entries came in.
    #[test]
    fn violation_names_the_first_group_in_key_order() {
        let unbalanced = |group_key: &str| BalancedEntry {
            group_key: group_key.into(),
            entry_type: "DEBIT".into(),
            amount: d("10.00"),
        };
        let keys = ["j-9", "j-10", "J-3", "j-2", "a", "j-1"];
        for rotation in 0..keys.len() {
            let mut order = keys.to_vec();
            order.rotate_left(rotation);
            let entries: Vec<_> = order.iter().map(|key| unbalanced(key)).collect();
            let error = check_balanced("ledger", &test_def(), &entries)
                .expect_err("every group is unbalanced");
            let crate::Error::BalanceViolation { detail, .. } = error else {
                panic!("expected a balance violation, got {error:?}");
            };
            assert_eq!(
                detail, "group 'J-3': debits 10.00 != credits 0",
                "{order:?}"
            );
        }
    }

    #[test]
    fn group_violation_matches_only_an_unbalanced_group() {
        assert!(group_violation("ledger", "j-1", d("5"), d("5")).is_none());
        let error = group_violation("ledger", "j-1", d("5"), d("4")).expect("unbalanced");
        assert!(matches!(
            error,
            crate::Error::BalanceViolation { ref collection, .. } if collection == "ledger"
        ));
    }

    #[test]
    fn multi_line_journal() {
        let entries = vec![
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("1000.00"),
            },
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("800.00"),
            },
            BalancedEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("200.00"),
            },
        ];
        assert!(check_balanced("ledger", &test_def(), &entries).is_ok());
    }

    #[test]
    fn empty_entries_ok() {
        assert!(check_balanced("ledger", &test_def(), &[]).is_ok());
    }

    #[test]
    fn extract_entry_from_json() {
        let doc = serde_json::json!({
            "journal_id": "j-001",
            "entry_type": "DEBIT",
            "amount": 100.50,
            "account_id": "cash"
        });
        let entry = extract_entry(&test_def(), &doc).unwrap();
        assert_eq!(entry.group_key, "j-001");
        assert_eq!(entry.entry_type, "DEBIT");
        // f64 conversion: 100.50 → Decimal
        assert!(entry.amount > d("100.49") && entry.amount < d("100.51"));
    }

    #[test]
    fn extract_entry_string_amount() {
        let doc = serde_json::json!({
            "journal_id": "j-002",
            "entry_type": "CREDIT",
            "amount": "250.75"
        });
        let entry = extract_entry(&test_def(), &doc).unwrap();
        assert_eq!(entry.amount, d("250.75"));
    }

    #[test]
    fn extract_entry_missing_field() {
        let doc = serde_json::json!({"journal_id": "j-001"});
        assert!(extract_entry(&test_def(), &doc).is_none());
    }

    fn leg(entry_type: &str, amount: &str) -> serde_json::Value {
        serde_json::json!({
            "journal_id": "j-001",
            "entry_type": entry_type,
            "amount": amount,
        })
    }

    #[test]
    fn insert_contributes_positive() {
        let doc = leg("DEBIT", "100.00");
        let entries = entries_for(&test_def(), &RowImages::Insert { new_doc: &doc });
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].amount, d("100.00"));
    }

    #[test]
    fn delete_contributes_negative() {
        let doc = leg("DEBIT", "100.00");
        let entries = entries_for(&test_def(), &RowImages::Delete { old_doc: &doc });
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].amount, d("-100.00"));
    }

    #[test]
    fn update_contributes_both_legs() {
        let old_doc = leg("DEBIT", "100.00");
        let new_doc = leg("DEBIT", "140.00");
        let entries = entries_for(
            &test_def(),
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].amount, d("-100.00"));
        assert_eq!(entries[1].amount, d("140.00"));
    }

    /// Deleting one leg of a balanced journal leaves the other behind, so the
    /// boundary's net effect is unbalanced and must be refused. Before the
    /// signed entries this was the hole: a delete contributed nothing at all.
    #[test]
    fn deleting_one_leg_is_refused() {
        let debit = leg("DEBIT", "100.00");
        let entries = entries_for(&test_def(), &RowImages::Delete { old_doc: &debit });
        assert!(check_balanced("ledger", &test_def(), &entries).is_err());
    }

    /// Deleting a whole journal nets to zero and is allowed.
    #[test]
    fn deleting_both_legs_is_allowed() {
        let debit = leg("DEBIT", "100.00");
        let credit = leg("CREDIT", "100.00");
        let def = test_def();
        let mut entries = entries_for(&def, &RowImages::Delete { old_doc: &debit });
        entries.extend(entries_for(&def, &RowImages::Delete { old_doc: &credit }));
        assert!(check_balanced("ledger", &def, &entries).is_ok());
    }

    /// An UPDATE that moves one leg's amount unbalances the group, even though
    /// the row count did not change.
    #[test]
    fn update_that_unbalances_is_refused() {
        let old_doc = leg("DEBIT", "100.00");
        let new_doc = leg("DEBIT", "140.00");
        let def = test_def();
        let entries = entries_for(
            &def,
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        );
        assert!(check_balanced("ledger", &def, &entries).is_err());
    }

    /// An UPDATE that leaves the balanced columns alone nets to zero.
    #[test]
    fn update_that_preserves_amount_is_allowed() {
        let old_doc = leg("DEBIT", "100.00");
        let new_doc = leg("DEBIT", "100.00");
        let def = test_def();
        let entries = entries_for(
            &def,
            &RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        );
        assert!(check_balanced("ledger", &def, &entries).is_ok());
    }
}
