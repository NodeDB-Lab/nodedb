//! BALANCED constraint: at commit time, for each distinct group_key value,
//! `SUM(amount WHERE entry_type = debit_value)` must equal
//! `SUM(amount WHERE entry_type = credit_value)`.
//!
//! Only checked within a single transaction boundary (cross-transaction
//! balance is application concern).

use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::bridge::envelope::ErrorCode;
use crate::bridge::physical_plan::BalancedDef;

/// A single insert tracked for balance validation.
pub struct InsertEntry {
    /// Value of the group_key column (e.g. journal_id).
    pub group_key: String,
    /// Value of the entry_type column (e.g. "DEBIT" or "CREDIT").
    pub entry_type: String,
    /// Monetary amount.
    pub amount: Decimal,
}

/// Extract a balanced entry from a JSON document using the constraint definition.
///
/// Returns `None` if any required field is missing (the insert is not part of
/// the balanced group and is ignored by the constraint).
pub fn extract_entry(def: &BalancedDef, doc: &serde_json::Value) -> Option<InsertEntry> {
    let obj = doc.as_object()?;

    let group_key = obj.get(&def.group_key_column)?.as_str().map(String::from)?;

    let entry_type = obj
        .get(&def.entry_type_column)?
        .as_str()
        .map(String::from)?;

    let amount = extract_decimal(obj.get(&def.amount_column)?)?;

    Some(InsertEntry {
        group_key,
        entry_type,
        amount,
    })
}

/// Validate the balanced constraint across all inserts in a transaction.
///
/// Groups entries by `group_key`, then for each group checks that
/// `SUM(debit amounts) == SUM(credit amounts)`. Returns the first
/// violation found, or `Ok(())` if all groups are balanced.
pub fn check_balanced(
    collection: &str,
    def: &BalancedDef,
    entries: &[InsertEntry],
) -> Result<(), ErrorCode> {
    // Group by group_key → (debit_sum, credit_sum).
    let mut groups: HashMap<&str, (Decimal, Decimal)> = HashMap::new();

    for entry in entries {
        let (debit_sum, credit_sum) = groups.entry(&entry.group_key).or_default();
        if entry.entry_type == def.debit_value {
            *debit_sum += entry.amount;
        } else if entry.entry_type == def.credit_value {
            *credit_sum += entry.amount;
        }
        // Unknown entry_type values are ignored (not debits or credits).
    }

    for (group_key, (debit_sum, credit_sum)) in &groups {
        if debit_sum != credit_sum {
            return Err(ErrorCode::BalanceViolation {
                collection: collection.to_string(),
                detail: format!(
                    "group '{}': debits {} != credits {}",
                    group_key, debit_sum, credit_sum
                ),
            });
        }
    }

    Ok(())
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
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("100.00"),
            },
            InsertEntry {
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
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("100.00"),
            },
            InsertEntry {
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
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("50.00"),
            },
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("50.00"),
            },
            // Group j-002: unbalanced
            InsertEntry {
                group_key: "j-002".into(),
                entry_type: "DEBIT".into(),
                amount: d("200.00"),
            },
            InsertEntry {
                group_key: "j-002".into(),
                entry_type: "CREDIT".into(),
                amount: d("150.00"),
            },
        ];
        assert!(check_balanced("ledger", &test_def(), &entries).is_err());
    }

    #[test]
    fn multi_line_journal() {
        let entries = vec![
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "DEBIT".into(),
                amount: d("1000.00"),
            },
            InsertEntry {
                group_key: "j-001".into(),
                entry_type: "CREDIT".into(),
                amount: d("800.00"),
            },
            InsertEntry {
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
}
