//! Append-only enforcement: reject UPDATE and DELETE on append-only collections.

use crate::bridge::envelope::ErrorCode;
use crate::bridge::physical_plan::AccountingOptions;

/// Check whether an UPDATE is allowed on this collection.
///
/// For append-only collections, UPDATEs are unconditionally rejected.
/// `old_value` being `Some` means the document already exists (UPDATE case).
pub fn check_point_put(
    collection: &str,
    opts: &AccountingOptions,
    old_value: &Option<Vec<u8>>,
) -> Result<(), ErrorCode> {
    if opts.append_only && old_value.is_some() {
        return Err(ErrorCode::AppendOnlyViolation {
            collection: collection.to_string(),
        });
    }
    Ok(())
}

/// Check whether a DELETE is allowed on this collection.
///
/// For append-only collections, DELETEs are unconditionally rejected.
pub fn check_point_delete(collection: &str, opts: &AccountingOptions) -> Result<(), ErrorCode> {
    if opts.append_only {
        return Err(ErrorCode::AppendOnlyViolation {
            collection: collection.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(append_only: bool) -> AccountingOptions {
        AccountingOptions {
            append_only,
            ..Default::default()
        }
    }

    #[test]
    fn insert_allowed_on_append_only() {
        assert!(check_point_put("ledger", &opts(true), &None).is_ok());
    }

    #[test]
    fn update_rejected_on_append_only() {
        let old = Some(vec![1, 2, 3]);
        assert!(check_point_put("ledger", &opts(true), &old).is_err());
    }

    #[test]
    fn update_allowed_when_not_append_only() {
        let old = Some(vec![1, 2, 3]);
        assert!(check_point_put("ledger", &opts(false), &old).is_ok());
    }

    #[test]
    fn delete_rejected_on_append_only() {
        assert!(check_point_delete("ledger", &opts(true)).is_err());
    }

    #[test]
    fn delete_allowed_when_not_append_only() {
        assert!(check_point_delete("ledger", &opts(false)).is_ok());
    }
}
