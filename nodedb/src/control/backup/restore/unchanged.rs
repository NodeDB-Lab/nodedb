// SPDX-License-Identifier: BUSL-1.1

//! Whether a restored collection row changes the destination's row.
//!
//! A RESTORE into a destination that already holds the backed-up collection
//! re-puts the same definition. A put bumps the descriptor version, and a
//! version bump drains every lease on the prior version. Concurrent writes
//! then wait for metadata Raft round trips for nothing. A row that changes
//! nothing skips the put, its drain and its version bump.

use crate::Error;
use crate::control::security::catalog::StoredCollection;

/// Whether `restored` equals the committed, active `existing` row in every
/// field except the ones the descriptor stamp assigns at propose time.
///
/// The stamp fields are `descriptor_version`, `constraint_version`,
/// `modification_hlc` and `incarnation`. A put onto an active row keeps that
/// row's incarnation, so a row equal in everything else leaves the catalog as
/// it is. A put onto an inactive row names a new incarnation, so it always
/// counts as a change. The rows are compared by their encoding, so a field
/// added to [`StoredCollection`] takes part without a change here.
pub(super) fn leaves_row_unchanged(
    existing: &StoredCollection,
    restored: &StoredCollection,
) -> Result<bool, Error> {
    if !existing.is_active {
        return Ok(false);
    }
    let mut stamped = restored.clone();
    stamped.descriptor_version = existing.descriptor_version;
    stamped.constraint_version = existing.constraint_version;
    stamped.modification_hlc = existing.modification_hlc;
    stamped.incarnation = existing.incarnation;
    Ok(encode(existing)? == encode(&stamped)?)
}

fn encode(row: &StoredCollection) -> Result<Vec<u8>, Error> {
    zerompk::to_msgpack_vec(row).map_err(|e| Error::Internal {
        detail: format!("restore: encode catalog row of '{}': {e}", row.name),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed() -> StoredCollection {
        let mut row = StoredCollection::new(1, "orders", "admin");
        row.descriptor_version = 4;
        row.constraint_version = 1;
        row.modification_hlc = nodedb_types::Hlc::new(900, 0);
        row.incarnation = nodedb_types::Hlc::new(100, 0);
        row
    }

    /// A backed-up row that differs only in the stamp fields changes nothing.
    #[test]
    fn a_row_differing_only_in_stamps_is_unchanged() {
        let existing = committed();
        let mut restored = existing.clone();
        restored.descriptor_version = 1;
        restored.constraint_version = 0;
        restored.modification_hlc = nodedb_types::Hlc::new(5, 0);
        restored.incarnation = nodedb_types::Hlc::ZERO;
        assert!(leaves_row_unchanged(&existing, &restored).expect("compare"));
    }

    /// A row that changes the definition is a real put.
    #[test]
    fn a_changed_definition_is_a_change() {
        let existing = committed();
        let mut restored = existing.clone();
        restored.fields.push(("v".into(), "BIGINT".into()));
        assert!(!leaves_row_unchanged(&existing, &restored).expect("compare"));

        let mut reactivated = existing.clone();
        reactivated.is_active = false;
        assert!(!leaves_row_unchanged(&reactivated, &existing).expect("compare"));
    }

    /// A put onto a dropped row names a new incarnation, even when the
    /// definitions match.
    #[test]
    fn a_put_onto_an_inactive_row_is_a_change() {
        let mut existing = committed();
        existing.is_active = false;
        let mut restored = existing.clone();
        restored.incarnation = nodedb_types::Hlc::ZERO;
        assert!(!leaves_row_unchanged(&existing, &restored).expect("compare"));
    }
}
