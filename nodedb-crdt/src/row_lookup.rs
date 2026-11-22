// SPDX-License-Identifier: Apache-2.0

//! Read-only lookup abstraction used by the constraint validator.
//!
//! Provides two probes against committed CRDT state: row existence (FK
//! checks) and field-value presence across all rows (UNIQUE checks).
//! `CrdtState` implements this trait; callers may supply any conforming view.

/// Read-only row and field lookup used by the constraint validator.
///
/// Implement this trait to supply the validator with a view of committed state.
/// All methods are read-only; the trait carries no mutation capability.
pub trait RowLookup {
    /// Return `true` if a row with the given `row_id` exists in `collection`.
    ///
    /// For FK and BiTemporalFK constraints the validator calls this to confirm
    /// the referenced row is present in the target collection.
    fn row_exists(&self, collection: &str, row_id: &str) -> bool;

    /// Return `true` if any row in `collection` has `field` equal to `value`.
    ///
    /// When `exclude_row_id` is `Some`, the row with that id is skipped, so a
    /// row does not collide with its own already-committed version. Pass `None`
    /// to consider every row.
    ///
    /// Used for UNIQUE constraint checking on every collection.
    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &loro::LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool;
}
