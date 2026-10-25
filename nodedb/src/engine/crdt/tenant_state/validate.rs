// SPDX-License-Identifier: BUSL-1.1

//! Apply-time validation of an already-committed CRDT row.
//!
//! After a peer delta is imported, the rows it *actually* wrote are re-read
//! from committed state and checked against the constraints installed for the
//! collection. Re-reading committed state (rather than trusting the sender's
//! claimed row) means a delta cannot smuggle a constraint violation past
//! validation by mislabelling which row or collection it touched.

use nodedb_crdt::validator::ValidationOutcome;
use nodedb_types::Surrogate;

use super::core::{TenantCrdtEngine, TenantRowLookup};

impl TenantCrdtEngine {
    /// Validate the committed row `(collection, row_id)` against the
    /// constraints installed for its collection.
    ///
    /// Returns [`ValidationOutcome::Accepted`] when the collection has no local
    /// state or the row is absent — a pure delete leaves nothing to check.
    /// Otherwise the row's full current field set is validated through the
    /// tenant-wide row/field lookup (so UNIQUE and FK probes see every
    /// collection plus the array-surrogate registry).
    pub fn validate_committed_row(
        &self,
        collection: &str,
        row_id: &str,
        surrogate: Surrogate,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> ValidationOutcome {
        let Some(state) = self.collections.get(collection) else {
            return ValidationOutcome::Accepted;
        };
        let Some(change) = state.build_change_from_row(collection, row_id, surrogate) else {
            return ValidationOutcome::Accepted;
        };
        let view = TenantRowLookup {
            collections: &self.collections,
            array_surrogate_ids: &self.array_surrogate_ids,
        };
        self.validator.validate(&view, &change, crs)
    }
}
