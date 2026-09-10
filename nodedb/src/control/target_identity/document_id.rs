// SPDX-License-Identifier: BUSL-1.1

//! Derive the user-visible `document_id` for a row written into a target
//! collection on behalf of another operation, and validate that a matched
//! existing row carries a registered surrogate.

use nodedb_types::Surrogate;

use super::pk::{TargetPk, extract_pk_value};
use crate::control::surrogate::fresh_identity_string;

/// The user-visible primary key (`document_id`) for a row written on this
/// target, mirroring the plain-`INSERT` identity path (`insert.rs`): an
/// auto-`_rowid` row's PK is the decimal surrogate the Data Plane also writes
/// into `_rowid`. A declared-PK row's PK is the field value extracted from
/// the body. A row with no content key is stored under its document storage
/// key.
///
/// Both forms come from `fresh_identity_string`, the same formatter
/// `assign_target_surrogate` binds a freshly minted row under.
pub(crate) fn derive_document_id(
    target_pk: &TargetPk,
    body: &[u8],
    surrogate: Surrogate,
) -> String {
    use nodedb_physical::FreshSurrogateKind;
    match target_pk {
        TargetPk::AutoRowId => fresh_identity_string(FreshSurrogateKind::AutoRowId, surrogate),
        TargetPk::Field { name, .. } => extract_pk_value(body, name).unwrap_or_else(|| {
            fresh_identity_string(FreshSurrogateKind::DocumentStorageKey, surrogate)
        }),
    }
}

/// A resolved UPDATE/DELETE arm must carry the target row's registered
/// surrogate. `None` means a non-surrogate-keyed row — unreachable for every
/// current (and every vector-indexed) collection — so fail the commit loudly
/// rather than emit a degraded, unindexed, unreplicated write. Shared by the
/// MERGE and `UPDATE ... FROM` expanders; `op_label` (e.g. `"MERGE"`,
/// `"UPDATE ... FROM"`) customizes the error message per caller.
pub(crate) fn require_surrogate(
    surrogate_u32: Option<u32>,
    doc_id: &str,
    op_label: &str,
) -> crate::Result<Surrogate> {
    match surrogate_u32 {
        Some(s) => Ok(Surrogate::new(s)),
        None => Err(crate::Error::PlanError {
            detail: format!(
                "{op_label} target row '{doc_id}' lacks a surrogate; \
                 collection is not surrogate-keyed"
            ),
        }),
    }
}
