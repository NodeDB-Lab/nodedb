// SPDX-License-Identifier: BUSL-1.1

//! Derive the user-visible `document_id` for a row written into a target
//! collection on behalf of another operation, and validate that a matched
//! existing row carries a registered surrogate.

use nodedb_types::Surrogate;

use super::pk::{TargetPk, extract_pk_value};

/// The user-visible primary key (`document_id`) for a row written on this
/// target, mirroring the plain-`INSERT` identity path (`insert.rs`): an
/// auto-`_rowid` row's PK is the decimal surrogate the Data Plane also writes
/// into `_rowid`; a declared-PK row's PK is the field value extracted from
/// the body.
pub(crate) fn derive_document_id(
    target_pk: &TargetPk,
    body: &[u8],
    surrogate: Surrogate,
) -> String {
    match target_pk {
        TargetPk::AutoRowId => surrogate.as_u32().to_string(),
        TargetPk::Field { name, .. } => {
            extract_pk_value(body, name).unwrap_or_else(|| surrogate.as_u32().to_string())
        }
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
