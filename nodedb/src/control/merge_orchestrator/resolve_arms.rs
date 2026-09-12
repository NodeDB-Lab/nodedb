// SPDX-License-Identifier: BUSL-1.1

//! MERGE/RESOLVE wire format shared by the autocommit orchestrator
//! ([`super::orchestrator::run_merge`]) and the COMMIT-time in-transaction
//! expander ([`super::expand_staged_merge`]): both resolve a merge's arms
//! through the SAME Data-Plane RESOLVE pass and decode its payload here.
//! Target-identity derivation (primary-key classification, surrogate
//! assignment, etc.) lives in [`crate::control::target_identity`] and is
//! shared with `INSERT ... SELECT` / `UPDATE ... FROM`.

/// The three resolved arms of a MERGE, decoded from the Data-Plane RESOLVE
/// pass. `updates` / `deletes` carry the EXISTING target row's storage key
/// (`doc_id`) and its registered `surrogate` — always present on `updates`
/// (every matched row is storage-keyed); `None` on `deletes` only for a
/// legacy non-surrogate-keyed row, unreachable for any surrogate-keyed
/// collection — and the arm's resolved body (post-image for updates, the
/// deleted row for deletes so its PK can be extracted). `inserts` carry
/// `(join_key, body)`.
///
/// An UPDATE arm additionally carries the target row's PRE-image as a fourth
/// element. A materialized-sum delta is the DIFFERENCE between the two images
/// and a join-key rewrite moves value between TWO targets, so the pre-image
/// join key has to be resolvable on the Control Plane; the post-image alone
/// cannot express either.
#[derive(Default)]
pub(crate) struct ResolvedMergeArms {
    pub(crate) updates: Vec<crate::query::ResolvedUpdateRowWire>,
    pub(crate) deletes: Vec<(String, Option<u32>, Vec<u8>)>,
    pub(crate) inserts: Vec<(String, Vec<u8>)>,
}

/// Decode the RESOLVE pass payload (a msgpack 3-tuple `(updates, deletes,
/// inserts)`; see `execute_merge_resolve`) into [`ResolvedMergeArms`].
pub(crate) fn decode_resolve(payload: &[u8]) -> crate::Result<ResolvedMergeArms> {
    if payload.is_empty() {
        return Ok(ResolvedMergeArms::default());
    }
    type Wire = (
        Vec<crate::query::ResolvedUpdateRowWire>,
        Vec<(String, Option<u32>, Vec<u8>)>,
        Vec<(String, Vec<u8>)>,
    );
    let (updates, deletes, inserts): Wire =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("merge resolve rows: {e}"),
        })?;
    Ok(ResolvedMergeArms {
        updates,
        deletes,
        inserts,
    })
}
