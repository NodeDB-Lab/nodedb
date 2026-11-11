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
///
/// `ordinals` holds each arm row's place in the join's output order, the
/// order PostgreSQL answers `RETURNING` rows in, one per row of its arm.
#[derive(Default)]
pub(crate) struct ResolvedMergeArms {
    pub(crate) updates: Vec<crate::query::ResolvedUpdateRowWire>,
    pub(crate) deletes: Vec<(String, Option<u32>, Vec<u8>)>,
    pub(crate) inserts: Vec<(String, Vec<u8>)>,
    pub(crate) ordinals: MergeArmOrdinals,
}

/// Each arm row's place in a MERGE join's output order, one per row of the
/// arm, in the arm's order.
#[derive(Default)]
pub(crate) struct MergeArmOrdinals {
    pub(crate) updates: Vec<u64>,
    pub(crate) deletes: Vec<u64>,
    pub(crate) inserts: Vec<u64>,
}

/// Decode the RESOLVE pass payload (a msgpack 4-tuple `(updates, deletes,
/// inserts, ordinals)`; see `execute_merge_resolve`) into
/// [`ResolvedMergeArms`]. An arm whose ordinals do not match its rows in
/// count is an error.
pub(crate) fn decode_resolve(payload: &[u8]) -> crate::Result<ResolvedMergeArms> {
    if payload.is_empty() {
        return Ok(ResolvedMergeArms::default());
    }
    type Wire = (
        Vec<crate::query::ResolvedUpdateRowWire>,
        Vec<(String, Option<u32>, Vec<u8>)>,
        Vec<(String, Vec<u8>)>,
        (Vec<u64>, Vec<u64>, Vec<u64>),
    );
    let (updates, deletes, inserts, (update_ordinals, delete_ordinals, insert_ordinals)): Wire =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("merge resolve rows: {e}"),
        })?;
    if update_ordinals.len() != updates.len()
        || delete_ordinals.len() != deletes.len()
        || insert_ordinals.len() != inserts.len()
    {
        return Err(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: "merge resolve rows: an arm's ordinals do not match its rows".into(),
        });
    }
    Ok(ResolvedMergeArms {
        updates,
        deletes,
        inserts,
        ordinals: MergeArmOrdinals {
            updates: update_ordinals,
            deletes: delete_ordinals,
            inserts: insert_ordinals,
        },
    })
}
