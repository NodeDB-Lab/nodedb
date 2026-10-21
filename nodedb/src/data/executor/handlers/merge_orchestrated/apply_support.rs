// SPDX-License-Identifier: BUSL-1.1

//! Row-level helpers for the MERGE APPLY pass: undo capture for the in-memory
//! indexes, row-level-security admission of the resolved arms, and post/pre-image
//! projection for a `RETURNING` clause.

use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::document::store::{RowIdentity, StorageKey};

use super::plan::MergePlanActions;

/// One committed Phase-A put captured for post-commit event emission:
/// `(row identity, new stored body borrowed from the plan, prior stored value)`.
/// The identity is the one INSERT minted for the row. The body borrows from
/// the merge plan (owned for the whole apply) rather than being cloned.
pub(super) type MergePutEvent<'a> = (RowIdentity, &'a [u8], Option<Vec<u8>>);

/// Record the in-memory index mutations a successful
/// [`crate::data::executor::core_loop::CoreLoop::apply_point_put`] performed as
/// undo entries. The vector, sparse and spatial indexes live OUTSIDE the
/// shared redb transaction, so dropping that transaction on abort does not
/// reverse them. Drains the outcome's undo
/// entries and leaves `prior_value` for the caller's event emission.
pub(super) fn record_put_index_undo(undo_log: &mut Vec<UndoEntry>, outcome: &mut PointPutOutcome) {
    undo_log.append(&mut outcome.memory_undo);
}

/// Decide every resolved arm of a MERGE against the target's compiled write
/// policy, BEFORE the apply pass writes anything.
///
/// Each arm is judged on the image it stores: the post-image for an UPDATE or
/// INSERT arm, the pre-image for a DELETE arm. Deciding the whole set up front
/// is what makes a rejection leave no partial merge behind — the apply pass
/// shares one transaction for its puts but cascades its deletes outside it, so
/// a mid-apply denial would not be fully reversible.
///
/// Every captured body is MessagePack for BOTH storage modes —
/// `collect_merge_plan` decodes a strict target's Binary Tuple and re-encodes
/// the resolved row before the apply pass sees it — so the decode takes no
/// strict schema.
pub(super) fn gate_merge_arms(
    plan: &MergePlanActions,
    rls_write_check: &nodedb_types::RlsWriteCheck,
    identity_column: &str,
    tid: u64,
    collection: &str,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    if matches!(
        rls_write_check.decision(),
        nodedb_types::WriteGateDecision::AdmitAll
    ) {
        return Ok(());
    }
    // Updates and deletes name their row by its storage key; inserts name
    // theirs by the source row's id, which is never a document storage key.
    let doc_arms = plan
        .updates
        .iter()
        .map(|u| (u.body.as_slice(), u.key))
        .chain(plan.deletes.iter().map(|d| (d.body.as_slice(), d.key)));
    for (body, key) in doc_arms {
        let identity = key.to_identity();
        rls_write_gate::admit_stored_row(
            rls_write_check,
            rls_write_gate::GatedRow {
                body,
                identity: &identity,
                strict_schema: None,
                identity_column,
            },
            tid,
            collection,
            crs,
        )?;
    }
    for insert in &plan.inserts {
        let identity =
            crate::engine::document::store::RowIdentity::from_user_key(insert.join_key.as_str());
        rls_write_gate::admit_stored_row(
            rls_write_check,
            rls_write_gate::GatedRow {
                body: &insert.body,
                identity: &identity,
                strict_schema: None,
                identity_column,
            },
            tid,
            collection,
            crs,
        )?;
    }
    Ok(())
}

/// Decode one merge row body into the `Value` document a RETURNING
/// projection reads. Same shape the point and bulk DML RETURNING paths
/// emit, so a MERGE row projects identically.
///
/// `key` is the row's storage key: every caller's `MergeUpdate::key`,
/// `MergeDelete::key`, or a freshly minted insert key. This function converts
/// it to the client-visible identity before decoding.
///
/// The schema argument is `None` unconditionally: a merge plan's captured
/// bodies are MessagePack for BOTH storage modes (`collect_merge_plan` decodes
/// a strict target's Binary Tuple and re-encodes the resolved row before the
/// apply pass ever sees it), so the strict decoder would have nothing to read.
/// `identity_column` is the column the identity renders under.
pub(super) fn returning_doc(
    body: &[u8],
    key: &StorageKey,
    identity_column: &str,
) -> crate::Result<nodedb_types::Value> {
    let identity = key.to_identity();
    super::super::returning_doc::from_stored(body, &identity, None, identity_column)
}
