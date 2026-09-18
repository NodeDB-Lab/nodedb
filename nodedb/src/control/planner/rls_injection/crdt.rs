// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for CRDT-engine operations.
//!
//! No CRDT read carries a filter slot: a document read returns the merged Loro
//! state and a delta export returns the oplog those states were built from.
//! Reads of row content therefore refuse while a policy applies, while reads
//! of collection configuration — the installed constraint set, the conflict
//! policy, the oplog version vector — carry no row content and pass.
//!
//! The two DML ops are the exception for the read half: a `RETURNING` clause on
//! them emits row bodies the handler holds in full, so they carry a post-fetch
//! filter slot and the policy lands there rather than refusing the statement.
//!
//! No CRDT write can be gated here at all: what gets persisted is always the
//! merge of the submitted change with the document's existing state, and that
//! merge runs in the Data Plane. A write policy on the collection therefore
//! refuses the statement rather than admitting an image it never saw.

use nodedb_physical::physical_plan::CrdtOp;

use super::context::RlsCtx;

const ROW_CONTENT_REASON: &str =
    "the CRDT read returns merged document state through a payload that carries no row filter";

const MERGED_IMAGE_REASON: &str = "the persisted state is the merge of this change with the document's existing CRDT state, so \
     no row image is available for the policy to be evaluated against";

/// Exhaustive over [`CrdtOp`] so a new CRDT operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_crdt(ctx: &RlsCtx<'_>, op: &mut CrdtOp) -> crate::Result<()> {
    match op {
        // Refuse: all four return stored row content — the current state, a
        // historical state, the oplog deltas those states were built from, or
        // the state a delta would produce — and none has a slot the policy
        // could occupy.
        CrdtOp::Read { collection, .. }
        | CrdtOp::ReadAtVersion { collection, .. }
        | CrdtOp::ExportDelta { collection, .. }
        | CrdtOp::PreviewApply { collection, .. } => {
            ctx.refuse_if_policy(collection, ROW_CONTENT_REASON)
        }

        // No-op: collection configuration and sync bookkeeping. The installed
        // constraint set, the conflict-resolution policy, and the oplog version
        // vector describe the collection, not its rows, so a row policy has
        // nothing to restrict in them.
        CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::GetVersionVector { .. } => Ok(()),

        // Inject: both surface stored row content through a `RETURNING` clause,
        // and that output is a read — the handler evaluates the filter against
        // each full pre-projection document, so a predicate on a column the
        // `RETURNING` list omits still decides the row. The row set shown
        // shrinks; the write and its affected count do not.
        CrdtOp::DocUpsert {
            collection,
            rls_filters,
            ..
        }
        | CrdtOp::DocDelete {
            collection,
            rls_filters,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            ctx.refuse_if_write_policy(collection, MERGED_IMAGE_REASON)
        }

        // Refuse: every one of these persists a state produced by merging a
        // delta, a list edit, a snapshot, or a historical version into the
        // document's existing Loro state, so the image a write policy decides
        // exists only after that merge runs in the Data Plane.
        //
        // The externally-submitted deltas that arrive over the sync transports
        // do not reach this pass: they are admitted through
        // `ExternalCrdtPostImagePolicy`, which evaluates the same write
        // policies against the Data Plane's authoritative post-image preview.
        CrdtOp::Apply { collection, .. }
        | CrdtOp::ApplyAuthenticated { collection, .. }
        | CrdtOp::ImportSnapshot { collection, .. }
        | CrdtOp::RestoreToVersion { collection, .. }
        | CrdtOp::CompactAtVersion { collection, .. }
        | CrdtOp::ListInsert { collection, .. }
        | CrdtOp::ListDelete { collection, .. }
        | CrdtOp::ListMove { collection, .. } => {
            ctx.refuse_if_write_policy(collection, MERGED_IMAGE_REASON)
        }

        // No-op: the constraint set and the conflict-resolution policy describe
        // the collection, not its rows, so no row policy restricts writing them.
        CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::SetPolicy { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::CrdtOp;

    use super::super::plan::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn crdt_read(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::Read {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            document_id: "d1".into(),
        })
    }

    /// A CRDT document read returns merged state with no filter slot.
    #[test]
    fn crdt_read_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("notes");
        let mut plan = crdt_read("notes");
        assert_refused(inject(&mut plan, &store), "notes");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn crdt_read_without_a_policy_is_untouched() {
        let mut plan = crdt_read("notes");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A CRDT `RETURNING` write ships row bodies back, so the policy lands in
    /// its post-filter slot rather than refusing the statement.
    #[test]
    fn doc_delete_receives_the_policy_filter() {
        let store = store_with_read_policy("notes");
        let mut plan = PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "notes",
            ),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Crdt(CrdtOp::DocDelete { rls_filters, .. }) => {
                assert!(!rls_filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// What a CRDT write persists is the merge of the submitted change with
    /// the document's existing state, so a write policy refuses the statement
    /// rather than admitting an image the planner never saw.
    #[test]
    fn doc_upsert_is_refused_under_a_write_policy() {
        use super::super::plan::test_support::{assert_write_refused, store_with_write_policy};

        let store = store_with_write_policy("notes");
        let mut plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "notes",
            ),
            document_id: "d1".into(),
            fields_json: "{}".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            partial: false,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_write_refused(inject(&mut plan, &store), "notes");
    }

    /// Reading the conflict policy discloses configuration, not rows.
    #[test]
    fn get_policy_is_allowed_under_a_read_policy() {
        let store = store_with_read_policy("notes");
        let mut plan = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "notes",
            ),
        });
        assert!(inject(&mut plan, &store).is_ok());
    }
}
