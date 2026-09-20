// SPDX-License-Identifier: BUSL-1.1

//! Grouped decode arm for `ReplicatedWrite` variants that produce
//! `PhysicalPlan::Crdt`.
//!
//! Delegated from `decode/entry.rs`'s single grouped match arm. `write` is
//! guaranteed by the caller to already be one of these variants — see
//! `entry_document::decode_arm` for the trailing-arm contract.

use super::super::decode_sync_engines::decode_returning;
use super::super::types::ReplicatedWrite;
use super::crdt;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::CrdtOp;

pub(super) fn decode_arm(write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
            surrogate,
        } => crdt::apply(crdt::ApplyArgs {
            collection,
            document_id,
            delta,
            peer_id: *peer_id,
            provenance_bytes: provenance,
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: None,
            auth_user_id: 0,
            auth_device_id: 0,
            auth_seq_no: 0,
            delta_signature: [0; 32],
            signing_required: false,
            authenticated: false,
            carried_surrogate: *surrogate,
        }),
        ReplicatedWrite::CrdtApplyFenced {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            surrogate,
        } => crdt::apply(crdt::ApplyArgs {
            collection,
            document_id,
            delta,
            peer_id: *peer_id,
            provenance_bytes: provenance,
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: Some(*expected_frontier_digest),
            auth_user_id: 0,
            auth_device_id: 0,
            auth_seq_no: 0,
            delta_signature: [0; 32],
            signing_required: false,
            authenticated: false,
            carried_surrogate: *surrogate,
        }),
        ReplicatedWrite::CrdtApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
            expected_frontier_digest,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
            surrogate,
        } => crdt::apply(crdt::ApplyArgs {
            collection,
            document_id,
            delta,
            peer_id: *peer_id,
            provenance_bytes: provenance,
            constraint_version_required: *constraint_version_required,
            expected_frontier_digest: *expected_frontier_digest,
            auth_user_id: *auth_user_id,
            auth_device_id: *auth_device_id,
            auth_seq_no: *auth_seq_no,
            delta_signature: *delta_signature,
            signing_required: *signing_required,
            authenticated: true,
            carried_surrogate: *surrogate,
        }),
        ReplicatedWrite::CrdtImportCollection {
            tenant_id,
            collection,
            bytes,
        } => Ok(crdt::import_collection(*tenant_id, collection, bytes)),
        ReplicatedWrite::CrdtListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            surrogate,
        } => crdt::list_insert(
            collection,
            document_id,
            list_path,
            *index,
            fields_json,
            *surrogate,
        ),
        ReplicatedWrite::CrdtListDelete {
            collection,
            document_id,
            list_path,
            index,
            surrogate,
        } => crdt::list_delete(collection, document_id, list_path, *index, *surrogate),
        ReplicatedWrite::CrdtListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate,
        } => crdt::list_move(
            collection,
            document_id,
            list_path,
            *from_index,
            *to_index,
            *surrogate,
        ),
        ReplicatedWrite::CrdtDocUpsert {
            collection,
            document_id,
            surrogate,
            fields_json,
            partial,
            verb,
            returning,
            rls_filters,
            // The row's own top-level `surrogate` is carried across the wire and
            // rebuilt via `Surrogate::new` — the live dispatch handler uses it to
            // gate and key the sparse-store materialization. `returning` rides on
            // the record so a replay re-executes this write for the originating
            // request, not only for the follower's own state.
        } => Ok(PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
            document_id: document_id.clone(),
            fields_json: fields_json.clone(),
            surrogate: nodedb_types::Surrogate::new(*surrogate),
            partial: *partial,
            verb: *verb,
            returning: decode_returning(returning)?,
            rls_filters: rls_filters.clone(),
        })),
        ReplicatedWrite::CrdtDocDelete {
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
        } => Ok(crdt::doc_delete(
            collection,
            document_id,
            *surrogate,
            decode_returning(returning)?,
            rls_filters,
        )),
        ReplicatedWrite::ConstraintChange {
            collection,
            op,
            constraint_version,
            constraints,
        } => Ok(crdt::constraint_change(
            collection,
            op,
            *constraint_version,
            constraints,
        )),
        _ => Err(crate::Error::Internal {
            detail: "entry_crdt::decode_arm called with a non-Crdt ReplicatedWrite variant \
                (dispatch bug in decode/entry.rs's grouped Crdt match arm)"
                .into(),
        }),
    }
}
