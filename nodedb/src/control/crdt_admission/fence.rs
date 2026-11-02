// SPDX-License-Identifier: BUSL-1.1

//! Stamp the previewed frontier on a delta and propose it fenced.

use nodedb_physical::physical_plan::CrdtOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};

use super::request::CrdtAdmissionOutcome;
use super::workflow::CrdtAdmissionWorkflow;

/// `plan` with `digest` as the frontier its apply expects.
pub(super) fn stamp_fence(plan: PhysicalPlan, digest: [u8; 32]) -> crate::Result<PhysicalPlan> {
    match plan {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest: None,
        }) => Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest: Some(digest),
        })),
        PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest: None,
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        }) => Ok(PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id,
            surrogate,
            provenance,
            constraint_version_required,
            expected_frontier_digest: Some(digest),
            auth_user_id,
            auth_device_id,
            auth_seq_no,
            delta_signature,
            signing_required,
        })),
        _ => Err(crate::Error::CrdtAdmissionInvalidPlan {
            reason: "validated CRDT Apply plan was changed before fence stamping",
        }),
    }
}

/// Propose the fenced apply and wait for this node's apply of it.
pub(super) async fn apply_fenced(
    workflow: &CrdtAdmissionWorkflow<'_>,
    plan: PhysicalPlan,
) -> crate::Result<CrdtAdmissionOutcome> {
    let raw = workflow.state.raw_async_raft_proposer()?;
    let entry = to_replicated_entry(
        workflow.tenant_id,
        workflow.database_id,
        workflow.vshard_id,
        &ReplicableWrite::decide_for_replication(&plan)?,
    )?
    .ok_or(crate::Error::CrdtAdmissionInvalidPlan {
        reason: "admitted CRDT Apply has no replicated form",
    })?
    // Every replica gives the write the source this node dispatches it
    // with.
    .with_event_source(workflow.event_source);
    let payload = workflow
        .propose(crate::control::wal_replication::propose_replicated_entry(
            workflow.state,
            raw,
            entry,
            workflow.deadline.tokio_instant(),
        ))
        .await?;
    // This node's apply of the entry recorded its commit HLC.
    Ok(CrdtAdmissionOutcome {
        payload,
        trimmed_ops: 0,
    })
}
