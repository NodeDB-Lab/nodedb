// SPDX-License-Identifier: BUSL-1.1

//! The one apply seam for a Control-Plane-orchestrated write (`MERGE`,
//! `UPDATE ... FROM`, `INSERT ... SELECT`): the resolved plan the orchestrator
//! built lands on its target vShard's owner and on every replica.
//!
//! On a cluster the plan proposes through Raft exactly as a plain replicated
//! write does; the proposer forwards to the group leader, so the coordinator
//! never has to own the vShard. Standalone, the plan dispatches to the local
//! Data Plane and mints the redo record the write funnel would have minted.

use std::sync::atomic::Ordering;

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::server::dispatch_utils::publish_origin_change_events;
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ReplicableWrite, propose_replicated_entry, to_replicated_entry,
};
use crate::types::{RequestId, VShardId};

/// Apply `plan`, a resolved write on `collection`, and return the Data-Plane
/// response the statement renders.
///
/// A Data-Plane verdict that reached the proposer as `Error::DataPlane` comes
/// back as an error `Response` carrying that code, the same shape a local
/// dispatch returns, so a caller reads `OllpRetryRequired` the one way on
/// both paths.
pub(crate) async fn apply_orchestrated_write(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let Some(proposer) = state.async_raft_proposer() else {
        let resp = dispatch_local(state, tenant_id, database_id, collection, plan, None).await?;
        // `dispatch_local` bypasses the funnel's post-apply redo minting, so a
        // vector-indexed target's write-set arrives unconsumed. Without it a
        // WAL-only restart rebuilds the index from pre-write records. No-op
        // on a target with no write-set.
        crate::control::server::wal_dispatch::mint_dispatch_local_redo(
            state
                .wal
                .appender(crate::wal::manager::NO_APPLY_KEY)
                // `dispatch_local` runs the write as a client write.
                .with_event_source(crate::event::EventSource::User),
            tenant_id,
            database_id,
            collection,
            &resp,
        )?;
        return Ok(resp);
    };

    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let replicable = ReplicableWrite::decide_for_replication(&plan)?;
    let entry =
        to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)?.ok_or_else(|| {
            crate::Error::Internal {
                detail: format!(
                    "orchestrated write on '{collection}' did not map to a replicated write; the \
                 orchestrator must propose its resolved shape"
                ),
            }
        })?;
    let request_id = RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
    match propose_replicated_entry(state, proposer, entry).await {
        Ok((payload, write_version)) => {
            let response = Response {
                request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                watermark_lsn: write_version,
                error_code: None,
                read_set_valid: None,
                read_version_lsn: write_version,
                write_set: Vec::new(),
            };
            // The proposing node handled this write exactly once, so it is
            // the one node that publishes the CDC change event.
            publish_origin_change_events(state, tenant_id, database_id, &plan, &response);
            Ok(response)
        }
        Err(crate::Error::DataPlane(code)) => Ok(data_plane_verdict(request_id, code)),
        Err(e) => Err(e),
    }
}

/// The error `Response` a local dispatch returns for a Data-Plane verdict.
fn data_plane_verdict(request_id: RequestId, code: ErrorCode) -> Response {
    Response {
        request_id,
        status: Status::Error,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(Vec::new()),
        watermark_lsn: crate::types::Lsn::ZERO,
        error_code: Some(Box::new(code)),
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Decide the target's write policy over the row images an orchestrator
/// resolved, here on the proposing node, so the proposed plan carries a
/// decided check a follower can apply without the writing identity.
///
/// `images` are the post-images of every row the write creates or rewrites
/// and the pre-images of every row it removes, MessagePack-encoded. A refused
/// image fails the statement before anything is written, on every replica.
pub(crate) fn decide_write_policy_over_images<'a>(
    rls_write_check: &nodedb_types::RlsWriteCheck,
    images: impl IntoIterator<Item = &'a [u8]>,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<()> {
    match rls_write_check.decision() {
        nodedb_types::WriteGateDecision::AdmitAll => Ok(()),
        nodedb_types::WriteGateDecision::Evaluate(predicate) => {
            for image in images {
                crate::control::security::rls::admit_compiled_write_image(
                    predicate,
                    image,
                    tenant_id.as_u64(),
                    collection,
                )?;
            }
            Ok(())
        }
        nodedb_types::WriteGateDecision::DenyNotInjected => Err(crate::Error::PlanError {
            detail: format!(
                "internal invariant break: the orchestrated write on '{collection}' reached its \
                 apply before RLS injection decided its write-policy check"
            ),
        }),
    }
}
