// SPDX-License-Identifier: BUSL-1.1

//! Admit a restore delta generated inside the vShard's admission slot.

use nodedb_physical::physical_plan::CrdtOp;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::control::state::SharedState;

use super::fence::{apply_fenced, stamp_fence};
use super::legs::{generate_restore_delta, preview};
use super::request::{CrdtAdmissionOutcome, CrdtRestoreAdmissionRequest};
use super::workflow::{CrdtAdmissionWorkflow, FRONTIER_RETRY_LIMIT};

/// Generate a restore delta and admit it without releasing the vShard slot.
/// A stale apply fence regenerates the delta from authoritative state before
/// retrying, so neither historical projection nor policy output is reused.
pub(crate) async fn dispatch_crdt_restore_admitted(
    state: &SharedState,
    request: CrdtRestoreAdmissionRequest<'_>,
) -> crate::Result<Option<CrdtAdmissionOutcome>> {
    let CrdtRestoreAdmissionRequest {
        tenant_id,
        database_id,
        collection,
        document_id,
        target_version_json,
        surrogate,
        peer_id,
        deadline,
        event_source,
        policy,
    } = request;
    // The restore writes a delta like any apply; the lease gates it on a
    // drained collection and lives until its outcome.
    let _lease = crate::control::server::shared::clone_write::collections_write_lease(
        state,
        tenant_id,
        database_id,
        [collection.to_owned()],
    )
    .await?;
    let workflow = CrdtAdmissionWorkflow::new(
        state,
        tenant_id,
        database_id,
        collection,
        deadline,
        event_source,
        policy,
    )?;
    workflow
        .run(|| {
            restore_locked(
                &workflow,
                document_id,
                target_version_json,
                surrogate,
                peer_id,
            )
        })
        .await
}

/// Generate, preview and propose the restore delta, inside the slot.
async fn restore_locked(
    workflow: &CrdtAdmissionWorkflow<'_>,
    document_id: &str,
    target_version_json: &str,
    surrogate: nodedb_types::Surrogate,
    peer_id: u64,
) -> crate::Result<Option<CrdtAdmissionOutcome>> {
    for _attempt in 0..FRONTIER_RETRY_LIMIT {
        let delta =
            generate_restore_delta(workflow, document_id, target_version_json, surrogate).await?;
        if delta.is_empty() {
            return Ok(None);
        }
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::from_stored(
                workflow.collection.to_owned(),
            ),
            document_id: document_id.to_owned(),
            delta: delta.clone(),
            peer_id,
            mutation_id: 0,
            surrogate,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        let preview = preview(workflow, document_id, &delta).await?;
        workflow.policy.evaluate(&preview)?;
        match apply_fenced(workflow, stamp_fence(plan, preview.frontier_digest)?).await {
            Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch { .. })) => {}
            result => return result.map(Some),
        }
    }
    Err(crate::Error::CrdtAdmissionRetriesExhausted {
        vshard_id: workflow.vshard_id,
        attempts: FRONTIER_RETRY_LIMIT,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use nodedb_types::{CrdtPreviewResult, Surrogate};

    use super::*;
    use crate::control::crdt_admission::test_support::{
        RecordingPolicy, fixture, respond_n, response,
    };
    use crate::control::server::shared::session::StatementDeadline;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, TenantId};

    #[tokio::test]
    async fn stale_restore_regenerates_before_retrying_admission() {
        let (state, side, _directory) = fixture();
        let delta = vec![0x91, 0x02];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: [0x84; 32],
        })
        .expect("preview payload");
        let request_index = Arc::new(AtomicUsize::new(0));
        let index = Arc::clone(&request_index);
        let responder = tokio::spawn(respond_n(Arc::clone(&state), side, 4, move |request| {
            let is_restore = matches!(
                &request.plan,
                PhysicalPlan::Crdt(CrdtOp::RestoreToVersion { .. })
            );
            match index.fetch_add(1, Ordering::SeqCst) {
                0 | 2 => assert!(is_restore),
                1 | 3 => assert!(matches!(
                    &request.plan,
                    PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
                )),
                _ => unreachable!("responder only handles four requests"),
            }
            let payload = if is_restore {
                delta.clone()
            } else {
                preview_payload.clone()
            };
            response(request.request_id, payload)
        }));
        let attempts = Arc::new(AtomicUsize::new(0));
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> = {
            let attempts = Arc::clone(&attempts);
            Arc::new(move |_shard, _key, _bytes, _deadline| {
                let attempts = Arc::clone(&attempts);
                Box::pin(async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch {
                            expected: [0x84; 32],
                            actual: [0; 32],
                        }))
                    } else {
                        Ok(Vec::new())
                    }
                })
            })
        };
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install proposer");
        let policy = RecordingPolicy {
            seen: Arc::new(Mutex::new(Vec::new())),
            reject: false,
        };
        dispatch_crdt_restore_admitted(
            &state,
            CrdtRestoreAdmissionRequest {
                tenant_id: TenantId::new(1),
                database_id: DatabaseId::DEFAULT,
                collection: "docs",
                document_id: "doc-1",
                target_version_json: "{}",
                surrogate: Surrogate::new(1),
                peer_id: 1,
                deadline: StatementDeadline::after(Duration::from_secs(1)),
                event_source: EventSource::User,
                policy: &policy,
            },
        )
        .await
        .expect("second restore admission succeeds");
        responder.await.expect("responder completes");
        assert_eq!(request_index.load(Ordering::SeqCst), 4);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
