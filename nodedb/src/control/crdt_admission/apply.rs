// SPDX-License-Identifier: BUSL-1.1

//! Preview, authorize, fence, and durably apply one CRDT delta.

use nodedb_physical::physical_plan::CrdtOp;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;

use super::fence::{apply_fenced, stamp_fence};
use super::legs::preview;
use super::request::{
    AuthorizedCrdtApplyAdmissionRequest, CrdtAdmissionOutcome, CrdtApplyAdmissionRequest,
};
use super::workflow::{CrdtAdmissionWorkflow, FRONTIER_RETRY_LIMIT};

/// Preview, authorize, fence, and durably apply one externally authorized delta.
pub async fn dispatch_authorized_crdt_apply_admitted_outcome(
    state: &SharedState,
    request: AuthorizedCrdtApplyAdmissionRequest<'_>,
) -> crate::Result<CrdtAdmissionOutcome> {
    let AuthorizedCrdtApplyAdmissionRequest {
        authorized,
        collection,
        deadline,
        event_source,
        policy,
    } = request;
    enforce_external_signing_policy(state, &authorized, collection)?;
    let task = authorized.into_physical_task();
    dispatch_crdt_apply_admitted_outcome(
        state,
        CrdtApplyAdmissionRequest {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            collection,
            plan: task.plan,
            deadline,
            event_source,
            policy,
        },
    )
    .await
}

/// `collection` is db-qualified. The catalog keys collections by the bare
/// name, so the lookup de-qualifies it first.
fn enforce_external_signing_policy(
    state: &SharedState,
    authorized: &AuthorizedTask,
    collection: &str,
) -> crate::Result<()> {
    let bare =
        nodedb_types::CollectionKey::from_qualified_str(authorized.database_id(), collection)?
            .name();
    let stored = state
        .credentials
        .catalog()
        .get_collection(
            authorized.database_id(),
            authorized.tenant_id().as_u64(),
            bare,
        )?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: authorized.tenant_id(),
            collection: bare.to_owned(),
        })?;
    if stored.crdt_signing_required
        && matches!(authorized.plan(), PhysicalPlan::Crdt(CrdtOp::Apply { .. }))
    {
        return Err(crate::Error::RejectedAuthz {
            tenant_id: authorized.tenant_id(),
            resource: format!("collection:{bare}:unsigned_crdt_delta_requires_authenticated_sync"),
        });
    }
    Ok(())
}

/// Preview, authorize, fence, and durably apply one trusted-internal CRDT delta.
pub(crate) async fn dispatch_crdt_apply_admitted_outcome(
    state: &SharedState,
    request: CrdtApplyAdmissionRequest<'_>,
) -> crate::Result<CrdtAdmissionOutcome> {
    let CrdtApplyAdmissionRequest {
        tenant_id,
        database_id,
        collection,
        plan,
        deadline,
        event_source,
        policy,
    } = request;
    // Every CRDT apply converges here; the lease gates it on a drained
    // collection and lives until the apply's outcome.
    let _lease = crate::control::server::shared::clone_write::write_lease(
        state,
        tenant_id,
        database_id,
        &plan,
    )
    .await?;
    let (document_id, delta) = match &plan {
        PhysicalPlan::Crdt(
            CrdtOp::Apply {
                collection: plan_collection,
                document_id,
                delta,
                expected_frontier_digest: None,
                ..
            }
            | CrdtOp::ApplyAuthenticated {
                collection: plan_collection,
                document_id,
                delta,
                expected_frontier_digest: None,
                ..
            },
        ) if plan_collection.as_str() == collection => (document_id.clone(), delta.clone()),
        PhysicalPlan::Crdt(
            CrdtOp::Apply {
                expected_frontier_digest: Some(_),
                ..
            }
            | CrdtOp::ApplyAuthenticated {
                expected_frontier_digest: Some(_),
                ..
            },
        ) => return Err(crate::Error::CrdtAdmissionCallerFence),
        _ => {
            return Err(crate::Error::CrdtAdmissionInvalidPlan {
                reason: "expected an unfenced CRDT Apply for the supplied collection",
            });
        }
    };
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
        .run(|| admit_apply_locked(&workflow, plan, &document_id, &delta))
        .await
}

async fn admit_apply_locked(
    workflow: &CrdtAdmissionWorkflow<'_>,
    plan: PhysicalPlan,
    document_id: &str,
    delta: &[u8],
) -> crate::Result<CrdtAdmissionOutcome> {
    for _attempt in 0..FRONTIER_RETRY_LIMIT {
        let preview = preview(workflow, document_id, delta).await?;
        workflow.policy.evaluate(&preview)?;
        let fenced = stamp_fence(plan.clone(), preview.frontier_digest)?;
        match apply_fenced(workflow, fenced).await {
            Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch { .. })) => {}
            // The trim count belongs to the preview that fenced *this* attempt.
            // A retry re-previews against the advanced frontier and produces its
            // own count, so the two are never mixed.
            result => {
                return result.map(|outcome| outcome.with_trimmed_ops(preview.trimmed_ops));
            }
        }
    }
    Err(crate::Error::CrdtAdmissionRetriesExhausted {
        vshard_id: workflow.vshard_id,
        attempts: FRONTIER_RETRY_LIMIT,
    })
}

/// Payload-only wrapper for an externally authorized caller.
pub async fn dispatch_authorized_crdt_apply_admitted(
    state: &SharedState,
    request: AuthorizedCrdtApplyAdmissionRequest<'_>,
) -> crate::Result<Vec<u8>> {
    Ok(
        dispatch_authorized_crdt_apply_admitted_outcome(state, request)
            .await?
            .payload,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use nodedb_types::CrdtPreviewResult;

    use super::*;
    use crate::control::crdt_admission::policy::TrustedInternalCrdtPolicy;
    use crate::control::crdt_admission::test_support::{
        RecordingPolicy, admission_request, admission_request_until, apply_plan,
        dispatch_crdt_apply_admitted, fixture, preview_payload, respond_n, response,
    };
    use crate::control::server::shared::session::StatementDeadline;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, TenantId};

    #[tokio::test]
    async fn admitted_apply_previews_exact_post_image_before_fenced_apply() {
        let (state, side, _directory) = fixture();
        let digest = [0x4d; 32];
        let post_image = vec![0x81, 0xa2, b'o', b'k', 0xc3];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: post_image.clone(),
            pre_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: digest,
        })
        .expect("preview payload");
        let preview_fields = Arc::new(Mutex::new(Vec::new()));
        let fields = Arc::clone(&preview_fields);
        let responder =
            tokio::spawn(respond_n(
                Arc::clone(&state),
                side,
                1,
                move |request| match request.plan {
                    PhysicalPlan::Crdt(CrdtOp::PreviewApply {
                        collection,
                        document_id,
                        delta,
                    }) => {
                        fields
                            .lock()
                            .expect("fields lock")
                            .push((collection, document_id, delta));
                        response(request.request_id, preview_payload.clone())
                    }
                    other => panic!("unexpected request: {other:?}"),
                },
            ));
        let apply_fences = Arc::new(Mutex::new(Vec::new()));
        let fences = Arc::clone(&apply_fences);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, bytes, _deadline| {
                let fences = Arc::clone(&fences);
                Box::pin(async move {
                    let entry =
                        crate::control::wal_replication::ReplicatedEntry::from_bytes(&bytes)
                            .expect("replicated entry");
                    if let crate::control::wal_replication::ReplicatedWrite::CrdtApplyFenced {
                        expected_frontier_digest,
                        ..
                    } = entry.write
                    {
                        fences
                            .lock()
                            .expect("fences lock")
                            .push(expected_frontier_digest);
                    }
                    Ok(Vec::new())
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install raw/sequenced proposer pair");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy {
            seen: Arc::clone(&seen),
            reject: false,
        };
        dispatch_crdt_apply_admitted(&state, admission_request(&policy))
            .await
            .expect("admitted apply");
        responder.await.expect("responder completes");
        assert_eq!(*seen.lock().expect("seen lock"), vec![post_image]);
        assert_eq!(preview_fields.lock().expect("fields lock").len(), 1);
        assert_eq!(*apply_fences.lock().expect("fences lock"), vec![digest]);
    }

    #[tokio::test]
    async fn denied_policy_dispatches_only_preview_and_does_not_advance_hlc() {
        let (state, side, _directory) = fixture();
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 0,
            trimmed_ops: 0,
            frontier_digest: [7; 32],
        })
        .expect("preview payload");
        let responder = tokio::spawn(respond_n(Arc::clone(&state), side, 1, move |request| {
            assert!(matches!(
                request.plan,
                PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
            ));
            response(request.request_id, preview_payload.clone())
        }));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy { seen, reject: true };
        let before_hlc = state.tenant_write_mark(1);
        let result = dispatch_crdt_apply_admitted(&state, admission_request(&policy)).await;
        responder.await.expect("responder completes");
        assert!(matches!(
            result,
            Err(crate::Error::CrdtAdmissionCallerFence)
        ));
        assert_eq!(
            state.tenant_write_mark(1),
            before_hlc,
            "policy rejection must not advance the tenant write HLC"
        );
        state.wal.sync().expect("sync wal");
        assert!(
            state.wal.replay().expect("replay wal").is_empty(),
            "policy rejection must not append a durable CRDT delta"
        );
    }

    #[tokio::test]
    async fn admitted_cluster_apply_uses_raw_proposer_with_fenced_entry() {
        let (state, side, _directory) = fixture();
        let digest = [0xa5; 32];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: digest,
        })
        .expect("preview payload");
        let responder = tokio::spawn(respond_n(Arc::clone(&state), side, 1, move |request| {
            assert!(matches!(
                request.plan,
                PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
            ));
            response(request.request_id, preview_payload.clone())
        }));
        let fenced = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&fenced);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, bytes, _deadline| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    let entry =
                        crate::control::wal_replication::ReplicatedEntry::from_bytes(&bytes)
                            .expect("replicated entry");
                    match entry.write {
                        crate::control::wal_replication::ReplicatedWrite::CrdtApplyFenced {
                            expected_frontier_digest,
                            ..
                        } => {
                            observed
                                .lock()
                                .expect("fence lock")
                                .push(expected_frontier_digest);
                        }
                        other => panic!("unexpected replicated write: {other:?}"),
                    }
                    Ok(Vec::new())
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install raw/sequenced proposer pair");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy {
            seen,
            reject: false,
        };
        dispatch_crdt_apply_admitted(&state, admission_request(&policy))
            .await
            .expect("cluster admitted apply");
        responder.await.expect("responder completes");
        assert_eq!(*fenced.lock().expect("fence lock"), vec![digest]);
    }

    #[tokio::test]
    async fn stale_frontier_retries_preview_and_policy_before_success() {
        let (state, side, _directory) = fixture();
        let digest = [0x82; 32];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 0,
            trimmed_ops: 0,
            frontier_digest: digest,
        })
        .expect("preview payload");
        let responder = tokio::spawn(respond_n(Arc::clone(&state), side, 2, move |request| {
            assert!(matches!(
                request.plan,
                PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
            ));
            response(request.request_id, preview_payload.clone())
        }));
        let fences = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&fences);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, _bytes, _deadline| {
                let count = Arc::clone(&count);
                Box::pin(async move {
                    if count.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch {
                            expected: digest,
                            actual: [0; 32],
                        }))
                    } else {
                        Ok(Vec::new())
                    }
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install raw/sequenced proposer pair");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy {
            seen: Arc::clone(&seen),
            reject: false,
        };
        dispatch_crdt_apply_admitted(&state, admission_request(&policy))
            .await
            .expect("second fence succeeds");
        responder.await.expect("responder completes");
        assert_eq!(fences.load(Ordering::SeqCst), 2);
        assert_eq!(seen.lock().expect("policy lock").len(), 2);
    }

    #[tokio::test]
    async fn stale_frontier_retries_preview_and_policy_then_exhausts_typed_error() {
        let (state, side, _directory) = fixture();
        let digest = [0x81; 32];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 0,
            trimmed_ops: 0,
            frontier_digest: digest,
        })
        .expect("preview payload");
        let responder = tokio::spawn(respond_n(
            Arc::clone(&state),
            side,
            FRONTIER_RETRY_LIMIT,
            move |request| {
                assert!(matches!(
                    request.plan,
                    PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
                ));
                response(request.request_id, preview_payload.clone())
            },
        ));
        let fenced_count = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&fenced_count);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, _bytes, _deadline| {
                let count = Arc::clone(&count);
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch {
                        expected: digest,
                        actual: [0; 32],
                    }))
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install raw/sequenced proposer pair");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy {
            seen: Arc::clone(&seen),
            reject: false,
        };
        let result = dispatch_crdt_apply_admitted(&state, admission_request(&policy)).await;
        responder.await.expect("responder completes");
        assert!(matches!(
            result,
            Err(crate::Error::CrdtAdmissionRetriesExhausted {
                attempts: FRONTIER_RETRY_LIMIT,
                ..
            })
        ));
        assert_eq!(fenced_count.load(Ordering::SeqCst), FRONTIER_RETRY_LIMIT);
        assert_eq!(
            seen.lock().expect("policy lock").len(),
            FRONTIER_RETRY_LIMIT
        );
    }

    #[tokio::test]
    async fn signed_delta_collection_rejects_plain_external_apply_before_preview() {
        let (state, _side, _directory) = fixture();
        let tenant_id = TenantId::new(1);
        let mut collection = crate::control::security::catalog::StoredCollection::stamped_for_test(
            1, "docs", "owner",
        );
        collection.crdt = true;
        collection.crdt_signing_required = true;
        state
            .credentials
            .catalog()
            .put_collection(DatabaseId::DEFAULT, &collection)
            .expect("store signed-delta collection");
        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs").vshard(),
            plan: apply_plan(),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let identity =
            crate::control::security::identity::AuthenticatedIdentity::new_internal_service(
                1,
                "crdt-test",
                tenant_id,
                Vec::new(),
                true,
                None,
                crate::control::security::identity::AuthenticatedIdentity::default_database_set(
                    true,
                ),
            );
        let authorized = crate::control::server::shared::authorization::authorize_task_set(
            &identity,
            std::slice::from_ref(&task),
            &state.permissions,
            &state.roles,
            &crate::control::security::audit::NoopAuditEmitter,
        )
        .expect("authorize test task")
        .into_tasks()
        .into_iter()
        .next()
        .expect("one authorized task");
        let result = dispatch_authorized_crdt_apply_admitted(
            &state,
            AuthorizedCrdtApplyAdmissionRequest {
                authorized,
                collection: "docs",
                deadline: StatementDeadline::after(Duration::from_millis(10)),
                event_source: EventSource::User,
                policy: &TrustedInternalCrdtPolicy,
            },
        )
        .await;
        assert!(matches!(result, Err(crate::Error::RejectedAuthz { .. })));
    }

    #[tokio::test]
    async fn generic_replicated_dispatch_rejects_unadmitted_apply() {
        let (state, _side, _directory) = fixture();
        let tenant_id = TenantId::new(1);
        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "docs").vshard(),
            plan: apply_plan(),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let identity =
            crate::control::security::identity::AuthenticatedIdentity::new_internal_service(
                1,
                "crdt-test",
                tenant_id,
                Vec::new(),
                true,
                None,
                crate::control::security::identity::AuthenticatedIdentity::default_database_set(
                    true,
                ),
            );
        let authorized = crate::control::server::shared::authorization::authorize_task_set(
            &identity,
            std::slice::from_ref(&task),
            &state.permissions,
            &state.roles,
            &crate::control::security::audit::NoopAuditEmitter,
        )
        .expect("authorize test task")
        .into_tasks()
        .into_iter()
        .next()
        .expect("one authorized task");
        let result = crate::control::server::sync::raft_dispatch::dispatch_write_replicated(
            &state,
            "docs",
            authorized,
            StatementDeadline::of_statement(&state),
            EventSource::User,
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }

    /// A deadline that passes while the preview is unanswered refuses the
    /// delta, and nothing is proposed.
    #[tokio::test]
    async fn a_deadline_before_the_proposal_is_a_definite_refusal() {
        let (state, _side, _directory) = fixture();
        let proposals = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&proposals);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, _bytes, _deadline| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Vec::new()) })
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install proposer");
        let policy = TrustedInternalCrdtPolicy;

        let result = dispatch_crdt_apply_admitted(
            &state,
            admission_request_until(&policy, StatementDeadline::after(Duration::from_millis(50))),
        )
        .await;

        assert!(
            matches!(result, Err(crate::Error::CrdtAdmissionTimeout { .. })),
            "got {result:?}"
        );
        assert_eq!(proposals.load(Ordering::SeqCst), 0, "nothing was proposed");
    }

    /// A deadline that passes while the proposal is in flight leaves the
    /// outcome unknown.
    #[tokio::test]
    async fn a_deadline_during_the_proposal_is_an_unknown_outcome() {
        let (state, side, _directory) = fixture();
        let payload = preview_payload([0x5a; 32]);
        let responder = tokio::spawn(respond_n(Arc::clone(&state), side, 1, move |request| {
            assert!(matches!(
                request.plan,
                PhysicalPlan::Crdt(CrdtOp::PreviewApply { .. })
            ));
            response(request.request_id, payload.clone())
        }));
        let proposals = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&proposals);
        let raw: Arc<crate::control::wal_replication::AsyncRaftProposer> =
            Arc::new(move |_shard, _key, _bytes, _deadline| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(std::future::pending::<crate::Result<Vec<u8>>>())
            });
        crate::control::vshard_admission::install_async_raft_proposer(
            &state,
            crate::control::vshard_admission::applying_submit(raw),
        )
        .expect("install proposer");
        let policy = TrustedInternalCrdtPolicy;

        let result = dispatch_crdt_apply_admitted(
            &state,
            admission_request_until(
                &policy,
                StatementDeadline::after(Duration::from_millis(300)),
            ),
        )
        .await;
        responder.await.expect("responder completes");

        assert!(
            matches!(result, Err(crate::Error::DeadlineExceeded { .. })),
            "got {result:?}"
        );
        assert_eq!(proposals.load(Ordering::SeqCst), 1, "the proposal was sent");
    }
}
