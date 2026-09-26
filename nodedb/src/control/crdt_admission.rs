// SPDX-License-Identifier: BUSL-1.1

//! Serialized preview, policy, and fenced CRDT apply admission.

use std::time::Duration;

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::CrdtPreviewResult;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
use crate::control::server::shared::authorization::AuthorizedTask;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicableWrite, to_replicated_entry};
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId, VShardId};

pub trait CrdtPostImagePolicy: Send + Sync {
    fn evaluate(&self, preview: &CrdtPreviewResult) -> crate::Result<()>;
}

/// Explicit policy for trusted internal callers.
pub struct TrustedInternalCrdtPolicy;

impl CrdtPostImagePolicy for TrustedInternalCrdtPolicy {
    fn evaluate(&self, _preview: &CrdtPreviewResult) -> crate::Result<()> {
        Ok(())
    }
}

const FRONTIER_RETRY_LIMIT: usize = 8;

pub struct AuthorizedCrdtApplyAdmissionRequest<'a> {
    pub authorized: AuthorizedTask,
    pub collection: &'a str,
    pub timeout: Duration,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}

pub struct CrdtApplyAdmissionRequest<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
    pub plan: PhysicalPlan,
    pub timeout: Duration,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}

pub struct CrdtAdmissionOutcome {
    pub payload: Vec<u8>,
    pub write_version: crate::types::Lsn,
    /// Operations the admitted delta encoded that the target document already
    /// knew, measured by the preview that fenced this apply. Distinguishes a
    /// client whose writes land from one whose writes are being absorbed —
    /// both otherwise produce the same successful payload.
    pub trimmed_ops: u64,
}

impl CrdtAdmissionOutcome {
    /// Attach the trim count measured by the preview that fenced this apply.
    fn with_trimmed_ops(self, trimmed_ops: u64) -> Self {
        Self {
            trimmed_ops,
            ..self
        }
    }
}

pub struct CrdtRestoreAdmissionRequest<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub target_version_json: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub peer_id: u64,
    pub timeout: Duration,
    pub event_source: EventSource,
    pub policy: &'a dyn CrdtPostImagePolicy,
}

struct CrdtAdmissionWorkflow<'a> {
    state: &'a SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    collection: &'a str,
    timeout: Duration,
    event_source: EventSource,
    policy: &'a dyn CrdtPostImagePolicy,
}

/// Whether an operation changes the Loro frontier and must serialize with an
/// admission preview when executed directly on a single-node Data Plane.
pub fn changes_crdt_frontier(op: &CrdtOp) -> bool {
    match op {
        CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::RestoreToVersion { .. } => true,
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. } => false,
    }
}

/// Preview, authorize, fence, and durably apply one externally authorized delta.
pub async fn dispatch_authorized_crdt_apply_admitted_outcome(
    state: &SharedState,
    request: AuthorizedCrdtApplyAdmissionRequest<'_>,
) -> crate::Result<CrdtAdmissionOutcome> {
    let AuthorizedCrdtApplyAdmissionRequest {
        authorized,
        collection,
        timeout,
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
            timeout,
            event_source,
            policy,
        },
    )
    .await
}

fn enforce_external_signing_policy(
    state: &SharedState,
    authorized: &AuthorizedTask,
    collection: &str,
) -> crate::Result<()> {
    let stored = state
        .credentials
        .catalog()
        .get_collection(
            authorized.database_id(),
            authorized.tenant_id().as_u64(),
            collection,
        )?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: authorized.tenant_id(),
            collection: collection.to_owned(),
        })?;
    if stored.crdt_signing_required
        && matches!(authorized.plan(), PhysicalPlan::Crdt(CrdtOp::Apply { .. }))
    {
        return Err(crate::Error::RejectedAuthz {
            tenant_id: authorized.tenant_id(),
            resource: format!(
                "collection:{collection}:unsigned_crdt_delta_requires_authenticated_sync"
            ),
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
        timeout,
        event_source,
        policy,
    } = request;
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
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let workflow = CrdtAdmissionWorkflow {
        state,
        tenant_id,
        database_id,
        vshard_id,
        collection,
        timeout,
        event_source,
        policy,
    };
    tokio::time::timeout(
        timeout,
        state.vshard_admission_sequencer.run(vshard_id, || {
            admit_apply_locked(&workflow, plan, &document_id, &delta)
        }),
    )
    .await
    .map_err(|_| crate::Error::CrdtAdmissionTimeout {
        vshard_id,
        timeout_ms: timeout_ms(timeout),
    })?
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

async fn preview(
    workflow: &CrdtAdmissionWorkflow<'_>,
    document_id: &str,
    delta: &[u8],
) -> crate::Result<CrdtPreviewResult> {
    let response = tokio::time::timeout(
        workflow.timeout,
        crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
            workflow.state,
            crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
                crate::control::server::shared::ddl::sync_dispatch::SystemReason::AdmittedContinuation,
                workflow.tenant_id,
                workflow.database_id,
                workflow.collection,
                PhysicalPlan::Crdt(CrdtOp::PreviewApply {
                    collection: nodedb_types::QualifiedCollection::from_stored(
                        workflow.collection.to_owned(),
                    ),
                    document_id: document_id.to_owned(),
                    delta: delta.to_vec(),
                }),
            ),
            workflow.timeout,
            workflow.event_source,
        ),
    )
    .await
    .map_err(|_| crate::Error::CrdtAdmissionTimeout {
        vshard_id: workflow.vshard_id,
        timeout_ms: timeout_ms(workflow.timeout),
    })??;
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    zerompk::from_msgpack(response.payload.as_bytes()).map_err(|error| crate::Error::Internal {
        detail: format!("CRDT preview payload decode: {error}"),
    })
}

fn stamp_fence(plan: PhysicalPlan, digest: [u8; 32]) -> crate::Result<PhysicalPlan> {
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

/// Payload-only compatibility wrapper for trusted internal test callers.
#[cfg(test)]
pub(crate) async fn dispatch_crdt_apply_admitted(
    state: &SharedState,
    request: CrdtApplyAdmissionRequest<'_>,
) -> crate::Result<Vec<u8>> {
    Ok(dispatch_crdt_apply_admitted_outcome(state, request)
        .await?
        .payload)
}

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
        timeout,
        event_source,
        policy,
    } = request;
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    let workflow = CrdtAdmissionWorkflow {
        state,
        tenant_id,
        database_id,
        vshard_id,
        collection,
        timeout,
        event_source,
        policy,
    };
    tokio::time::timeout(
        timeout,
        state.vshard_admission_sequencer.run(vshard_id, || async {
            for _attempt in 0..FRONTIER_RETRY_LIMIT {
                let delta =
                    generate_restore_delta(&workflow, document_id, target_version_json, surrogate)
                        .await?;
                if delta.is_empty() {
                    return Ok(None);
                }
                let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
                    collection: nodedb_types::QualifiedCollection::from_stored(
                        collection.to_owned(),
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
                let preview = preview(&workflow, document_id, &delta).await?;
                workflow.policy.evaluate(&preview)?;
                match apply_fenced(&workflow, stamp_fence(plan, preview.frontier_digest)?).await {
                    Err(crate::Error::DataPlane(ErrorCode::CrdtFrontierMismatch { .. })) => {}
                    result => return result.map(Some),
                }
            }
            Err(crate::Error::CrdtAdmissionRetriesExhausted {
                vshard_id,
                attempts: FRONTIER_RETRY_LIMIT,
            })
        }),
    )
    .await
    .map_err(|_| crate::Error::CrdtAdmissionTimeout {
        vshard_id,
        timeout_ms: timeout_ms(timeout),
    })?
}

async fn generate_restore_delta(
    workflow: &CrdtAdmissionWorkflow<'_>,
    document_id: &str,
    target_version_json: &str,
    surrogate: nodedb_types::Surrogate,
) -> crate::Result<Vec<u8>> {
    let response =
        crate::control::server::shared::ddl::sync_dispatch::dispatch_system_response_with_source(
            workflow.state,
            crate::control::server::shared::ddl::sync_dispatch::SystemTask::new(
                crate::control::server::shared::ddl::sync_dispatch::SystemReason::AdmittedContinuation,
                workflow.tenant_id,
                workflow.database_id,
                workflow.collection,
                PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
                    collection: nodedb_types::QualifiedCollection::from_stored(
                        workflow.collection.to_owned(),
                    ),
                    document_id: document_id.to_owned(),
                    target_version_json: target_version_json.to_owned(),
                    surrogate,
                }),
            ),
            workflow.timeout,
            workflow.event_source,
        )
        .await?;
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    Ok(response.payload.to_vec())
}

async fn apply_fenced(
    workflow: &CrdtAdmissionWorkflow<'_>,
    plan: PhysicalPlan,
) -> crate::Result<CrdtAdmissionOutcome> {
    if let Some(raw) = workflow.state.raw_async_raft_proposer() {
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
        let outcome = tokio::time::timeout(
            workflow.timeout,
            crate::control::wal_replication::propose_replicated_entry(workflow.state, raw, entry),
        )
        .await
        .map_err(|_| crate::Error::CrdtAdmissionTimeout {
            vshard_id: workflow.vshard_id,
            timeout_ms: timeout_ms(workflow.timeout),
        })??;
        // This node's apply of the entry recorded its commit HLC.
        return Ok(CrdtAdmissionOutcome {
            payload: outcome.0,
            write_version: outcome.1,
            trimmed_ops: 0,
        });
    }
    let response = tokio::time::timeout(
        workflow.timeout,
        crate::control::server::dispatch_utils::dispatch_autocommit_write(
            workflow.state,
            crate::control::server::dispatch_utils::AutocommitWrite {
                tenant_id: workflow.tenant_id,
                database_id: workflow.database_id,
                vshard_id: workflow.vshard_id,
                plan,
                trace_id: crate::types::TraceId::ZERO,
                event_source: workflow.event_source,
                txn_id: None,
            },
        ),
    )
    .await
    .map_err(|_| crate::Error::CrdtAdmissionTimeout {
        vshard_id: workflow.vshard_id,
        timeout_ms: timeout_ms(workflow.timeout),
    })??;
    if response.status != Status::Ok {
        return Err(response_error(&response));
    }
    Ok(CrdtAdmissionOutcome {
        payload: response.payload.to_vec(),
        write_version: response.read_version_lsn,
        trimmed_ops: 0,
    })
}

fn timeout_ms(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
}

fn response_error(response: &crate::bridge::envelope::Response) -> crate::Error {
    match response.error_code.as_deref() {
        Some(code) => crate::Error::DataPlane(code.clone()),
        None => crate::Error::Internal {
            detail: String::from_utf8_lossy(&response.payload).into_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use nodedb_types::{CrdtPreviewResult, Surrogate};
    use tokio::time::Duration;

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Response, Status};
    use crate::control::state::SharedState;
    use crate::types::{Lsn, RequestId};
    use crate::wal::WalManager;
    use nodedb_types::QualifiedCollection;

    fn collection() -> String {
        "docs".to_owned()
    }

    fn admission_request<'a>(policy: &'a dyn CrdtPostImagePolicy) -> CrdtApplyAdmissionRequest<'a> {
        CrdtApplyAdmissionRequest {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: "docs",
            plan: apply_plan(),
            timeout: Duration::from_secs(1),
            event_source: EventSource::User,
            policy,
        }
    }

    fn apply_plan() -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "doc-1".into(),
            delta: vec![0x91, 0x01],
            peer_id: 7,
            mutation_id: 9,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        })
    }

    fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("admission.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, side, directory)
    }

    fn response(request_id: RequestId, payload: Vec<u8>) -> Response {
        Response {
            request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    async fn respond_n<F>(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        n: usize,
        mut f: F,
    ) where
        F: FnMut(crate::bridge::envelope::Request) -> Response + Send + 'static,
    {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut handled = 0;
        while handled < n && Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                let response = f(request.inner);
                side.response_tx
                    .try_push(BridgeResponse { inner: response })
                    .expect("fake data-plane response queue has capacity");
                handled += 1;
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        assert_eq!(handled, n, "fake data-plane received expected requests");
        state.poll_and_route_responses();
    }

    struct RecordingPolicy {
        seen: Arc<Mutex<Vec<Vec<u8>>>>,
        reject: bool,
    }

    impl CrdtPostImagePolicy for RecordingPolicy {
        fn evaluate(&self, preview: &CrdtPreviewResult) -> crate::Result<()> {
            self.seen
                .lock()
                .expect("test policy lock")
                .push(preview.post_image_msgpack.clone());
            if self.reject {
                Err(crate::Error::CrdtAdmissionCallerFence)
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn admitted_local_apply_previews_exact_post_image_before_fenced_apply() {
        let (state, side, _directory) = fixture();
        let digest = [0x4d; 32];
        let post_image = vec![0x81, 0xa2, b'o', b'k', 0xc3];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: post_image.clone(),
            imported_ops: 1,
            trimmed_ops: 0,
            frontier_digest: digest,
        })
        .expect("preview payload");
        let preview_fields = Arc::new(Mutex::new(Vec::new()));
        let apply_fences = Arc::new(Mutex::new(Vec::new()));
        let fields = Arc::clone(&preview_fields);
        let fences = Arc::clone(&apply_fences);
        let responder =
            tokio::spawn(respond_n(
                Arc::clone(&state),
                side,
                2,
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
                    PhysicalPlan::Crdt(CrdtOp::Apply {
                        expected_frontier_digest,
                        ..
                    }) => {
                        fences
                            .lock()
                            .expect("fences lock")
                            .push(expected_frontier_digest);
                        response(request.request_id, Vec::new())
                    }
                    other => panic!("unexpected request: {other:?}"),
                },
            ));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let policy = RecordingPolicy {
            seen: Arc::clone(&seen),
            reject: false,
        };
        dispatch_crdt_apply_admitted(&state, admission_request(&policy))
            .await
            .expect("admitted local apply");
        responder.await.expect("responder completes");
        assert_eq!(*seen.lock().expect("seen lock"), vec![post_image]);
        assert_eq!(preview_fields.lock().expect("fields lock").len(), 1);
        assert_eq!(
            *apply_fences.lock().expect("fences lock"),
            vec![Some(digest)]
        );
    }

    #[tokio::test]
    async fn denied_policy_dispatches_only_preview_and_does_not_advance_hlc() {
        let (state, side, _directory) = fixture();
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
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
                    Ok((Vec::new(), Lsn::ZERO))
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(&state, raw)
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
                        Ok((Vec::new(), Lsn::ZERO))
                    }
                })
            });
        crate::control::vshard_admission::install_async_raft_proposer(&state, raw)
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
    async fn stale_restore_regenerates_before_retrying_admission() {
        let (state, side, _directory) = fixture();
        let delta = vec![0x91, 0x02];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
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
                        Ok((Vec::new(), Lsn::ZERO))
                    }
                })
            })
        };
        crate::control::vshard_admission::install_async_raft_proposer(&state, raw)
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
                surrogate: Surrogate::ZERO,
                peer_id: 1,
                timeout: Duration::from_secs(1),
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

    #[tokio::test]
    async fn stale_frontier_retries_preview_and_policy_then_exhausts_typed_error() {
        let (state, side, _directory) = fixture();
        let digest = [0x81; 32];
        let preview_payload = zerompk::to_msgpack_vec(&CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
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
        crate::control::vshard_admission::install_async_raft_proposer(&state, raw)
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
        let mut collection =
            crate::control::security::catalog::StoredCollection::new(1, "docs", "owner");
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
            vshard_id: VShardId::from_collection_in_database(DatabaseId::DEFAULT, "docs"),
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
                timeout: Duration::from_millis(10),
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
            vshard_id: VShardId::from_collection_in_database(DatabaseId::DEFAULT, "docs"),
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
            Duration::from_millis(10),
            EventSource::User,
            None,
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }

    fn assert_frontier_mutation(op: CrdtOp) {
        assert!(changes_crdt_frontier(&op), "{op:?} must serialize");
    }

    fn assert_frontier_read(op: CrdtOp) {
        assert!(!changes_crdt_frontier(&op), "{op:?} must not serialize");
    }

    #[test]
    fn frontier_classifier_covers_every_crdt_operation_category() {
        let surrogate = Surrogate::ZERO;
        assert_frontier_mutation(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert_frontier_mutation(CrdtOp::ImportSnapshot {
            tenant_id: 1,
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            bytes: Vec::new(),
        });
        assert_frontier_mutation(CrdtOp::RestoreToVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            target_version_json: "{}".into(),
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            index: 0,
            fields_json: "{}".into(),
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            index: 0,
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListMove {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            from_index: 0,
            to_index: 1,
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            fields_json: "{}".into(),
            surrogate,
            partial: false,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_frontier_mutation(CrdtOp::DocDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            surrogate,
            returning: None,
            rls_filters: Vec::new(),
        });

        assert_frontier_read(CrdtOp::Read {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
        });
        assert_frontier_read(CrdtOp::PreviewApply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            delta: Vec::new(),
        });
        assert_frontier_read(CrdtOp::GetVersionVector {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::ExportDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            from_version_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::CompactAtVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            target_version_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        assert_frontier_read(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            constraint_version: 1,
        });
        assert_frontier_read(CrdtOp::ReadConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::SetPolicy {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            policy_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::GetPolicy {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::ReadAtVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            version_vector_json: "{}".into(),
        });
    }
}
