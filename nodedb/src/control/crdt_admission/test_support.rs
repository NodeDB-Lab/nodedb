// SPDX-License-Identifier: BUSL-1.1

//! Fixtures shared by the admission tests: a one-core state, a fake Data
//! Plane responder, and a recording policy.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::{CrdtPreviewResult, QualifiedCollection, Surrogate};

use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId};
use crate::wal::WalManager;

use super::apply::dispatch_crdt_apply_admitted_outcome;
use super::policy::CrdtPostImagePolicy;
use super::request::CrdtApplyAdmissionRequest;

/// Payload-only wrapper over the trusted-internal admission.
pub(super) async fn dispatch_crdt_apply_admitted(
    state: &SharedState,
    request: CrdtApplyAdmissionRequest<'_>,
) -> crate::Result<Vec<u8>> {
    Ok(dispatch_crdt_apply_admitted_outcome(state, request)
        .await?
        .payload)
}

pub(super) fn admission_request<'a>(
    policy: &'a dyn CrdtPostImagePolicy,
) -> CrdtApplyAdmissionRequest<'a> {
    admission_request_until(policy, StatementDeadline::after(Duration::from_secs(1)))
}

pub(super) fn admission_request_until<'a>(
    policy: &'a dyn CrdtPostImagePolicy,
    deadline: StatementDeadline,
) -> CrdtApplyAdmissionRequest<'a> {
    CrdtApplyAdmissionRequest {
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        collection: "docs",
        plan: apply_plan(),
        deadline,
        event_source: EventSource::User,
        policy,
    }
}

pub(super) fn apply_plan() -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        document_id: "doc-1".into(),
        delta: vec![0x91, 0x01],
        peer_id: 7,
        mutation_id: 9,
        surrogate: Surrogate::new(1),
        provenance: None,
        constraint_version_required: 0,
        expected_frontier_digest: None,
    })
}

pub(super) fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("temporary WAL directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&directory.path().join("admission.wal")).expect("test WAL"),
    );
    let (dispatcher, mut sides) = Dispatcher::new(1, 64);
    let side = sides.pop().expect("one data side");
    let state = SharedState::new(dispatcher, wal).expect("shared state");
    (state, side, directory)
}

pub(super) fn response(request_id: RequestId, payload: Vec<u8>) -> Response {
    Response {
        request_id,
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: payload.into(),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_vshards: crate::types::ReadShards::new(),
        write_set: Vec::new(),
    }
}

/// An encoded preview answer with frontier `digest`.
pub(super) fn preview_payload(digest: [u8; 32]) -> Vec<u8> {
    zerompk::to_msgpack_vec(&CrdtPreviewResult {
        post_image_msgpack: vec![0xc0],
        pre_image_msgpack: vec![0xc0],
        imported_ops: 1,
        trimmed_ops: 0,
        frontier_digest: digest,
    })
    .expect("preview payload")
}

/// Answer `n` Data-Plane requests with `f`, within two seconds.
pub(super) async fn respond_n<F>(
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

pub(super) struct RecordingPolicy {
    pub(super) seen: Arc<Mutex<Vec<Vec<u8>>>>,
    pub(super) reject: bool,
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
