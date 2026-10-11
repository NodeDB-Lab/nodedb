// SPDX-License-Identifier: BUSL-1.1

//! Test fake: a Data Plane core that acknowledges every request.

use std::sync::Arc;

use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide};
use crate::bridge::envelope::{Payload, Response, Status};
use crate::types::Lsn;

use super::SharedState;

/// Answer every request on `side` with `Ok` and route the answers to
/// `state`'s waiters, for as long as the task runs. The caller spawns it on
/// the test's tokio runtime.
pub(crate) async fn acknowledge_every_request(
    state: Arc<SharedState>,
    mut side: CoreChannelDataSide,
) {
    loop {
        while let Ok(request) = side.request_rx.try_pop() {
            side.response_tx
                .try_push(BridgeResponse {
                    inner: Response {
                        request_id: request.inner.request_id,
                        status: Status::Ok,
                        attempt: 1,
                        partial: false,
                        payload: Payload::empty(),
                        watermark_lsn: Lsn::ZERO,
                        error_code: None,
                        stage_vote: None,
                        read_versions: crate::types::ReadVersions::new(),
                        write_set: Vec::new(),
                    },
                })
                .expect("fake data-plane response queue has capacity");
        }
        state.poll_and_route_responses();
        tokio::task::yield_now().await;
    }
}
