// SPDX-License-Identifier: BUSL-1.1

//! Base-snapshot fan: dispatch `CreateSnapshot` to every local core and keep
//! each core's encoded `CoreSnapshot` beside its core id.

use std::time::Instant;

use futures::future::join_all;

use crate::bridge::envelope::Response;
use crate::control::server::exchange::core_outcome::{
    CoreOutcome, classify_core_response, require_every_core,
};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores_until;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

/// Capture every local core for one base snapshot.
///
/// Returns one `(core_id, encoded CoreSnapshot)` per core, in core order.
/// A base needs every core: a core that fails, refuses, or answers empty fails
/// the whole capture. Every core's request expires at `deadline`.
pub(crate) async fn capture_base_on_local_cores(
    state: &SharedState,
    deadline: Instant,
) -> crate::Result<Vec<(usize, Vec<u8>)>> {
    crate::control::server::broadcast::broadcast_call_count_increment();
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let receivers = eager_dispatch_to_all_cores_until(
        state,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        TraceId::generate(),
        None,
        deadline,
        |_| PhysicalPlan::Meta(MetaOp::CreateSnapshot),
    )?;

    let collects = receivers
        .into_iter()
        .map(|(core_id, request_id, mut rx)| async move {
            let context = format!("base snapshot on core {core_id}");
            // A core image is one terminal frame, which the byte ceiling
            // applies to only when it streams.
            let collected = crate::control::local_dispatch::collect_under_deadline(
                &mut rx,
                crate::control::local_dispatch::DeadlineCollect {
                    request_id,
                    deadline,
                    max_result_bytes,
                    context: &context,
                },
            )
            .await;
            (core_id, collected)
        });
    let outcomes = join_all(collects)
        .await
        .into_iter()
        .map(|(core_id, collected)| core_image(core_id, classify_core_response(collected)));
    require_every_core(outcomes)
}

/// One core's image, or the error that fails the capture.
fn core_image(core_id: usize, outcome: CoreOutcome<Response>) -> CoreOutcome<(usize, Vec<u8>)> {
    let Some(response) = outcome? else {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("core {core_id} refused CreateSnapshot; a base needs every core"),
        });
    };
    if response.payload.is_empty() {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("core {core_id} answered CreateSnapshot with no image"),
        });
    }
    Ok(Some((core_id, response.payload.as_ref().to_vec())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload, Status};
    use crate::types::{Lsn, RequestId};

    fn response(status: Status, payload: &[u8], code: Option<ErrorCode>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload.to_vec()),
            watermark_lsn: Lsn::ZERO,
            error_code: code.map(Box::new),
            stage_vote: None,
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        }
    }

    #[test]
    fn an_answering_core_keeps_its_id() {
        let image = core_image(3, Ok(Some(response(Status::Ok, b"img", None))));
        assert_eq!(image.unwrap(), Some((3, b"img".to_vec())));
    }

    #[test]
    fn a_refusing_core_fails_the_capture() {
        let err = core_image(2, Ok(None)).unwrap_err();
        assert!(err.to_string().contains("core 2"), "{err}");
    }

    #[test]
    fn an_empty_image_fails_the_capture() {
        let err = core_image(1, Ok(Some(response(Status::Ok, b"", None)))).unwrap_err();
        assert!(err.to_string().contains("no image"), "{err}");
    }

    #[test]
    fn one_failed_core_fails_every_core() {
        let outcomes = vec![
            core_image(0, Ok(Some(response(Status::Ok, b"a", None)))),
            core_image(1, Ok(None)),
        ];
        assert!(require_every_core(outcomes).is_err());
    }
}
