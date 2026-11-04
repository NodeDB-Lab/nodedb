// SPDX-License-Identifier: BUSL-1.1

//! Read this node's stored bytes from every local Data Plane core.

use std::collections::HashMap;
use std::time::Instant;

use futures::future::join_all;
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::bridge::envelope::Response;
use crate::control::server::exchange::core_outcome::{
    CoreOutcome, classify_core_response, require_every_core,
};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores_until;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// One core's answer row: `(database_id, tenant_id, stored bytes)`.
type CoreRow = (u64, u64, u64);

/// Send `MetaOp::QueryStorageBytes` to every local core and sum the answers
/// per `(database, tenant)`.
///
/// Every core must answer by `deadline`. A core that fails, refuses or misses
/// the deadline fails the whole read: a partial sum reads as a smaller total.
pub async fn query_local_storage_bytes(
    state: &SharedState,
    led_vshards: Vec<u32>,
    deadline: Instant,
) -> crate::Result<HashMap<(DatabaseId, TenantId), u64>> {
    crate::control::server::broadcast::broadcast_call_count_increment();
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let plan = PhysicalPlan::Meta(MetaOp::QueryStorageBytes { led_vshards });
    let receivers = eager_dispatch_to_all_cores_until(
        state,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        TraceId::generate(),
        None,
        deadline,
        |_| plan.clone(),
    )?;

    let collects = receivers
        .into_iter()
        .map(|(core_id, request_id, mut rx)| async move {
            let context = format!("storage bytes on core {core_id}");
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
        .map(|(core_id, collected)| core_rows(core_id, classify_core_response(collected)));

    let mut totals: HashMap<(DatabaseId, TenantId), u64> = HashMap::new();
    for rows in require_every_core(outcomes)? {
        for (database_id, tenant_id, bytes) in rows {
            let total = totals
                .entry((DatabaseId::new(database_id), TenantId::new(tenant_id)))
                .or_insert(0);
            *total = total.saturating_add(bytes);
        }
    }
    Ok(totals)
}

/// One core's rows, or the error that fails the read.
fn core_rows(core_id: usize, outcome: CoreOutcome<Response>) -> CoreOutcome<Vec<CoreRow>> {
    let Some(response) = outcome? else {
        return Err(crate::Error::Internal {
            detail: format!("core {core_id} refused QueryStorageBytes; the read needs every core"),
        });
    };
    let rows: Vec<CoreRow> = zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("storage bytes of core {core_id}: {e}"),
        }
    })?;
    Ok(Some(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};

    fn response(payload: Vec<u8>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            stage_vote: None,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }

    #[test]
    fn a_core_answer_decodes_as_rows() {
        let rows: Vec<CoreRow> = vec![(0, 1, 10), (5, 2, 7)];
        let payload = zerompk::to_msgpack_vec(&rows).unwrap();
        let decoded = core_rows(2, Ok(Some(response(payload)))).unwrap();
        assert_eq!(decoded, Some(rows));
    }

    #[test]
    fn a_refusing_core_fails_the_read() {
        let err = core_rows(3, Ok(None)).unwrap_err();
        assert!(err.to_string().contains("core 3"), "{err}");
    }

    #[test]
    fn an_undecodable_answer_fails_the_read() {
        let err = core_rows(1, Ok(Some(response(vec![0xc1])))).unwrap_err();
        assert!(err.to_string().contains("core 1"), "{err}");
    }
}
