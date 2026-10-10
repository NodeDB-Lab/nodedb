// SPDX-License-Identifier: BUSL-1.1

//! Send each core its share of a snapshot install over the SPSC bridge and
//! wait for every core to acknowledge.

use std::time::{Duration, Instant};

use futures::future::join_all;

use crate::bridge::envelope::{
    Admission, ErrorCode, ExemptReason, PhysicalPlan, Priority, Request, Response, Status,
};
use crate::control::local_dispatch::{DeadlineCollect, collect_under_deadline};
use crate::control::server::shared::ddl::sync_dispatch::SystemReason;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::error::SnapshotInstallError;

/// Dispatch `plans[i]` to core `i` and wait for every core's response.
///
/// Returns only after every enqueued share has answered or timed out, so a
/// re-install never runs beside a share of the previous attempt. The first
/// core error is returned. An install is complete only when every core
/// answered `Ok`.
pub async fn install_on_every_core(
    state: &SharedState,
    group_id: u64,
    plans: Vec<PhysicalPlan>,
    timeout: Duration,
) -> Result<(), SnapshotInstallError> {
    let deadline = Instant::now() + timeout;
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let trace_id = TraceId::generate();
    let mut first_error: Option<SnapshotInstallError> = None;
    let mut pending = Vec::with_capacity(plans.len());

    for (core_id, plan) in plans.into_iter().enumerate() {
        let request_id = state.next_request_id();
        let request = Request {
            request_id,
            tenant_id: TenantId::new(0),
            database_id: DatabaseId::DEFAULT,
            // `dispatch_to_core` bypasses vShard routing. The share names its
            // own database, tenant, and collection per entry.
            vshard_id: VShardId::new(core_id as u32),
            plan,
            deadline,
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: SystemReason::ClusterSnapshot.event_source(),
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
        };
        let rx = state.tracker.register(request_id);
        let dispatched = state
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request);
        match dispatched {
            Ok(()) => pending.push((core_id, request_id, rx)),
            Err(source) => {
                state.tracker.cancel(&request_id);
                first_error.get_or_insert(SnapshotInstallError::CoreInstall {
                    group_id,
                    core_id,
                    source,
                });
            }
        }
    }

    let node_id = state.node_id;
    let outcomes = join_all(
        pending
            .into_iter()
            .map(|(core_id, request_id, mut rx)| async move {
                let context = format!("snapshot install on core {core_id}");
                let result = collect_under_deadline(
                    &mut rx,
                    DeadlineCollect {
                        request_id,
                        deadline,
                        max_result_bytes,
                        context: &context,
                    },
                )
                .await
                .and_then(installed);
                (
                    core_id,
                    result.and_then(|()| injected_fault(node_id, core_id)),
                )
            }),
    )
    .await;

    for (core_id, result) in outcomes {
        if let Err(source) = result {
            first_error.get_or_insert(SnapshotInstallError::CoreInstall {
                group_id,
                core_id,
                source,
            });
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// A core's install response: `Ok` only for a completed install. Unlike a
/// read, a `NotFound` refusal is an error here.
fn installed(resp: Response) -> crate::Result<()> {
    match resp.status {
        Status::Ok => Ok(()),
        Status::Partial | Status::Error => Err(crate::Error::DataPlane(
            resp.error_code
                .as_deref()
                .cloned()
                .unwrap_or_else(|| ErrorCode::Internal {
                    detail: "snapshot install share returned a non-Ok status with no error code"
                        .into(),
                }),
        )),
    }
}

/// Fail point `snapshot_install::node<N>::core<C>`: core `C` of node `N`
/// reports its share as failed after it installed it.
fn injected_fault(node_id: u64, core_id: usize) -> crate::Result<()> {
    #[cfg(feature = "failpoints")]
    if let Some(detail) =
        crate::fail_point::eval_fail(&format!("snapshot_install::node{node_id}::core{core_id}"))
    {
        return Err(crate::Error::DataPlane(ErrorCode::Internal { detail }));
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = (node_id, core_id);
    Ok(())
}
