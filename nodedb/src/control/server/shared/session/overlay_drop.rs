// SPDX-License-Identifier: BUSL-1.1

//! Transaction staging-overlay teardown on the current vShard leader.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::gateway::RouteDecision;
use crate::control::gateway::retry::{RetryPacing, retry_not_leader};
use crate::control::state::SharedState;

use super::deadline::{self, StatementDeadline};
use super::leader_forward::{forward_to_leader, resolve_leader};
use super::outcome::TxnDataPlane;
use crate::control::server::graph_dispatch::cluster_resolve::gateway_shared;

/// Release a transaction's staging overlay on the vShard's current leader.
///
/// The drop runs on its own node-default deadline. Its statement can have
/// timed out, and that dead deadline refuses every send before it runs.
pub(super) async fn drop_txn_overlay(
    state: &SharedState,
    dp: &impl TxnDataPlane,
    tenant_id: crate::types::TenantId,
    vshard_id: crate::types::VShardId,
    txn_id: crate::types::TxnId,
) -> crate::Result<()> {
    let _cleanup = deadline::enter_cleanup(state.tuning.network.default_deadline_secs);
    let drop_plan = PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id });
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: crate::types::DatabaseId::DEFAULT,
        plan: drop_plan.clone(),
        post_set_op: PostSetOp::None,
        txn_id: Some(txn_id),
    };
    match resolve_leader(&task, state) {
        RouteDecision::Local => {
            dp.dispatch_no_wal(task).await?;
            Ok(())
        }
        _ => {
            let shared = gateway_shared(state)?;
            let routing_ref = shared.cluster_routing.as_deref();
            // Reads the cleanup deadline installed above.
            let cleanup_deadline = StatementDeadline::of_statement(&shared).tokio_instant();
            let pacing = RetryPacing::from_tuning(&shared.tuning.cluster_transport);
            retry_not_leader(routing_ref, cleanup_deadline, pacing, |attempt| {
                let task = task.clone();
                let drop_plan = drop_plan.clone();
                async move {
                    match resolve_leader(&task, state) {
                        RouteDecision::Local => dp.dispatch_no_wal(task).await,
                        remote => {
                            forward_to_leader(state, remote, task, &drop_plan, attempt.deadline)
                                .await
                        }
                    }
                }
            })
            .await
            .map(|_| ())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::{Payload, Response, Status};
    use crate::control::server::shared::session::conn_scope;
    use crate::types::{Lsn, RequestId, TenantId, TxnId, VShardId};
    use crate::wal::WalManager;

    /// Records the statement deadline each dispatch runs under.
    #[derive(Default)]
    struct DeadlineDp {
        seen: Mutex<Vec<Option<Instant>>>,
    }

    impl TxnDataPlane for DeadlineDp {
        fn dispatch_no_wal<'a>(
            &'a self,
            _task: PhysicalTask,
        ) -> Pin<Box<dyn Future<Output = crate::Result<Response>> + Send + 'a>> {
            self.seen.lock().unwrap().push(deadline::current());
            Box::pin(async move {
                Ok(Response {
                    request_id: RequestId::new(1),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    stage_vote: None,
                    read_vshards: crate::types::ReadShards::new(),
                    write_set: Vec::new(),
                })
            })
        }

        fn event_source(&self) -> crate::event::EventSource {
            crate::event::EventSource::User
        }
    }

    /// A drop after its statement timed out runs on a fresh node-default
    /// deadline, and the statement's deadline returns afterwards.
    #[tokio::test]
    async fn an_overlay_drop_outlives_its_timed_out_statement() {
        let dir = tempfile::tempdir().unwrap();
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap(),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).unwrap();
        let default_secs = state.tuning.network.default_deadline_secs;
        let dp = DeadlineDp::default();

        conn_scope::scoped(async {
            let _statement = deadline::enter(Some(Duration::from_millis(1)), default_secs);
            let dead = deadline::current().expect("the statement installs a deadline");
            tokio::time::sleep(Duration::from_millis(5)).await;

            drop_txn_overlay(
                &state,
                &dp,
                TenantId::new(1),
                VShardId::new(3),
                TxnId::new(7),
            )
            .await
            .unwrap();

            let seen = dp.seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            let cleanup = seen[0].expect("the drop runs under a deadline");
            assert!(
                cleanup > Instant::now() + Duration::from_secs(default_secs.saturating_sub(1)),
                "the drop ran under the statement's dead deadline"
            );
            assert_eq!(deadline::current(), Some(dead));
        })
        .await;
    }
}
