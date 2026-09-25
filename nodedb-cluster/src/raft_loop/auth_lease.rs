// SPDX-License-Identifier: BUSL-1.1

//! Answer authorization lease renewals and barriers through the host hook.

use crate::error::Result;
use crate::forward::PlanExecutor;
use crate::rpc_codec::{
    AuthBarrierOutcome, AuthBarrierRequest, AuthBarrierResponse, AuthLeaseRenewOutcome,
    AuthLeaseRenewRequest, AuthLeaseRenewResponse, RaftRpc,
};

use super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    pub(super) async fn handle_auth_lease_renew_rpc(
        &self,
        req: AuthLeaseRenewRequest,
    ) -> Result<RaftRpc> {
        let response = match &self.auth_lease {
            Some(service) => service.renew(req).await,
            None => AuthLeaseRenewResponse {
                outcome: AuthLeaseRenewOutcome::NotLeader { leader_hint: None },
            },
        };
        Ok(RaftRpc::AuthLeaseRenewResponse(response))
    }

    pub(super) async fn handle_auth_barrier_rpc(&self, req: AuthBarrierRequest) -> Result<RaftRpc> {
        let response = match &self.auth_lease {
            Some(service) => service.barrier(req).await,
            None => AuthBarrierResponse {
                outcome: AuthBarrierOutcome::NotLeader { leader_hint: None },
            },
        };
        Ok(RaftRpc::AuthBarrierResponse(response))
    }
}
