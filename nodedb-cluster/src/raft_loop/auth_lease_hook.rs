// SPDX-License-Identifier: BUSL-1.1

//! Hook for the authorization lease service.
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular). The lease table,
//! the coverage rules and the barrier wait live in `nodedb` behind this
//! `Send + Sync` hook. The transport calls it when a lease renewal or an
//! authorization barrier reaches this node. Cluster-only tests leave the
//! `RaftLoop` field `None`, and such a request is answered `NotLeader`.

use crate::rpc_codec::{
    AuthBarrierRequest, AuthBarrierResponse, AuthLeaseRenewRequest, AuthLeaseRenewResponse,
};

#[async_trait::async_trait]
pub trait AuthLeaseService: Send + Sync + 'static {
    /// Grant or withhold the sender's lease from its coverage report.
    async fn renew(&self, req: AuthLeaseRenewRequest) -> AuthLeaseRenewResponse;

    /// Hold the answer until no lease holder can plan against state older
    /// than the request's targets.
    async fn barrier(&self, req: AuthBarrierRequest) -> AuthBarrierResponse;
}
