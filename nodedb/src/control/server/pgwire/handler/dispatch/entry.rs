// SPDX-License-Identifier: BUSL-1.1

//! Public dispatch entry points, and the write-HLC bookkeeping wrapper around
//! them.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::resolve::DistributedReadCapture;
use crate::types::{Lsn, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Dispatch a single physical task and wait for the response.
    ///
    /// In cluster mode, writes propose to Raft first and execute only after
    /// quorum commit; reads bypass Raft. `identity` must be passed for every
    /// externally derived task.
    pub(in crate::control::server::pgwire::handler) async fn dispatch_authorized_task(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
    ) -> crate::Result<Response> {
        let mut shard_watermarks = Vec::new();
        let mut distributed_reads = Vec::new();
        self.dispatch_task_hlc(
            task,
            user_id,
            identity,
            &mut shard_watermarks,
            &mut distributed_reads,
        )
        .await
    }

    /// Dispatch a task and return the response, per-shard watermark LSNs a fan
    /// gather observed, and per-side read captures a shuffle JOIN produced.
    /// Used by the transactional read-recording seam.
    pub(in crate::control::server::pgwire::handler) async fn dispatch_authorized_task_with_watermarks(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
    ) -> crate::Result<(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>)> {
        let mut shard_watermarks = Vec::new();
        let mut distributed_reads = Vec::new();
        let resp = self
            .dispatch_task_hlc(
                task,
                user_id,
                identity,
                &mut shard_watermarks,
                &mut distributed_reads,
            )
            .await?;
        Ok((resp, shard_watermarks, distributed_reads))
    }

    async fn dispatch_task_hlc(
        &self,
        task: PhysicalTask,
        user_id: Option<Arc<str>>,
        identity: &AuthenticatedIdentity,
        shard_watermarks: &mut Vec<(VShardId, Lsn)>,
        distributed_reads: &mut Vec<DistributedReadCapture>,
    ) -> crate::Result<Response> {
        let tenant_id = task.tenant_id;
        let result = self
            .dispatch_task_inner(task, user_id, identity, shard_watermarks, distributed_reads)
            .await;
        // Advances per-tenant write-HLC on any successful dispatch; used by RESTORE's
        // staleness gate. Backup captures its watermark after fan-out, so it dominates.
        if let Ok(ref resp) = result
            && resp.status == crate::bridge::envelope::Status::Ok
        {
            self.state.advance_tenant_write_hlc(tenant_id.as_u64());
        }
        result
    }
}
