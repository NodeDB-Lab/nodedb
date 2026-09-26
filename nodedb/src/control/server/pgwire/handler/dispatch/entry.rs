// SPDX-License-Identifier: BUSL-1.1

//! Public dispatch entry points.
//!
//! A write records its commit HLC on the tenant's observed high-water where it
//! commits: the write funnel for a local append, the Raft proposer and each
//! replica's apply for a replicated entry.

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
        self.dispatch_task_inner(
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
            .dispatch_task_inner(
                task,
                user_id,
                identity,
                &mut shard_watermarks,
                &mut distributed_reads,
            )
            .await?;
        Ok((resp, shard_watermarks, distributed_reads))
    }
}
