// SPDX-License-Identifier: BUSL-1.1

//! Propose a write to Raft and shape the response once it applies.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::server::dispatch_utils::publish_origin_change_events;

use super::super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::dispatch_replicated_write`]: the entry to
/// propose, the proposer, and the identity + plan its origin CDC publish needs.
pub(super) struct ReplicatedWrite<'a> {
    pub(super) entry: crate::control::wal_replication::ReplicatedEntry,
    pub(super) proposer: &'a Arc<crate::control::wal_replication::AsyncRaftProposer>,
    pub(super) authorized: crate::control::server::shared::authorization::AuthorizedTask,
}

impl NodeDbPgHandler {
    /// Dispatch a write through Raft: propose → register waiter → await apply.
    /// `ProposeTracker` is race-safe against an entry applying before register.
    ///
    /// Also the origin CDC publish site; replicas publish nothing (`ChangeFeedOwner::Unowned`).
    pub(super) async fn dispatch_replicated_write(
        &self,
        args: ReplicatedWrite<'_>,
    ) -> crate::Result<Response> {
        let ReplicatedWrite {
            entry,
            proposer,
            authorized,
        } = args;
        let task = authorized.into_physical_task();
        let tenant_id = task.tenant_id;
        let database_id = task.database_id;
        let plan = task.plan;
        let request_id = self.next_request_id();

        // `write_version` is the post-write `coll_write_lsn`, surfaced so the session
        // can floor a later read-set at it (read-your-writes for cross-shard OCC).
        let (payload, write_version) =
            crate::control::wal_replication::propose_replicated_entry(&self.state, proposer, entry)
                .await?;

        let response = Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            // Authoritative participant WAL LSN — CDC ordering must use it, not zero.
            watermark_lsn: write_version,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: write_version,
            write_set: Vec::new(),
        };

        // Propose returned: entry is committed and applied. Publish once, from this plan.
        publish_origin_change_events(&self.state, tenant_id, database_id, &plan, &response);

        Ok(response)
    }
}
