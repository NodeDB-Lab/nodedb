// SPDX-License-Identifier: BUSL-1.1

//! Propose a write to Raft and shape the response once it applies.

use std::sync::Arc;

use crate::bridge::envelope::Response;

use super::super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::dispatch_replicated_write`]: the entry to
/// propose and the proposer.
pub(super) struct ReplicatedWrite<'a> {
    pub(super) entry: crate::control::wal_replication::ReplicatedEntry,
    pub(super) proposer: &'a Arc<crate::control::wal_replication::AsyncRaftProposer>,
}

impl NodeDbPgHandler {
    /// Dispatch a write through Raft: propose → register waiter → await apply.
    /// `ProposeTracker` is race-safe against an entry applying before register.
    ///
    /// Every replica publishes the write's change events as it applies the
    /// entry (`ChangeFeedOwner::Replicated`).
    pub(super) async fn dispatch_replicated_write(
        &self,
        args: ReplicatedWrite<'_>,
    ) -> crate::Result<Response> {
        let ReplicatedWrite { entry, proposer } = args;
        let request_id = self.next_request_id();

        // `write_versions` are the versions the write stamped. The session
        // floors its later reads of the written vShards at them.
        let (payload, write_versions) = crate::control::wal_replication::propose_replicated_entry(
            &self.state,
            proposer,
            entry,
            crate::control::wal_replication::statement_propose_deadline(&self.state),
        )
        .await?;

        let response = Response {
            request_id,
            status: crate::bridge::envelope::Status::Ok,
            attempt: 1,
            partial: false,
            payload: payload.into(),
            // A write carries no read watermark.
            watermark_lsn: crate::types::Lsn::ZERO,
            error_code: None,
            stage_vote: None,
            read_versions: write_versions,
            write_set: Vec::new(),
        };

        Ok(response)
    }
}
