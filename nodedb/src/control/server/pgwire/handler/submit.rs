// SPDX-License-Identifier: BUSL-1.1

//! `SubmitArgs` + `submit_to_data_plane`: the pgwire adapter onto the shared
//! Control-Plane write funnel, used by both `dispatch_local` and
//! `dispatch_task_no_wal` (see `dispatch.rs`).

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::types::{DatabaseId, TraceId};

use super::core::NodeDbPgHandler;

/// Inputs for [`NodeDbPgHandler::submit_to_data_plane`]: the request identity,
/// the plan, and the optional transaction id + the write's durability handling.
pub(super) struct SubmitArgs {
    pub(super) tenant_id: crate::types::TenantId,
    pub(super) vshard_id: crate::types::VShardId,
    pub(super) database_id: DatabaseId,
    pub(super) plan: crate::bridge::envelope::PhysicalPlan,
    pub(super) user_id: Option<Arc<str>>,
    /// Roles of the identity that wrote the write. Empty for a write no
    /// user made.
    pub(super) user_roles: Vec<String>,
    pub(super) txn_id: Option<crate::types::TxnId>,
    /// Who owns this write's durable redo record — see [`WalDurability`].
    pub(super) durability: WalDurability,
}

impl NodeDbPgHandler {
    /// Consume a clone-checked authorization capability at the local Data
    /// Plane boundary.
    pub(super) async fn submit_authorized_to_data_plane(
        &self,
        checked: crate::control::server::shared::clone_write::CloneCheckedTask,
        user_id: Option<Arc<str>>,
        durability: WalDurability,
    ) -> crate::Result<Response> {
        // The write lease lives until the submit returns its outcome.
        let (authorized, _lease) = checked.into_parts();
        let user_roles = authorized.identity().role_names();
        let task = authorized.into_physical_task();
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id,
            user_roles,
            txn_id: task.txn_id,
            durability,
        })
        .await
    }

    /// Submit a plan through the shared Control-Plane write funnel: admit, make
    /// durable, enqueue, collect, and publish. Shared by `dispatch_local` and
    /// `dispatch_task_no_wal`.
    pub(super) async fn submit_to_data_plane(&self, args: SubmitArgs) -> crate::Result<Response> {
        let SubmitArgs {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            user_id,
            user_roles,
            txn_id,
            durability,
        } = args;
        submit_write(
            &self.state,
            SubmitWrite {
                tenant_id,
                database_id,
                vshard_id,
                stream: self.request_stream(),
                plan,
                trace_id: TraceId::generate(),
                event_source: crate::event::EventSource::User,
                txn_id,
                user_id,
                user_roles,
                durability,
                ordering: WriteOrdering::Gate,
                // This node alone applies the write: `dispatch_local` is reached
                // only for a staged write or a plan with no replicated entry. The
                // replicated path stages its events under its entry instead —
                // see [`ChangeFeedOwner`].
                change_feed: ChangeFeedOwner::LocalApply,
            },
        )
        .await
        .map(|outcome| outcome.response)
    }
}
