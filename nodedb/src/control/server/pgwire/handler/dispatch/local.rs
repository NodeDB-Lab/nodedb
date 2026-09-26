// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a task directly to the local Data Plane, with or without a
//! per-task WAL append.

use std::sync::Arc;

use crate::bridge::envelope::Response;
use crate::control::server::dispatch_utils::WalDurability;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::super::submit::SubmitArgs;
use super::authorize::reject_unadmitted_crdt_apply;

impl NodeDbPgHandler {
    /// Dispatch a task directly to the local Data Plane (single-node or reads).
    ///
    /// WAL append happens inside the write funnel, under the admission guard just
    /// before enqueue, so LSN order equals apply order. Reads bypass the WAL entirely.
    pub(super) async fn dispatch_local(
        &self,
        checked: crate::control::server::shared::clone_write::CloneCheckedTask,
        user_id: Option<Arc<str>>,
    ) -> crate::Result<Response> {
        self.submit_authorized_to_data_plane(
            checked,
            user_id,
            WalDurability::AppendHere {
                now_override: None,
                apply_key: 0,
                commit_hlc: None,
            },
        )
        .await
    }

    /// Dispatch a task to the Data Plane WITHOUT individual WAL append.
    ///
    /// Used by COMMIT after the transaction is written as one `RecordType::Transaction`
    /// record — per-task WAL would double-write.
    pub(in crate::control::server::pgwire::handler) async fn dispatch_task_no_wal(
        &self,
        task: PhysicalTask,
    ) -> crate::Result<Response> {
        // Without this, a transaction begun before the freeze could COMMIT mid-scan and
        // break the as-of contract.
        use crate::control::security::identity::{Permission, required_permission};
        let perm = required_permission(&task.plan);
        if matches!(perm, Permission::Write | Permission::Admin)
            && self.state.materialize_freeze.is_frozen(task.database_id)
        {
            return Err(crate::Error::SourceFrozen {
                database_id: task.database_id,
            });
        }
        reject_unadmitted_crdt_apply(&task.plan)?;
        let txn_id = task.txn_id;
        // The caller owns the transaction's durability, so the task carries no
        // WAL record of its own.
        self.submit_to_data_plane(SubmitArgs {
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            database_id: task.database_id,
            plan: task.plan,
            user_id: None,
            txn_id,
            // No per-task TTL instant (see `flush_transaction_buffer`), so a TTL-bearing
            // KV write falls back to `epoch_system_ms` at apply time.
            durability: WalDurability::CallerSupplied {
                wal_lsn: None,
                resolved_now_ms: None,
                minted: None,
            },
        })
        .await
    }
}
