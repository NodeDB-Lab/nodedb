// SPDX-License-Identifier: BUSL-1.1

//! The durable write every RESTORE re-issue goes through.
//!
//! A restored row installed straight into a Data-Plane map has no WAL record
//! and no Raft entry: it is lost on restart, and only the one node that
//! installed it holds it. A re-issued row is a normal write instead: every
//! replica of its group applies it, and each one's WAL makes it durable.

use std::time::Duration;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::{MintedRecords, RecordOwner};
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Dispatch timeout of one re-issued write. Generous: one restored
/// collection's rows may travel in a single write.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);

/// Write `plan`, restored into `collection`, durably.
///
/// Branches identically to a normal write:
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: append the redo under an outcome-floor window, then
///   `sync_dispatch::dispatch_system`, which closes the window.
///
/// Both branches give the write's events [`crate::event::EventSource::Restore`].
pub async fn reissue_plan_durably(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<()> {
    let vshard = VShardId::from_collection_in_database(database_id, collection);

    if let Some(proposer) = state.async_raft_proposer() {
        let entry = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard,
            &crate::control::wal_replication::ReplicableWrite::decide_for_replication(&plan)?,
        )?
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "restore reissue: the plan restored into '{collection}' did not map to a \
                 replicated write"
            ),
        })?
        // Every replica applies the write as restored: AFTER triggers do not
        // fire again for it.
        .with_event_source(crate::event::EventSource::Restore);
        let (_, write_version) =
            crate::control::wal_replication::propose_replicated_entry(state, proposer, entry)
                .await?;
        tracing::debug!(
            collection,
            vshard_id = vshard.as_u32(),
            write_version = write_version.as_u64(),
            "restore: re-issued write applied on this node"
        );
        return Ok(());
    }

    // Single-node: WAL first (durable for restart replay), then install live.
    // The record's outcome-floor window opens before the append and closes
    // from the install's outcome.
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id: vshard,
    };
    let minted = MintedRecords::open(&state.outcome_floor);
    if let Err(error) = minted.append_plan(
        &state.wal,
        owner,
        &plan,
        sync_dispatch::SystemReason::BackupRestore.event_source(),
    ) {
        // Any record appended before the error never reaches a core.
        minted.cancel(&state.wal, owner, 0).await?;
        return Err(error);
    }
    sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::BackupRestore,
            tenant_id,
            database_id,
            collection,
            plan,
        )
        .with_minted(minted),
        REISSUE_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Log one restore re-issue step: what it writes, where, and who proposes
/// it, so every step of a restore shows in the log.
pub(super) fn log_reissue_step(
    state: &SharedState,
    step: &'static str,
    collection: &str,
    vshard: VShardId,
    rows: usize,
) {
    let group_id =
        crate::control::security::auth_fence::cluster::group_of_vshard(state, vshard.as_u32()).ok();
    tracing::info!(
        step,
        collection,
        vshard_id = vshard.as_u32(),
        group_id = ?group_id,
        rows,
        proposer_node = state.node_id,
        replicated = state.async_raft_proposer().is_some(),
        "restore: re-issuing rows"
    );
}
