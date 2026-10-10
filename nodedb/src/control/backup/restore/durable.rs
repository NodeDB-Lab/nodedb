// SPDX-License-Identifier: BUSL-1.1

//! The durable write every RESTORE re-issue goes through.
//!
//! A restored row installed straight into a Data-Plane map has no WAL record
//! and no Raft entry: it is lost on restart, and only the one node that
//! installed it holds it. A re-issued row is a normal write instead: every
//! replica of its group applies it, and each one's WAL makes it durable.

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{HomedRecord, RecordHomes, TenantId, VShardId};

use super::target::DatabaseTarget;

/// Write `plan`, restored into `collection`, durably, on the collection's
/// home vShard. `collection` is the bare catalog name in `database_id`. Graph
/// edges never come here: they re-issue per endpoint home as redo.
///
/// Proposes the write as a normal replicated write does:
/// `to_replicated_entry` + `propose_replicated_entry`. The entry gives the
/// write's events [`crate::event::EventSource::Restore`] and carries
/// `target.restore_id`, so every replica marks it under that restore.
pub(crate) async fn reissue_plan_durably(
    state: &SharedState,
    tenant_id: TenantId,
    target: DatabaseTarget,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<()> {
    let database_id = target.dest;
    let vshard = RecordHomes::of(HomedRecord::Row(nodedb_types::CollectionKey::from_bare(
        database_id,
        collection,
    )))
    .owner();

    // The entry logs resolved rows: a timeseries ingest resolves here, before
    // its entry exists.
    let resolved = crate::control::write_resolve::resolve_for_log(
        state,
        crate::control::write_resolve::WriteResolveContext {
            tenant_id,
            database_id,
        },
        vshard,
        &plan,
    )
    .await?;
    let plan = resolved.unwrap_or(plan);

    let proposer = state.async_raft_proposer()?;
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
    .with_event_source(crate::event::EventSource::Restore)
    .with_restore_id(target.restore_id);
    let deadline = crate::control::wal_replication::statement_propose_deadline(state);
    let (_, write_versions) =
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry, deadline)
            .await?;
    tracing::debug!(
        collection,
        vshard_id = vshard.as_u32(),
        write_version = ?write_versions.of(vshard),
        "restore: re-issued write applied on this node"
    );
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
        "restore: re-issuing rows"
    );
}
