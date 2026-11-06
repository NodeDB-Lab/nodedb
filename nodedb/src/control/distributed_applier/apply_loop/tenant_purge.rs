// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::TenantPurge` entry.
//!
//! The steps run in this order on every replica, with nothing else of the
//! group in flight:
//!
//! 1. Every local core purges the tenant's data on the entry's vShards and
//!    checkpoints its memory-only engines.
//! 2. One `CollectionTombstoned` record per collection of the tenant, fsynced.
//!    Replay re-applies the redb-backed engines' records with no floor, so
//!    without it a pre-purge record brings a purged row back at the next boot.
//!    Every record below the barrier is durable without replay: redb rows
//!    commit at apply, and step 1 checkpointed the memory-only engines.
//! 3. A `ProposalApplied` marker under the entry's key, fsynced.
//!
//! A crash before step 3 leaves the entry unmarked, so its re-delivery purges
//! again. The purge is idempotent, and no later entry of the group started.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
use nodedb_types::id::QualifiedCollection;

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// How long every core gets to answer one group's purge.
const PURGE_DEADLINE: Duration = Duration::from_secs(300);

/// The key of the removed-document count every core answers.
const PURGED_COUNT: &str = "documents_removed";

/// The fields of one committed purge.
pub(super) struct TenantPurgeEntry {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
    /// Bare catalog name of every collection of the tenant.
    pub collections: Vec<String>,
    pub vshards: Vec<u32>,
}

/// Apply one purge, with nothing else of its group in flight.
pub(super) fn prepare_tenant_purge_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: TenantPurgeEntry,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let outcome = apply_tenant_purge(ctx, pos, &entry).await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

async fn apply_tenant_purge(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &TenantPurgeEntry,
) -> EntryOutcome {
    let result = purge_and_mark(ctx, pos, entry)
        .await
        .map(|()| AppliedWrite::new(Vec::new()));
    if let Err(error) = &result {
        tracing::warn!(
            group_id = pos.group_id,
            index = pos.log_index,
            tenant_id = entry.tenant_id.as_u64(),
            %error,
            "applying a committed tenant purge failed"
        );
    }
    // A failure is this replica's own: re-delivery retries the idempotent purge.
    let durable = result.is_ok();
    let applied = ledger_outcome(&result);
    ctx.tracker
        .complete(pos.group_id, pos.log_index, pos.applied_key, result);
    EntryOutcome::Applied {
        durable,
        result: Some(applied),
    }
}

async fn purge_and_mark(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &TenantPurgeEntry,
) -> crate::Result<()> {
    let state = ctx.state;
    let plan = PhysicalPlan::Meta(MetaOp::PurgeTenant {
        tenant_id: entry.tenant_id.as_u64(),
        collections: entry
            .collections
            .iter()
            .map(|name| {
                QualifiedCollection::new(entry.database_id, name)
                    .as_str()
                    .to_string()
            })
            .collect(),
        vshards: entry.vshards.clone(),
    });
    crate::control::server::broadcast::broadcast_count_to_all_cores_until(
        state,
        entry.tenant_id,
        entry.database_id,
        plan,
        TraceId::generate(),
        PURGED_COUNT,
        Instant::now() + PURGE_DEADLINE,
    )
    .await?;

    let barrier = state.wal.next_lsn().as_u64();
    let appender = state.wal.appender(NO_APPLY_KEY);
    for name in &entry.collections {
        appender.append_collection_tombstone(entry.tenant_id, entry.database_id, name, barrier)?;
    }
    state.wal.sync()?;

    let marked = state
        .wal
        .appender(pos.applied_key)
        .append_proposal_applied(
            entry.tenant_id,
            VShardId::new(entry.vshard_id),
            entry.database_id,
        )?;
    if let Some(lsn) = marked {
        state.wal.wait_durable(lsn).await?;
    }
    Ok(())
}
