// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::RetentionDrop` entry.
//!
//! Every replica of the group runs the decision the entry carries, at this log
//! position, with nothing else of the group in flight. A marker under the entry's
//! key then records the apply. A crash before the marker leaves the entry
//! unmarked, so its re-delivery drops again, and a drop is idempotent. A core that
//! cannot apply yet refuses, and the re-delivery retries it.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::control::retention_log::run_retention;
use crate::control::wal_replication::types::RetentionOp;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// The fields of one committed retention decision.
pub(super) struct RetentionDropEntry {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
    pub op: RetentionOp,
}

/// Apply one drop, with nothing else of its group in flight.
pub(super) fn prepare_retention_drop_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: RetentionDropEntry,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let outcome = apply_retention_drop(ctx, pos, &entry).await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

async fn apply_retention_drop(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &RetentionDropEntry,
) -> EntryOutcome {
    let result = drop_and_mark(ctx, pos, entry)
        .await
        .map(|()| AppliedWrite::new(Vec::new()));
    if let Err(error) = &result {
        tracing::warn!(
            group_id = pos.group_id,
            index = pos.log_index,
            tenant_id = entry.tenant_id.as_u64(),
            %error,
            "applying a committed retention drop failed; re-delivery retries it"
        );
    }
    let durable = result.is_ok();
    let applied = ledger_outcome(&result);
    ctx.tracker
        .complete(pos.group_id, pos.log_index, pos.applied_key, result);
    EntryOutcome::Applied {
        durable,
        result: Some(applied),
    }
}

async fn drop_and_mark(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &RetentionDropEntry,
) -> crate::Result<()> {
    let state = ctx.state;
    run_retention(state, entry.tenant_id, entry.database_id, &entry.op).await?;
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
