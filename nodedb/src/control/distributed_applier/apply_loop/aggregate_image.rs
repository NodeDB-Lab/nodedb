// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::TimeseriesAggregateImage` entry.
//!
//! Every replica installs the image at this log position, with nothing else of
//! the group in flight, so every re-issued row of the root is applied here. A
//! marker under the entry's key then records the apply. A crash before the marker
//! leaves the entry unmarked, so its re-delivery installs again. Nothing of the
//! group applied between the two, so the second install finds the same rows.

use nodedb_physical::physical_plan::MetaOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::control::server::exchange::gather::gather_all_cores;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// The fields of one committed aggregate image.
pub(super) struct AggregateImageEntry {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
    /// The root, as the Data Plane stores it.
    pub collection: String,
    pub image: Vec<u8>,
}

/// Apply one image, with nothing else of its group in flight.
pub(super) fn prepare_aggregate_image_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: AggregateImageEntry,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let outcome = apply_aggregate_image(ctx, pos, entry).await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

async fn apply_aggregate_image(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: AggregateImageEntry,
) -> EntryOutcome {
    let (group_id, index) = (pos.group_id, pos.log_index);
    let root = entry.collection.clone();
    let result = install_and_mark(ctx, pos, entry)
        .await
        .map(|()| AppliedWrite::new(Vec::new()));
    if let Err(error) = &result {
        tracing::warn!(
            group_id,
            index,
            collection = %root,
            %error,
            "installing a committed aggregate image failed; re-delivery retries it"
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

async fn install_and_mark(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: AggregateImageEntry,
) -> crate::Result<()> {
    let state = ctx.state;
    let AggregateImageEntry {
        database_id,
        tenant_id,
        vshard_id,
        collection,
        image,
    } = entry;
    gather_all_cores(
        state,
        tenant_id,
        database_id,
        PhysicalPlan::Meta(MetaOp::InstallAggregateImage {
            root: collection,
            image,
        }),
        TraceId::generate(),
        None,
    )
    .await?;
    let marked = state
        .wal
        .appender(pos.applied_key)
        .append_proposal_applied(tenant_id, VShardId::new(vshard_id), database_id)?;
    if let Some(lsn) = marked {
        state.wal.wait_durable(lsn).await?;
    }
    Ok(())
}
