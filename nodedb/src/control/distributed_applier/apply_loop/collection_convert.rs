// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::CollectionConvert` entry.
//!
//! The steps run in this order on every replica, with nothing else of the
//! group in flight:
//!
//! 1. The collection's home core checkpoints its memory-only engines.
//! 2. One `CollectionTombstoned` record, fsynced. A pre-conversion record holds
//!    a body the target schema can refuse on replay. Every record below the
//!    barrier is durable without replay: redb rows commit at apply, and step 1
//!    checkpointed the memory-only engines.
//! 3. The home core rewrites the rows and records the conversion in one redb
//!    transaction, then takes the target mode. The record makes a re-delivery
//!    answer without a rewrite.
//! 4. A `ProposalApplied` marker under the entry's key, fsynced.
//!
//! A row the target refuses is every replica's verdict at this log position.
//! The entry concludes as a final refusal with no row changed.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, StorageMode};
use nodedb_types::{CollectionKey, QualifiedCollection};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_committed_apply,
};
use crate::control::wal_replication::CollectionIncarnation;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;

use super::collection_route::{self, CollectionRoute};
use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::EntryOutcome;
use super::start::Prepared;
use super::write_dispatch::conclude_generic_entry;

/// How long the home core gets for each step of one conversion.
const CONVERT_DEADLINE: Duration = Duration::from_secs(3600);

/// The fields of one committed conversion.
pub(super) struct CollectionConvertEntry {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
    /// The collection the entry names, with its proposer's incarnation.
    pub incarnations: Vec<CollectionIncarnation>,
    /// Bare catalog name.
    pub collection: String,
    pub target_type: String,
    pub schema_json: String,
    pub source_storage_mode: StorageMode,
    pub target_storage_mode: StorageMode,
    pub generation: u64,
    pub incarnation: nodedb_types::Hlc,
}

/// Apply one conversion, with nothing else of its group in flight.
pub(super) fn prepare_collection_convert_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: CollectionConvertEntry,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let outcome = apply_collection_convert(ctx, pos, entry).await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

async fn apply_collection_convert(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: CollectionConvertEntry,
) -> EntryOutcome {
    let state = ctx.state;
    // A conversion of an incarnation this replica no longer holds has nothing
    // to rewrite. The gate stays held until the core answers.
    let routed = collection_route::route(
        state,
        entry.tenant_id.as_u64(),
        entry.database_id,
        &entry.incarnations,
    )
    .await;
    let _gates = match routed {
        Ok(CollectionRoute::Apply(gates)) => gates,
        Ok(CollectionRoute::Superseded) => {
            ctx.tracker.complete(
                pos.group_id,
                pos.log_index,
                pos.applied_key,
                Err(crate::Error::DataPlane(ErrorCode::NotFound)),
            );
            return EntryOutcome::Applied {
                durable: true,
                result: None,
            };
        }
        Err(error) => {
            ctx.tracker
                .complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
            return EntryOutcome::Applied {
                durable: false,
                result: None,
            };
        }
    };
    let submitted = convert_and_mark(ctx, pos, &entry).await;
    let refused = submitted.as_ref().is_ok_and(is_row_verdict);
    match conclude_generic_entry(ctx.tracker, pos, submitted) {
        EntryOutcome::Applied { durable, result } => EntryOutcome::Applied {
            durable: durable || refused,
            result,
        },
        other => other,
    }
}

/// Whether the core refused the conversion over the rows it read. The rewrite
/// rolled back, and every replica reads the same rows, so the group's durable
/// prefix extends past the entry. An internal error is this replica's own, and
/// a re-delivery retries it.
fn is_row_verdict(response: &Response) -> bool {
    response.status != Status::Ok
        && response
            .error_code
            .as_ref()
            .is_some_and(|code| !matches!(**code, ErrorCode::Internal { .. }))
}

/// Run one step on the collection's home core.
async fn on_home_core(
    ctx: ApplyContext<'_>,
    entry: &CollectionConvertEntry,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    dispatch_committed_apply(
        ctx.state,
        SystemTask::new(
            SystemReason::DdlApply,
            entry.tenant_id,
            CollectionKey::from_bare(entry.database_id, &entry.collection),
            plan,
        ),
        Instant::now() + CONVERT_DEADLINE,
        SystemReason::DdlApply.event_source(),
    )
    .await
}

async fn convert_and_mark(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &CollectionConvertEntry,
) -> crate::Result<Response> {
    let state = ctx.state;
    let checkpoint = on_home_core(ctx, entry, PhysicalPlan::Meta(MetaOp::Checkpoint)).await?;
    if checkpoint.status != Status::Ok {
        return Err(crate::Error::Internal {
            detail: format!(
                "CONVERT of '{}': the home core refused the checkpoint",
                entry.collection
            ),
        });
    }

    let barrier = state.wal.next_lsn().as_u64();
    state
        .wal
        .appender(NO_APPLY_KEY)
        .append_collection_tombstone(
            entry.tenant_id,
            entry.database_id,
            &entry.collection,
            barrier,
        )?;
    state.wal.sync()?;

    let plan = PhysicalPlan::Meta(MetaOp::ConvertCollection {
        collection: QualifiedCollection::new(entry.database_id, &entry.collection),
        target_type: entry.target_type.clone(),
        schema_json: entry.schema_json.clone(),
        source_storage_mode: entry.source_storage_mode.clone(),
        target_storage_mode: entry.target_storage_mode.clone(),
        generation: entry.generation,
        incarnation: entry.incarnation,
    });
    let response = on_home_core(ctx, entry, plan).await?;
    if response.status == Status::Ok {
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
    }
    Ok(response)
}
