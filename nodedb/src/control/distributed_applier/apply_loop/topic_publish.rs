// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::TopicPublish` entry.
//!
//! The message is appended to this replica's catalog in the same durable
//! redb transaction that advances the topic's applied position, so an entry
//! re-delivered at the same position appends nothing. A second committed copy
//! of the proposal, at another position, is caught by the proposal ledger:
//! the apply writes a `ProposalApplied` WAL marker under the entry's key and
//! waits for it to be durable before it reports.

use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::AppliedWrite;
use crate::event::topic::{ReplicatedPublish, ReplicatedPublishOutcome, apply_replicated_publish};
use crate::types::{DatabaseId, TenantId, VShardId};

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// The fields of one committed publication.
pub(super) struct TopicPublishEntry {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
    pub topic: String,
    pub payload: String,
    pub event_time: u64,
    pub origin: Option<crate::event::topic::types::PublishOrigin>,
}

/// Apply one publication, with nothing else of its group in flight, and
/// resolve its waiter with the assigned sequence.
pub(super) fn prepare_topic_publish_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: TopicPublishEntry,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let outcome = apply_topic_publish(ctx, pos, &entry).await;
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

async fn apply_topic_publish(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &TopicPublishEntry,
) -> EntryOutcome {
    let outcome = apply_replicated_publish(
        ctx.state,
        ReplicatedPublish {
            database_id: entry.database_id,
            tenant_id: entry.tenant_id.as_u64(),
            topic: &entry.topic,
            payload: &entry.payload,
            event_time: entry.event_time,
            position: pos.change_position(ctx.state, entry.vshard_id),
            origin: entry.origin,
        },
    );
    let outcome = match outcome {
        Ok(ReplicatedPublishOutcome::Appended(sequence)) => mark_applied(ctx, pos, entry)
            .await
            .map(|()| ReplicatedPublishOutcome::Appended(sequence)),
        other => other,
    };
    // A missing topic is every replica's verdict at this position, so it is
    // the entry's final outcome. A storage failure is retried on re-delivery.
    let durable = match &outcome {
        Ok(_) | Err(crate::Error::UndefinedObject { .. }) => true,
        Err(_) => false,
    };
    let result = outcome.and_then(|outcome| {
        let sequence = match outcome {
            ReplicatedPublishOutcome::Appended(sequence) => sequence,
            // A re-delivery after a restart repeats an entry, and no waiter
            // of the first delivery survives it. A committed message the
            // topic already holds has no sequence of its own to report.
            ReplicatedPublishOutcome::AlreadyApplied => 0,
        };
        zerompk::to_msgpack_vec(&sequence)
            .map(AppliedWrite::new)
            .map_err(|error| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("topic publish result: {error}"),
            })
    });
    if let Err(error) = &result {
        tracing::warn!(
            group_id = pos.group_id,
            index = pos.log_index,
            topic = %entry.topic,
            %error,
            "applying committed topic publication failed"
        );
    }
    let applied = ledger_outcome(&result);
    ctx.tracker
        .complete(pos.group_id, pos.log_index, pos.applied_key, result);
    EntryOutcome::Applied {
        durable,
        result: Some(applied),
    }
}

/// Make the entry's proposal key durable in the WAL, so the proposal ledger
/// recovers it after a restart.
async fn mark_applied(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    entry: &TopicPublishEntry,
) -> crate::Result<()> {
    let Some(lsn) = ctx
        .state
        .wal
        .appender(pos.applied_key)
        .append_proposal_applied(
            entry.tenant_id,
            VShardId::new(entry.vshard_id),
            entry.database_id,
        )?
    else {
        return Ok(());
    };
    ctx.state.wal.wait_durable(lsn).await
}
