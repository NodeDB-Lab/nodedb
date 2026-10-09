// SPDX-License-Identifier: BUSL-1.1

//! The Event Plane lane state a Raft group snapshot carries.
//!
//! A follower caught up by a snapshot never applies the entries it covers,
//! so its Event Plane never holds their trigger actions or committed
//! messages. The snapshot carries them for the group's partitions, with the
//! partitions' cursors. The follower can then own the partitions without
//! skipping an event.
//!
//! The builder captures under the group's apply fence, after every consumer
//! delivered every event its core emitted before the capture. So the capture
//! holds every event of the entries at or below the cut. A WAL record that
//! reached no core emits no event, so the wait never depends on one.

use std::collections::HashSet;
use std::time::Duration;

use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::event::topic::committed::cursor::{delivered_through, raise_delivered_here};
use crate::event::topic::types::PublishOrigin;
use crate::types::snapshot::{GroupEventLane, HeldAtPosition, PartitionCursor};
use crate::wal::RedoPublish;

use super::cursor::{fired_through, raise_fired_here};

/// Longest a capture waits for the consumers to deliver the emitted events.
const DELIVERY_WAIT: Duration = Duration::from_secs(30);

fn lane_error(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Internal {
        detail: format!("group snapshot event lane: {detail}"),
    }
}

/// Every partition of `vshards`, in order: each vShard is one partition.
fn partitions_of(vshards: &HashSet<u32>) -> Vec<u32> {
    let mut partitions: Vec<u32> = vshards.iter().copied().collect();
    partitions.sort_unstable();
    partitions
}

fn cursor_row(partition: u32, cursor: CdcOffset) -> Option<PartitionCursor> {
    (cursor != CdcOffset::ZERO).then_some((partition, cursor.epoch, cursor.index, cursor.sequence))
}

fn held_row(partition: u32, position: CdcOffset, bytes: Vec<u8>) -> HeldAtPosition {
    (
        partition,
        position.epoch,
        position.index,
        position.sequence,
        bytes,
    )
}

/// Capture the lane state of `vshards`. Runs under the group's apply fence,
/// once every entry at or below the cut applied here.
pub async fn capture(state: &SharedState, vshards: &HashSet<u32>) -> crate::Result<GroupEventLane> {
    let Some(ledgers) = state.sink_ledgers.get() else {
        return Ok(GroupEventLane::default());
    };
    // Every entry at or below the cut applied, so its events left the cores
    // before this read. Without counters no consumer runs to hold anything.
    if let Some(emitted) = state.authorization_fence.emitted_snapshot()
        && !ledgers
            .actions
            .delivered
            .wait_through(&emitted, DELIVERY_WAIT)
            .await
    {
        return Err(lane_error(format!(
            "the Event Plane did not deliver the events the cores emitted ({emitted:?}) \
             within {DELIVERY_WAIT:?}"
        )));
    }
    let mut lane = GroupEventLane::default();
    for partition in partitions_of(vshards) {
        for (position, bytes) in ledgers.actions.ledger.rows_of(partition)? {
            lane.held_actions.push(held_row(partition, position, bytes));
        }
        lane.action_cursors
            .extend(cursor_row(partition, fired_through(state, partition)));
        for (origin, publish) in
            ledgers
                .publishes
                .held_after(partition, CdcOffset::ZERO, usize::MAX)?
        {
            let bytes = zerompk::to_msgpack_vec(&publish).map_err(lane_error)?;
            lane.held_publishes
                .push(held_row(partition, origin.position, bytes));
        }
        lane.publish_cursors
            .extend(cursor_row(partition, delivered_through(state, partition)));
    }
    Ok(lane)
}

/// Install `lane`, a snapshot of `vshards` cut at `cut_index`, on this node.
///
/// A partition's rows at or below the cut are the builder's: this node never
/// applies those entries. Each cursor rises to the builder's.
pub fn install(
    state: &SharedState,
    vshards: &HashSet<u32>,
    cut_index: u64,
    lane: GroupEventLane,
) -> crate::Result<()> {
    let Some(ledgers) = state.sink_ledgers.get() else {
        return Ok(());
    };
    let GroupEventLane {
        held_actions,
        action_cursors,
        held_publishes,
        publish_cursors,
    } = lane;
    for partition in partitions_of(vshards) {
        let rows: Vec<(CdcOffset, Vec<u8>)> = held_actions
            .iter()
            .filter(|row| row.0 == partition)
            .map(|(_, epoch, index, sequence, bytes)| {
                (CdcOffset::at(*epoch, *index, *sequence), bytes.clone())
            })
            .collect();
        ledgers.actions.ledger.replace_through(
            partition,
            Some(CdcOffset::whole_write(0, cut_index)),
            &rows,
        )?;
    }
    for (partition, epoch, index, sequence, bytes) in held_publishes {
        let publish: RedoPublish = zerompk::from_msgpack(&bytes).map_err(lane_error)?;
        let origin = PublishOrigin {
            partition,
            position: CdcOffset::at(epoch, index, sequence),
        };
        ledgers.publishes.hold(&origin, &publish)?;
    }
    for (partition, epoch, index, sequence) in action_cursors {
        raise_fired_here(state, partition, CdcOffset::at(epoch, index, sequence))?;
    }
    for (partition, epoch, index, sequence) in publish_cursors {
        raise_delivered_here(state, partition, CdcOffset::at(epoch, index, sequence))?;
    }
    ledgers.actions.wake.notify_one();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_covers_the_partitions_of_its_vshards_in_order() {
        let vshards: HashSet<u32> = [3, 1].into_iter().collect();
        assert_eq!(partitions_of(&vshards), vec![1, 3]);
    }

    #[test]
    fn a_zero_cursor_is_not_carried() {
        assert_eq!(cursor_row(4, CdcOffset::ZERO), None);
        assert_eq!(
            cursor_row(4, CdcOffset::data_event(0, 7, 1)),
            Some((4, 0, 7, CdcOffset::data_event(0, 7, 1).sequence))
        );
    }
}
