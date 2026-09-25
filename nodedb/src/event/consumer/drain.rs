// SPDX-License-Identifier: BUSL-1.1

//! Take events off one core's ring, and learn when the ring dropped some.
//!
//! The core numbers its events contiguously. An event that does not follow
//! the last one taken means the ring dropped the numbers in between. So does
//! a ring that ran empty while the core's emitted counter, read before the
//! drain, is above the last number taken: the core emitted events the ring
//! no longer holds.
//!
//! A tail drop hands the dropped numbers to WAL catch-up: the cursor counts
//! them as taken, since each one's record is in the WAL recovery replays.
//!
//! The cursor also advances the consumer's safe prefix. Before a drain it
//! reads the final-outcome bound, then the emitted counter. Every record at
//! or below the bound was applied and emitted its events before the bound
//! was read, so once the cursor has taken every event up to the counter, and
//! the ring dropped none of them, every event of those records was taken.

use crate::event::bus::EventConsumerRx;
use crate::event::consumer_helpers::{DRAIN_BATCH_LIMIT, detect_sequence_gap, record_event};
use crate::event::metrics::CoreMetrics;
use crate::event::types::WriteEvent;
use crate::types::Lsn;

/// The events one drain took, in ring order.
#[derive(Debug)]
pub struct Drained {
    pub events: Vec<WriteEvent>,
    /// The ring dropped at least one event before or during this drain.
    pub dropped: bool,
}

/// A final-outcome bound and the emitted counter read after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SafeSnapshot {
    final_bound: Lsn,
    emitted: u64,
}

/// Position of the consumer in one core's event numbers.
#[derive(Debug, Default)]
pub struct RingCursor {
    last_sequence: u64,
    pending: Option<SafeSnapshot>,
}

impl RingCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remember a snapshot to advance the safe prefix by, unless one is
    /// waiting. Read `final_bound` before `emitted`.
    pub fn take_snapshot(&mut self, final_bound: Lsn, emitted: u64) {
        if self.pending.is_none() {
            self.pending = Some(SafeSnapshot {
                final_bound,
                emitted,
            });
        }
    }

    /// The bound of the waiting snapshot once every event it counted was
    /// taken. Clears the snapshot.
    pub fn settled_bound(&mut self) -> Option<Lsn> {
        match self.pending {
            Some(snapshot) if self.last_sequence >= snapshot.emitted => {
                self.pending = None;
                Some(snapshot.final_bound)
            }
            _ => None,
        }
    }

    /// Take up to [`DRAIN_BATCH_LIMIT`] events. `emitted_before` is the core's
    /// emitted counter, read before this call.
    pub fn drain(
        &mut self,
        rx: &mut EventConsumerRx,
        metrics: &CoreMetrics,
        core_id: usize,
        emitted_before: u64,
    ) -> Drained {
        let mut events = Vec::new();
        let mut dropped = false;
        let mut emptied = true;
        while let Some(event) = rx.try_recv() {
            if event.sequence != self.last_sequence.saturating_add(1) {
                detect_sequence_gap(core_id, &event, self.last_sequence, metrics);
                dropped = true;
            }
            self.last_sequence = self.last_sequence.max(event.sequence);
            events.push(event);
            if events.len() >= DRAIN_BATCH_LIMIT as usize {
                emptied = false;
                break;
            }
        }
        if emptied && self.last_sequence < emitted_before {
            metrics.record_drop(emitted_before - self.last_sequence);
            tracing::warn!(
                core_id,
                last_taken = self.last_sequence,
                emitted = emitted_before,
                "event ring dropped its newest events; WAL catch-up recovers them"
            );
            dropped = true;
            // The dropped numbers belong to WAL catch-up from here on: each
            // one's record is in the WAL the recovery replays. The cursor
            // counts them as taken, so a later snapshot can settle and the
            // next event on the ring is not a gap.
            self.last_sequence = emitted_before;
        }
        if dropped {
            // The waiting snapshot counted events the ring dropped.
            self.pending = None;
        }
        for event in &events {
            record_event(core_id, event, metrics);
        }
        Drained { events, dropped }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn make_event(seq: u64) -> WriteEvent {
        WriteEvent {
            sequence: seq,
            collection: Arc::from("test"),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
            lsn: Lsn::new(seq * 10),
            record: None,
            database_id: DatabaseId::new(7),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"data".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[test]
    fn contiguous_events_are_taken_without_a_drop() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        for seq in 1..=3 {
            producers[0].emit(make_event(seq));
        }
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, 3);
        assert_eq!(drained.events.len(), 3);
        assert!(!drained.dropped);
    }

    /// Every event taken off the ring is returned, including the one after a
    /// gap: each is a real event, and the guard decides whether to deliver
    /// it.
    #[test]
    fn a_gap_is_reported_and_every_event_is_returned() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        producers[0].emit(make_event(1));
        producers[0].emit(make_event(2));
        producers[0].emit(make_event(4));
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, 4);
        let sequences: Vec<u64> = drained.events.iter().map(|e| e.sequence).collect();
        assert_eq!(sequences, vec![1, 2, 4]);
        assert!(drained.dropped);
    }

    /// A ring that ran empty below the emitted counter dropped its newest
    /// events, even though no later event shows a gap.
    #[test]
    fn a_tail_drop_is_reported() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 4);
        for seq in 1..=4 {
            assert!(producers[0].emit(make_event(seq)));
        }
        assert!(!producers[0].emit(make_event(5)));
        let emitted = consumers[0].progress().emitted();
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, emitted);
        assert_eq!(drained.events.len(), 4);
        assert!(drained.dropped);
    }

    /// After a tail drop the cursor stops reporting it: the dropped numbers
    /// are recovery's, so the next empty drain reports nothing and a new
    /// snapshot settles.
    #[test]
    fn a_tail_drop_is_reported_once_and_later_snapshots_settle() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 4);
        for seq in 1..=4 {
            assert!(producers[0].emit(make_event(seq)));
        }
        assert!(!producers[0].emit(make_event(5)));
        let emitted = consumers[0].progress().emitted();
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        assert!(
            cursor
                .drain(&mut consumers[0], &metrics, 0, emitted)
                .dropped
        );

        cursor.take_snapshot(Lsn::new(50), emitted);
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, emitted);
        assert!(!drained.dropped, "a tail drop is reported once");
        assert_eq!(cursor.settled_bound(), Some(Lsn::new(50)));

        producers[0].emit(make_event(emitted + 1));
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, emitted + 1);
        assert!(
            !drained.dropped,
            "the next event after a tail drop is not a gap"
        );
    }

    #[test]
    fn the_snapshot_settles_once_its_events_are_taken() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        producers[0].emit(make_event(1));
        cursor.take_snapshot(Lsn::new(50), 2);
        cursor.drain(&mut consumers[0], &metrics, 0, 1);
        assert_eq!(cursor.settled_bound(), None);
        producers[0].emit(make_event(2));
        cursor.drain(&mut consumers[0], &metrics, 0, 2);
        assert_eq!(cursor.settled_bound(), Some(Lsn::new(50)));
        assert_eq!(cursor.settled_bound(), None);
    }

    #[test]
    fn a_drop_discards_the_waiting_snapshot() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        let metrics = CoreMetrics::new();
        let mut cursor = RingCursor::new();
        cursor.take_snapshot(Lsn::new(50), 3);
        producers[0].emit(make_event(1));
        producers[0].emit(make_event(3));
        let drained = cursor.drain(&mut consumers[0], &metrics, 0, 3);
        assert!(drained.dropped);
        assert_eq!(cursor.settled_bound(), None);
    }
}
