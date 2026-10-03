// SPDX-License-Identifier: BUSL-1.1

//! Number the events of one WAL record per row.
//!
//! The Data Plane producer numbers a record's events as it emits them, and
//! WAL catch-up numbers the events it rebuilds from the same record the same
//! way. The n-th event of a record that names a given row and write kind gets
//! occurrence `n - 1` on both paths, so the Event Plane can recognise a
//! rebuilt event it already delivered from the ring, and the reverse.
//!
//! The events of one position arrive close together, but not always back to
//! back: a write's group spreads its rows over several WAL records, and
//! catch-up reads other writes' records between them. So the numbering keeps
//! the counters of the most recent positions it saw, and forgets the oldest
//! beyond [`POSITIONS_KEPT`].

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::types::Lsn;

use super::types::{RowId, WriteEvent, WriteOp};

/// How many recent record positions keep their counters. A write's group
/// appends every part while its order fence holds the vShard, so its parts
/// sit within far fewer records of each other than this.
pub const POSITIONS_KEPT: usize = 4096;

/// The occurrence counters of one record position.
type Counters = HashMap<(Arc<str>, RowId, bool), u32>;

/// Occurrence counters of the recent record positions.
#[derive(Debug, Default)]
pub struct RecordNumbering {
    counters: HashMap<Lsn, Counters>,
    /// Positions in the order their first event arrived.
    order: VecDeque<Lsn>,
}

impl RecordNumbering {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stamp `event` with its occurrence within its record position. An
    /// event that reproduces no record is left alone.
    pub fn stamp(&mut self, event: &mut WriteEvent) {
        let Some(position) = &mut event.record else {
            return;
        };
        if !self.counters.contains_key(&position.lsn) {
            self.order.push_back(position.lsn);
            if self.order.len() > POSITIONS_KEPT
                && let Some(oldest) = self.order.pop_front()
            {
                self.counters.remove(&oldest);
            }
        }
        let count = self
            .counters
            .entry(position.lsn)
            .or_default()
            .entry((
                Arc::clone(&event.collection),
                event.row_id.clone(),
                is_delete(event.op),
            ))
            .or_insert(0);
        position.occurrence = *count;
        *count += 1;
    }
}

/// Whether `op` removes rows. An insert and an update of one row share a
/// kind: the ring and catch-up can tell them apart differently.
pub fn is_delete(op: WriteOp) -> bool {
    match op {
        WriteOp::Delete | WriteOp::BulkDelete { .. } => true,
        WriteOp::Insert
        | WriteOp::Update
        | WriteOp::BulkInsert { .. }
        | WriteOp::Heartbeat
        | WriteOp::Publish => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::{EventSource, RecordPosition};
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn event(lsn: u64, row: &str, op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 0,
            collection: Arc::from("orders"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key(row)),
            lsn: Lsn::new(lsn),
            record: Some(RecordPosition::first(Lsn::new(lsn))),
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: Some(crate::event::test_utils::test_commit_hlc()),
            image_fault: None,
        }
    }

    #[test]
    fn a_repeated_row_counts_up_and_a_new_record_starts_over() {
        let mut numbering = RecordNumbering::new();
        let mut events = vec![
            event(9, "a", WriteOp::Insert),
            event(9, "a", WriteOp::Update),
            event(9, "b", WriteOp::Insert),
            event(9, "a", WriteOp::Delete),
            event(10, "a", WriteOp::Insert),
        ];
        for event in &mut events {
            numbering.stamp(event);
        }
        let occurrences: Vec<u32> = events
            .iter()
            .map(|e| e.record.map(|r| r.occurrence).unwrap_or(u32::MAX))
            .collect();
        assert_eq!(occurrences, vec![0, 1, 0, 0, 0]);
    }

    /// A position whose events arrive in two runs, with another position's
    /// events between them, keeps counting where its first run stopped.
    #[test]
    fn a_position_split_by_another_keeps_counting() {
        let mut numbering = RecordNumbering::new();
        let mut events = vec![
            event(9, "a", WriteOp::Insert),
            event(10, "a", WriteOp::Insert),
            event(9, "a", WriteOp::Update),
        ];
        for event in &mut events {
            numbering.stamp(event);
        }
        let occurrences: Vec<u32> = events
            .iter()
            .map(|e| e.record.map(|r| r.occurrence).unwrap_or(u32::MAX))
            .collect();
        assert_eq!(occurrences, vec![0, 0, 1]);
    }
}
