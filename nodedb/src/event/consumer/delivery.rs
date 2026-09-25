// SPDX-License-Identifier: BUSL-1.1

//! Deliver each event to the side effects once.
//!
//! An event can reach the consumer twice: from the ring, and rebuilt from the
//! WAL by catch-up. An event that reproduces a WAL record is named by its
//! record position, collection, row and write kind. Both paths name the same
//! event the same way.
//!
//! `safe` is a prefix of the WAL: every event of every record at or below it
//! was delivered. The guard remembers the events it delivered above it, and
//! forgets them once the prefix covers them. An event that reproduces no
//! record never reaches catch-up, so only its ring copy arrives, and the
//! guard delivers it as it comes.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::event::record_numbering::is_delete;
use crate::event::types::{RowId, WriteEvent};
use crate::types::Lsn;

/// An event's name within its record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventKey {
    occurrence: u32,
    collection: Arc<str>,
    row: RowId,
    delete: bool,
}

/// The consumer's record of what it delivered.
#[derive(Debug)]
pub struct DeliveryGuard {
    safe: Lsn,
    /// Events delivered above `safe`, by record LSN.
    delivered: BTreeMap<u64, HashSet<EventKey>>,
}

impl DeliveryGuard {
    /// A guard whose prefix is `safe`, the persisted watermark.
    pub fn new(safe: Lsn) -> Self {
        Self {
            safe,
            delivered: BTreeMap::new(),
        }
    }

    /// Every event of every record at or below this LSN was delivered.
    pub fn safe(&self) -> Lsn {
        self.safe
    }

    /// Number of delivered events remembered above the prefix.
    pub fn remembered(&self) -> usize {
        self.delivered.values().map(HashSet::len).sum()
    }

    /// Whether `event` still needs delivery. Records it as delivered when it
    /// does, so a second copy is refused.
    pub fn admit(&mut self, event: &WriteEvent) -> bool {
        let Some(position) = event.record else {
            return true;
        };
        if !position.lsn.is_ahead_of(self.safe) {
            return false;
        }
        self.delivered
            .entry(position.lsn.as_u64())
            .or_default()
            .insert(EventKey {
                occurrence: position.occurrence,
                collection: Arc::clone(&event.collection),
                row: event.row_id.clone(),
                delete: is_delete(event.op),
            })
    }

    /// Raise the prefix to `lsn` and forget the events it now covers. Returns
    /// whether the prefix moved.
    pub fn advance_safe(&mut self, lsn: Lsn) -> bool {
        if !lsn.is_ahead_of(self.safe) {
            return false;
        }
        self.safe = lsn;
        self.delivered = self.delivered.split_off(&lsn.as_u64().saturating_add(1));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::{EventSource, RecordPosition, WriteOp};
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn event(record: Option<(u64, u32)>, row: &str, op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 0,
            collection: Arc::from("orders"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key(row)),
            lsn: Lsn::new(record.map(|(lsn, _)| lsn).unwrap_or(1)),
            record: record.map(|(lsn, occurrence)| RecordPosition {
                lsn: Lsn::new(lsn),
                occurrence,
            }),
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
        }
    }

    #[test]
    fn a_second_copy_of_an_event_is_refused() {
        let mut guard = DeliveryGuard::new(Lsn::new(5));
        assert!(guard.admit(&event(Some((6, 0)), "a", WriteOp::Insert)));
        // Catch-up rebuilds the same write as an update: same event.
        assert!(!guard.admit(&event(Some((6, 0)), "a", WriteOp::Update)));
        // A later write of the same row in the same record is another event.
        assert!(guard.admit(&event(Some((6, 1)), "a", WriteOp::Update)));
        assert!(guard.admit(&event(Some((6, 0)), "a", WriteOp::Delete)));
    }

    #[test]
    fn an_event_at_or_below_the_prefix_was_delivered() {
        let mut guard = DeliveryGuard::new(Lsn::new(5));
        assert!(!guard.admit(&event(Some((5, 0)), "a", WriteOp::Insert)));
        assert!(!guard.admit(&event(Some((3, 0)), "a", WriteOp::Insert)));
    }

    #[test]
    fn an_event_with_no_record_is_always_delivered() {
        let mut guard = DeliveryGuard::new(Lsn::new(5));
        assert!(guard.admit(&event(None, "a", WriteOp::Update)));
        assert!(guard.admit(&event(None, "a", WriteOp::Update)));
    }

    #[test]
    fn advancing_the_prefix_forgets_what_it_covers() {
        let mut guard = DeliveryGuard::new(Lsn::ZERO);
        assert!(guard.admit(&event(Some((3, 0)), "a", WriteOp::Insert)));
        assert!(guard.admit(&event(Some((7, 0)), "b", WriteOp::Insert)));
        assert!(guard.advance_safe(Lsn::new(5)));
        assert_eq!(guard.remembered(), 1);
        assert!(!guard.admit(&event(Some((7, 0)), "b", WriteOp::Insert)));
        assert!(!guard.advance_safe(Lsn::new(4)));
        assert_eq!(guard.safe(), Lsn::new(5));
    }
}
