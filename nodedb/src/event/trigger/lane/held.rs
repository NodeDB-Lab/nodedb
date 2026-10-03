// SPDX-License-Identifier: BUSL-1.1

//! The durable form of an event whose actions the lane fires.
//!
//! A held event keeps what the AFTER triggers and DEFINE EVENT actions read:
//! its scope, operation, row identity, row images and source. The lane
//! rebuilds a [`WriteEvent`] from it when it fires, named by the event's
//! replicated position instead of this node's WAL LSN.

use std::sync::Arc;

use crate::event::cdc::CdcOffset;
use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::position::action_identity;

const OP_INSERT: u8 = 1;
const OP_UPDATE: u8 = 2;
const OP_DELETE: u8 = 3;
const OP_BULK_INSERT: u8 = 4;
const OP_BULK_DELETE: u8 = 5;

/// The code and row count a held event stores `op` as. `None` for an
/// operation that writes no row.
fn op_code(op: WriteOp) -> Option<(u8, u32)> {
    Some(match op {
        WriteOp::Insert => (OP_INSERT, 1),
        WriteOp::Update => (OP_UPDATE, 1),
        WriteOp::Delete => (OP_DELETE, 1),
        WriteOp::BulkInsert { count } => (OP_BULK_INSERT, count),
        WriteOp::BulkDelete { count } => (OP_BULK_DELETE, count),
        WriteOp::Heartbeat | WriteOp::Publish => return None,
    })
}

/// The operation `code` and `count` name.
fn write_op(code: u8, count: u32) -> Option<WriteOp> {
    Some(match code {
        OP_INSERT => WriteOp::Insert,
        OP_UPDATE => WriteOp::Update,
        OP_DELETE => WriteOp::Delete,
        OP_BULK_INSERT => WriteOp::BulkInsert { count },
        OP_BULK_DELETE => WriteOp::BulkDelete { count },
        _ => return None,
    })
}

/// One held event.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct HeldAction {
    pub database_id: u64,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub collection: String,
    /// The operation's code.
    pub op: u8,
    /// The rows a bulk operation wrote. `1` for a single-row write.
    pub count: u32,
    /// The row's identity. `None` for a write that names no single row.
    pub row: Option<String>,
    /// The event source's WAL code.
    pub source: u8,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
    pub system_time_ms: Option<i64>,
    pub valid_time_ms: Option<i64>,
    pub commit_hlc: Option<u64>,
}

impl HeldAction {
    /// The held form of `event`. `None` for an event that writes no row or
    /// writes an edge: neither runs row actions.
    pub fn of(event: &WriteEvent) -> Option<Self> {
        let row = match &event.row_id {
            RowId::Row(identity) => Some(identity.as_str().to_owned()),
            RowId::Batch => None,
            RowId::Edge(_) | RowId::Heartbeat => return None,
        };
        let (op, count) = op_code(event.op)?;
        Some(Self {
            database_id: event.database_id.as_u64(),
            tenant_id: event.tenant_id.as_u64(),
            vshard_id: event.vshard_id.as_u32(),
            collection: event.collection.to_string(),
            op,
            count,
            row,
            source: event.source.wal_code(),
            new_value: event.new_value.as_deref().map(<[u8]>::to_vec),
            old_value: event.old_value.as_deref().map(<[u8]>::to_vec),
            system_time_ms: event.system_time_ms,
            valid_time_ms: event.valid_time_ms,
            commit_hlc: event.commit_hlc,
        })
    }

    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("held trigger action encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("held trigger action decode: {e}"),
        })
    }

    /// The event the lane fires the actions of: this held event at
    /// `position` of `partition`.
    ///
    /// Its `lsn` and `sequence` carry the position's action identity, which
    /// every replica shares. The actions name their source write by them, so
    /// a body fired again by a later owner carries the same key and applies
    /// once.
    pub fn to_event(&self, partition: u32, position: CdcOffset) -> crate::Result<WriteEvent> {
        let source =
            EventSource::from_wal_code(self.source).ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "held trigger action of '{}' names unknown source code {}",
                    self.collection, self.source
                ),
            })?;
        let op = write_op(self.op, self.count).ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "held trigger action of '{}' names unknown operation code {}",
                self.collection, self.op
            ),
        })?;
        let (source_lsn, source_sequence) = action_identity(partition, position);
        Ok(WriteEvent {
            sequence: source_sequence,
            collection: Arc::from(self.collection.as_str()),
            op,
            row_id: match &self.row {
                Some(row) => RowId::row(nodedb_types::RowIdentity::from_user_key(row)),
                None => RowId::Batch,
            },
            lsn: Lsn::new(source_lsn),
            record: None,
            database_id: DatabaseId::new(self.database_id),
            tenant_id: TenantId::new(self.tenant_id),
            vshard_id: VShardId::new(self.vshard_id),
            source,
            new_value: self.new_value.as_deref().map(Arc::from),
            old_value: self.old_value.as_deref().map(Arc::from),
            system_time_ms: self.system_time_ms,
            valid_time_ms: self.valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc: self.commit_hlc,
            // Delivery holds no event whose image did not render.
            image_fault: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_event_round_trips_its_row_and_op() {
        let event = WriteEvent {
            sequence: 9,
            collection: Arc::from("orders"),
            op: WriteOp::Update,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("o-1")),
            lsn: Lsn::new(40),
            record: None,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(7),
            source: EventSource::User,
            new_value: Some(Arc::from(&[1u8, 2][..])),
            old_value: Some(Arc::from(&[3u8][..])),
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: Some(5),
            image_fault: None,
        };
        let held = HeldAction::of(&event).expect("a row write is held");
        let back = HeldAction::from_bytes(&held.to_bytes().expect("encode")).expect("decode");
        assert_eq!(back, held);
        let fired = back
            .to_event(7, CdcOffset::data_event(0, 12, 1))
            .expect("rebuild");
        assert_eq!(fired.op, WriteOp::Update);
        assert_eq!(fired.row_id.as_str(), "o-1");
        assert_eq!(fired.vshard_id, VShardId::new(7));
        assert_eq!(fired.lsn, Lsn::new(12));
        assert_eq!(fired.new_value.as_deref(), Some(&[1u8, 2][..]));
    }

    #[test]
    fn an_edge_or_heartbeat_is_not_held() {
        let mut event = WriteEvent {
            sequence: 1,
            collection: Arc::from("orders"),
            op: WriteOp::Heartbeat,
            row_id: RowId::Heartbeat,
            lsn: Lsn::new(1),
            record: None,
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
            commit_hlc: None,
            image_fault: None,
        };
        assert!(HeldAction::of(&event).is_none());
        event.op = WriteOp::Insert;
        assert!(HeldAction::of(&event).is_none());
    }
}
