// SPDX-License-Identifier: BUSL-1.1

//! The spooled form of an event held back until its write may leave: by a
//! committed redo install until the record settles, and by a journalled
//! task until its write set is stored.

use std::sync::Arc;

use nodedb_types::{RowIdentity, Surrogate};

use crate::event::image_fault::ImageFault;
use crate::event::types::{EdgeEndpoints, RecordPosition, RowId};
use crate::event::{EventSource, WriteEvent};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::spooled_op::{SpooledOp, bytes_weight, undecodable};

/// The row a held event names.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
enum SpooledRowId {
    Row(RowIdentity),
    Batch,
    Edge {
        src: String,
        label: String,
        dst: String,
        src_surrogate: u32,
        dst_surrogate: u32,
    },
    Heartbeat,
}

impl SpooledRowId {
    fn of(row: &RowId) -> Self {
        match row {
            RowId::Row(identity) => Self::Row(identity.clone()),
            RowId::Batch => Self::Batch,
            RowId::Edge(edge) => Self::Edge {
                src: edge.src().to_string(),
                label: edge.label().to_string(),
                dst: edge.dst().to_string(),
                src_surrogate: edge.src_surrogate().as_u32(),
                dst_surrogate: edge.dst_surrogate().as_u32(),
            },
            RowId::Heartbeat => Self::Heartbeat,
        }
    }

    fn row_id(self) -> RowId {
        match self {
            Self::Row(identity) => RowId::Row(identity),
            Self::Batch => RowId::Batch,
            Self::Edge {
                src,
                label,
                dst,
                src_surrogate,
                dst_surrogate,
            } => RowId::edge(
                EdgeEndpoints {
                    src,
                    src_surrogate: Surrogate::new(src_surrogate),
                    dst,
                    dst_surrogate: Surrogate::new(dst_surrogate),
                },
                label,
            ),
            Self::Heartbeat => RowId::Heartbeat,
        }
    }
}

fn fault_code(fault: ImageFault) -> u8 {
    match fault {
        ImageFault::New => 0,
        ImageFault::Old => 1,
        ImageFault::Both => 2,
    }
}

fn fault_of(code: u8) -> crate::Result<ImageFault> {
    match code {
        0 => Ok(ImageFault::New),
        1 => Ok(ImageFault::Old),
        2 => Ok(ImageFault::Both),
        code => Err(undecodable("image fault", format_args!("code {code}"))),
    }
}

/// A held [`WriteEvent`] as a spill file holds it. A held event has no
/// sequence yet: the send numbers it.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct SpooledEvent {
    collection: String,
    op: SpooledOp,
    row: SpooledRowId,
    lsn: u64,
    /// `(lsn, occurrence)` of the record position.
    record: Option<(u64, u32)>,
    database_id: u64,
    tenant_id: u64,
    vshard_id: u32,
    /// [`EventSource::wal_code`].
    source: u8,
    new_value: Option<Vec<u8>>,
    old_value: Option<Vec<u8>>,
    system_time_ms: Option<i64>,
    valid_time_ms: Option<i64>,
    user_id: Option<String>,
    statement_digest: Option<String>,
    commit_hlc: Option<u64>,
    image_fault: Option<u8>,
}

impl From<&WriteEvent> for SpooledEvent {
    fn from(event: &WriteEvent) -> Self {
        Self {
            collection: event.collection.to_string(),
            op: SpooledOp::of(event.op),
            row: SpooledRowId::of(&event.row_id),
            lsn: event.lsn.as_u64(),
            record: event
                .record
                .map(|record| (record.lsn.as_u64(), record.occurrence)),
            database_id: event.database_id.as_u64(),
            tenant_id: event.tenant_id.as_u64(),
            vshard_id: event.vshard_id.as_u32(),
            source: event.source.wal_code(),
            new_value: event.new_value.as_deref().map(<[u8]>::to_vec),
            old_value: event.old_value.as_deref().map(<[u8]>::to_vec),
            system_time_ms: event.system_time_ms,
            valid_time_ms: event.valid_time_ms,
            user_id: event.user_id.as_deref().map(str::to_string),
            statement_digest: event.statement_digest.as_deref().map(str::to_string),
            commit_hlc: event.commit_hlc,
            image_fault: event.image_fault.map(fault_code),
        }
    }
}

impl SpooledEvent {
    /// The event this record holds, with sequence 0.
    pub(in crate::data::executor) fn into_event(self) -> crate::Result<WriteEvent> {
        let source = EventSource::from_wal_code(self.source)
            .ok_or_else(|| undecodable("event source", format_args!("code {}", self.source)))?;
        Ok(WriteEvent {
            sequence: 0,
            collection: Arc::from(self.collection.as_str()),
            op: self.op.op()?,
            row_id: self.row.row_id(),
            lsn: Lsn::new(self.lsn),
            record: self.record.map(|(lsn, occurrence)| RecordPosition {
                lsn: Lsn::new(lsn),
                occurrence,
            }),
            database_id: DatabaseId::new(self.database_id),
            tenant_id: TenantId::new(self.tenant_id),
            vshard_id: VShardId::new(self.vshard_id),
            source,
            new_value: self.new_value.map(Arc::from),
            old_value: self.old_value.map(Arc::from),
            system_time_ms: self.system_time_ms,
            valid_time_ms: self.valid_time_ms,
            user_id: self.user_id.map(Arc::from),
            statement_digest: self.statement_digest.map(Arc::from),
            commit_hlc: self.commit_hlc,
            image_fault: self.image_fault.map(fault_of).transpose()?,
        })
    }
}

/// The RAM a held [`WriteEvent`] keeps: the event and the bytes it owns.
/// Shared images count in full, as the event may hold the last reference.
pub(in crate::data::executor) fn held_event_weight(event: &WriteEvent) -> usize {
    let text = |value: &Option<Arc<str>>| value.as_deref().map_or(0, str::len);
    let row = match &event.row_id {
        RowId::Edge(edge) => edge.src().len() + edge.label().len() + edge.dst().len(),
        RowId::Row(_) | RowId::Batch | RowId::Heartbeat => 0,
    };
    std::mem::size_of::<WriteEvent>()
        + event.collection.len()
        + event.row_id.as_str().len()
        + row
        + bytes_weight(event.new_value.as_deref())
        + bytes_weight(event.old_value.as_deref())
        + text(&event.user_id)
        + text(&event.statement_digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WriteOp;

    fn event(row_id: RowId, fault: Option<ImageFault>) -> WriteEvent {
        WriteEvent {
            sequence: 0,
            collection: Arc::from("orders"),
            op: WriteOp::BulkInsert { count: 3 },
            row_id,
            lsn: Lsn::new(42),
            record: Some(RecordPosition {
                lsn: Lsn::new(42),
                occurrence: 2,
            }),
            database_id: DatabaseId::new(3),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(17),
            source: EventSource::Trigger,
            new_value: Some(Arc::from(vec![1u8, 2].as_slice())),
            old_value: None,
            system_time_ms: Some(10),
            valid_time_ms: None,
            user_id: Some(Arc::from("alice")),
            statement_digest: None,
            commit_hlc: Some(77),
            image_fault: fault,
        }
    }

    #[test]
    fn every_held_event_round_trips() {
        let rows = [
            RowId::row(RowIdentity::from_user_key("o1")),
            RowId::Batch,
            RowId::edge(
                EdgeEndpoints {
                    src: "a",
                    src_surrogate: Surrogate::new(5),
                    dst: "b",
                    dst_surrogate: Surrogate::new(6),
                },
                "knows",
            ),
            RowId::Heartbeat,
        ];
        let faults = [
            None,
            Some(ImageFault::New),
            Some(ImageFault::Old),
            Some(ImageFault::Both),
        ];
        for (row, fault) in rows.into_iter().zip(faults) {
            let original = event(row, fault);
            let spooled = SpooledEvent::from(&original);
            assert!(held_event_weight(&original) > 0);
            let bytes = zerompk::to_msgpack_vec(&spooled).expect("encode");
            let decoded: SpooledEvent = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, spooled);
            let back = decoded.into_event().expect("event");
            assert_eq!(format!("{back:?}"), format!("{original:?}"));
        }
    }

    #[test]
    fn an_unknown_source_does_not_decode() {
        let mut spooled = SpooledEvent::from(&event(RowId::Batch, None));
        spooled.source = 0;
        assert!(spooled.into_event().is_err());
    }
}
