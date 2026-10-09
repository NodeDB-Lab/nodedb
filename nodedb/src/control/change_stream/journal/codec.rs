// SPDX-License-Identifier: BUSL-1.1

//! Key and value encodings of the change-feed journal.
//!
//! Keys are big-endian so a partition's rows sort in position order.

use nodedb_types::RowIdentity;

use crate::control::change_stream::{
    ChangeEvent, ChangeOperation, ChangePartition, PositionedChange, SequencedChangeEvent,
};
use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, Lsn, TenantId};

pub(super) const PARTITION_KEY_LEN: usize = 9;
const POSITION_LEN: usize = 24;
pub(super) const ROW_KEY_LEN: usize = PARTITION_KEY_LEN + POSITION_LEN;
const FEED_LEN: usize = 2 * POSITION_LEN + 8;

const TAG_GROUP: u8 = 0;

fn word(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

pub(super) fn partition_key(partition: ChangePartition) -> [u8; PARTITION_KEY_LEN] {
    let ChangePartition(group) = partition;
    let (tag, id) = (TAG_GROUP, group);
    let mut out = [0u8; PARTITION_KEY_LEN];
    out[0] = tag;
    out[1..].copy_from_slice(&id.to_be_bytes());
    out
}

pub(super) fn decode_partition(bytes: &[u8]) -> Option<ChangePartition> {
    let id = word(bytes, 1)?;
    match *bytes.first()? {
        TAG_GROUP => Some(ChangePartition(id)),
        _ => None,
    }
}

fn encode_position(position: CdcOffset, out: &mut [u8]) {
    out[..8].copy_from_slice(&position.epoch.to_be_bytes());
    out[8..16].copy_from_slice(&position.index.to_be_bytes());
    out[16..24].copy_from_slice(&position.sequence.to_be_bytes());
}

fn decode_position(bytes: &[u8]) -> Option<CdcOffset> {
    Some(CdcOffset::at(
        word(bytes, 0)?,
        word(bytes, 8)?,
        word(bytes, 16)?,
    ))
}

pub(super) fn row_key(partition: ChangePartition, position: CdcOffset) -> [u8; ROW_KEY_LEN] {
    let mut out = [0u8; ROW_KEY_LEN];
    out[..PARTITION_KEY_LEN].copy_from_slice(&partition_key(partition));
    encode_position(position, &mut out[PARTITION_KEY_LEN..]);
    out
}

/// The first and last row key `partition` can hold.
pub(super) fn row_bounds(partition: ChangePartition) -> ([u8; ROW_KEY_LEN], [u8; ROW_KEY_LEN]) {
    let mut last = row_key(partition, CdcOffset::ZERO);
    last[PARTITION_KEY_LEN..].fill(u8::MAX);
    (row_key(partition, CdcOffset::ZERO), last)
}

pub(super) fn row_position(key: &[u8]) -> Option<CdcOffset> {
    decode_position(key.get(PARTITION_KEY_LEN..)?)
}

/// What the journal holds of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FeedRecord {
    /// The journal holds every event of the partition above this position.
    pub after: CdcOffset,
    /// The position up to which the journal holds the partition.
    pub through: CdcOffset,
    /// Rows the partition holds.
    pub rows: u64,
}

impl FeedRecord {
    pub fn encode(&self) -> [u8; FEED_LEN] {
        let mut out = [0u8; FEED_LEN];
        encode_position(self.after, &mut out[..POSITION_LEN]);
        encode_position(self.through, &mut out[POSITION_LEN..2 * POSITION_LEN]);
        out[2 * POSITION_LEN..].copy_from_slice(&self.rows.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Some(Self {
            after: decode_position(bytes.get(..POSITION_LEN)?)?,
            through: decode_position(bytes.get(POSITION_LEN..2 * POSITION_LEN)?)?,
            rows: word(bytes, 2 * POSITION_LEN)?,
        })
    }
}

/// One journaled change.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(super) struct StoredChange {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub document_id: String,
    pub operation: String,
    pub timestamp_ms: u64,
    pub lsn: u64,
}

impl StoredChange {
    pub fn of(event: &SequencedChangeEvent) -> Self {
        Self {
            database_id: event.database_id().as_u64(),
            tenant_id: event.tenant_id.as_u64(),
            collection: event.collection.clone(),
            document_id: event.document_id.to_string(),
            operation: event.operation.as_str().to_owned(),
            timestamp_ms: event.timestamp_ms,
            lsn: event.lsn.as_u64(),
        }
    }

    pub fn at(self, position: CdcOffset) -> PositionedChange {
        PositionedChange {
            position,
            database_id: DatabaseId::new(self.database_id),
            event: ChangeEvent {
                lsn: Lsn::new(self.lsn),
                tenant_id: TenantId::new(self.tenant_id),
                collection: self.collection,
                // The journal stores the identity as text; wrap it verbatim.
                document_id: RowIdentity::from_user_key(self.document_id),
                operation: match self.operation.as_str() {
                    "UPDATE" => ChangeOperation::Update,
                    "DELETE" => ChangeOperation::Delete,
                    _ => ChangeOperation::Insert,
                },
                timestamp_ms: self.timestamp_ms,
                after: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_feeds_round_trip_and_sort_by_position() {
        for partition in [ChangePartition(7), ChangePartition(3)] {
            assert_eq!(decode_partition(&partition_key(partition)), Some(partition));
            let low = row_key(partition, CdcOffset::data_event(0, 5, 1));
            let high = row_key(partition, CdcOffset::data_event(0, 5, 2));
            assert!(low < high);
            assert_eq!(row_position(&high), Some(CdcOffset::data_event(0, 5, 2)));
            let (first, last) = row_bounds(partition);
            assert!(first <= low && high <= last);
        }
        let feed = FeedRecord {
            after: CdcOffset::whole_index(4),
            through: CdcOffset::whole_index(9),
            rows: 3,
        };
        assert_eq!(FeedRecord::decode(&feed.encode()), Some(feed));
    }
}
