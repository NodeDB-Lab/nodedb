// SPDX-License-Identifier: BUSL-1.1

//! Replicated positions of applied writes, and the WAL payload that keeps a
//! Raft entry's position durable on each replica.
//!
//! A `ChangePosition` payload is a fixed head ([`ChangePositionMarker`]),
//! then a tail ([`MarkerChanges`]) that says which change events the entry
//! publishes. A marker with no tail records no change events.

use crate::wal::RedoRowChange;

/// Where a committed data-group entry sits in its group's Raft log. Every
/// replica that applies the entry sees the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicatedPosition {
    /// The epoch at which the entry's vShard moved to `group_id`.
    pub epoch: u64,
    pub group_id: u64,
    pub log_index: u64,
}

/// Byte length of an encoded [`ChangePositionMarker`]: the payload's head.
pub const MARKER_LEN: usize = 32;

/// Payload of a `ChangePosition` WAL record: the entry's proposal key, and
/// the entry's position. The records the entry's apply writes carry the same
/// key in their headers, which is how recovery links them to the position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangePositionMarker {
    pub apply_key: u64,
    pub position: ReplicatedPosition,
}

impl ChangePositionMarker {
    pub fn to_bytes(&self) -> [u8; MARKER_LEN] {
        let mut out = [0u8; MARKER_LEN];
        out[0..8].copy_from_slice(&self.apply_key.to_le_bytes());
        out[8..16].copy_from_slice(&self.position.group_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.position.log_index.to_le_bytes());
        out[24..32].copy_from_slice(&self.position.epoch.to_le_bytes());
        out
    }

    /// The head of a `ChangePosition` payload. A tail after it is not read.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MarkerDecodeError> {
        let bytes: &[u8; MARKER_LEN] = bytes
            .get(..MARKER_LEN)
            .and_then(|head| head.try_into().ok())
            .ok_or(MarkerDecodeError { len: bytes.len() })?;
        let word = |at: usize| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[at..at + 8]);
            u64::from_le_bytes(buf)
        };
        Ok(Self {
            apply_key: word(0),
            position: ReplicatedPosition {
                group_id: word(8),
                log_index: word(16),
                epoch: word(24),
            },
        })
    }

    /// The payload of a marker with `changes` as its tail.
    pub fn with_changes(&self, changes: &MarkerChanges) -> crate::Result<Vec<u8>> {
        let mut out = self.to_bytes().to_vec();
        zerompk::write_msgpack(&mut out, changes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("change position marker tail encode: {e}"),
        })?;
        Ok(out)
    }
}

/// What a `ChangePosition` payload's tail says of its entry's change events.
///
/// The write funnel appends the marker before the entry's first record, so
/// the tail is durable no later than any record of the entry. A restart that
/// delivers the entry again stages its events from the tail, at the marker's
/// LSN.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum MarkerChanges {
    /// The row changes the entry's write yields, as the write funnel derived
    /// them from its plan. They publish once the write applied.
    Rows(Vec<RedoRowChange>),
    /// The row changes the entry's `TransactionRedo` record names. The
    /// record holds them whole, so the marker does not repeat them.
    OfRedo,
    /// The write's records stay in the WAL, and it yields no change event:
    /// its apply answered a refusal. A later marker of the same entry
    /// carries this tail.
    Withheld,
}

impl MarkerChanges {
    /// The tail of the `ChangePosition` payload `bytes`. `None` when the
    /// payload has no tail.
    pub fn of_payload(bytes: &[u8]) -> crate::Result<Option<Self>> {
        let Some(tail) = bytes.get(MARKER_LEN..).filter(|tail| !tail.is_empty()) else {
            return Ok(None);
        };
        zerompk::from_msgpack(tail)
            .map(Some)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("change position marker tail decode: {e}"),
            })
    }
}

/// A `ChangePosition` payload shorter than its head.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ChangePosition payload is {len} bytes, expected at least {MARKER_LEN}")]
pub struct MarkerDecodeError {
    pub len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_round_trips() {
        let marker = ChangePositionMarker {
            apply_key: 0xDEAD_BEEF,
            position: ReplicatedPosition {
                epoch: 3,
                group_id: 7,
                log_index: 1_234,
            },
        };
        assert_eq!(
            ChangePositionMarker::from_bytes(&marker.to_bytes()),
            Ok(marker)
        );
    }

    #[test]
    fn a_short_payload_is_refused() {
        assert_eq!(
            ChangePositionMarker::from_bytes(&[0u8; 24]),
            Err(MarkerDecodeError { len: 24 })
        );
    }

    /// A tail rides after the head: the head reads as before, and the tail
    /// reads back whole. A payload with no tail has none.
    #[test]
    fn a_tail_round_trips_after_the_head() {
        let marker = ChangePositionMarker {
            apply_key: 9,
            position: ReplicatedPosition {
                epoch: 1,
                group_id: 2,
                log_index: 3,
            },
        };
        let rows = MarkerChanges::Rows(vec![RedoRowChange {
            collection: "orders".into(),
            row: "o1".into(),
            kind: crate::wal::RedoRowKind::Update,
        }]);
        let payload = marker.with_changes(&rows).expect("encode");
        assert_eq!(ChangePositionMarker::from_bytes(&payload), Ok(marker));
        assert_eq!(
            MarkerChanges::of_payload(&payload).expect("decode"),
            Some(rows)
        );
        let withheld = marker
            .with_changes(&MarkerChanges::Withheld)
            .expect("encode");
        assert_eq!(
            MarkerChanges::of_payload(&withheld).expect("decode"),
            Some(MarkerChanges::Withheld)
        );
        assert_eq!(
            MarkerChanges::of_payload(&marker.to_bytes()).expect("decode"),
            None
        );
    }
}
