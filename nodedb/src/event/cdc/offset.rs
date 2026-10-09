// SPDX-License-Identifier: BUSL-1.1

//! Lossless CDC positions.
//!
//! A position is `(epoch, index, sequence)`, ordered lexicographically within
//! one partition.
//!
//! - `epoch` counts the partition's moves between data groups. Each data group
//!   numbers its own log, so a move restarts the index; the epoch rises with
//!   it, so positions never go backwards. A committed `ReassignVShard`
//!   metadata entry is the one event that raises it. No path proposes one, so
//!   every position has epoch `0`.
//! - `index` names the write that produced the event. In a cluster it is the
//!   Raft log index of the data-group entry. On a single node it is the WAL
//!   LSN of the write's record. On a durable topic it is the message's log
//!   position.
//! - `sequence` orders the events of one write. It carries `2 × ordinal` for
//!   a data event and `2 × ordinal + 1` for that event's late-data
//!   correction.
//!
//! Every replica applies a write at the same position, so a cursor from one
//! node is valid on every node.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Bits of `sequence` that carry the ordinal and correction flag.
const ORDINAL_BITS: u32 = 32;
/// The largest sequence a data event or its correction takes.
const ORDINAL_MASK: u64 = (1 << ORDINAL_BITS) - 1;

/// A lossless position in a CDC stream partition.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CdcOffset {
    /// Moves of the partition between data groups. See the module docs.
    pub epoch: u64,
    /// Position of the write that produced the event.
    pub index: u64,
    /// Event sequence that orders the events of one write.
    pub sequence: u64,
}

impl From<u64> for CdcOffset {
    /// A bare index acknowledges every event of that write, in epoch 0.
    fn from(index: u64) -> Self {
        Self::whole_index(index)
    }
}

impl PartialEq<u64> for CdcOffset {
    /// Equal to the epoch-0 whole-index acknowledgement of `index`, or to
    /// `ZERO` for `0`.
    fn eq(&self, index: &u64) -> bool {
        *self == Self::whole_index(*index) || (*index == 0 && *self == Self::ZERO)
    }
}

impl CdcOffset {
    /// The initial cursor before every position.
    pub const ZERO: Self = Self {
        epoch: 0,
        index: 0,
        sequence: 0,
    };

    /// A position in epoch 0.
    pub const fn new(index: u64, sequence: u64) -> Self {
        Self::at(0, index, sequence)
    }

    pub const fn at(epoch: u64, index: u64, sequence: u64) -> Self {
        Self {
            epoch,
            index,
            sequence,
        }
    }

    /// The position after every event of the epoch-0 write at `index`.
    pub const fn whole_index(index: u64) -> Self {
        Self::whole_write(0, index)
    }

    /// The position after every event of the write at `(epoch, index)`.
    pub const fn whole_write(epoch: u64, index: u64) -> Self {
        Self::at(epoch, index, u64::MAX)
    }

    /// The position of the data event with ordinal `ordinal` (1-based) in the
    /// write at `(epoch, index)`.
    pub const fn data_event(epoch: u64, index: u64, ordinal: u64) -> Self {
        let sequence = if ordinal > (ORDINAL_MASK >> 1) {
            ORDINAL_MASK - 1
        } else {
            ordinal * 2
        };
        Self::at(epoch, index, sequence)
    }

    /// The ordinal of the data event this position belongs to. A correction
    /// shares its data event's ordinal.
    pub const fn ordinal(self) -> u64 {
        (self.sequence & ORDINAL_MASK) / 2
    }

    /// The position reserved for the late-data correction of the data event
    /// at `self`.
    pub const fn correction(self) -> Self {
        Self::at(self.epoch, self.index, self.sequence | 1)
    }

    /// Canonical text token accepted by `COMMIT OFFSET`.
    pub fn token(self) -> String {
        self.to_string()
    }
}

impl fmt::Display for CdcOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.epoch, self.index, self.sequence)
    }
}

/// Error returned when an offset token is neither `<epoch>:<index>:<sequence>`
/// nor `<epoch>:<index>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCdcOffsetError {
    token: String,
}

impl fmt::Display for ParseCdcOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid CDC offset '{}'; expected <epoch>:<index>:<sequence>, or <epoch>:<index> to acknowledge every event of that write",
            self.token
        )
    }
}

impl std::error::Error for ParseCdcOffsetError {}

impl FromStr for CdcOffset {
    type Err = ParseCdcOffsetError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        let invalid = || ParseCdcOffsetError {
            token: token.to_string(),
        };
        let number = |part: &str| part.parse::<u64>().map_err(|_| invalid());
        let parts: Vec<&str> = token.split(':').collect();
        match parts.as_slice() {
            [epoch, index, sequence] => {
                Ok(Self::at(number(epoch)?, number(index)?, number(sequence)?))
            }
            [epoch, index] => Ok(Self::whole_write(number(epoch)?, number(index)?)),
            _ => Err(invalid()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_order_by_epoch_then_index_then_sequence() {
        assert!(CdcOffset::new(10, 2) > CdcOffset::new(10, 1));
        assert!(CdcOffset::new(11, 0) > CdcOffset::new(10, u64::MAX));
        assert!(CdcOffset::whole_index(10) > CdcOffset::data_event(0, 10, 7));
        assert!(CdcOffset::whole_index(10) < CdcOffset::data_event(0, 11, 1));
        // A move to a new data group restarts the index but raises the epoch.
        assert!(CdcOffset::data_event(1, 3, 1) > CdcOffset::data_event(0, 9_000, 4));
    }

    #[test]
    fn a_correction_sorts_between_its_event_and_the_next_event() {
        let first = CdcOffset::data_event(0, 5, 1);
        let second = CdcOffset::data_event(0, 5, 2);
        assert!(first < first.correction());
        assert!(first.correction() < second);
        assert_eq!(first.correction().ordinal(), first.ordinal());
        assert_eq!(second.ordinal(), 2);
    }

    #[test]
    fn equal_positions_from_two_replicas_compare_equal() {
        let leader = CdcOffset::data_event(2, 42, 3);
        let follower = CdcOffset::data_event(2, 42, 3);
        assert_eq!(leader, follower);
        assert_eq!(leader.cmp(&follower), std::cmp::Ordering::Equal);
    }

    #[test]
    fn parses_canonical_and_whole_write_tokens() {
        assert_eq!("1:12:3".parse(), Ok(CdcOffset::at(1, 12, 3)));
        assert_eq!("0:12".parse(), Ok(CdcOffset::whole_index(12)));
        assert!("12".parse::<CdcOffset>().is_err());
        let error = "0:12:bad".parse::<CdcOffset>().unwrap_err();
        assert!(error.to_string().contains("<epoch>:<index>:<sequence>"));
        assert!(":3".parse::<CdcOffset>().is_err());
    }

    #[test]
    fn token_round_trips() {
        let offset = CdcOffset::at(3, 9, 4);
        assert_eq!(offset.token().parse(), Ok(offset));
    }
}
