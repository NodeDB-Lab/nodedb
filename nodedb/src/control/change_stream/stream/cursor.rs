// SPDX-License-Identifier: BUSL-1.1

//! Resumable positions in the Control-Plane change stream.
//!
//! The stream is a set of partitions. Each partition is one totally ordered
//! feed that every node numbers alike. `ChangePartition(g)` holds the writes
//! of data group `g`, at their Raft log index. A committed Calvin slice is one
//! of them: it installs from a data-group entry.
//!
//! A cursor holds, per partition, the position of the last event it
//! consumed. A cursor taken on one node therefore resumes on any node that
//! holds the same feed.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::event::cdc::CdcOffset;

use super::SequencedChangeEvent;

const TOKEN_PREFIX: &str = "v2:";
/// Partitions one cursor holds at most: every data group of a large cluster.
const MAX_ENTRIES: usize = 8192;
/// Upper bound on one entry's text: a tag, a 20-digit id, and three
/// 20-digit numbers with their separators.
const MAX_ENTRY_LEN: usize = 1 + 20 + 1 + 3 * 20 + 2;
const MAX_TOKEN_LEN: usize = TOKEN_PREFIX.len() + MAX_ENTRIES * (MAX_ENTRY_LEN + 1);

/// One totally ordered feed of the change stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangePartition(
    /// The data group whose writes the partition holds, at their Raft log index.
    pub u64,
);

/// What a consumer does with the next live event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorStep {
    /// A new event: the cursor advanced past it.
    Deliver,
    /// An event the cursor already covers.
    Skip,
    /// The node's feed has a hole the cursor sits below. The consumer must
    /// reset.
    Reset,
}

/// Opaque, versioned resume position across every partition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeCursor {
    positions: BTreeMap<ChangePartition, CdcOffset>,
}

impl ChangeCursor {
    /// The last consumed position in `partition`, if any.
    pub fn position(&self, partition: ChangePartition) -> Option<CdcOffset> {
        self.positions.get(&partition).copied()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (ChangePartition, CdcOffset)> + '_ {
        self.positions.iter().map(|(p, o)| (*p, *o))
    }

    /// Raise `partition` to `position`. A lower position leaves it as is.
    pub(crate) fn raise(&mut self, partition: ChangePartition, position: CdcOffset) {
        let held = self.positions.entry(partition).or_insert(position);
        if *held < position {
            *held = position;
        }
    }

    /// Whether the cursor consumed `event` already.
    pub fn covers(&self, event: &SequencedChangeEvent) -> bool {
        self.position(event.partition())
            .is_some_and(|held| held >= event.position())
    }

    /// Advance past `event` when it is new. Reports a reset when the node's
    /// feed of the event's partition has a hole above this cursor.
    pub fn accept(&mut self, event: &SequencedChangeEvent) -> CursorStep {
        let partition = event.partition();
        let position = event.position();
        let Some(held) = self.position(partition) else {
            self.positions.insert(partition, position);
            return CursorStep::Deliver;
        };
        if held < event.floor() {
            return CursorStep::Reset;
        }
        if held >= position {
            return CursorStep::Skip;
        }
        self.positions.insert(partition, position);
        CursorStep::Deliver
    }
}

impl fmt::Display for ChangeCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(TOKEN_PREFIX)?;
        for (index, (partition, position)) in self.positions.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            let ChangePartition(group) = partition;
            write!(formatter, "g{group}")?;
            write!(
                formatter,
                "@{}.{}.{}",
                position.epoch, position.index, position.sequence
            )?;
        }
        Ok(())
    }
}

/// Strict opaque-cursor parsing failure. Details are intentionally not exposed
/// to clients, which prevents token parsing from becoming a compatibility API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorParseError;

impl fmt::Display for CursorParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid change cursor")
    }
}

impl std::error::Error for CursorParseError {}

impl FromStr for ChangeCursor {
    type Err = CursorParseError;

    fn from_str(token: &str) -> Result<Self, Self::Err> {
        if token.len() > MAX_TOKEN_LEN {
            return Err(CursorParseError);
        }
        let rest = token.strip_prefix(TOKEN_PREFIX).ok_or(CursorParseError)?;
        let mut positions = BTreeMap::new();
        if rest.is_empty() {
            return Ok(Self { positions });
        }
        for entry in rest.split(',') {
            if positions.len() == MAX_ENTRIES {
                return Err(CursorParseError);
            }
            let (partition, position) = parse_entry(entry)?;
            if positions.insert(partition, position).is_some() {
                return Err(CursorParseError);
            }
        }
        Ok(Self { positions })
    }
}

fn parse_entry(entry: &str) -> Result<(ChangePartition, CdcOffset), CursorParseError> {
    let (partition, position) = entry.split_once('@').ok_or(CursorParseError)?;
    let partition = match partition.as_bytes().first() {
        Some(b'g') => ChangePartition(number(&partition[1..])?),
        _ => return Err(CursorParseError),
    };
    let mut parts = position.split('.');
    let (Some(epoch), Some(index), Some(sequence), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(CursorParseError);
    };
    Ok((
        partition,
        CdcOffset::at(number(epoch)?, number(index)?, number(sequence)?),
    ))
}

/// A canonical decimal `u64`: digits only, no leading zero.
fn number(text: &str) -> Result<u64, CursorParseError> {
    if text.is_empty()
        || text.len() > 20
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(CursorParseError);
    }
    text.parse().map_err(|_| CursorParseError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation};
    use crate::types::{DatabaseId, Lsn, TenantId};

    fn event(
        partition: ChangePartition,
        position: CdcOffset,
        floor: CdcOffset,
    ) -> SequencedChangeEvent {
        SequencedChangeEvent::new(
            partition,
            position,
            floor,
            DatabaseId::DEFAULT,
            ChangeEvent {
                lsn: Lsn::new(1),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: nodedb_types::RowIdentity::from_user_key("a"),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1,
                after: None,
            },
        )
    }

    #[test]
    fn token_round_trips_and_is_strict() {
        let mut cursor = ChangeCursor::default();
        cursor.raise(ChangePartition(3), CdcOffset::data_event(0, 42, 1));
        cursor.raise(ChangePartition(7), CdcOffset::data_event(0, 9, 1));
        let token = cursor.to_string();
        assert!(token.starts_with("v2:"));
        assert_eq!(token.parse::<ChangeCursor>(), Ok(cursor));
        assert_eq!("v2:".parse::<ChangeCursor>(), Ok(ChangeCursor::default()));
        for invalid in [
            "v1:000000000000000000000000000000ab:1",
            "v2:g01@0.1.2",
            "v2:g1@0.1",
            "v2:g1@0.1.2,g1@0.1.3",
            "v2:x1@0.1.2",
            "v2:l@0.1.2",
            "v2:l7@0.1.2",
            "v2:c1@0.1.2",
            "v2:g1@0.-1.2",
        ] {
            assert!(invalid.parse::<ChangeCursor>().is_err(), "{invalid}");
        }
    }

    #[test]
    fn accept_delivers_new_events_and_skips_covered_ones() {
        let group = ChangePartition(1);
        let mut cursor = ChangeCursor::default();
        let first = event(group, CdcOffset::data_event(0, 5, 1), CdcOffset::ZERO);
        let second = event(group, CdcOffset::data_event(0, 6, 1), CdcOffset::ZERO);
        assert_eq!(cursor.accept(&first), CursorStep::Deliver);
        assert_eq!(cursor.accept(&second), CursorStep::Deliver);
        assert_eq!(cursor.accept(&first), CursorStep::Skip);
        assert!(cursor.covers(&second));
        // Another partition's positions never compare with this one.
        let other = event(
            ChangePartition(2),
            CdcOffset::data_event(0, 1, 1),
            CdcOffset::ZERO,
        );
        assert_eq!(cursor.accept(&other), CursorStep::Deliver);
    }

    #[test]
    fn a_hole_above_the_cursor_resets() {
        let group = ChangePartition(1);
        let mut cursor = ChangeCursor::default();
        cursor.raise(group, CdcOffset::whole_index(10));
        let past_hole = event(
            group,
            CdcOffset::data_event(0, 30, 1),
            CdcOffset::whole_index(20),
        );
        assert_eq!(cursor.accept(&past_hole), CursorStep::Reset);
    }
}
