// SPDX-License-Identifier: BUSL-1.1

//! The record group of one write.
//!
//! A write that stores rows its apply decides journals them after apply, in
//! one or more `WriteGroup` part records. Every part names the write's
//! origin: the write's forward record, which an announcing `WriteGroup`
//! record follows, or the opening `WriteGroup` record the write appended
//! before dispatch. A group is whole when every part it announced is present.
//! A point-in-time restore keeps a group only whole (see
//! [`GroupMembership`]). Boot settles every group the log shows broken before
//! any replay: it journals the parts the core stored beside the write's
//! effects, or cancels a write whose effects never became durable (see
//! `crate::bootstrap::write_group_settle`). Restart replay then applies every
//! record present.
//!
//! The Event Plane names every event of the write by the origin's LSN, on the
//! live path and in WAL catch-up alike.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::RedoSubRecord;

/// Where one `WriteGroup` record sits in its write's group.
///
/// Part `0` starts a group. With origin `0` it is the opening record, whose
/// own LSN is the origin. With any other origin and `parts` `0` it announces
/// the group of the forward record at that LSN, and carries no rows. Every
/// group starts with one of the two, so a group whose parts never arrived
/// shows in the stream. Part `0` with another origin and `parts` `n > 0` is
/// the `n`th continuation of a committed transaction record at the origin.
/// Announcements and continuations are members of the group, never parts.
///
/// A committed transaction record that journals no rows after apply is whole
/// at append: no announcement follows it and no part. Split over the WAL
/// record limit, each of its continuations names the continuation count in
/// `closed`, and the group is whole once the last continuation is present.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct WriteGroup {
    /// LSN of the group's origin record. `0` on the opening record, whose
    /// own LSN is the origin.
    pub origin: u64,
    /// `0` on the opening record, `1..=parts` on the parts.
    pub part: u32,
    /// How many parts the write appended. `0` on the opening record.
    pub parts: u32,
    /// On a continuation of a record whole at append: how many continuations
    /// the record has. `0` on every other record.
    pub closed: u32,
}

impl WriteGroup {
    /// The descriptor of a group's opening record.
    pub const OPENING: Self = Self {
        origin: 0,
        part: 0,
        parts: 0,
        closed: 0,
    };

    /// Part `part` of `parts` of the group at `origin`.
    pub fn part_of(origin: u64, part: u32, parts: u32) -> Self {
        Self {
            origin,
            part,
            parts,
            closed: 0,
        }
    }

    /// The record that announces the group of the forward record at `origin`.
    pub fn announcing(origin: u64) -> Self {
        Self::part_of(origin, 0, 0)
    }

    /// The `index`th continuation (from 1) of the committed transaction
    /// record at `origin`: more of its sub-records, in order. Parts follow
    /// the record.
    pub fn continuing(origin: u64, index: u32) -> Self {
        Self::part_of(origin, 0, index)
    }

    /// The `index`th of the `count` continuations of the committed
    /// transaction record at `origin`, a record whole at append.
    pub fn continuing_closed(origin: u64, index: u32, count: u32) -> Self {
        Self {
            closed: count,
            ..Self::continuing(origin, index)
        }
    }

    /// Whether this record is a continuation, and of a record whole at
    /// append, its index and the continuation count.
    pub fn closed_continuation(&self) -> Option<(u32, u32)> {
        (self.opens() && self.origin != 0 && self.closed > 0).then_some((self.parts, self.closed))
    }

    /// Whether this record starts its group: the opening record, or the
    /// announcement of a forward origin.
    pub fn opens(&self) -> bool {
        self.part == 0
    }

    /// The origin LSN of the group a record at `lsn` belongs to.
    pub fn origin_at(&self, lsn: u64) -> u64 {
        if self.opens() && self.origin == 0 {
            lsn
        } else {
            self.origin
        }
    }
}

/// The payload of a `WriteGroup` WAL record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct WriteGroupRecord {
    pub group: WriteGroup,
    /// Engine-native sub-records, applied in order on replay, in the payload
    /// shape each engine's own per-op record uses.
    pub ops: Vec<RedoSubRecord>,
    /// `Some` on a continuation of a committed transaction record too large
    /// for one WAL record: the row metadata its events read (see
    /// [`super::continuation`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redo: Option<super::continuation::ContinuedRedo>,
}

impl WriteGroupRecord {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("write group record encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("write group record decode: {e}"),
        })
    }
}

/// The groups a stream of `WriteGroup` records names, and which of them are
/// whole at a cut.
#[derive(Debug, Default)]
pub struct GroupMembership {
    groups: BTreeMap<u64, GroupSeen>,
}

#[derive(Debug, Default)]
struct GroupSeen {
    /// The part count the group's parts announce.
    parts: Option<u32>,
    /// `(part number, LSN)` of every part seen.
    seen: BTreeSet<(u32, u64)>,
    /// LSNs of the group's announcements and continuations.
    members: BTreeSet<u64>,
    /// On a record whole at append: the continuation count, and the LSN of
    /// the last continuation when seen.
    closed: Option<(u32, Option<u64>)>,
}

impl GroupSeen {
    /// Whether the group is whole at or below `through`: every announced
    /// part is present, or the record is whole at append and its last
    /// continuation is present.
    fn whole_through(&self, through: u64) -> bool {
        if let Some((_, last)) = self.closed {
            return last.is_some_and(|lsn| lsn <= through);
        }
        self.parts.is_some_and(|parts| {
            (1..=parts).all(|part| {
                self.seen
                    .iter()
                    .any(|(number, lsn)| *number == part && *lsn <= through)
            })
        })
    }
}

impl GroupMembership {
    /// Record the `WriteGroup` record at `lsn` with descriptor `group`.
    pub fn observe(&mut self, lsn: u64, group: WriteGroup) {
        let origin = group.origin_at(lsn);
        let seen = self.groups.entry(origin).or_default();
        if !group.opens() {
            seen.parts = Some(seen.parts.map_or(group.parts, |n| n.max(group.parts)));
            seen.seen.insert((group.part, lsn));
        } else if lsn != origin {
            seen.members.insert(lsn);
        }
        if let Some((index, count)) = group.closed_continuation() {
            let last = seen.closed.and_then(|(_, last)| last);
            seen.closed = Some((count, if index == count { Some(lsn) } else { last }));
        }
    }

    /// The LSNs at or below `through` of every group that is not whole at or
    /// below `through`: a part announced but missing, or a part above
    /// `through`. A group the stream shows only by its opening record or its
    /// announcement, with no part, is not whole either. A record whole at
    /// append is not whole while its last continuation is above `through`.
    pub fn broken_through(&self, through: u64) -> Vec<u64> {
        let mut broken = Vec::new();
        for (&origin, group) in &self.groups {
            let whole = group.whole_through(through);
            if whole {
                continue;
            }
            if origin <= through {
                broken.push(origin);
            }
            broken.extend(
                group
                    .seen
                    .iter()
                    .map(|(_, lsn)| *lsn)
                    .chain(group.members.iter().copied())
                    .filter(|lsn| *lsn <= through),
            );
        }
        broken.sort_unstable();
        broken.dedup();
        broken
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(origin: u64, part: u32, parts: u32) -> WriteGroup {
        WriteGroup::part_of(origin, part, parts)
    }

    /// A record whole at append with two continuations is whole once its
    /// last continuation is present, with no part. A cut between the
    /// continuations drops the record and the first one.
    #[test]
    fn a_record_whole_at_append_needs_only_its_continuations() {
        let mut groups = GroupMembership::default();
        groups.observe(71, WriteGroup::continuing_closed(70, 1, 2));
        groups.observe(72, WriteGroup::continuing_closed(70, 2, 2));
        assert!(groups.broken_through(80).is_empty());
        assert_eq!(groups.broken_through(71), vec![70, 71]);
        assert_eq!(
            WriteGroup::continuing_closed(70, 2, 2).closed_continuation(),
            Some((2, 2))
        );
        assert_eq!(WriteGroup::continuing(70, 2).closed_continuation(), None);
    }

    #[test]
    fn a_whole_group_is_kept_and_a_split_one_is_dropped_entire() {
        let mut groups = GroupMembership::default();
        // Forward record at 10, parts at 12 and 15.
        groups.observe(12, part(10, 1, 2));
        groups.observe(15, part(10, 2, 2));
        assert!(groups.broken_through(15).is_empty(), "whole at 15");
        assert_eq!(
            groups.broken_through(13),
            vec![10, 12],
            "a cut between the parts drops the origin and the first part"
        );
        assert_eq!(groups.broken_through(9), Vec::<u64>::new());
    }

    #[test]
    fn an_opening_record_without_parts_is_broken() {
        let mut groups = GroupMembership::default();
        groups.observe(20, WriteGroup::OPENING);
        assert_eq!(groups.broken_through(30), vec![20]);
        groups.observe(22, part(20, 1, 1));
        assert!(groups.broken_through(30).is_empty());
    }

    /// A forward record at 40 whose announcement at 41 shows no part is a
    /// broken group, the announcement with it. Its part makes it whole.
    #[test]
    fn an_announced_forward_origin_without_parts_is_broken() {
        let mut groups = GroupMembership::default();
        groups.observe(41, WriteGroup::announcing(40));
        assert_eq!(groups.broken_through(50), vec![40, 41]);
        groups.observe(43, part(40, 1, 1));
        assert!(groups.broken_through(50).is_empty());
        assert_eq!(WriteGroup::announcing(40).origin_at(41), 40);
        assert_eq!(WriteGroup::OPENING.origin_at(41), 41);
    }

    /// A continuation is a member of its record's group: a group cut before
    /// its parts drops it with the record.
    #[test]
    fn a_continuation_falls_with_its_broken_group() {
        let mut groups = GroupMembership::default();
        groups.observe(51, WriteGroup::continuing(50, 1));
        groups.observe(52, WriteGroup::continuing(50, 2));
        groups.observe(53, WriteGroup::announcing(50));
        assert_eq!(groups.broken_through(60), vec![50, 51, 52, 53]);
        groups.observe(55, part(50, 1, 1));
        assert!(groups.broken_through(60).is_empty());
        assert_eq!(WriteGroup::continuing(50, 1).origin_at(51), 50);
    }

    #[test]
    fn a_missing_part_breaks_the_group() {
        let mut groups = GroupMembership::default();
        groups.observe(31, part(30, 1, 3));
        groups.observe(33, part(30, 3, 3));
        assert_eq!(groups.broken_through(40), vec![30, 31, 33]);
    }

    #[test]
    fn records_round_trip() {
        let record = WriteGroupRecord {
            group: part(7, 1, 1),
            ops: vec![RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::Put as u32,
                payload: vec![1, 2],
            }],
            redo: None,
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(
            WriteGroupRecord::from_bytes(&bytes).expect("decode"),
            record
        );
    }
}
