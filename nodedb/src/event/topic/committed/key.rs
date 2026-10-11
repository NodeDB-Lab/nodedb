// SPDX-License-Identifier: BUSL-1.1

//! The replica-independent name of a committed message, and the lease its
//! delivery runs under.
//!
//! A message is named by the change-feed partition of the record that carries
//! it and a position in that partition: the record's replicated position with
//! the message's ordinal. The apply that installs the record stamps that
//! position on the message itself ([`crate::wal::PublishPosition`]): the
//! record takes its data-group entry's `(epoch, log index)`.

use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::event::topic::types::PublishOrigin;
use crate::wal::RedoPublish;

/// Byte length of an encoded key: partition, epoch, index, sequence.
pub(crate) const KEY_LEN: usize = 4 + 8 + 8 + 8;

/// The durable key of one message. Keys of one partition sort by position.
/// The trigger action lane keys its held events the same way.
pub(crate) fn key_bytes(origin: &PublishOrigin) -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    key[..4].copy_from_slice(&origin.partition.to_be_bytes());
    key[4..12].copy_from_slice(&origin.position.epoch.to_be_bytes());
    key[12..20].copy_from_slice(&origin.position.index.to_be_bytes());
    key[20..].copy_from_slice(&origin.position.sequence.to_be_bytes());
    key
}

/// Decode a key [`key_bytes`] wrote.
pub(crate) fn origin_of_key(key: &[u8]) -> Option<PublishOrigin> {
    if key.len() != KEY_LEN {
        return None;
    }
    let word = |range: std::ops::Range<usize>| -> Option<u64> {
        Some(u64::from_be_bytes(key.get(range)?.try_into().ok()?))
    };
    Some(PublishOrigin {
        partition: u32::from_be_bytes(key.get(..4)?.try_into().ok()?),
        position: CdcOffset::at(word(4..12)?, word(12..20)?, word(20..28)?),
    })
}

/// The origin of the record's `ordinal`-th message `publish`.
///
/// The apply that installed the record stamped its replicated position on
/// the message. `None` for a message no apply stamped: it has no position on
/// any partition.
pub(super) fn origin_of_event(ordinal: u32, publish: &RedoPublish) -> Option<PublishOrigin> {
    let position = publish.position?;
    Some(PublishOrigin {
        partition: position.partition,
        position: CdcOffset::data_event(position.epoch, position.index, u64::from(ordinal) + 1),
    })
}

/// The lease this node delivers `partition`'s committed messages under:
/// `(group, term)` of the leader lease of the partition's data group. `None`
/// when another node delivers it, or before `start_raft` wires the node's
/// Raft. The trigger action lane fires a partition's actions under the same
/// lease.
pub(crate) fn delivery_lease(state: &SharedState, partition: u32) -> Option<(u64, u64)> {
    let gate = state.raft_read_gate.get()?;
    let routing = state.cluster_routing.as_ref()?;
    let group_id = routing
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .group_for_vshard(partition)
        .ok()?;
    gate.leader_lease_term(group_id)
        .map(|term| (group_id, term))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_round_trips_and_sorts_by_partition_then_position() {
        let origin = |partition, index, ordinal| PublishOrigin {
            partition,
            position: CdcOffset::data_event(0, index, ordinal),
        };
        let first = origin(3, 7, 1);
        assert_eq!(origin_of_key(&key_bytes(&first)), Some(first));
        assert!(key_bytes(&origin(3, 7, 2)) > key_bytes(&first));
        assert!(key_bytes(&origin(3, 8, 1)) > key_bytes(&origin(3, 7, 2)));
        assert!(key_bytes(&origin(4, 1, 1)) > key_bytes(&origin(3, 8, 1)));
        assert_eq!(origin_of_key(&[0u8; 3]), None);
    }

    /// A stamped message is named by its stamp alone, and a message no apply
    /// stamped names no origin.
    #[test]
    fn a_message_is_named_by_its_stamp() {
        use crate::event::topic::committed::event::tests::publish;

        let mut stamped = publish("feed", "a");
        stamped.position = Some(crate::wal::PublishPosition {
            partition: 9,
            epoch: 2,
            index: 500,
        });
        assert_eq!(
            origin_of_event(0, &stamped),
            Some(PublishOrigin {
                partition: 9,
                position: CdcOffset::data_event(2, 500, 1),
            })
        );
        assert_eq!(origin_of_event(1, &publish("feed", "a")), None);
    }
}
