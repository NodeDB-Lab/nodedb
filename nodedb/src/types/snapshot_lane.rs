// SPDX-License-Identifier: BUSL-1.1

//! The Event Plane lane state a Raft group snapshot carries.

/// One `(partition, epoch, index, sequence)` position with its held row.
pub type HeldAtPosition = (u32, u64, u64, u64, Vec<u8>);

/// One partition's cursor, as `(partition, epoch, index, sequence)`.
pub type PartitionCursor = (u32, u64, u64, u64);

/// The Event Plane lane state a Raft group snapshot carries for its vShards.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct GroupEventLane {
    /// Held trigger actions, each an encoded held event.
    pub held_actions: Vec<HeldAtPosition>,
    /// The trigger firing cursors of the group's partitions.
    pub action_cursors: Vec<PartitionCursor>,
    /// Held committed messages, each an encoded `RedoPublish`.
    pub held_publishes: Vec<HeldAtPosition>,
    /// The message delivery cursors of the group's partitions.
    pub publish_cursors: Vec<PartitionCursor>,
}
