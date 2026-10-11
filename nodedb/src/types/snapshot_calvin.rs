// SPDX-License-Identifier: BUSL-1.1

//! The Calvin state a Raft data-group snapshot carries.
//!
//! A replica's state of a vShard is its data group's log through some index
//! plus every Calvin transaction the sequencer sequenced for the vShard.
//! Calvin installs bypass the data group's log, so a snapshot of the group's
//! storage holds Calvin transactions too, and carries which ones.

/// The Calvin cut of a data-group snapshot.
///
/// The builder places a cut marker in the sequencer log and waits until the
/// scheduler of every group vShard on the builder passed it. Every input
/// sequenced below the marker then finished there. The capture fences every
/// Calvin install of the group's vShards, so the storage it reads holds
/// exactly the positions each vShard's applied state names.
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
pub struct GroupCalvinCut {
    /// The sequencer log index of the marker. The snapshot holds every
    /// Calvin input of its vShards sequenced at or below it.
    pub through: u64,
    /// The applied state of each group vShard whose scheduler runs on the
    /// builder.
    pub vshards: Vec<VShardCalvinState>,
    /// The stored dependent-read barrier log of each unfinished txn of a
    /// group vShard. It holds every barrier entry of the group's log at or
    /// below the snapshot's index, which the receiver never applies.
    pub barrier_logs: Vec<CutBarrierLog>,
}

/// One vShard's applied Calvin positions in a [`GroupCalvinCut`].
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
pub struct VShardCalvinState {
    pub vshard_id: u32,
    /// Every position of every epoch at or below this is applied. `u64::MAX`
    /// means none is.
    pub fully_applied_epoch: u64,
    /// The applied `(epoch, position)` pairs above the watermark.
    pub tail: Vec<(u64, u32)>,
}

/// One txn's stored dependent-read barrier log in a [`GroupCalvinCut`].
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
pub struct CutBarrierLog {
    pub vshard_id: u32,
    pub epoch: u64,
    pub position: u32,
    /// The encoded barrier log.
    pub log: Vec<u8>,
}
