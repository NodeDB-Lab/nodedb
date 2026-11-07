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
    /// Every backup cut barrier the builder applied in the group at or below
    /// the snapshot's index whose cut window is open, in cut key order. The
    /// receiver never applies them, and a cut waiting there needs them.
    pub cut_barriers: Vec<CoveredCutBarrier>,
    /// The group's durable cut floors at or below the snapshot's index, in
    /// index order. Every later entry records a commit HLC above them.
    pub cut_floors: Vec<CutFloor>,
}

/// A backup cut's barrier a [`GroupCalvinCut`] covers.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CoveredCutBarrier {
    /// The cut's watermark: HLC wall time in nanoseconds.
    pub hlc: u64,
    /// The cluster restore point the cut takes, `0` for a backup's cut.
    pub restore_point: u64,
    /// A database backup's capture request.
    pub capture: Option<nodedb_physical::physical_plan::CutCaptureRequest>,
    /// What a mirror cut tells the source's mirrors.
    pub mirror: Option<nodedb_types::MirrorCutMark>,
    /// The log index of the cut's first barrier in the group.
    pub log_index: u64,
}

/// One durable cut floor of a group in a [`GroupCalvinCut`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CutFloor {
    /// The barrier's log index.
    pub index: u64,
    /// The barrier's watermark: HLC wall time in nanoseconds.
    pub watermark: u64,
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
    /// The mirror database whose source issued the positions. `None` names
    /// this cluster's own sequencer.
    pub mirror_database: Option<u64>,
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
