// SPDX-License-Identifier: BUSL-1.1

//! Plans too large for one sequencer entry, carried as ordered parts.
//!
//! A multi-part transaction is sequenced once, by a header in an epoch batch.
//! The header is the transaction's [`TxClass`] with its full read and write
//! sets and no plans, so every participant takes all its locks at the
//! header's position. Its plans travel as [`PlanPart`]s, each in its own
//! `SequencerEntry::TxnPart` after the header. A participant stages the
//! transaction once every part that targets it has arrived.
//!
//! - A part holds consecutive whole tasks in the [`super::part_tasks`] form:
//!   each task's own encoding, byte end, and homes. A target decodes only
//!   the tasks that route to it. `first_task` is the index of its first
//!   task in the whole transaction.
//! - A task too large for one part travels as a run of chunk parts. Each
//!   holds one byte range of the task's own encoding and nothing else.
//! - A part targets the vShards its tasks route to. Only those receive it.
//! - The header's manifest names how many parts target each participant, so
//!   a participant knows when it holds all of its parts.
//!
//! The coordinator streams the parts to the sequencer leader after it
//! submits the header. The manifest names the stream.

use serde::{Deserialize, Serialize};

use super::transaction::TxClass;

/// The name of one multi-part transaction's part stream: the coordinator
/// node and a sequence that node never reuses.
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
pub struct PartStreamId {
    pub node: u64,
    pub seq: u64,
}

/// Where a chunk part's bytes sit in the encoding of its one task.
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
pub struct TaskChunk {
    /// Offset of the part's bytes in the task's encoding.
    pub offset: u64,
    /// Length of the task's whole encoding.
    pub total_len: u64,
}

/// One part of a multi-part transaction's plans.
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
pub struct PlanPart {
    /// Index of the part's first task in the whole transaction. For a chunk
    /// part, the index of its one task.
    pub first_task: u32,
    /// The part's tasks in the [`super::part_tasks`] form, or for a chunk
    /// part, one byte range of its task's encoding.
    pub plans: Vec<u8>,
    /// Set on a chunk part.
    #[serde(default)]
    #[msgpack(default)]
    pub chunk: Option<TaskChunk>,
}

/// A part with its index and the vShards it targets, as the coordinator
/// streams it and the sequencer leader queues it.
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
pub struct StreamedPart {
    pub index: u32,
    /// The vShards the part's tasks route to, sorted.
    pub targets: Vec<u32>,
    pub part: PlanPart,
}

/// How many parts target one participating vShard.
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
pub struct VShardParts {
    pub vshard: u32,
    pub parts: u32,
}

/// The plan manifest of a multi-part transaction. Its size grows with the
/// participant count, never with the plan bytes.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MultiPartPlans {
    /// The stream that carries the parts to the sequencer leader.
    pub stream: PartStreamId,
    /// How many parts the transaction carries.
    pub part_count: u32,
    /// How many tasks the whole transaction carries.
    pub total_tasks: u32,
    /// Whether any task of the whole transaction is a user write, as opposed
    /// to a derived side effect. Opaque to the sequencer: the host decides it.
    pub user_write: bool,
    /// Whether any task of the whole transaction lies outside `body_plans`:
    /// the client wrote, beside any trigger body. Opaque to the sequencer.
    pub client_write: bool,
    /// How many parts target each vShard, sorted by vShard. A vShard absent
    /// here receives no part.
    pub per_vshard: Vec<VShardParts>,
}

impl MultiPartPlans {
    /// How many parts target `vshard`.
    pub fn parts_for(&self, vshard: u32) -> u32 {
        self.per_vshard
            .binary_search_by_key(&vshard, |entry| entry.vshard)
            .map_or(0, |at| self.per_vshard[at].parts)
    }
}

impl TxClass {
    /// Whether the transaction carries its plans as parts.
    pub fn is_multi_part(&self) -> bool {
        self.multi_part.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> MultiPartPlans {
        MultiPartPlans {
            stream: PartStreamId { node: 3, seq: 11 },
            part_count: 3,
            total_tasks: 7,
            user_write: true,
            client_write: true,
            per_vshard: vec![
                VShardParts {
                    vshard: 1,
                    parts: 1,
                },
                VShardParts {
                    vshard: 4,
                    parts: 2,
                },
            ],
        }
    }

    #[test]
    fn a_vshard_waits_only_for_the_parts_that_target_it() {
        let manifest = manifest();
        assert_eq!(manifest.parts_for(4), 2);
        assert_eq!(manifest.parts_for(1), 1);
        assert_eq!(manifest.parts_for(7), 0);
    }

    #[test]
    fn a_manifest_and_a_chunk_part_round_trip() {
        let manifest = manifest();
        let bytes = zerompk::to_msgpack_vec(&manifest).expect("encode");
        let decoded: MultiPartPlans = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, manifest);

        let part = StreamedPart {
            index: 2,
            targets: vec![4],
            part: PlanPart {
                first_task: 5,
                plans: vec![7; 16],
                chunk: Some(TaskChunk {
                    offset: 16,
                    total_len: 40,
                }),
            },
        };
        let bytes = zerompk::to_msgpack_vec(&part).expect("encode");
        let decoded: StreamedPart = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, part);
    }
}
