// SPDX-License-Identifier: BUSL-1.1

//! The shape a submitted transaction and its streamed parts must have for
//! the sequencer to carry them.
//!
//! The per-entry caps bound one sequencer log entry, never a transaction:
//!
//! - A single-entry transaction carries its plans in `plans`. They fit one
//!   entry with its config pins: at most `max_plans_bytes` bytes, for at
//!   most `max_participating_vshards` participants.
//! - A multi-part transaction's header carries no plans. Its config pins
//!   fit one entry. Its manifest names how many parts target each
//!   participant.
//! - Each streamed part fits one entry the same way, counting its targets.
//!   [`PartCursor`] checks the parts in order against the manifest. No cap
//!   bounds the whole transaction.

use std::collections::BTreeMap;

use crate::calvin::sequencer::error::SequencerError;
use crate::calvin::types::{MultiPartPlans, StreamedPart, TxClass};

/// The caps one sequencer entry obeys.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EntryLimits {
    pub max_plans_bytes: usize,
    pub max_participating_vshards: usize,
}

/// Refuse a transaction the sequencer cannot carry.
pub(crate) fn check_entry_shape(
    tx_class: &TxClass,
    limits: EntryLimits,
) -> Result<(), SequencerError> {
    let Some(manifest) = &tx_class.multi_part else {
        // The header carries its plans and its config pins in one entry.
        let bytes = tx_class.plans.len() + tx_class.config_pins.len();
        if bytes > limits.max_plans_bytes {
            return Err(SequencerError::TxnTooLarge {
                bytes,
                limit: limits.max_plans_bytes,
            });
        }
        let vshards = tx_class.participating_vshards().len();
        if vshards > limits.max_participating_vshards {
            return Err(SequencerError::FanoutTooWide {
                vshards,
                limit: limits.max_participating_vshards,
            });
        }
        return Ok(());
    };
    if !tx_class.plans.is_empty() {
        return Err(malformed("the header carries plans beside its parts"));
    }
    // The parts carry no pins. The header carries them all in one entry.
    if tx_class.config_pins.len() > limits.max_plans_bytes {
        return Err(SequencerError::TxnTooLarge {
            bytes: tx_class.config_pins.len(),
            limit: limits.max_plans_bytes,
        });
    }
    check_manifest(tx_class, manifest)
}

fn check_manifest(tx_class: &TxClass, manifest: &MultiPartPlans) -> Result<(), SequencerError> {
    if manifest.part_count == 0 || manifest.total_tasks == 0 || manifest.per_vshard.is_empty() {
        return Err(malformed("the manifest names no parts, tasks or targets"));
    }
    let sorted = manifest
        .per_vshard
        .windows(2)
        .all(|pair| pair[0].vshard < pair[1].vshard);
    if !sorted {
        return Err(malformed("the manifest's vShards are unsorted"));
    }
    let participants = tx_class.participating_vshards();
    for entry in &manifest.per_vshard {
        if entry.parts == 0 || entry.parts > manifest.part_count {
            return Err(malformed(&format!(
                "vShard {} is owed {} of {} parts",
                entry.vshard, entry.parts, manifest.part_count
            )));
        }
        if participants
            .binary_search_by_key(&entry.vshard, |vshard| vshard.as_u32())
            .is_err()
        {
            return Err(malformed(&format!(
                "the manifest targets vShard {}, which does not participate",
                entry.vshard
            )));
        }
    }
    Ok(())
}

/// A task split over chunk parts, part way through.
#[derive(Debug, Clone, Copy)]
struct OpenChunk {
    task: u32,
    next_offset: u64,
    total_len: u64,
}

/// The check of one stream's parts, in index order, against its manifest.
#[derive(Debug)]
pub(crate) struct PartCursor {
    part_count: u32,
    total_tasks: u32,
    next_index: u32,
    /// Parts each vShard is still owed.
    owed: BTreeMap<u32, u32>,
    last_first_task: Option<u32>,
    open_chunk: Option<OpenChunk>,
}

impl PartCursor {
    pub(crate) fn new(manifest: &MultiPartPlans) -> Self {
        Self {
            part_count: manifest.part_count,
            total_tasks: manifest.total_tasks,
            next_index: 0,
            owed: manifest
                .per_vshard
                .iter()
                .map(|entry| (entry.vshard, entry.parts))
                .collect(),
            last_first_task: None,
            open_chunk: None,
        }
    }

    /// The index of the next part the stream owes.
    pub(crate) fn next_index(&self) -> u32 {
        self.next_index
    }

    /// Whether every part arrived.
    pub(crate) fn is_complete(&self) -> bool {
        self.next_index == self.part_count
    }

    /// Take `streamed`, the stream's next part. A refused part changes
    /// nothing.
    pub(crate) fn admit(
        &mut self,
        streamed: &StreamedPart,
        limits: EntryLimits,
    ) -> Result<(), SequencerError> {
        let part = &streamed.part;
        let targets = &streamed.targets;
        if streamed.index != self.next_index || self.next_index >= self.part_count {
            return Err(malformed(&format!(
                "part {} arrived where part {} of {} was owed",
                streamed.index, self.next_index, self.part_count
            )));
        }
        if part.plans.len() > limits.max_plans_bytes {
            return Err(SequencerError::TxnTooLarge {
                bytes: part.plans.len(),
                limit: limits.max_plans_bytes,
            });
        }
        if targets.len() > limits.max_participating_vshards {
            return Err(SequencerError::FanoutTooWide {
                vshards: targets.len(),
                limit: limits.max_participating_vshards,
            });
        }
        if part.plans.is_empty() || targets.is_empty() {
            return Err(malformed("a part carries no plans or no targets"));
        }
        if targets.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(malformed("a part's targets are unsorted"));
        }
        if let Some(stray) = targets
            .iter()
            .find(|target| self.owed.get(target).is_none_or(|owed| *owed == 0))
        {
            return Err(malformed(&format!(
                "a part targets vShard {stray}, which is owed no more parts"
            )));
        }
        let (open_chunk, last_first_task) = self.next_task_state(streamed)?;
        let last = streamed.index + 1 == self.part_count;
        if last {
            let owed_after =
                |vshard: &u32, owed: &u32| *owed - u32::from(targets.binary_search(vshard).is_ok());
            if open_chunk.is_some() || self.owed.iter().any(|(v, o)| owed_after(v, o) > 0) {
                return Err(malformed(
                    "the last part leaves a task or a vShard's parts unfinished",
                ));
            }
        }
        for target in targets {
            if let Some(owed) = self.owed.get_mut(target) {
                *owed -= 1;
            }
        }
        self.open_chunk = open_chunk;
        self.last_first_task = Some(last_first_task);
        self.next_index += 1;
        Ok(())
    }

    /// The chunk state and last first task after `streamed`, or why its
    /// tasks do not follow the ones before it.
    fn next_task_state(
        &self,
        streamed: &StreamedPart,
    ) -> Result<(Option<OpenChunk>, u32), SequencerError> {
        let part = &streamed.part;
        let len = part.plans.len() as u64;
        if let Some(open) = self.open_chunk {
            let continues = part.chunk.is_some_and(|chunk| {
                part.first_task == open.task
                    && chunk.offset == open.next_offset
                    && chunk.total_len == open.total_len
            });
            if !continues {
                return Err(malformed(&format!(
                    "part {} does not continue task {} at byte {}",
                    streamed.index, open.task, open.next_offset
                )));
            }
            return Ok((advance(open, len)?, open.task));
        }
        let in_order = match self.last_first_task {
            None => part.first_task == 0,
            Some(last) => part.first_task > last,
        };
        if !in_order || part.first_task >= self.total_tasks {
            return Err(malformed(&format!(
                "part {} starts at task {} of {}, out of order",
                streamed.index, part.first_task, self.total_tasks
            )));
        }
        let open_chunk = match part.chunk {
            None => None,
            Some(chunk) if chunk.offset == 0 && chunk.total_len > 0 => advance(
                OpenChunk {
                    task: part.first_task,
                    next_offset: 0,
                    total_len: chunk.total_len,
                },
                len,
            )?,
            Some(_) => return Err(malformed("a task's first chunk does not start at byte 0")),
        };
        Ok((open_chunk, part.first_task))
    }
}

/// `open` after `len` more bytes: still open, or `None` once whole.
fn advance(open: OpenChunk, len: u64) -> Result<Option<OpenChunk>, SequencerError> {
    let next_offset = open.next_offset.saturating_add(len);
    if next_offset > open.total_len {
        return Err(malformed(&format!(
            "task {} chunks run past its {} bytes",
            open.task, open.total_len
        )));
    }
    Ok((next_offset < open.total_len).then_some(OpenChunk {
        next_offset,
        ..open
    }))
}

fn malformed(detail: &str) -> SequencerError {
    SequencerError::MalformedParts {
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::TenantId;
    use nodedb_types::id::{CollectionKey, DatabaseId};

    use super::*;
    use crate::calvin::types::{
        EngineKeySet, PartStreamId, PlanPart, ReadWriteSet, SortedVec, TaskChunk, VShardParts,
        VersionedReadSet,
    };

    const LIMITS: EntryLimits = EntryLimits {
        max_plans_bytes: 4,
        max_participating_vshards: 1,
    };

    /// A class over two collections on distinct vShards, and those vShards.
    fn two_vshard_class() -> (TxClass, u32, u32) {
        let mut seen: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            let Some((first, first_vshard)) = seen.clone() else {
                seen = Some((name, vshard));
                continue;
            };
            if first_vshard == vshard {
                continue;
            }
            let write_set = ReadWriteSet::new(vec![
                EngineKeySet::Document {
                    collection: first,
                    surrogates: SortedVec::new(vec![1]),
                },
                EngineKeySet::Document {
                    collection: name,
                    surrogates: SortedVec::new(vec![2]),
                },
            ]);
            let class = TxClass::new(
                ReadWriteSet::new(vec![]),
                write_set,
                Vec::new(),
                TenantId::new(1),
                None,
                VersionedReadSet::default(),
            )
            .expect("valid class");
            return (class, first_vshard.min(vshard), first_vshard.max(vshard));
        }
        panic!("no two distinct-vshard collections in 512 tries");
    }

    fn manifest(part_count: u32, total_tasks: u32, per_vshard: &[(u32, u32)]) -> MultiPartPlans {
        MultiPartPlans {
            stream: PartStreamId { node: 1, seq: 1 },
            part_count,
            total_tasks,
            user_write: true,
            client_write: true,
            per_vshard: per_vshard
                .iter()
                .map(|&(vshard, parts)| VShardParts { vshard, parts })
                .collect(),
        }
    }

    fn part(index: u32, first_task: u32, bytes: usize, target: u32) -> StreamedPart {
        StreamedPart {
            index,
            targets: vec![target],
            part: PlanPart {
                first_task,
                plans: vec![7; bytes],
                chunk: None,
            },
        }
    }

    fn chunk(
        index: u32,
        task: u32,
        offset: u64,
        bytes: usize,
        total: u64,
        target: u32,
    ) -> StreamedPart {
        let mut streamed = part(index, task, bytes, target);
        streamed.part.chunk = Some(TaskChunk {
            offset,
            total_len: total,
        });
        streamed
    }

    #[test]
    fn a_single_entry_over_the_entry_caps_is_refused() {
        let (mut class, _, _) = two_vshard_class();
        assert!(matches!(
            check_entry_shape(&class, LIMITS),
            Err(SequencerError::FanoutTooWide { vshards: 2, .. })
        ));
        class.plans = vec![0; 5];
        let wide = EntryLimits {
            max_participating_vshards: 8,
            ..LIMITS
        };
        assert!(matches!(
            check_entry_shape(&class, wide),
            Err(SequencerError::TxnTooLarge { bytes: 5, .. })
        ));
    }

    /// The header's config pins count against the entry's byte cap, both
    /// beside single-entry plans and on a multi-part header.
    #[test]
    fn config_pins_count_against_the_entry_byte_cap() {
        let (mut class, low, high) = two_vshard_class();
        let wide = EntryLimits {
            max_participating_vshards: 8,
            ..LIMITS
        };
        class.plans = vec![0; 2];
        class.set_config_pins(vec![0u8; 2].into());
        check_entry_shape(&class, wide).expect("plans and pins fit one entry");
        class.set_config_pins(vec![0u8; 3].into());
        assert!(matches!(
            check_entry_shape(&class, wide),
            Err(SequencerError::TxnTooLarge { bytes: 5, .. })
        ));

        class.plans = Vec::new();
        class.multi_part = Some(manifest(2, 2, &[(low, 1), (high, 1)]));
        class.set_config_pins(vec![0u8; 4].into());
        check_entry_shape(&class, LIMITS).expect("a header's pins fit one entry");
        class.set_config_pins(vec![0u8; 5].into());
        assert!(matches!(
            check_entry_shape(&class, LIMITS),
            Err(SequencerError::TxnTooLarge { bytes: 5, .. })
        ));
    }

    #[test]
    fn a_header_manifest_must_name_participants() {
        let (mut class, low, high) = two_vshard_class();
        class.multi_part = Some(manifest(2, 2, &[(low, 1), (high, 1)]));
        check_entry_shape(&class, LIMITS).expect("a header over both participants");
        for bad in [
            manifest(0, 2, &[(low, 1)]),
            manifest(2, 2, &[(high, 1), (low, 1)]),
            manifest(2, 2, &[(low, 3)]),
            manifest(2, 2, &[(u32::MAX, 1)]),
        ] {
            class.multi_part = Some(bad);
            assert!(matches!(
                check_entry_shape(&class, LIMITS),
                Err(SequencerError::MalformedParts { .. })
            ));
        }
    }

    /// Whole-task parts and a chunked task, each within the entry caps,
    /// carry a transaction over them.
    #[test]
    fn parts_that_each_fit_an_entry_carry_a_larger_transaction() {
        let (_, low, high) = two_vshard_class();
        let mut cursor = PartCursor::new(&manifest(4, 3, &[(low, 1), (high, 3)]));
        let parts = [
            part(0, 0, 4, low),
            chunk(1, 1, 0, 4, 10, high),
            chunk(2, 1, 4, 4, 10, high),
            chunk(3, 1, 8, 2, 10, high),
        ];
        for streamed in &parts {
            cursor.admit(streamed, LIMITS).expect("fits");
        }
        assert!(cursor.is_complete());
    }

    #[test]
    fn a_part_that_breaks_the_stream_is_refused_and_changes_nothing() {
        let (_, low, high) = two_vshard_class();
        let m = manifest(2, 2, &[(low, 1), (high, 1)]);
        let cases = [
            (part(1, 0, 1, low), "an index gap"),
            (part(0, 1, 1, low), "a first part past task 0"),
            (part(0, 0, 5, low), "over the byte cap"),
            (part(0, 0, 1, u32::MAX), "a stray target"),
            (chunk(0, 0, 3, 1, 4, low), "a first chunk past byte 0"),
        ];
        for (streamed, what) in &cases {
            let mut cursor = PartCursor::new(&m);
            assert!(cursor.admit(streamed, LIMITS).is_err(), "{what}");
            assert_eq!(cursor.next_index(), 0, "{what}");
        }

        let mut cursor = PartCursor::new(&m);
        cursor.admit(&part(0, 0, 1, low), LIMITS).expect("first");
        assert!(
            cursor.admit(&part(1, 1, 1, low), LIMITS).is_err(),
            "the last part leaves the other vShard's part owed"
        );
        let mut chunked = PartCursor::new(&manifest(2, 1, &[(low, 2)]));
        chunked
            .admit(&chunk(0, 0, 0, 2, 5, low), LIMITS)
            .expect("first chunk");
        assert!(
            chunked.admit(&chunk(1, 0, 3, 2, 5, low), LIMITS).is_err(),
            "a chunk that skips bytes"
        );
        assert!(
            chunked.admit(&chunk(1, 0, 2, 2, 5, low), LIMITS).is_err(),
            "the last chunk leaves the task unfinished"
        );
    }
}
