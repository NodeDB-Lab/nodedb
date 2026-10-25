// SPDX-License-Identifier: BUSL-1.1

//! The plan bytes of one whole-task part, each task readable on its own.
//!
//! A part targets the vShards of all its tasks, but each target runs only
//! the tasks that route to it. The part names each task's byte end and
//! homes, so a target decodes only its own tasks.
//!
//! Wire: a msgpack array `[ends, homes, tasks]`.
//! - `ends`: each task's exclusive end offset in `tasks`, strictly ascending.
//!   The last end is the length of `tasks`.
//! - `homes`: each task's homes, as sorted vShard ids with no repeats.
//!   A task always has at least one home.
//! - `tasks`: every task's own encoding, concatenated, as msgpack bin.
//!
//! A chunk part carries raw bytes of one task's encoding, never this form.

use zerompk::{FromMessagePack, ToMessagePack};

/// Bytes the `[ends, homes, tasks]` frame adds, at most: the outer array
/// header plus the headers of `ends`, `homes` and `tasks`.
pub const PART_TASKS_FRAME_BYTES: usize = 1 + 5 + 5 + 5;

/// Bytes one msgpack `u32` or one array or bin header takes, at most.
const MAX_HEADER_BYTES: usize = 5;

/// Bytes one task of `homes` homes adds beside its own encoding, at most:
/// its end, its homes array header, and each home.
pub fn part_task_overhead(homes: usize) -> usize {
    MAX_HEADER_BYTES + MAX_HEADER_BYTES + MAX_HEADER_BYTES.saturating_mul(homes)
}

/// Why part plan bytes are not a valid `[ends, homes, tasks]` form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartTasksError {
    #[error("part tasks do not decode: {detail}")]
    Decode { detail: String },
    #[error("part tasks name {ends} ends but {homes} home sets")]
    CountMismatch { ends: usize, homes: usize },
    #[error("part task {task} ends at or before the task before it")]
    EndsUnsorted { task: usize },
    #[error("part tasks end at byte {last_end} of {len} bytes")]
    EndsMismatch { last_end: u32, len: usize },
    #[error("part task {task} homes are empty, out of order, or repeat")]
    HomesUnsorted { task: usize },
    #[error("part tasks of {len} bytes overflow a u32 offset")]
    TooLarge { len: usize },
    #[error("part tasks do not encode: {detail}")]
    Encode { detail: String },
}

/// Whether `homes` is non-empty, sorted, and free of repeats.
fn homes_valid(homes: &[u32]) -> bool {
    !homes.is_empty() && homes.windows(2).all(|pair| pair[0] < pair[1])
}

/// Builds the plan bytes of one whole-task part, task by task.
#[derive(Debug, Default)]
pub struct PartTasksBuilder {
    ends: Vec<u32>,
    homes: Vec<Vec<u32>>,
    tasks: Vec<u8>,
    /// Sum of [`part_task_overhead`] over the tasks pushed.
    overhead: usize,
}

impl PartTasksBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many tasks the builder holds.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The encoded size of the tasks pushed so far, at most.
    pub fn encoded_len(&self) -> usize {
        PART_TASKS_FRAME_BYTES
            .saturating_add(self.tasks.len())
            .saturating_add(self.overhead)
    }

    /// The encoded size, at most, once a task of `task_len` bytes and
    /// `homes` homes is pushed.
    pub fn encoded_len_with(&self, task_len: usize, homes: usize) -> usize {
        self.encoded_len()
            .saturating_add(task_len)
            .saturating_add(part_task_overhead(homes))
    }

    /// Push one task: its own encoding and its homes, sorted with no repeats.
    pub fn push(&mut self, task: &[u8], homes: &[u32]) -> Result<(), PartTasksError> {
        if !homes_valid(homes) {
            return Err(PartTasksError::HomesUnsorted {
                task: self.ends.len(),
            });
        }
        let len = self.tasks.len().saturating_add(task.len());
        let end = u32::try_from(len).map_err(|_| PartTasksError::TooLarge { len })?;
        self.tasks.extend_from_slice(task);
        self.ends.push(end);
        self.homes.push(homes.to_vec());
        self.overhead = self
            .overhead
            .saturating_add(part_task_overhead(homes.len()));
        Ok(())
    }

    /// The part's plan bytes. Leaves the builder empty for the next part.
    pub fn take(&mut self) -> Result<Vec<u8>, PartTasksError> {
        let bytes = zerompk::to_msgpack_vec(&*self).map_err(|e| PartTasksError::Encode {
            detail: e.to_string(),
        })?;
        *self = Self::default();
        Ok(bytes)
    }
}

impl ToMessagePack for PartTasksBuilder {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(3)?;
        self.ends.write(writer)?;
        self.homes.write(writer)?;
        writer.write_binary(&self.tasks)
    }
}

/// The decoded plan bytes of one whole-task part. Borrows the task bytes.
#[derive(Debug)]
pub struct PartTasks<'a> {
    ends: Vec<u32>,
    homes: Vec<Vec<u32>>,
    tasks: &'a [u8],
}

impl<'de> FromMessagePack<'de> for PartTasks<'de> {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(3)?;
        let ends = Vec::<u32>::read(reader)?;
        let homes = Vec::<Vec<u32>>::read(reader)?;
        let tasks = <&'de [u8]>::read(reader)?;
        Ok(Self { ends, homes, tasks })
    }
}

impl<'a> PartTasks<'a> {
    /// Decode and check `bytes`. Every task range and home set is checked
    /// here, so the readers below never fail.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, PartTasksError> {
        let decoded: Self = zerompk::from_msgpack(bytes).map_err(|e| PartTasksError::Decode {
            detail: e.to_string(),
        })?;
        decoded.check()?;
        Ok(decoded)
    }

    fn check(&self) -> Result<(), PartTasksError> {
        if self.ends.len() != self.homes.len() {
            return Err(PartTasksError::CountMismatch {
                ends: self.ends.len(),
                homes: self.homes.len(),
            });
        }
        let mut previous = 0u32;
        for (task, &end) in self.ends.iter().enumerate() {
            if end <= previous {
                return Err(PartTasksError::EndsUnsorted { task });
            }
            previous = end;
        }
        if previous as usize != self.tasks.len() {
            return Err(PartTasksError::EndsMismatch {
                last_end: previous,
                len: self.tasks.len(),
            });
        }
        match self.homes.iter().position(|homes| !homes_valid(homes)) {
            Some(task) => Err(PartTasksError::HomesUnsorted { task }),
            None => Ok(()),
        }
    }

    /// How many tasks the part holds.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The homes of the part's task `task`, sorted. Empty past the last task.
    pub fn homes(&self, task: usize) -> &[u32] {
        self.homes.get(task).map(Vec::as_slice).unwrap_or(&[])
    }

    /// The own encoding of the part's task `task`. Callers pass an index
    /// below [`Self::len`], and `check` holds every such range inside `tasks`.
    fn bytes_of(&self, task: usize) -> &'a [u8] {
        let start = match task {
            0 => 0,
            _ => self.ends[task - 1] as usize,
        };
        &self.tasks[start..self.ends[task] as usize]
    }

    /// Every task whose homes hold `vshard`, in part order, as its index in
    /// the part and its own encoding.
    pub fn tasks_for(&self, vshard: u32) -> impl Iterator<Item = (usize, &'a [u8])> + '_ {
        self.homes
            .iter()
            .enumerate()
            .filter(move |(_, homes)| homes.binary_search(&vshard).is_ok())
            .map(move |(task, _)| (task, self.bytes_of(task)))
    }

    /// Every task in part order, as its own encoding and its homes.
    pub fn iter(&self) -> impl Iterator<Item = (&'a [u8], &[u32])> + '_ {
        self.homes
            .iter()
            .enumerate()
            .map(move |(task, homes)| (self.bytes_of(task), homes.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three_tasks() -> Vec<u8> {
        let mut builder = PartTasksBuilder::new();
        builder.push(b"aa", &[1]).expect("push");
        builder.push(b"bbb", &[2, 5]).expect("push");
        builder.push(b"c", &[5]).expect("push");
        assert_eq!(builder.len(), 3);
        let bound = builder.encoded_len();
        let bytes = builder.take().expect("encode");
        assert!(bytes.len() <= bound, "the size bound holds");
        assert!(builder.is_empty(), "take empties the builder");
        bytes
    }

    #[test]
    fn each_target_reads_only_its_own_tasks() {
        let bytes = three_tasks();
        let tasks = PartTasks::decode(&bytes).expect("decode");
        assert_eq!(tasks.len(), 3);
        let at = |vshard| tasks.tasks_for(vshard).collect::<Vec<_>>();
        assert_eq!(at(1), vec![(0, &b"aa"[..])]);
        assert_eq!(at(2), vec![(1, &b"bbb"[..])]);
        assert_eq!(at(5), vec![(1, &b"bbb"[..]), (2, &b"c"[..])]);
        assert!(at(9).is_empty());
        assert_eq!(tasks.homes(1), &[2, 5]);
        let every: Vec<_> = tasks.iter().collect();
        assert_eq!(
            every,
            vec![
                (&b"aa"[..], &[1u32][..]),
                (&b"bbb"[..], &[2, 5][..]),
                (&b"c"[..], &[5][..])
            ]
        );
    }

    #[test]
    fn unsorted_homes_are_refused_at_push() {
        let mut builder = PartTasksBuilder::new();
        assert_eq!(
            builder.push(b"a", &[3, 1]),
            Err(PartTasksError::HomesUnsorted { task: 0 })
        );
        assert_eq!(
            builder.push(b"a", &[]),
            Err(PartTasksError::HomesUnsorted { task: 0 })
        );
    }

    #[test]
    fn malformed_part_tasks_are_refused_at_decode() {
        let encode = |ends: Vec<u32>, homes: Vec<Vec<u32>>, tasks: &[u8]| {
            let builder = PartTasksBuilder {
                ends,
                homes,
                tasks: tasks.to_vec(),
                overhead: 0,
            };
            zerompk::to_msgpack_vec(&builder).expect("encode")
        };
        let refused = |bytes: Vec<u8>| PartTasks::decode(&bytes).err();
        assert_eq!(
            refused(encode(vec![2, 2], vec![vec![1], vec![1]], b"ab")),
            Some(PartTasksError::EndsUnsorted { task: 1 })
        );
        assert_eq!(
            refused(encode(vec![1], vec![vec![1]], b"ab")),
            Some(PartTasksError::EndsMismatch {
                last_end: 1,
                len: 2
            })
        );
        assert_eq!(
            refused(encode(vec![2], vec![vec![4, 4]], b"ab")),
            Some(PartTasksError::HomesUnsorted { task: 0 })
        );
        assert_eq!(
            refused(encode(vec![2], vec![], b"ab")),
            Some(PartTasksError::CountMismatch { ends: 1, homes: 0 })
        );
        assert!(matches!(
            refused(vec![0xc1]),
            Some(PartTasksError::Decode { .. })
        ));
    }
}
