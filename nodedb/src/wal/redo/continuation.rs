// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction record too large for one WAL record.
//!
//! One WAL record holds at most the writer's `max_payload` bytes. A larger
//! `TransactionRedo` record splits: the origin record keeps every field and
//! the sub-records that fit, and each continuation is a `WriteGroup` record
//! of the origin's group (see [`super::group::WriteGroup::continuing`]) with
//! the next sub-records in order. A continuation carries the row metadata its
//! events read. Replay applies a continuation's sub-records at the origin's
//! LSN, after the origin's own, so the split changes no applied order. A
//! restore keeps the continuations only with their whole group.

use serde::{Deserialize, Serialize};

use super::record::{RedoRecord, RedoSubRecord};
use super::row_changes::RedoRowChange;
use super::row_sources::RedoRowSource;

/// Bytes one sub-record's encoding adds beyond its payload, at most.
pub const SUB_RECORD_OVERHEAD: usize = 32;

/// Bytes a record's encoding adds beyond its fields, at most.
pub const RECORD_OVERHEAD: usize = 64;

/// The row metadata of a committed transaction record a continuation's
/// events read: which rows ran with another source, and each row's net kind.
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
pub struct ContinuedRedo {
    pub row_sources: Vec<RedoRowSource>,
    pub row_changes: Vec<RedoRowChange>,
}

/// A committed transaction record split to fit the WAL record limit.
#[derive(Debug)]
pub struct SplitRedo {
    /// The origin record: every field, with the sub-records that fit.
    pub origin: RedoRecord,
    /// The sub-records of each continuation, in order.
    pub continuations: Vec<Vec<RedoSubRecord>>,
    /// The row metadata every continuation carries.
    pub metadata: ContinuedRedo,
}

/// Split `record` so no record exceeds `max_payload` bytes.
pub fn split_redo(record: &RedoRecord, max_payload: usize) -> crate::Result<SplitRedo> {
    let metadata = ContinuedRedo {
        row_sources: record.row_sources.clone(),
        row_changes: record.row_changes.clone(),
    };
    let mut origin = record.clone();
    origin.ops = Vec::new();
    let origin_budget = remaining(max_payload, origin.to_bytes()?.len())?;
    let continuation_budget = remaining(
        max_payload,
        super::group::WriteGroupRecord {
            schemas: Vec::new(),
            group: super::group::WriteGroup::continuing(u64::MAX, u32::MAX),
            ops: Vec::new(),
            redo: Some(metadata.clone()),
        }
        .to_bytes()?
        .len(),
    )?;

    let mut chunks: Vec<Vec<RedoSubRecord>> = vec![Vec::new()];
    let mut used = 0usize;
    for op in &record.ops {
        let cost = op.payload.len().saturating_add(SUB_RECORD_OVERHEAD);
        let budget = if chunks.len() == 1 {
            origin_budget
        } else {
            continuation_budget
        };
        let fits = used.saturating_add(cost) <= budget;
        let chunk_empty = chunks.last().is_none_or(Vec::is_empty);
        if !fits && !chunk_empty {
            chunks.push(Vec::new());
            used = 0;
        }
        let budget = if chunks.len() == 1 {
            origin_budget
        } else {
            continuation_budget
        };
        if cost > budget {
            return Err(crate::Error::Internal {
                detail: format!(
                    "a transaction sub-record of {} bytes exceeds the WAL record limit of \
                     {max_payload} bytes",
                    op.payload.len()
                ),
            });
        }
        used = used.saturating_add(cost);
        if let Some(chunk) = chunks.last_mut() {
            chunk.push(op.clone());
        }
    }
    let mut chunks = chunks.into_iter();
    origin.ops = chunks.next().unwrap_or_default();
    Ok(SplitRedo {
        origin,
        continuations: chunks.collect(),
        metadata,
    })
}

/// The bytes left for sub-records in a record whose other fields encode to
/// `fixed` bytes.
fn remaining(max_payload: usize, fixed: usize) -> crate::Result<usize> {
    max_payload
        .checked_sub(fixed.saturating_add(RECORD_OVERHEAD))
        .ok_or(crate::Error::Internal {
            detail: format!(
                "a transaction record's row metadata of {fixed} bytes exceeds the WAL record \
                 limit of {max_payload} bytes"
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(bytes: usize) -> RedoSubRecord {
        RedoSubRecord {
            record_type: nodedb_wal::record::RecordType::Put as u32,
            payload: vec![7; bytes],
        }
    }

    fn record(ops: Vec<RedoSubRecord>) -> RedoRecord {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
    }

    /// Every record the split writes fits the limit, and the sub-records
    /// keep their order across the origin and its continuations.
    #[test]
    fn a_large_record_splits_under_the_limit_in_order() {
        let max = 4096;
        let ops: Vec<RedoSubRecord> = (0..40).map(|i| op(200 + i)).collect();
        let split = split_redo(&record(ops.clone()), max).expect("split");
        assert!(!split.continuations.is_empty());
        assert!(split.origin.to_bytes().expect("encode").len() <= max);
        for (index, chunk) in (1u32..).zip(&split.continuations) {
            let bytes = super::super::group::WriteGroupRecord {
                schemas: Vec::new(),
                group: super::super::group::WriteGroup::continuing(9, index),
                ops: chunk.clone(),
                redo: Some(split.metadata.clone()),
            }
            .to_bytes()
            .expect("encode");
            assert!(
                bytes.len() <= max,
                "continuation {index} is {} bytes",
                bytes.len()
            );
        }
        let rejoined: Vec<RedoSubRecord> = split
            .origin
            .ops
            .iter()
            .chain(split.continuations.iter().flatten())
            .cloned()
            .collect();
        assert_eq!(rejoined, ops);
    }

    #[test]
    fn a_sub_record_over_the_limit_is_refused() {
        assert!(split_redo(&record(vec![op(5000)]), 4096).is_err());
    }
}
