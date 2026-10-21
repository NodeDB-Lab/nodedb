// SPDX-License-Identifier: BUSL-1.1

//! The rows a KV batch put writes, judged by its intent.
//!
//! The live handler, the resolve handler, and transaction staging all judge a
//! batch through [`plan_kv_batch`], each with its own view of stored state.
//! So the three paths store the same rows with the same bodies.

use std::borrow::Cow;
use std::collections::HashMap;

use nodedb_physical::physical_plan::{DeclaredColumn, KvBatchIntent};

use super::conflict_merge::merge_kv_conflict_body;
use super::crud::kv_duplicate_key;
use super::declared_body::coerce_kv_body;

/// One row a batch put writes.
pub(in crate::data::executor) struct PlannedKvRow<'a> {
    /// The row's position in the batch's `entries`.
    pub index: usize,
    pub key: &'a [u8],
    /// The body stored under `key` before this row: stored state, or the
    /// body an earlier row of the same batch wrote.
    pub prior: Option<Vec<u8>>,
    /// The body this row stores. Borrowed when it is the entry verbatim.
    pub stored: Cow<'a, [u8]>,
}

impl PlannedKvRow<'_> {
    /// Whether this row replaced a body rather than created the key.
    pub fn updated(&self) -> bool {
        self.prior.is_some()
    }
}

/// The rows `entries` write under `intent`, in `entries` order. `read`
/// returns the stored body of a key, or `None` when it is absent.
///
/// - `Put` and `Upsert` write every entry.
/// - `Insert` refuses the whole batch with `unique_violation` on a stored or
///   repeated key.
/// - `InsertIfAbsent` skips a stored or repeated key.
/// - `Merge` stores the merge of its updates into the stored body, or the
///   entry when the key is absent. A repeated key merges into the body its
///   earlier row stored.
///
/// The `Merge` write policy is not decided here. Each caller decides it
/// against the stored bodies, through its own gate.
pub(in crate::data::executor) fn plan_kv_batch<'a>(
    collection: &str,
    entries: &'a [(Vec<u8>, Vec<u8>)],
    intent: &KvBatchIntent,
    declared: &[DeclaredColumn],
    crs: &nodedb_crs::CrsContext<'_>,
    mut read: impl FnMut(&[u8]) -> Option<Vec<u8>>,
) -> crate::Result<Vec<PlannedKvRow<'a>>> {
    // The latest planned row of each key the batch has written.
    let mut latest: HashMap<&'a [u8], usize> = HashMap::with_capacity(entries.len());
    let mut rows: Vec<PlannedKvRow<'a>> = Vec::with_capacity(entries.len());
    for (index, (key, value)) in entries.iter().enumerate() {
        let key = key.as_slice();
        let prior = match latest.get(key) {
            Some(&row) => rows.get(row).map(|earlier| earlier.stored.to_vec()),
            None => read(key),
        };
        // An entry stores its body re-typed to the declared columns, the
        // body the point write stores. Borrowed when no value changes.
        let stored: Cow<'a, [u8]> = match intent {
            KvBatchIntent::Put | KvBatchIntent::Upsert => coerce_kv_body(value, declared)?,
            KvBatchIntent::Insert => {
                if prior.is_some() {
                    return Err(kv_duplicate_key(collection, key));
                }
                coerce_kv_body(value, declared)?
            }
            KvBatchIntent::InsertIfAbsent => {
                if prior.is_some() {
                    continue;
                }
                coerce_kv_body(value, declared)?
            }
            KvBatchIntent::Merge { updates, .. } => Cow::Owned(merge_kv_conflict_body(
                prior.as_deref(),
                value,
                updates,
                declared,
                crs,
            )?),
        };
        latest.insert(key, rows.len());
        rows.push(PlannedKvRow {
            index,
            key,
            prior,
            stored,
        });
    }
    Ok(rows)
}

/// The verb a `Merge` batch reports: `update` when every row replaced a
/// body, `insert` otherwise. A statement that both inserts and updates
/// reports `INSERT`, as the per-row tags fold.
pub(in crate::data::executor) fn merge_batch_verb(rows: &[PlannedKvRow<'_>]) -> &'static str {
    if !rows.is_empty() && rows.iter().all(PlannedKvRow::updated) {
        "update"
    } else {
        "insert"
    }
}
