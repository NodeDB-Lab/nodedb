// SPDX-License-Identifier: BUSL-1.1

//! UNIQUE of one collection, judged on the claims and last writes a
//! streamed validate folded.
//!
//! `unique::check_unique_post_state` walks the record's last writes in
//! record order and each row's values in set order. Per unique path, in
//! config order, it refuses:
//!
//! 1. the first value a second live row claims, at that row's position;
//! 2. else the first claimed value a stored row holds that is not the
//!    claim's owner and that the record does not write.
//!
//! A claim is live when its op is the last write of its row. The claims sit
//! in a tiered map keyed `(collection, path, value, op)`, so a scan by
//! `(collection, path)` yields each value's claims together, in op order.
//! The judge keeps the smallest `(op, value)` position of each kind, so it
//! names the value the whole-record walk names.

use crate::data::executor::enforcement::unique::violation;
use crate::engine::document::store::{DocumentEngine, IndexPath};

use super::post_state::{KeyPrefix, PostState};

/// The collection a UNIQUE finalize judges, and the stored index its claims
/// meet.
pub(super) struct UniqueFinal<'a> {
    pub(super) collection: &'a str,
    pub(super) paths: &'a [IndexPath],
    pub(super) doc_engine: DocumentEngine<'a>,
}

impl PostState {
    /// The UNIQUE refusal of the collection `scope` names. `Ok(Some)` is
    /// the verdict: a violation, or a stored index that did not read, as
    /// the whole-record check reports it. `Err` when a spilled run does
    /// not read: a failure of this node.
    pub(super) fn unique_violation(
        &self,
        scope: &UniqueFinal<'_>,
    ) -> crate::Result<Option<crate::Error>> {
        for (position, path) in (0u32..).zip(scope.paths) {
            if !path.unique {
                continue;
            }
            if let Some(value) = self.second_live_owner(scope.collection, position)? {
                return Ok(Some(violation(scope.collection, path, &value)));
            }
            if let Some(refusal) = self.foreign_stored_owner(scope, position, path)? {
                return Ok(Some(refusal));
            }
        }
        Ok(None)
    }

    /// Whether op `op` is the last write of row `surrogate`.
    fn is_last_write(&self, collection: &str, surrogate: u32, op: u64) -> crate::Result<bool> {
        let last = self
            .written
            .get(&(collection.to_string(), surrogate), self.reader())?;
        Ok(last.is_some_and(|last| *last == op))
    }

    /// The value a second live row claims under the path at `position`,
    /// the first by the position of that row's claim.
    fn second_live_owner(&self, collection: &str, position: u32) -> crate::Result<Option<String>> {
        let bounds = KeyPrefix::tuple(4)
            .then(&collection)?
            .then(&position)?
            .bounds();
        let mut group: Option<(String, u32)> = None;
        let mut first: Option<(u64, String)> = None;
        for item in self.claims.scan(bounds, self.reader()) {
            let ((_, _, value, op), surrogate) = item?;
            if !self.is_last_write(collection, *surrogate, op)? {
                continue;
            }
            let live = match &mut group {
                Some((held, count)) if *held == value => {
                    *count += 1;
                    *count
                }
                _ => {
                    group = Some((value.clone(), 1));
                    1
                }
            };
            if live == 2
                && first
                    .as_ref()
                    .is_none_or(|(kept_op, kept)| (op, &value) < (*kept_op, kept))
            {
                first = Some((op, value));
            }
        }
        Ok(first.map(|(_, value)| value))
    }

    /// The first claim, by its owner's position, whose value a stored row
    /// holds that is not the owner and that the record does not write. Every
    /// value has one live owner here: [`Self::second_live_owner`] found
    /// none with two.
    fn foreign_stored_owner(
        &self,
        scope: &UniqueFinal<'_>,
        position: u32,
        path: &IndexPath,
    ) -> crate::Result<Option<crate::Error>> {
        let collection = scope.collection;
        let bounds = KeyPrefix::tuple(4)
            .then(&collection)?
            .then(&position)?
            .bounds();
        let mut first: Option<(u64, String, crate::Error)> = None;
        for item in self.claims.scan(bounds, self.reader()) {
            let ((_, _, value, op), owner) = item?;
            let owner = *owner;
            if first
                .as_ref()
                .is_some_and(|(kept_op, kept, _)| (op, &value) >= (*kept_op, kept))
            {
                continue;
            }
            if !self.is_last_write(collection, owner, op)? {
                continue;
            }
            let refusal = match scope
                .doc_engine
                .index_lookup(collection, &path.path, &value, None)
            {
                Err(error) => Some(error),
                Ok(stored) => {
                    let mut foreign = false;
                    for key in &stored {
                        let holder = key.surrogate().as_u32();
                        if holder != owner
                            && !self
                                .written
                                .contains(&(collection.to_string(), holder), self.reader())?
                        {
                            foreign = true;
                            break;
                        }
                    }
                    foreign.then(|| violation(collection, path, &value))
                }
            };
            if let Some(refusal) = refusal {
                first = Some((op, value, refusal));
            }
        }
        Ok(first.map(|(_, _, refusal)| refusal))
    }
}
