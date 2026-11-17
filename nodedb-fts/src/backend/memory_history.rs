// SPDX-License-Identifier: Apache-2.0

//! The text history of [`MemoryBackend`].
//!
//! Ordered maps keep the range shapes a persistent backend uses: the live
//! starts by ordinal, and the ended versions by end ordinal. A read at a cut
//! ranges over the rows written after it.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_types::Surrogate;

use super::history::{FtsHistoryRead, FtsHistoryWrite};
use super::memory::{IndexKey, MemoryBackend, MemoryError, index_key};
use crate::history::{EndedKey, EndedRow, EndedVersion, LiveKey, LiveRow, TextCut};
use crate::scope::IndexScope;

/// `(index, sys_to, surrogate, sys_from)`: one ended version.
type EndedMapKey = (IndexKey, i64, Surrogate, i64);

/// The history rows of every index the backend holds.
#[derive(Debug, Default, Clone)]
pub(super) struct MemoryHistory {
    /// `(index, surrogate) → sys_from` of each live version with a start.
    live_from: BTreeMap<(IndexKey, Surrogate), i64>,
    /// `(index, sys_from, surrogate)`: the live starts by ordinal.
    live_by_from: BTreeSet<(IndexKey, i64, Surrogate)>,
    /// Ended versions to their analyzed tokens.
    ended: BTreeMap<EndedMapKey, Vec<String>>,
}

impl MemoryHistory {
    pub(super) fn estimated_clone_bytes(&self) -> usize {
        use crate::retained_size::{strings, sum, tree, vector};
        sum([
            tree::<((IndexKey, Surrogate), i64)>(self.live_from.len()),
            tree::<(IndexKey, i64, Surrogate)>(self.live_by_from.len()),
            tree::<(EndedMapKey, Vec<String>)>(self.ended.len()),
            sum(self
                .live_from
                .keys()
                .map(|(key, _)| strings([&key.2, &key.3]))),
            sum(self
                .live_by_from
                .iter()
                .map(|(key, _, _)| strings([&key.2, &key.3]))),
            sum(self.ended.iter().map(|((key, ..), tokens)| {
                sum([strings([&key.2, &key.3]), vector(tokens), strings(tokens)])
            })),
        ])
    }

    /// Keep only the rows of the indexes `keep` admits.
    pub(super) fn retain(&mut self, keep: impl Fn(&IndexKey) -> bool) {
        self.live_from.retain(|k, _| keep(&k.0));
        self.live_by_from.retain(|k| keep(&k.0));
        self.ended.retain(|k, _| keep(&k.0));
    }
}

fn scope_of(key: &IndexKey) -> IndexScope<'_> {
    IndexScope::from_key(&key.2, &key.3)
}

fn owned_index(database_id: u64, tid: u64, collection: &str, field_key: &str) -> IndexKey {
    (
        database_id,
        tid,
        collection.to_string(),
        field_key.to_string(),
    )
}

impl FtsHistoryRead for MemoryBackend {
    type Error = MemoryError;

    fn live_from(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<i64>, MemoryError> {
        let key = (index_key(database_id, tid, index), surrogate);
        Ok(self.history.borrow().live_from.get(&key).copied())
    }

    fn live_started_after(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> Result<Vec<Surrogate>, MemoryError> {
        let Some(from) = cut.first_after() else {
            return Ok(Vec::new());
        };
        let key = index_key(database_id, tid, index);
        let history = self.history.borrow();
        Ok(history
            .live_by_from
            .range((key.clone(), from, Surrogate::ZERO)..)
            .take_while(|(k, _, _)| *k == key)
            .map(|(_, _, surrogate)| *surrogate)
            .collect())
    }

    fn ended_seen_at(
        &self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        cut: TextCut,
    ) -> Result<Vec<EndedVersion>, MemoryError> {
        let Some(from) = cut.first_after() else {
            return Ok(Vec::new());
        };
        let key = index_key(database_id, tid, index);
        let history = self.history.borrow();
        Ok(history
            .ended
            .range((key.clone(), from, Surrogate::ZERO, i64::MIN)..)
            .take_while(|((k, ..), _)| *k == key)
            .filter(|((_, sys_to, _, sys_from), _)| cut.sees(*sys_from, *sys_to))
            .map(|((_, sys_to, surrogate, sys_from), tokens)| EndedVersion {
                surrogate: *surrogate,
                sys_from: *sys_from,
                sys_to: *sys_to,
                tokens: tokens.clone(),
            })
            .collect())
    }

    fn scan_ended(&self, visit: &mut dyn FnMut(EndedRow<'_>)) -> Result<(), MemoryError> {
        for (key, sys_to, surrogate, sys_from) in self.history.borrow().ended.keys() {
            visit(EndedRow {
                database_id: key.0,
                tid: key.1,
                index: scope_of(key),
                surrogate: *surrogate,
                sys_from: *sys_from,
                sys_to: *sys_to,
            });
        }
        Ok(())
    }

    fn scan_live(&self, visit: &mut dyn FnMut(LiveRow<'_>)) -> Result<(), MemoryError> {
        for (key, sys_from, surrogate) in &self.history.borrow().live_by_from {
            visit(LiveRow {
                database_id: key.0,
                tid: key.1,
                index: scope_of(key),
                surrogate: *surrogate,
                sys_from: *sys_from,
            });
        }
        Ok(())
    }
}

impl FtsHistoryWrite for MemoryBackend {
    fn set_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_from: i64,
    ) -> Result<(), MemoryError> {
        let key = index_key(database_id, tid, index);
        let history = self.history.get_mut();
        if let Some(prior) = history.live_from.insert((key.clone(), surrogate), sys_from) {
            history
                .live_by_from
                .remove(&(key.clone(), prior, surrogate));
        }
        history.live_by_from.insert((key, sys_from, surrogate));
        Ok(())
    }

    fn clear_live_from(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<i64>, MemoryError> {
        let key = index_key(database_id, tid, index);
        let history = self.history.get_mut();
        let prior = history.live_from.remove(&(key.clone(), surrogate));
        if let Some(sys_from) = prior {
            history.live_by_from.remove(&(key, sys_from, surrogate));
        }
        Ok(prior)
    }

    fn put_ended(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        version: &EndedVersion,
    ) -> Result<(), MemoryError> {
        let key = (
            index_key(database_id, tid, index),
            version.sys_to,
            version.surrogate,
            version.sys_from,
        );
        self.history
            .get_mut()
            .ended
            .insert(key, version.tokens.clone());
        Ok(())
    }

    fn take_ended_at(
        &mut self,
        database_id: u64,
        tid: u64,
        index: IndexScope<'_>,
        surrogate: Surrogate,
        sys_to: i64,
    ) -> Result<Option<EndedVersion>, MemoryError> {
        let key = index_key(database_id, tid, index);
        let history = self.history.get_mut();
        let found = history
            .ended
            .range((key.clone(), sys_to, surrogate, i64::MIN)..)
            .next()
            .map(|(k, _)| k.clone())
            .filter(|(k, to, s, _)| *k == key && *to == sys_to && *s == surrogate);
        let Some(found) = found else {
            return Ok(None);
        };
        Ok(history
            .ended
            .remove_entry(&found)
            .map(|((_, sys_to, surrogate, sys_from), tokens)| EndedVersion {
                surrogate,
                sys_from,
                sys_to,
                tokens,
            }))
    }

    fn remove_ended(&mut self, keys: &[EndedKey]) -> Result<(), MemoryError> {
        let history = self.history.get_mut();
        for k in keys {
            let index = owned_index(k.database_id, k.tid, &k.collection, &k.field_key);
            history
                .ended
                .remove(&(index, k.sys_to, k.surrogate, k.sys_from));
        }
        Ok(())
    }

    fn remove_live(&mut self, keys: &[LiveKey]) -> Result<(), MemoryError> {
        let history = self.history.get_mut();
        for k in keys {
            let index = owned_index(k.database_id, k.tid, &k.collection, &k.field_key);
            history
                .live_by_from
                .remove(&(index.clone(), k.sys_from, k.surrogate));
            history.live_from.remove(&(index, k.surrogate));
        }
        Ok(())
    }
}
