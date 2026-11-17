// SPDX-License-Identifier: BUSL-1.1

//! A map with a hot tier in RAM and sorted spill runs on disk.
//!
//! Every version carries the `seq` of the write that produced it: the
//! overlay's journal position. The hot tier holds each key's newest
//! version. Each spill writes the hot tier as one sorted run and clears it.
//! For any key, versions grow newer from the oldest run to the newest run,
//! and the hot tier is newer than every run. A lookup probes the hot tier,
//! then the runs from newest to oldest, and the first version found wins.
//! A run whose key range excludes the key is skipped without a read. In any
//! other run the lookup reads one index page and one block, and decodes the
//! block only when the block's bloom admits the key.
//! A tombstone is a value like any other, so a tombstone hides every older
//! version.
//!
//! Keys order in a run by their MessagePack bytes. The order is stable and
//! groups every key that shares an encoded prefix.

use std::borrow::Cow;
use std::hash::Hash;
use std::path::Path;

use hashbrown::{Equivalent, HashMap};

use super::weight::{ENTRY_OVERHEAD, SpillWeight};
use crate::data::executor::handlers::spill::run::record::{decode_value, encode_value};
use crate::data::executor::handlers::spill::run::{BlockReader, SortedRun};

/// Key bound of a tiered map.
pub(in crate::data::executor) trait TieredKey:
    Eq + Hash + Clone + SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>
{
}

impl<T> TieredKey for T where
    T: Eq
        + Hash
        + Clone
        + SpillWeight
        + zerompk::ToMessagePack
        + for<'de> zerompk::FromMessagePack<'de>
{
}

/// Value bound of a tiered map.
pub(in crate::data::executor) trait TieredValue:
    Clone + SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>
{
}

impl<T> TieredValue for T where
    T: Clone + SpillWeight + zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>
{
}

/// A hot-tier version.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::data::executor) struct Hot<V> {
    pub value: V,
    pub seq: u64,
}

/// The path named in a key or value codec error of the hot tier.
pub(super) const HOT_TIER: &str = "hot-tier";

/// A tiered map. See the module doc.
pub(in crate::data::executor) struct TieredMap<K, V> {
    pub(super) hot: HashMap<K, Hot<V>>,
    pub(super) hot_bytes: usize,
    /// Oldest first.
    pub(super) runs: Vec<SortedRun>,
}

impl<K, V> Default for TieredMap<K, V> {
    fn default() -> Self {
        Self {
            hot: HashMap::new(),
            hot_bytes: 0,
            runs: Vec::new(),
        }
    }
}

impl<K, V> std::fmt::Debug for TieredMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredMap")
            .field("hot", &self.hot.len())
            .field("hot_bytes", &self.hot_bytes)
            .field("runs", &self.runs.len())
            .finish()
    }
}

/// Encode a key for a run or a lookup.
pub(in crate::data::executor) fn encode_key<K: zerompk::ToMessagePack>(
    key: &K,
) -> crate::Result<Vec<u8>> {
    Ok(encode_value(Path::new(HOT_TIER), key)?)
}

impl<K: TieredKey, V: TieredValue> TieredMap<K, V> {
    /// No version in any tier.
    pub(in crate::data::executor) fn is_empty(&self) -> bool {
        self.hot.is_empty() && self.runs.is_empty()
    }

    /// Whether any version lives in a run.
    pub(in crate::data::executor) fn has_runs(&self) -> bool {
        !self.runs.is_empty()
    }

    /// Weight of the hot tier.
    pub(in crate::data::executor) fn hot_bytes(&self) -> usize {
        self.hot_bytes
    }

    /// Bytes the runs keep in RAM: one summary per index page of each run.
    pub(in crate::data::executor) fn run_heap_bytes(&self) -> usize {
        self.runs.iter().map(SortedRun::resident_bytes).sum()
    }

    /// The hot version of `key`, a borrowed form of `K`.
    pub(in crate::data::executor) fn hot<Q>(&self, key: &Q) -> Option<&Hot<V>>
    where
        Q: ?Sized + Hash + Equivalent<K>,
    {
        self.hot.get(key)
    }

    /// Stage a version of `key` written at `seq`. Returns the hot version
    /// it replaced.
    pub(in crate::data::executor) fn insert(
        &mut self,
        key: K,
        value: V,
        seq: u64,
    ) -> Option<Hot<V>> {
        let key_weight = key.spill_weight() + ENTRY_OVERHEAD;
        self.hot_bytes += key_weight + value.spill_weight();
        let prev = self.hot.insert(key, Hot { value, seq });
        if let Some(prev) = &prev {
            self.hot_bytes = self
                .hot_bytes
                .saturating_sub(key_weight + prev.value.spill_weight());
        }
        prev
    }

    /// Remove the hot version of `key`.
    pub(in crate::data::executor) fn remove_hot(&mut self, key: &K) -> Option<Hot<V>> {
        let prev = self.hot.remove(key);
        if let Some(prev) = &prev {
            self.hot_bytes = self
                .hot_bytes
                .saturating_sub(key.spill_weight() + ENTRY_OVERHEAD + prev.value.spill_weight());
        }
        prev
    }

    /// Put back the hot version an undo entry recorded: `Some` restores it,
    /// `None` removes the hot version.
    pub(in crate::data::executor) fn restore(&mut self, key: K, prev: Option<(V, u64)>) {
        match prev {
            Some((value, seq)) => {
                self.insert(key, value, seq);
            }
            None => {
                self.remove_hot(&key);
            }
        }
    }

    /// The newest version of `key` in any tier.
    ///
    /// `key` is any borrowed form of `K` that hashes and compares like it
    /// and encodes to the same MessagePack bytes, so a lookup builds no
    /// owned key. The hot tier is probed by hash. Only when it misses and a
    /// run exists, the key is encoded once into the reader's reused buffer
    /// and the runs are probed with those bytes.
    pub(in crate::data::executor) fn get<'a, Q>(
        &'a self,
        key: &Q,
        reader: &BlockReader,
    ) -> crate::Result<Option<Cow<'a, V>>>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        for<'q> &'q Q: zerompk::ToMessagePack,
    {
        if let Some(hot) = self.hot.get(key) {
            return Ok(Some(Cow::Borrowed(&hot.value)));
        }
        if self.runs.is_empty() {
            return Ok(None);
        }
        let found = reader.with_encoded_key(&key, |key_bytes| {
            self.probe_runs(
                key_bytes,
                reader,
                |_| true,
                |run, value| decode_value(run.path(), value).map_err(crate::Error::from),
            )
        })?;
        Ok(found.map(Cow::Owned))
    }

    /// Whether any tier holds a version of `key`. `key` is a borrowed form
    /// of `K`, as [`get`](Self::get) takes.
    pub(in crate::data::executor) fn contains<Q>(
        &self,
        key: &Q,
        reader: &BlockReader,
    ) -> crate::Result<bool>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        for<'q> &'q Q: zerompk::ToMessagePack,
    {
        if self.hot.contains_key(key) {
            return Ok(true);
        }
        if self.runs.is_empty() {
            return Ok(false);
        }
        let found = reader.with_encoded_key(&key, |key_bytes| {
            self.probe_runs(key_bytes, reader, |_| true, |_, _| Ok(()))
        })?;
        Ok(found.is_some())
    }

    /// The newest version of `key` written before journal position `bound`:
    /// the key's value at a savepoint marker. `key` is a borrowed form of
    /// `K`, as [`get`](Self::get) takes.
    pub(in crate::data::executor) fn get_before<Q>(
        &self,
        key: &Q,
        bound: u64,
        reader: &BlockReader,
    ) -> crate::Result<Option<V>>
    where
        Q: ?Sized + Hash + Equivalent<K>,
        for<'q> &'q Q: zerompk::ToMessagePack,
    {
        if let Some(hot) = self.hot.get(key)
            && hot.seq < bound
        {
            return Ok(Some(hot.value.clone()));
        }
        if self.runs.is_empty() {
            return Ok(None);
        }
        reader.with_encoded_key(&key, |key_bytes| {
            self.probe_runs(
                key_bytes,
                reader,
                |seq| seq < bound,
                |run, value| decode_value(run.path(), value).map_err(crate::Error::from),
            )
        })
    }

    /// Probe the runs newest first for `key_bytes`. A run is skipped
    /// without a read when `admit` refuses its oldest version: every
    /// `admit` here is `seq < bound` or always true, so a refused oldest
    /// version means no version of the run passes. `take` receives the
    /// first admitted version's value bytes.
    fn probe_runs<T>(
        &self,
        key_bytes: &[u8],
        reader: &BlockReader,
        admit: impl Fn(u64) -> bool + Copy,
        take: impl Fn(&SortedRun, &[u8]) -> crate::Result<T>,
    ) -> crate::Result<Option<T>> {
        for run in self.runs.iter().rev() {
            if !admit(run.min_seq()) {
                continue;
            }
            if let Some(found) = run.find(key_bytes, reader, admit, |_, value| take(run, value))? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }
}
