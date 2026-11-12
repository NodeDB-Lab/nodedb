// SPDX-License-Identifier: BUSL-1.1

//! Read-side views over a [`TxnOverlay`]: the staged entries of one
//! collection, and the overlay's size.
//!
//! Each scan merges the hot tier with every spilled run and yields each row
//! once, with its newest staged version, in key-byte order. A scan reads the
//! runs through bounded buffers.

use std::borrow::Cow;

use nodedb_types::RowIdentity;

use super::staged::{CollKey, Staged, TxnOverlay};

impl TxnOverlay {
    /// Every staged `(surrogate, Staged)` pair of a collection, in ascending
    /// surrogate order: a `u32`'s minimal MessagePack encoding sorts as the
    /// number does. Yields nothing if the collection has no overlay entries.
    pub fn iter_for_collection<'a>(
        &'a self,
        coll_key: &CollKey,
    ) -> impl Iterator<Item = crate::Result<(u32, Cow<'a, Staged>)>> + 'a {
        let poisoned = self.guard().err();
        let scan = match poisoned {
            Some(_) => None,
            None => self
                .collections
                .get(coll_key)
                .map(|overlay| overlay.by_surrogate.iter()),
        };
        poisoned
            .map(Err)
            .into_iter()
            .chain(scan.into_iter().flatten())
    }

    /// Every staged `(identity, Staged)` pair of a collection.
    ///
    /// Unlike [`iter_for_collection`](Self::iter_for_collection) (keyed by
    /// surrogate, the Document scan's row identity), this is keyed by the
    /// row's client identity -- the identity a KV scan merge needs, since a
    /// KV row's scan identity is its raw key bytes, not a surrogate.
    pub fn iter_doc_entries_for_collection<'a>(
        &'a self,
        coll_key: &CollKey,
    ) -> impl Iterator<Item = crate::Result<(RowIdentity, Cow<'a, Staged>)>> + 'a {
        self.iter_staged_bindings_for_collection(coll_key)
            .map(|item| item.map(|(doc_id, _, staged)| (doc_id, staged)))
    }

    /// Every staged row of a collection by client identity, with the
    /// surrogate its binding names and its staged mutation. A binding whose
    /// surrogate holds no staged value (a TTL-only row) is left out.
    pub fn iter_staged_bindings_for_collection<'a>(
        &'a self,
        coll_key: &CollKey,
    ) -> impl Iterator<Item = crate::Result<(RowIdentity, u32, Cow<'a, Staged>)>> + 'a {
        let poisoned = self.guard().err();
        let overlay = match poisoned {
            Some(_) => None,
            None => self.collections.get(coll_key),
        };
        let reader = self.space.reader();
        let entries = overlay.into_iter().flat_map(move |overlay| {
            overlay
                .doc_id_to_surrogate
                .iter()
                .filter_map(move |binding| match binding {
                    Err(e) => Some(Err(e)),
                    Ok((doc_id, surrogate)) => {
                        let surrogate = surrogate.into_owned();
                        match overlay.by_surrogate.get(&surrogate, reader) {
                            Ok(Some(staged)) => Some(Ok((doc_id, surrogate, staged))),
                            Ok(None) => None,
                            Err(e) => Some(Err(e)),
                        }
                    }
                })
        });
        poisoned.map(Err).into_iter().chain(entries)
    }

    /// True if no collection has any staged mutation or truncate marker.
    pub fn is_empty(&self) -> bool {
        self.truncated.is_empty()
            && self
                .collections
                .values()
                .all(|overlay| overlay.by_surrogate.is_empty())
    }

    /// Total number of staged rows across all collections. Counts each row
    /// once across the hot tier and the runs.
    pub fn len(&self) -> crate::Result<usize> {
        self.guard()?;
        let mut count = 0usize;
        for overlay in self.collections.values() {
            for item in overlay.by_surrogate.iter() {
                item?;
                count += 1;
            }
        }
        Ok(count)
    }
}
