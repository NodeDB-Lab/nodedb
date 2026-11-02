// SPDX-License-Identifier: Apache-2.0

//! Per-key chains of the row locations of ended versions.

use std::collections::HashMap;

use crate::pk_index::RowLocation;

/// Primary key to the locations of its ended versions, oldest first.
///
/// The PK index binds a key's open version. The chain holds every version a
/// write ended and a purge has not removed, so a read at an ordinal finds an
/// older version of one key without a scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionChains {
    inner: HashMap<Vec<u8>, Vec<RowLocation>>,
}

impl VersionChains {
    /// Chains with no key.
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild from `(key, locations oldest first)` entries.
    pub fn from_entries(entries: impl IntoIterator<Item = (Vec<u8>, Vec<RowLocation>)>) -> Self {
        Self {
            inner: entries
                .into_iter()
                .filter(|(_, locs)| !locs.is_empty())
                .collect(),
        }
    }

    /// Every chain as `(key, locations oldest first)`, ascending by key.
    pub fn to_entries(&self) -> Vec<(Vec<u8>, Vec<RowLocation>)> {
        let mut entries: Vec<_> = self
            .inner
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Number of keys with a chain.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no key has a chain.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append `loc` as the newest ended version of `pk`.
    pub fn push(&mut self, pk: Vec<u8>, loc: RowLocation) {
        self.inner.entry(pk).or_default().push(loc);
    }

    /// The ended versions of `pk`, oldest first.
    pub fn get(&self, pk: &[u8]) -> &[RowLocation] {
        self.inner.get(pk).map(Vec::as_slice).unwrap_or_default()
    }

    /// The newest ended version of `pk`.
    pub fn last(&self, pk: &[u8]) -> Option<RowLocation> {
        self.inner.get(pk).and_then(|locs| locs.last().copied())
    }

    /// Remove `loc` from the chain of `pk`. Returns whether it was there.
    pub fn remove(&mut self, pk: &[u8], loc: RowLocation) -> bool {
        let Some(locs) = self.inner.get_mut(pk) else {
            return false;
        };
        let Some(pos) = locs.iter().rposition(|l| *l == loc) else {
            return false;
        };
        locs.remove(pos);
        if locs.is_empty() {
            self.inner.remove(pk);
        }
        true
    }

    /// Move every location in `segment_id` through `remap`. A `None` drops
    /// the location.
    pub fn remap_segment(&mut self, segment_id: u64, remap: impl Fn(u32) -> Option<RowLocation>) {
        self.inner.retain(|_, locs| {
            locs.retain_mut(|loc| {
                if loc.segment_id != segment_id {
                    return true;
                }
                match remap(loc.row_index) {
                    Some(new_loc) => {
                        *loc = new_loc;
                        true
                    }
                    None => false,
                }
            });
            !locs.is_empty()
        });
    }

    /// Keep only the locations `keep` admits. A key left with none loses
    /// its chain.
    pub fn retain(&mut self, mut keep: impl FnMut(&RowLocation) -> bool) {
        self.inner.retain(|_, locs| {
            locs.retain(&mut keep);
            !locs.is_empty()
        });
    }

    /// Total number of chained locations.
    pub fn location_count(&self) -> usize {
        self.inner.values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(segment_id: u64, row_index: u32) -> RowLocation {
        RowLocation {
            segment_id,
            row_index,
        }
    }

    #[test]
    fn remap_moves_and_drops_locations() {
        let mut c = VersionChains::new();
        c.push(b"a".to_vec(), loc(0, 0));
        c.push(b"a".to_vec(), loc(0, 5));
        c.push(b"b".to_vec(), loc(1, 2));
        c.remap_segment(0, |row| (row < 3).then_some(loc(4, row)));
        assert_eq!(c.get(b"a"), &[loc(4, 0)]);
        assert_eq!(c.get(b"b"), &[loc(1, 2)]);
        assert!(c.remove(b"a", loc(4, 0)));
        assert!(c.get(b"a").is_empty());
        assert_eq!(c.len(), 1);
    }
}
