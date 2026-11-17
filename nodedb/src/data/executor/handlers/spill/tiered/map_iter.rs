// SPDX-License-Identifier: BUSL-1.1

//! Merged scan over a tiered map's hot tier and runs.
//!
//! The scan yields each key once, with its newest version, in key-byte
//! order. It streams every run through one bounded buffer and holds one
//! record per run. Runs open on the first `next`, so building a scan does
//! no I/O. A scan with a start key reads one index page per run through the
//! map's block reader to find its start block.

use std::borrow::Cow;
use std::path::Path;

use super::map::{Hot, TieredKey, TieredMap, TieredValue, encode_key};
use crate::data::executor::handlers::spill::run::record::decode_value;
use crate::data::executor::handlers::spill::run::{BlockReader, RunRecord, RunStream, SortedRun};

/// Which keys a scan yields.
#[derive(Debug, Clone, Default)]
pub(in crate::data::executor) struct ScanBounds {
    /// Yield only keys whose bytes sort after these.
    pub after: Option<Vec<u8>>,
    /// Yield only keys whose bytes start with these.
    pub prefix: Option<Vec<u8>>,
}

impl ScanBounds {
    fn admits(&self, key: &[u8]) -> bool {
        self.after.as_deref().is_none_or(|after| key > after)
            && self
                .prefix
                .as_deref()
                .is_none_or(|prefix| key.starts_with(prefix))
    }

    /// Whether `key` and every later key fall outside the prefix.
    fn past_end(&self, key: &[u8]) -> bool {
        self.prefix
            .as_deref()
            .is_some_and(|prefix| !key.starts_with(prefix) && key > prefix)
    }

    fn start_key(&self) -> Option<&[u8]> {
        self.after.as_deref().or(self.prefix.as_deref())
    }
}

enum State {
    Fresh,
    Running,
    Done,
}

struct Cursor {
    stream: RunStream,
    head: Option<RunRecord>,
}

/// A merged scan. See the module doc.
pub(in crate::data::executor) struct TieredIter<'a, K, V> {
    hot: std::vec::IntoIter<(Vec<u8>, &'a K, &'a Hot<V>)>,
    hot_head: Option<(Vec<u8>, &'a K, &'a Hot<V>)>,
    runs: &'a [SortedRun],
    /// Reads the index page a start key needs. `None` for a scan with no
    /// start key.
    reader: Option<&'a BlockReader>,
    cursors: Vec<Cursor>,
    bounds: ScanBounds,
    state: State,
    /// A hot key that did not encode. The first `next` returns it.
    hot_error: Option<crate::Error>,
}

impl<K: TieredKey, V: TieredValue> TieredMap<K, V> {
    /// Scan every key within `bounds`. `reader` reads the index page each
    /// run needs to start at the bounds' start key.
    pub(in crate::data::executor) fn scan<'a>(
        &'a self,
        bounds: ScanBounds,
        reader: &'a BlockReader,
    ) -> TieredIter<'a, K, V> {
        self.iter_with(bounds, Some(reader))
    }

    /// Scan every key.
    pub(in crate::data::executor) fn iter(&self) -> TieredIter<'_, K, V> {
        self.iter_with(ScanBounds::default(), None)
    }

    fn iter_with<'a>(
        &'a self,
        bounds: ScanBounds,
        reader: Option<&'a BlockReader>,
    ) -> TieredIter<'a, K, V> {
        TieredIter {
            hot: Vec::new().into_iter(),
            hot_head: None,
            runs: &self.runs,
            reader,
            cursors: Vec::new(),
            bounds,
            state: State::Fresh,
            hot_error: None,
        }
        .with_hot(&self.hot)
    }
}

impl<'a, K: TieredKey, V: TieredValue> TieredIter<'a, K, V> {
    fn with_hot(mut self, hot: &'a hashbrown::HashMap<K, Hot<V>>) -> Self {
        let mut sorted = Vec::with_capacity(hot.len());
        for (key, version) in hot {
            match encode_key(key) {
                Ok(bytes) => {
                    if self.bounds.admits(&bytes) {
                        sorted.push((bytes, key, version));
                    }
                }
                Err(e) => {
                    self.hot_error = Some(e);
                    break;
                }
            }
        }
        sorted.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.hot = sorted.into_iter();
        self.hot_head = self.hot.next();
        self
    }

    fn open(&mut self) -> crate::Result<()> {
        if let Some(e) = self.hot_error.take() {
            return Err(e);
        }
        let start = self.bounds.start_key().map(<[u8]>::to_vec);
        for run in self.runs {
            // With no reader the stream starts at the run's first record,
            // and `first_admitted` skips the records before the bounds.
            let mut stream = match (start.as_deref(), self.reader) {
                (Some(key), Some(reader)) => RunStream::from_key(run, key, reader)?,
                _ => RunStream::from_start(run)?,
            };
            let head = Self::first_admitted(&mut stream, &self.bounds)?;
            self.cursors.push(Cursor { stream, head });
        }
        Ok(())
    }

    fn first_admitted(
        stream: &mut RunStream,
        bounds: &ScanBounds,
    ) -> crate::Result<Option<RunRecord>> {
        while let Some(record) = stream.next_record()? {
            if bounds.admits(&record.key) {
                return Ok(Some(record));
            }
            if bounds.past_end(&record.key) {
                return Ok(None);
            }
        }
        Ok(None)
    }

    fn step(&mut self) -> crate::Result<Option<(K, Cow<'a, V>)>> {
        let mut min: Option<Vec<u8>> = self.hot_head.as_ref().map(|(bytes, _, _)| bytes.clone());
        for cursor in &self.cursors {
            if let Some(record) = &cursor.head
                && min.as_ref().is_none_or(|m| record.key < *m)
            {
                min = Some(record.key.clone());
            }
        }
        let Some(min) = min else {
            return Ok(None);
        };
        if self.bounds.past_end(&min) {
            return Ok(None);
        }

        let item = match &self.hot_head {
            Some((bytes, key, version)) if *bytes == min => {
                let key: &'a K = key;
                let version: &'a Hot<V> = version;
                Some((key.clone(), Cow::Borrowed(&version.value)))
            }
            _ => None,
        };
        let item = match item {
            Some(item) => item,
            None => {
                let mut found = None;
                for (index, cursor) in self.cursors.iter().enumerate().rev() {
                    if let Some(record) = &cursor.head
                        && record.key == min
                    {
                        found = Some((index, record));
                        break;
                    }
                }
                let Some((index, record)) = found else {
                    return Ok(None);
                };
                let path = self.runs.get(index).map_or(Path::new(""), SortedRun::path);
                let key: K = decode_value(path, &record.key)?;
                let value: V = decode_value(path, &record.value)?;
                (key, Cow::Owned(value))
            }
        };

        if self
            .hot_head
            .as_ref()
            .is_some_and(|(bytes, _, _)| *bytes == min)
        {
            self.hot_head = self.hot.next();
        }
        for cursor in &mut self.cursors {
            while cursor.head.as_ref().is_some_and(|record| record.key == min) {
                cursor.head = cursor.stream.next_record()?;
            }
            if let Some(record) = &cursor.head
                && !self.bounds.admits(&record.key)
            {
                cursor.head = None;
            }
        }
        Ok(Some(item))
    }
}

impl<'a, K: TieredKey, V: TieredValue> Iterator for TieredIter<'a, K, V> {
    type Item = crate::Result<(K, Cow<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if matches!(self.state, State::Done) {
            return None;
        }
        if matches!(self.state, State::Fresh) {
            self.state = State::Running;
            if let Err(e) = self.open() {
                self.state = State::Done;
                return Some(Err(e));
            }
        }
        match self.step() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => {
                self.state = State::Done;
                None
            }
            Err(e) => {
                self.state = State::Done;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::space::SpillSpace;
    use super::*;

    #[test]
    fn scan_merges_tiers_and_newest_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mut space = SpillSpace::new(dir.path().join("t"), "t", 0);
        let mut map: TieredMap<u32, String> = TieredMap::default();
        for i in 0..100u32 {
            map.insert(i, format!("old{i}"), u64::from(i));
        }
        map.spill(&mut space, "v").unwrap();
        for i in (0..100u32).step_by(2) {
            map.insert(i, format!("mid{i}"), 100 + u64::from(i));
        }
        map.spill(&mut space, "v").unwrap();
        map.insert(4, "hot4".into(), 500);
        map.insert(1_000, "hot1000".into(), 501);

        let all: Vec<(u32, String)> = map
            .iter()
            .map(|item| item.map(|(k, v)| (k, v.into_owned())))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(all.len(), 101);
        let get = |k: u32| {
            all.iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get(4).unwrap(), "hot4");
        assert_eq!(get(6).unwrap(), "mid6");
        assert_eq!(get(7).unwrap(), "old7");
        assert_eq!(get(1_000).unwrap(), "hot1000");
    }

    #[test]
    fn scan_with_prefix_and_resume_bound() {
        let dir = tempfile::tempdir().unwrap();
        let mut space = SpillSpace::new(dir.path().join("t"), "t", 0);
        let mut map: TieredMap<(String, String, String), u32> = TieredMap::default();
        for (i, src) in ["a", "b", "c"].iter().enumerate() {
            for dst in ["x", "y"] {
                map.insert(
                    (src.to_string(), "l".to_string(), dst.to_string()),
                    i as u32,
                    i as u64,
                );
            }
        }
        map.spill(&mut space, "e").unwrap();
        map.insert(("b".into(), "l".into(), "z".into()), 9, 10);

        let mut prefix = vec![0x93];
        prefix.extend(encode_key(&"b".to_string()).unwrap());
        let b: Vec<_> = map
            .scan(
                ScanBounds {
                    after: None,
                    prefix: Some(prefix),
                },
                space.reader(),
            )
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(b.len(), 3);
        assert!(b.iter().all(|(k, _)| k.0 == "b"));

        let first = map.iter().next().unwrap().unwrap().0;
        let rest = map
            .scan(
                ScanBounds {
                    after: Some(encode_key(&first).unwrap()),
                    prefix: None,
                },
                space.reader(),
            )
            .count();
        assert_eq!(rest, 6);
    }
}
