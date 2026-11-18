// SPDX-License-Identifier: BUSL-1.1

//! A record's row-source groups, held in tiered maps that spill.
//!
//! The lookup answers as `crate::wal::RowSourceIndex` answers: a row's own
//! group, else its collection's whole-collection group. A later group of
//! the same row or collection replaces an earlier one.

use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::handlers::spill::tiered::{SpillSpace, TieredMap};
use crate::event::EventSource;
use crate::wal::RedoRowSource;

/// The row-source groups of one record.
#[derive(Default)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct HeldRowSources {
    /// `(collection, row)` to the row's source, as its WAL code.
    rows: TieredMap<(String, String), u8>,
    /// Collection to the source of every row of it, as its WAL code.
    collections: TieredMap<String, u8>,
    /// The position the next group row takes.
    seq: u64,
}

impl HeldRowSources {
    /// Index `group`. A group whose code names no source is skipped: its
    /// rows keep the record's source.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn add(
        &mut self,
        group: RedoRowSource,
    ) {
        if EventSource::from_wal_code(group.event_source).is_none() {
            return;
        }
        if group.rows.is_empty() {
            let seq = self.next_seq();
            self.collections
                .insert(group.collection, group.event_source, seq);
            return;
        }
        for row in group.rows {
            let seq = self.next_seq();
            self.rows
                .insert((group.collection.clone(), row), group.event_source, seq);
        }
    }

    fn next_seq(&mut self) -> u64 {
        let seq = self.seq;
        self.seq += 1;
        seq
    }

    /// The source listed for `row` of `collection`: the row's own group,
    /// else its collection's whole-collection group.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn source_of(
        &self,
        collection: &str,
        row: &str,
        reader: &BlockReader,
    ) -> crate::Result<Option<EventSource>> {
        if !self.rows.is_empty() {
            let key = (collection.to_owned(), row.to_owned());
            if let Some(code) = self.rows.get(&key, reader)? {
                return Ok(EventSource::from_wal_code(*code));
            }
        }
        if self.collections.is_empty() {
            return Ok(None);
        }
        let key = collection.to_owned();
        Ok(self
            .collections
            .get(&key, reader)?
            .and_then(|code| EventSource::from_wal_code(*code)))
    }

    pub(super) fn hot_bytes(&self) -> usize {
        self.rows.hot_bytes() + self.collections.hot_bytes()
    }

    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        self.rows.spill(space, "row-sources")?;
        self.collections.spill(space, "collection-sources")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::RowSourceIndex;

    fn group(collection: &str, source: EventSource, rows: &[&str]) -> RedoRowSource {
        RedoRowSource {
            collection: collection.into(),
            event_source: source.wal_code(),
            rows: rows.iter().map(|row| row.to_string()).collect(),
        }
    }

    #[test]
    fn spilled_groups_answer_as_the_whole_record_index() {
        let groups = vec![
            group("a", EventSource::Trigger, &["r1", "r2"]),
            group("b", EventSource::Deferred, &[]),
            group("a", EventSource::User, &["r2"]),
            RedoRowSource {
                collection: "c".into(),
                event_source: 0,
                rows: Vec::new(),
            },
            group("b", EventSource::Trigger, &["r9"]),
        ];
        let whole = RowSourceIndex::new(&groups);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("rows"), "test", 0);
        let mut held = HeldRowSources::default();
        for (index, group) in groups.into_iter().enumerate() {
            held.add(group);
            if index % 2 == 0 {
                held.spill(&mut space).expect("spill");
            }
        }
        for collection in ["a", "b", "c", "d"] {
            for row in ["r1", "r2", "r9", "r0"] {
                assert_eq!(
                    held.source_of(collection, row, space.reader())
                        .expect("lookup"),
                    whole.source_of(collection, row),
                    "{collection}/{row}"
                );
            }
        }
    }

    #[test]
    fn a_row_source_key_round_trips() {
        let key = ("orders".to_string(), "o1".to_string());
        let bytes = zerompk::to_msgpack_vec(&key).expect("encode");
        let back: (String, String) = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, key);
    }
}
