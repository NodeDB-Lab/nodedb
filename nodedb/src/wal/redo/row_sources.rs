// SPDX-License-Identifier: BUSL-1.1

//! Per-row provenance of a committed redo record.
//!
//! A redo record carries one event source, the source its transaction ran
//! with. A client statement and the BEFORE and SYNC AFTER bodies it fired
//! commit in one record, and the bodies' rows must fire no triggers. The
//! record therefore lists, per `(collection, source)` group, the rows whose
//! writes ran with a source other than the record's.
//!
//! A group names each row the way the row's event names it: a document or
//! CRDT row by its identity, a KV row by its key's text, a graph edge by its
//! `(src, label, dst)` row id, and a node-label delta by its node id on the
//! label stream. A group with no rows covers every row of its collection. The
//! Data-Plane resolve builds the groups from the overlay, which tags each
//! staged row only body writes staged, and names each collection only body
//! writes named. Array cells and the index and columnar engines emit no row
//! events, so no group names their rows.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::event::EventSource;

/// One `(collection, source)` group of a record's rows.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RedoRowSource {
    /// The collection, as the transaction overlay keys it.
    pub collection: String,
    /// The group's source, as [`EventSource::wal_code`] encodes it.
    pub event_source: u8,
    /// The rows of the group. Empty: every row of the collection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<String>,
}

/// Lookup over a record's row groups.
#[derive(Debug, Default)]
pub struct RowSourceIndex {
    rows: HashMap<(String, String), EventSource>,
    collections: HashMap<String, EventSource>,
}

impl RowSourceIndex {
    /// Index `groups`. A group whose code names no source is skipped: its rows
    /// keep the record's source.
    pub fn new(groups: &[RedoRowSource]) -> Self {
        let mut index = Self::default();
        for group in groups {
            let Some(source) = EventSource::from_wal_code(group.event_source) else {
                continue;
            };
            if group.rows.is_empty() {
                index.collections.insert(group.collection.clone(), source);
            } else {
                for row in &group.rows {
                    index
                        .rows
                        .insert((group.collection.clone(), row.clone()), source);
                }
            }
        }
        index
    }

    /// Whether the record lists no row under another source.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.collections.is_empty()
    }

    /// The source listed for `row` of `collection`: the row's own group, else
    /// its collection's whole-collection group.
    pub fn source_of(&self, collection: &str, row: &str) -> Option<EventSource> {
        if !self.rows.is_empty()
            && let Some(source) = self.rows.get(&(collection.to_owned(), row.to_owned()))
        {
            return Some(*source);
        }
        self.collections.get(collection).copied()
    }

    /// The source listed for every row of `collection`, when a
    /// whole-collection group covers it.
    pub fn collection_source(&self, collection: &str) -> Option<EventSource> {
        self.collections.get(collection).copied()
    }
}

/// Group `(collection, row)` pairs written under `source` into one
/// [`RedoRowSource`] per collection, in collection order.
pub fn group_rows(
    source: EventSource,
    rows: impl IntoIterator<Item = (String, String)>,
) -> Vec<RedoRowSource> {
    let mut by_collection: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for (collection, row) in rows {
        if seen.insert((collection.clone(), row.clone())) {
            by_collection.entry(collection).or_default().push(row);
        }
    }
    by_collection
        .into_iter()
        .map(|(collection, mut rows)| {
            rows.sort();
            RedoRowSource {
                collection,
                event_source: source.wal_code(),
                rows,
            }
        })
        .collect()
}

/// A whole-collection group for each of `collections`, under `source`.
pub fn whole_collections(
    source: EventSource,
    collections: impl IntoIterator<Item = String>,
) -> Vec<RedoRowSource> {
    let mut names: Vec<String> = collections.into_iter().collect();
    names.sort();
    names.dedup();
    names
        .into_iter()
        .map(|collection| RedoRowSource {
            collection,
            event_source: source.wal_code(),
            rows: Vec::new(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listed_row_takes_its_group_source_and_others_keep_none() {
        let mut groups = group_rows(
            EventSource::Trigger,
            [
                ("orders".to_string(), "o-body".to_string()),
                ("orders".to_string(), "o-body".to_string()),
            ],
        );
        groups.extend(whole_collections(
            EventSource::Trigger,
            ["audit".to_string()],
        ));
        let index = RowSourceIndex::new(&groups);

        assert_eq!(
            index.source_of("orders", "o-body"),
            Some(EventSource::Trigger)
        );
        assert_eq!(index.source_of("orders", "o-client"), None);
        assert_eq!(index.source_of("audit", "any"), Some(EventSource::Trigger));
        assert_eq!(groups[0].rows, vec!["o-body".to_string()]);
    }

    #[test]
    fn a_group_with_an_unknown_code_is_skipped() {
        let index = RowSourceIndex::new(&[RedoRowSource {
            collection: "c".into(),
            event_source: 0,
            rows: Vec::new(),
        }]);
        assert!(index.is_empty());
    }
}
