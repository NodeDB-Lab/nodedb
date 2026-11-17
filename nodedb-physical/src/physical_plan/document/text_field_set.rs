// SPDX-License-Identifier: Apache-2.0

//! The fields a collection's full-text inverted index writes and the fields
//! it serves reads from, with the analyzer and fuzzy default it applies.
//!
//! A write indexes every field a full-text index declares, building or
//! ready, so a build catches every write that lands during it. A read runs
//! only against a field a ready index declares. A building index holds a
//! partial view of the collection, so a read of it refuses.

use nodedb_types::text_search::TextColumnFault;

/// The indexed and readable full-text fields of one collection.
///
/// Both lists are sorted and deduped. Every readable field is indexed.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TextFieldSet {
    /// Every field a building or ready full-text index declares. A write
    /// indexes these.
    indexed: Vec<String>,
    /// Every field a ready full-text index declares. A read runs only
    /// against these.
    readable: Vec<String>,
    /// The analyzer the collection's full-text indexes name. `None` means
    /// the standard analyzer. Every write and every query tokenizes with it.
    #[serde(default)]
    analyzer: Option<String>,
    /// Whether a search of the collection falls back to fuzzy matching by
    /// default.
    #[serde(default)]
    fuzzy_default: bool,
}

impl TextFieldSet {
    /// The set whose indexed fields are `indexed` and `readable` together,
    /// and whose readable fields are `readable`.
    pub fn new(indexed: Vec<String>, readable: Vec<String>) -> Self {
        let mut readable = readable;
        readable.sort();
        readable.dedup();
        let mut indexed = indexed;
        indexed.extend(readable.iter().cloned());
        indexed.sort();
        indexed.dedup();
        Self {
            indexed,
            readable,
            analyzer: None,
            fuzzy_default: false,
        }
    }

    /// This set with the analyzer `analyzer` and the fuzzy default
    /// `fuzzy_default`.
    pub fn with_analysis(mut self, analyzer: Option<String>, fuzzy_default: bool) -> Self {
        self.analyzer = analyzer;
        self.fuzzy_default = fuzzy_default;
        self
    }

    /// The analyzer the collection's full-text indexes name. `None` means
    /// the standard analyzer.
    pub fn analyzer(&self) -> Option<&str> {
        self.analyzer.as_deref()
    }

    /// Whether a search falls back to fuzzy matching by default.
    pub fn fuzzy_default(&self) -> bool {
        self.fuzzy_default
    }

    /// The set where every field of `fields` is indexed and readable. A
    /// store with no index build states declares its fields this way.
    pub fn all_readable(fields: Vec<String>) -> Self {
        Self::new(Vec::new(), fields)
    }

    /// The fields a write indexes, sorted and deduped.
    pub fn indexed(&self) -> &[String] {
        &self.indexed
    }

    /// The fields a read runs against, sorted and deduped.
    pub fn readable(&self) -> &[String] {
        &self.readable
    }

    /// Whether no full-text index declares any field. A write then touches
    /// no inverted-index state.
    pub fn is_empty(&self) -> bool {
        self.indexed.is_empty()
    }

    /// Replace the indexed fields with `indexed`. A readable field that
    /// `indexed` drops stops being readable.
    pub fn replace_indexed(&mut self, indexed: Vec<String>) {
        let mut indexed = indexed;
        indexed.sort();
        indexed.dedup();
        self.readable
            .retain(|field| indexed.binary_search(field).is_ok());
        self.indexed = indexed;
    }

    /// Why a full-text read of `field` cannot run, or `None` when a ready
    /// index serves it. A `None` field reads the whole document, which any
    /// ready index serves.
    pub fn read_fault(&self, field: Option<&str>) -> Option<TextColumnFault> {
        let (readable, indexed) = match field {
            None => (!self.readable.is_empty(), !self.indexed.is_empty()),
            Some(name) => (
                contains(&self.readable, name),
                contains(&self.indexed, name),
            ),
        };
        if readable {
            None
        } else if indexed {
            Some(TextColumnFault::IndexBuilding)
        } else {
            Some(TextColumnFault::NotIndexed)
        }
    }
}

/// Whether the sorted list `fields` holds `name`.
fn contains(fields: &[String], name: &str) -> bool {
    fields.binary_search_by(|f| f.as_str().cmp(name)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(fields: &[&str]) -> Vec<String> {
        fields.iter().map(|f| f.to_string()).collect()
    }

    #[test]
    fn readable_fields_are_indexed_and_both_lists_sort() {
        let set = TextFieldSet::new(
            names(&["title", "body", "title"]),
            names(&["notes", "body"]),
        );
        assert_eq!(set.indexed(), names(&["body", "notes", "title"]).as_slice());
        assert_eq!(set.readable(), names(&["body", "notes"]).as_slice());
    }

    #[test]
    fn a_read_fault_follows_the_field_state() {
        let set = TextFieldSet::new(names(&["title"]), names(&["body"]));
        assert_eq!(set.read_fault(Some("body")), None);
        assert_eq!(
            set.read_fault(Some("title")),
            Some(TextColumnFault::IndexBuilding)
        );
        assert_eq!(
            set.read_fault(Some("other")),
            Some(TextColumnFault::NotIndexed)
        );
        assert_eq!(set.read_fault(None), None);
    }

    #[test]
    fn the_whole_document_needs_a_readable_field() {
        let building = TextFieldSet::new(names(&["title"]), Vec::new());
        assert_eq!(
            building.read_fault(None),
            Some(TextColumnFault::IndexBuilding)
        );
        assert_eq!(
            TextFieldSet::default().read_fault(None),
            Some(TextColumnFault::NotIndexed)
        );
    }

    #[test]
    fn replacing_the_indexed_fields_keeps_only_their_readable_fields() {
        let mut set = TextFieldSet::all_readable(names(&["body", "title"]));
        set.replace_indexed(names(&["title", "abstract"]));
        assert_eq!(set.indexed(), names(&["abstract", "title"]).as_slice());
        assert_eq!(set.readable(), names(&["title"]).as_slice());
    }
}
