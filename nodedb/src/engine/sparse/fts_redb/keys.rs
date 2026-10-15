// SPDX-License-Identifier: BUSL-1.1

//! The single place FTS table keys are formed.
//!
//! Per-index keys carry `(database_id, tenant_id, collection, field)`, where
//! `field` is the index's field key: empty for the whole-document index.
//! Scans over many indexes start at a [`KeyOwner`]'s lower bound and stop at
//! the first key it does not own, so no name needs an upper-bound sentinel.

use nodedb_fts::IndexScope;
use nodedb_types::Surrogate;

/// `(db, tid, collection, field, term, surrogate)`: POSTINGS. One row per
/// document per term. `term` is its own tuple component, so every row of
/// term `ab` sorts apart from every row of term `abc`.
pub(crate) type PostingKey<'a> = (u64, u64, &'a str, &'a str, &'a str, u32);
/// `(db, tid, collection, field, subkey | segment_id)`: INDEX_META, SEGMENTS.
pub(crate) type FieldStrKey<'a> = (u64, u64, &'a str, &'a str, &'a str);
/// `(db, tid, collection, field, surrogate)`: DOC_LENGTHS, DOC_TERMS.
pub(crate) type FieldDocKey<'a> = (u64, u64, &'a str, &'a str, u32);
/// `(db, tid, collection, field)`: STATS.
pub(crate) type StatsKey<'a> = (u64, u64, &'a str, &'a str);
/// `(db, tid, collection, surrogate)`: DOC_FIELDS.
pub(crate) type DocFieldsKey<'a> = (u64, u64, &'a str, u32);

/// POSTINGS key of the posting of `surrogate` under `term` in one index.
pub(crate) fn posting_key<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    term: &'a str,
    surrogate: Surrogate,
) -> PostingKey<'a> {
    (
        database_id,
        tid,
        index.collection(),
        index.field_key(),
        term,
        surrogate.as_u32(),
    )
}

/// The lowest and highest POSTINGS keys of `term` in one index. The range
/// between them, both ends included, holds exactly the term's posting list.
pub(crate) fn term_bounds<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    term: &'a str,
) -> (PostingKey<'a>, PostingKey<'a>) {
    (
        posting_key(database_id, tid, index, term, Surrogate::new(0)),
        posting_key(database_id, tid, index, term, Surrogate::new(u32::MAX)),
    )
}

/// INDEX_META / SEGMENTS key of `suffix` in one index.
fn field_str_key<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    suffix: &'a str,
) -> FieldStrKey<'a> {
    (
        database_id,
        tid,
        index.collection(),
        index.field_key(),
        suffix,
    )
}

/// INDEX_META key of `subkey` in one index.
pub(crate) fn meta_key<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    subkey: &'a str,
) -> FieldStrKey<'a> {
    field_str_key(database_id, tid, index, subkey)
}

/// SEGMENTS key of `segment_id` in one index.
pub(crate) fn segment_key<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    segment_id: &'a str,
) -> FieldStrKey<'a> {
    field_str_key(database_id, tid, index, segment_id)
}

/// DOC_LENGTHS / DOC_TERMS key of one document in one index.
pub(crate) fn doc_key<'a>(
    database_id: u64,
    tid: u64,
    index: IndexScope<'a>,
    surrogate: Surrogate,
) -> FieldDocKey<'a> {
    (
        database_id,
        tid,
        index.collection(),
        index.field_key(),
        surrogate.as_u32(),
    )
}

/// STATS key of one index.
pub(crate) fn stats_key<'a>(database_id: u64, tid: u64, index: IndexScope<'a>) -> StatsKey<'a> {
    (database_id, tid, index.collection(), index.field_key())
}

/// DOC_FIELDS key of one document.
pub(crate) fn doc_fields_key(
    database_id: u64,
    tid: u64,
    collection: &str,
    surrogate: Surrogate,
) -> DocFieldsKey<'_> {
    (database_id, tid, collection, surrogate.as_u32())
}

/// The set of keys a scan covers: one index, one collection, or one tenant.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyOwner<'a> {
    database_id: u64,
    tid: u64,
    collection: Option<&'a str>,
    field: Option<&'a str>,
}

impl<'a> KeyOwner<'a> {
    /// Every key of one index.
    pub(crate) fn index(database_id: u64, tid: u64, index: IndexScope<'a>) -> Self {
        Self {
            database_id,
            tid,
            collection: Some(index.collection()),
            field: Some(index.field_key()),
        }
    }

    /// Every key of every index of one collection.
    pub(crate) fn collection(database_id: u64, tid: u64, collection: &'a str) -> Self {
        Self {
            database_id,
            tid,
            collection: Some(collection),
            field: None,
        }
    }

    /// Every key of one `(database, tenant)`.
    pub(crate) fn tenant(database_id: u64, tid: u64) -> Self {
        Self {
            database_id,
            tid,
            collection: None,
            field: None,
        }
    }

    /// The database every owned key carries.
    pub(crate) fn database_id(&self) -> u64 {
        self.database_id
    }

    /// The tenant every owned key carries.
    pub(crate) fn tid(&self) -> u64 {
        self.tid
    }

    /// Whether a key with these leading components belongs to this owner.
    /// `field` is `None` for a table without a field component.
    pub(crate) fn owns(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: Option<&str>,
    ) -> bool {
        database_id == self.database_id
            && tid == self.tid
            && self.collection.is_none_or(|c| c == collection)
            && match (self.field, field) {
                (Some(own), Some(key)) => own == key,
                _ => true,
            }
    }

    fn collection_bound(&self) -> &'a str {
        self.collection.unwrap_or("")
    }

    fn field_bound(&self) -> &'a str {
        self.field.unwrap_or("")
    }

    /// Lowest POSTINGS key this owner covers.
    pub(crate) fn posting_start(&self) -> PostingKey<'a> {
        (
            self.database_id,
            self.tid,
            self.collection_bound(),
            self.field_bound(),
            "",
            0,
        )
    }

    /// Lowest INDEX_META / SEGMENTS key this owner covers.
    pub(crate) fn str_start(&self) -> FieldStrKey<'a> {
        (
            self.database_id,
            self.tid,
            self.collection_bound(),
            self.field_bound(),
            "",
        )
    }

    /// Lowest DOC_LENGTHS / DOC_TERMS key this owner covers.
    pub(crate) fn doc_start(&self) -> FieldDocKey<'a> {
        (
            self.database_id,
            self.tid,
            self.collection_bound(),
            self.field_bound(),
            0,
        )
    }

    /// Lowest STATS key this owner covers.
    pub(crate) fn stats_start(&self) -> StatsKey<'a> {
        (
            self.database_id,
            self.tid,
            self.collection_bound(),
            self.field_bound(),
        )
    }

    /// Lowest DOC_FIELDS key this owner covers.
    pub(crate) fn doc_fields_start(&self) -> DocFieldsKey<'a> {
        (self.database_id, self.tid, self.collection_bound(), 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_and_field_keys_differ_only_in_the_field_component() {
        let doc = IndexScope::document("c");
        let title = IndexScope::field("c", "title").unwrap();
        let s = Surrogate::new(7);
        assert_eq!(posting_key(1, 2, doc, "t", s), (1, 2, "c", "", "t", 7));
        assert_eq!(
            posting_key(1, 2, title, "t", s),
            (1, 2, "c", "title", "t", 7)
        );
        assert_eq!(meta_key(1, 2, title, "m"), (1, 2, "c", "title", "m"));
        assert_eq!(stats_key(1, 2, title), (1, 2, "c", "title"));
        assert_eq!(doc_key(1, 2, doc, Surrogate::new(7)), (1, 2, "c", "", 7));
    }

    #[test]
    fn owners_cover_exactly_their_keys() {
        let title = IndexScope::field("c", "title").unwrap();
        let index = KeyOwner::index(1, 2, title);
        assert!(index.owns(1, 2, "c", Some("title")));
        assert!(!index.owns(1, 2, "c", Some("")));
        assert!(!index.owns(1, 2, "c", Some("title2")));
        assert!(!index.owns(1, 2, "cc", Some("title")));

        let coll = KeyOwner::collection(1, 2, "c");
        assert!(coll.owns(1, 2, "c", Some("")));
        assert!(coll.owns(1, 2, "c", Some("\u{10ffff}field")));
        assert!(coll.owns(1, 2, "c", None));
        assert!(!coll.owns(1, 2, "c\0", Some("")));
        assert!(!coll.owns(1, 3, "c", Some("")));

        let tenant = KeyOwner::tenant(1, 2);
        assert!(tenant.owns(1, 2, "anything", Some("x")));
        assert!(!tenant.owns(2, 2, "anything", Some("x")));
    }

    #[test]
    fn owner_lower_bounds_sort_before_every_owned_key() {
        let coll = KeyOwner::collection(1, 2, "c");
        assert!(coll.posting_start() <= (1, 2, "c", "", "", 0));
        assert!(coll.str_start() <= (1, 2, "c", "", ""));
        assert!(coll.doc_start() <= (1, 2, "c", "", 0));
        assert!(coll.stats_start() <= (1, 2, "c", ""));
        assert!(coll.doc_fields_start() <= (1, 2, "c", 0));
        let tenant = KeyOwner::tenant(1, 2);
        assert!(tenant.str_start() <= (1, 2, "", "", ""));
    }
}
