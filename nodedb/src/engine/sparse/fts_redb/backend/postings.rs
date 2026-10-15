// SPDX-License-Identifier: BUSL-1.1

//! Posting-list operations against the `POSTINGS` table, each in its own
//! transaction. Rows are keyed by
//! `(database_id, tenant_id, collection, field, term, surrogate)`: one row
//! per document per term (see the `posting_rows` sibling module).

use nodedb_fts::IndexScope;
use nodedb_fts::posting::Posting;
use nodedb_types::Surrogate;
use redb::ReadableDatabase;

use super::core::RedbFtsBackend;
use super::posting_rows;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::keys::KeyOwner;
use crate::engine::sparse::fts_redb::scan;
use crate::engine::sparse::fts_redb::tables::POSTINGS;

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<Vec<Posting>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    posting_rows::read_list(&table, database_id, tid, index, term)
}

/// The number of documents holding a posting under `term`.
pub(super) fn doc_count(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<usize> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    posting_rows::count(&table, database_id, tid, index, term)
}

/// Run `apply` on the POSTINGS table in one committed write transaction.
fn with_postings<R>(
    backend: &RedbFtsBackend,
    apply: impl FnOnce(&mut posting_rows::PostingTable<'_>) -> crate::Result<R>,
) -> crate::Result<R> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    let out = {
        let mut table = write_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        apply(&mut table)?
    };
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(out)
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    postings: &[Posting],
) -> crate::Result<()> {
    with_postings(backend, |table| {
        posting_rows::replace_list(table, database_id, tid, index, term, postings)
    })
}

pub(super) fn remove(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
) -> crate::Result<()> {
    with_postings(backend, |table| {
        posting_rows::remove_list(table, database_id, tid, index, term).map(|_| ())
    })
}

pub(super) fn upsert_one(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    posting: &Posting,
) -> crate::Result<()> {
    with_postings(backend, |table| {
        posting_rows::put(table, database_id, tid, index, term, posting)
    })
}

pub(super) fn remove_one(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    term: &str,
    doc_id: Surrogate,
) -> crate::Result<()> {
    with_postings(backend, |table| {
        posting_rows::remove(table, database_id, tid, index, term, doc_id).map(|_| ())
    })
}

pub(super) fn collection_terms(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
) -> crate::Result<Vec<String>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(POSTINGS)
        .map_err(|e| redb_err("open postings", e))?;
    let terms = scan::posting_terms(&table, KeyOwner::index(database_id, tid, index))
        .map_err(|e| redb_err("postings range", e))?;
    Ok(terms.into_iter().map(|(_, term)| term).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_fts::backend::FtsBackend;

    use super::*;
    use crate::engine::durability_gate::GatedDatabase;

    const DB: u64 = 0;
    const T: u64 = 1;
    const DOCS: IndexScope<'static> = IndexScope::document("docs");

    fn open_temp() -> (RedbFtsBackend, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("postings.redb")).unwrap();
        let backend = RedbFtsBackend::open(Arc::new(GatedDatabase::new(db))).unwrap();
        (backend, dir)
    }

    fn posting(doc: u32, freq: u32) -> Posting {
        Posting {
            doc_id: Surrogate::new(doc),
            term_freq: freq,
            positions: (0..freq).collect(),
        }
    }

    fn ids(list: &[Posting]) -> Vec<u32> {
        list.iter().map(|p| p.doc_id.as_u32()).collect()
    }

    /// Every POSTINGS row of the database, as raw keys.
    fn all_rows(backend: &RedbFtsBackend) -> Vec<(String, u32)> {
        let txn = backend.db.begin_read().unwrap();
        let table = txn.open_table(POSTINGS).unwrap();
        let rows = scan::posting_keys(&table, KeyOwner::tenant(DB, T)).unwrap();
        rows.into_iter().map(|(_, _, term, s)| (term, s)).collect()
    }

    /// Adding a document to a term that already holds N postings writes one
    /// row and leaves every other row's bytes untouched, whatever N is.
    #[test]
    fn upsert_into_a_long_list_touches_one_row() {
        let (backend, _dir) = open_temp();
        for n in 1..=200u32 {
            backend
                .upsert_posting(DB, T, DOCS, "common", &posting(n, 1))
                .unwrap();
        }
        let before = {
            let txn = backend.db.begin_read().unwrap();
            let table = txn.open_table(POSTINGS).unwrap();
            scan::posting_rows(&table, DB, T, "docs").unwrap()
        };
        assert_eq!(before.len(), 200, "one row per document");

        backend
            .upsert_posting(DB, T, DOCS, "common", &posting(201, 1))
            .unwrap();

        let after = {
            let txn = backend.db.begin_read().unwrap();
            let table = txn.open_table(POSTINGS).unwrap();
            scan::posting_rows(&table, DB, T, "docs").unwrap()
        };
        assert_eq!(after.len(), 201);
        for (old, new) in before.iter().zip(&after) {
            assert_eq!(
                (old.surrogate, &old.value),
                (new.surrogate, &new.value),
                "an existing row changed"
            );
        }
        let added = &after[200];
        assert_eq!(added.surrogate, 201);
        assert_eq!(
            added.value,
            posting_rows::encode(&posting(201, 1)).unwrap(),
            "the new row holds exactly one posting"
        );
        assert_eq!(
            backend.read_postings(DB, T, DOCS, "common").unwrap().len(),
            201
        );
    }

    #[test]
    fn upsert_replaces_and_remove_drops_one_document() {
        let (backend, _dir) = open_temp();
        for doc in [3, 1, 2] {
            backend
                .upsert_posting(DB, T, DOCS, "t", &posting(doc, 1))
                .unwrap();
        }
        backend
            .upsert_posting(DB, T, DOCS, "t", &posting(2, 3))
            .unwrap();
        let list = backend.read_postings(DB, T, DOCS, "t").unwrap();
        assert_eq!(ids(&list), vec![1, 2, 3], "surrogate order");
        assert_eq!(list[1].term_freq, 3);
        assert_eq!(list[1].positions, vec![0, 1, 2]);

        backend
            .remove_posting(DB, T, DOCS, "t", Surrogate::new(2))
            .unwrap();
        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "t").unwrap()),
            vec![1, 3]
        );
        assert_eq!(doc_count(&backend, DB, T, DOCS, "t").unwrap(), 2);
    }

    #[test]
    fn write_postings_replaces_the_whole_list() {
        let (backend, _dir) = open_temp();
        backend
            .write_postings(DB, T, DOCS, "t", &[posting(1, 1), posting(2, 1)])
            .unwrap();
        backend
            .write_postings(DB, T, DOCS, "t", &[posting(2, 2), posting(5, 1)])
            .unwrap();
        let list = backend.read_postings(DB, T, DOCS, "t").unwrap();
        assert_eq!(ids(&list), vec![2, 5], "stale document 1 is gone");
        assert_eq!(list[0].term_freq, 2);

        backend.write_postings(DB, T, DOCS, "t", &[]).unwrap();
        assert!(all_rows(&backend).is_empty());
    }

    /// Terms that share a prefix keep separate lists through write, read,
    /// remove, term listing, and purge.
    #[test]
    fn terms_sharing_a_prefix_stay_separate() {
        let (backend, _dir) = open_temp();
        backend
            .upsert_posting(DB, T, DOCS, "ab", &posting(1, 1))
            .unwrap();
        backend
            .upsert_posting(DB, T, DOCS, "abc", &posting(1, 2))
            .unwrap();
        backend
            .upsert_posting(DB, T, DOCS, "abc", &posting(2, 1))
            .unwrap();
        backend
            .upsert_posting(DB, T, DOCS, "a", &posting(u32::MAX, 1))
            .unwrap();

        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "ab").unwrap()),
            vec![1]
        );
        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "abc").unwrap()),
            vec![1, 2]
        );
        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "a").unwrap()),
            vec![u32::MAX]
        );
        assert_eq!(
            backend.collection_terms(DB, T, DOCS).unwrap(),
            vec!["a", "ab", "abc"]
        );

        backend.remove_postings(DB, T, DOCS, "ab").unwrap();
        assert!(backend.read_postings(DB, T, DOCS, "ab").unwrap().is_empty());
        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "abc").unwrap()),
            vec![1, 2]
        );
        backend
            .remove_posting(DB, T, DOCS, "abc", Surrogate::new(1))
            .unwrap();
        assert_eq!(
            ids(&backend.read_postings(DB, T, DOCS, "a").unwrap()),
            vec![u32::MAX]
        );
        assert_eq!(
            backend.collection_terms(DB, T, DOCS).unwrap(),
            vec!["a", "abc"]
        );

        let title = IndexScope::field("docs", "title").unwrap();
        backend
            .upsert_posting(DB, T, title, "ab", &posting(7, 1))
            .unwrap();
        backend
            .upsert_posting(DB, T, IndexScope::document("docs2"), "ab", &posting(8, 1))
            .unwrap();
        assert_eq!(backend.collection_terms(DB, T, title).unwrap(), vec!["ab"]);

        let removed = backend.purge_collection(DB, T, "docs").unwrap();
        assert_eq!(removed, 3, "the three posting rows of `docs`");
        assert_eq!(all_rows(&backend), vec![("ab".to_string(), 8)]);
    }
}
