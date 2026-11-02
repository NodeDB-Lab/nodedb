// SPDX-License-Identifier: BUSL-1.1

//! Current-state scans over the versioned document and index tables.
//!
//! Every scan yields one row per document: its current version. Rows whose
//! current version is a tombstone or an erasure are skipped. Superseded
//! versions are skipped by key and never decoded.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::ReadableDatabase;
use tracing::debug;

use super::btree::{SparseEngine, redb_err};
use super::btree_versioned::doc::DOCUMENTS_VERSIONED;
use super::btree_versioned::index::INDEXES_VERSIONED;
use super::btree_versioned::index_visible::{EntryRange, for_each_visible_entry};
use super::btree_versioned::key::{coll_prefix, coll_prefix_end, tenant_prefix, tenant_prefix_end};
use super::btree_versioned::visible::{ScanRange, for_each_visible};

impl SparseEngine {
    /// Visit the current version of every document of `collection`, in
    /// doc-id order, until `visit` breaks or `stop` trips. `stop` is
    /// consulted once per stored version.
    pub(in crate::engine::sparse) fn for_each_current_document(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        stop: &dyn Fn() -> bool,
        mut visit: impl FnMut(&StorageKey, &[u8]) -> crate::Result<ControlFlow<()>>,
    ) -> crate::Result<()> {
        let lo = coll_prefix(database_id, tenant_id, collection);
        let hi = coll_prefix_end(database_id, tenant_id, collection);
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        for_each_visible(
            &table,
            ScanRange {
                coll: collection,
                prefix: &lo,
                start: &lo,
                end: &hi,
            },
            None,
            stop,
            |row| visit(&row.doc_id, row.value.body),
        )
    }

    /// The current version of every document in a collection, up to
    /// `limit` rows, as `(document_id, body)` pairs.
    pub fn scan_documents(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let mut results = Vec::with_capacity(limit.min(256));
        if limit == 0 {
            return Ok(results);
        }
        self.for_each_current_document(
            database_id,
            tenant_id,
            collection,
            &super::scan_stop::never_stop,
            |doc_id, body| {
                results.push((*doc_id, body.to_vec()));
                Ok(if results.len() >= limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            },
        )?;
        debug!(collection, count = results.len(), "document scan");
        Ok(results)
    }

    /// Streaming [`Self::scan_documents`]: yields each `(document_id, body)`
    /// row to `f` as it is read, holding the read transaction open across
    /// calls. Peak memory is one row. An `Err` from `f` ends the scan and is
    /// returned.
    pub fn scan_documents_for_each<F>(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
        mut f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&StorageKey, &[u8]) -> crate::Result<()>,
    {
        let mut count = 0usize;
        if limit == 0 {
            return Ok(());
        }
        self.for_each_current_document(
            database_id,
            tenant_id,
            collection,
            &super::scan_stop::never_stop,
            |doc_id, body| {
                f(doc_id, body)?;
                count += 1;
                Ok(if count >= limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            },
        )?;
        debug!(collection, count, "streaming document scan");
        Ok(())
    }

    /// Count current index entries of `field` by value. Backs index-only
    /// `GROUP BY field` + `COUNT(*)`: no document is read.
    pub fn scan_index_groups(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
    ) -> crate::Result<Vec<(String, usize)>> {
        // A value holding `:` can sort between the entries of a shorter
        // value, so equal values are not always adjacent: count by value.
        let mut groups: std::collections::BTreeMap<String, usize> = Default::default();
        self.for_each_current_index_entry(
            database_id,
            tenant_id,
            collection,
            field,
            |value, _| {
                *groups.entry(value.to_string()).or_default() += 1;
            },
        )?;
        let result: Vec<(String, usize)> = groups.into_iter().collect();
        debug!(collection, field, groups = result.len(), "index group scan");
        Ok(result)
    }

    /// [`Self::scan_index_groups`] counting only entries whose document is
    /// in `doc_ids`, sorted by count descending. A facet query evaluates its
    /// WHERE predicate once into `doc_ids` and counts every facet field
    /// against it.
    pub fn scan_index_groups_filtered(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        doc_ids: &std::collections::HashSet<StorageKey>,
    ) -> crate::Result<Vec<(String, usize)>> {
        let mut groups: std::collections::HashMap<String, usize> = Default::default();
        self.for_each_current_index_entry(
            database_id,
            tenant_id,
            collection,
            field,
            |value, doc_id| {
                if doc_ids.contains(&doc_id) {
                    *groups.entry(value.to_string()).or_default() += 1;
                }
            },
        )?;
        let mut result: Vec<(String, usize)> = groups.into_iter().collect();
        result.sort_by_key(|r| std::cmp::Reverse(r.1));
        debug!(
            collection,
            field,
            groups = result.len(),
            "filtered index group scan"
        );
        Ok(result)
    }

    /// Visit every current `(value, doc_id)` entry of `field`, in value
    /// order.
    fn for_each_current_index_entry(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field: &str,
        mut visit: impl FnMut(&str, StorageKey),
    ) -> crate::Result<()> {
        let field_prefix = format!("{database_id}:{tenant_id}:{collection}:{field}:");
        let hi = format!("{database_id}:{tenant_id}:{collection}:{field};");
        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(INDEXES_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        for_each_visible_entry(
            &table,
            EntryRange {
                coll: collection,
                prefix: &field_prefix,
                start: &field_prefix,
                end: &hi,
            },
            None,
            |value, doc_id| {
                visit(value, doc_id);
                Ok(ControlFlow::Continue(()))
            },
        )
    }

    /// Scan the current version of every document, evaluating `predicate`
    /// during the scan. A row that does not match is never copied.
    ///
    /// `predicate` receives the row's doc-id key and body, and returns true
    /// to keep the row. The key carries a schemaless row's identity when its
    /// body has no `id` field, so a predicate that checks `id` needs it.
    ///
    /// `stop` is consulted once per stored version. It ends the scan where it
    /// stands, so the caller that owns the signal (a statement deadline)
    /// decides whether the short result is an answer or an error. Pass
    /// [`never_stop`](crate::engine::sparse::scan_stop::never_stop) when
    /// nothing can cut the scan short.
    pub fn scan_documents_filtered(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        limit: usize,
        predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
        stop: &dyn Fn() -> bool,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let mut results = Vec::with_capacity(limit.min(256));
        if limit == 0 {
            return Ok(results);
        }
        self.for_each_current_document(
            database_id,
            tenant_id,
            collection,
            stop,
            |doc_id, body| {
                if predicate(doc_id, body) {
                    results.push((*doc_id, body.to_vec()));
                }
                Ok(if results.len() >= limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            },
        )?;
        debug!(collection, count = results.len(), "filtered document scan");
        Ok(results)
    }

    /// Every row of `table_def` keyed under `(database_id, tenant_id)`, as
    /// raw `(key, value)` pairs.
    pub(in crate::engine::sparse) fn scan_table_for_tenant(
        &self,
        table_def: redb::TableDefinition<&str, &[u8]>,
        database_id: u64,
        tenant_id: u64,
        label: &str,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        use redb::ReadableTable;
        let lo = tenant_prefix(database_id, tenant_id);
        let hi = tenant_prefix_end(database_id, tenant_id);

        let read_txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = read_txn
            .open_table(table_def)
            .map_err(|e| redb_err("open table", e))?;

        let range = table
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err(label, e))?;

        let mut results = Vec::new();
        for entry in range {
            let entry = entry.map_err(|e| redb_err("entry", e))?;
            results.push((entry.0.value().to_string(), entry.1.value().to_vec()));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    #[test]
    fn for_each_matches_scan_documents() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine.put(0, 1, "users", &key(2), b"bob", 11).unwrap();
        engine.put(0, 1, "users", &key(3), b"carol", 12).unwrap();

        let materialized = engine.scan_documents(0, 1, "users", usize::MAX).unwrap();

        let mut streamed: Vec<(StorageKey, Vec<u8>)> = Vec::new();
        engine
            .scan_documents_for_each(0, 1, "users", usize::MAX, |doc_id, bytes| {
                streamed.push((*doc_id, bytes.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(materialized.len(), 3);
        assert_eq!(materialized, streamed);
    }

    #[test]
    fn scan_yields_the_current_version_once_per_row() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"a1", 10).unwrap();
        engine.put(0, 1, "users", &key(1), b"a2", 20).unwrap();
        engine.put(0, 1, "users", &key(2), b"b1", 15).unwrap();
        engine.delete(0, 1, "users", &key(2), 25).unwrap();
        engine.put(0, 1, "users", &key(3), b"c1", 30).unwrap();
        let rows = engine.scan_documents(0, 1, "users", usize::MAX).unwrap();
        assert_eq!(
            rows,
            vec![(key(1), b"a2".to_vec()), (key(3), b"c1".to_vec())]
        );
    }

    #[test]
    fn for_each_respects_limit() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine.put(0, 1, "users", &key(2), b"bob", 11).unwrap();
        engine.put(0, 1, "users", &key(3), b"carol", 12).unwrap();

        let materialized = engine.scan_documents(0, 1, "users", 2).unwrap();

        let mut streamed: Vec<(StorageKey, Vec<u8>)> = Vec::new();
        engine
            .scan_documents_for_each(0, 1, "users", 2, |doc_id, bytes| {
                streamed.push((*doc_id, bytes.to_vec()));
                Ok(())
            })
            .unwrap();

        assert_eq!(materialized.len(), 2);
        assert_eq!(materialized, streamed);
    }

    /// The stop signal ends the row loop where it stands. A caller that owns
    /// the signal (a statement deadline) turns the short result into an
    /// error rather than reporting it as the answer.
    #[test]
    fn filtered_scan_stops_on_the_stop_signal() {
        let (engine, _dir) = open_temp();
        for i in 0..8 {
            engine.put(0, 1, "users", &key(i), b"body", 10).unwrap();
        }

        let visited = std::cell::Cell::new(0usize);
        let rows = engine
            .scan_documents_filtered(
                0,
                1,
                "users",
                usize::MAX,
                &|_: &StorageKey, _: &[u8]| {
                    visited.set(visited.get() + 1);
                    true
                },
                &|| visited.get() >= 3,
            )
            .unwrap();

        assert!(rows.len() <= 3, "the scan ends at the stop signal");
        assert!(visited.get() <= 3, "no row past the signal is evaluated");
    }

    #[test]
    fn filtered_scan_runs_to_completion_without_a_stop() {
        let (engine, _dir) = open_temp();
        for i in 0..8 {
            engine.put(0, 1, "users", &key(i), b"body", 10).unwrap();
        }

        let rows = engine
            .scan_documents_filtered(
                0,
                1,
                "users",
                usize::MAX,
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();

        assert_eq!(rows.len(), 8, "never_stop must not cut the scan short");
    }

    #[test]
    fn for_each_propagates_callback_error() {
        let (engine, _dir) = open_temp();
        engine.put(0, 1, "users", &key(1), b"alice", 10).unwrap();
        engine.put(0, 1, "users", &key(2), b"bob", 11).unwrap();

        let mut seen = 0usize;
        let result =
            engine.scan_documents_for_each(0, 1, "users", usize::MAX, |_doc_id, _bytes| {
                seen += 1;
                Err(crate::Error::Internal {
                    detail: "stop".to_string(),
                })
            });

        assert!(result.is_err());
        assert_eq!(seen, 1);
    }

    #[test]
    fn index_groups_count_current_entries_only() {
        let (engine, _dir) = open_temp();
        let put = |value: &str, id: u32, sys_from: i64| {
            engine
                .versioned_index_put(VersionedIndexEntry {
                    database_id: 0,
                    tenant: 1,
                    coll: "users",
                    field: "city",
                    value,
                    doc_id: &key(id),
                    sys_from,
                })
                .unwrap();
        };
        put("oslo", 1, 10);
        put("oslo", 2, 10);
        put("rome", 3, 10);
        engine
            .versioned_index_tombstone(VersionedIndexEntry {
                database_id: 0,
                tenant: 1,
                coll: "users",
                field: "city",
                value: "oslo",
                doc_id: &key(2),
                sys_from: 20,
            })
            .unwrap();
        put("rome", 2, 20);
        let groups = engine.scan_index_groups(0, 1, "users", "city").unwrap();
        assert_eq!(
            groups,
            vec![("oslo".to_string(), 1), ("rome".to_string(), 2)]
        );
    }
}
