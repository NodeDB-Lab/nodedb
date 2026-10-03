// SPDX-License-Identifier: BUSL-1.1

//! Full-text postings for restored documents.
//!
//! A snapshot carries document rows but no full-text postings, and the raw
//! row install bypasses the write path that indexes text. After the rows
//! land, every restored collection's current rows are re-indexed here from
//! their stored bodies, so a restored row is findable by its words and a
//! row the restore replaced no longer matches its old words.
//!
//! Each collection re-indexes in one write transaction, through the same
//! per-row helper an UPDATE uses.

use std::collections::BTreeMap;

use nodedb_types::StorageKey;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::update_reindex_text::UpdateTextReindex;
use crate::engine::sparse::btree_versioned::VersionedScanParams;
use crate::types::{DatabaseId, TenantId};

/// `(database_id, tenant_id, collection)` of a restored document key.
///
/// Plain keys are `{db}:{tenant}:{collection}:{doc_id}`; versioned keys append
/// `\x00{sys_from}`. A document id never holds `:`, so the collection is
/// everything between the tenant and the last `:`.
fn collection_of(key: &str) -> Option<(u64, u64, String)> {
    let mut parts = key.splitn(3, ':');
    let db = parts.next()?.parse().ok()?;
    let tid = parts.next()?.parse().ok()?;
    let rest = parts.next()?.split('\x00').next()?;
    let (collection, _doc_id) = rest.rsplit_once(':')?;
    Some((db, tid, collection.to_string()))
}

impl CoreLoop {
    /// Re-index the text of every current row of every collection the
    /// snapshot restored rows into. Returns the rows re-indexed. The first
    /// failure fails the restore, as a failed row install does.
    pub(super) fn restore_text_index(
        &mut self,
        snap: &crate::types::TenantDataSnapshot,
    ) -> crate::Result<u64> {
        // A collection is versioned when the snapshot carries versioned rows
        // for it: its current rows live in the versioned table.
        let mut collections: BTreeMap<(u64, u64, String), bool> = BTreeMap::new();
        let keys = snap
            .documents
            .iter()
            .map(|(key, _)| (key.as_str(), false))
            .chain(
                snap.documents_versioned
                    .iter()
                    .map(|(key, _)| (key.as_str(), true)),
            );
        for (key, versioned) in keys {
            let parsed = collection_of(key).ok_or_else(|| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("restore: document key {key:?} names no collection"),
            })?;
            *collections.entry(parsed).or_insert(false) |= versioned;
        }
        let mut reindexed = 0u64;
        for ((db, tid, collection), versioned) in collections {
            reindexed += self.reindex_collection_text(db, tid, &collection, versioned)?;
        }
        Ok(reindexed)
    }

    /// Re-index the text of every current row of one collection in one
    /// transaction. `versioned` reads the current version of each row from
    /// the versioned table.
    fn reindex_collection_text(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        versioned: bool,
    ) -> crate::Result<u64> {
        let rows: Vec<(StorageKey, Vec<u8>)> = if versioned {
            self.sparse.versioned_scan_as_of(
                VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: usize::MAX,
                },
                &|_, _| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )?
        } else {
            self.sparse
                .scan_documents(database_id, tid, collection, usize::MAX)?
        };
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let txn = self.sparse.begin_write()?;
        let mut reindexed = 0u64;
        for (key, body) in &rows {
            // A registered collection's stored rows must decode. An
            // unregistered one indexes what `decode_document` reads, and a
            // body it cannot read has no fields to index, as on the insert
            // path.
            let doc = match self.doc_configs.get(&config_key) {
                Some(cfg) => Some(self.decode_stored_document(cfg, body)?),
                None => crate::data::executor::doc_format::decode_document(body).ok(),
            };
            let Some(doc) = doc else {
                continue;
            };
            self.update_reindex_text(
                &txn,
                UpdateTextReindex {
                    database_id,
                    tid,
                    collection,
                    surrogate: key.surrogate(),
                    new_doc: &doc,
                },
            )?;
            reindexed += 1;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("restore text reindex commit ({collection}): {e}"),
        })?;
        Ok(reindexed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;

    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::engine::sparse::inverted::IndexDocScope;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "restore_fts";

    #[test]
    fn plain_and_versioned_keys_name_their_collection() {
        assert_eq!(
            collection_of("0:7:docs:0000002a"),
            Some((0, 7, "docs".to_string()))
        );
        assert_eq!(
            collection_of("3:7:docs:0000002a\x0000000000000000001234"),
            Some((3, 7, "docs".to_string()))
        );
        assert_eq!(collection_of("not-a-key"), None);
    }

    fn searchable(core: &CoreLoop, term: &str) -> bool {
        !core
            .inverted
            .search(
                DB,
                TenantId::new(TID),
                COLL,
                FtsSearchParams {
                    query: term,
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .expect("search")
            .is_empty()
    }

    /// A snapshot install writes a row's body straight into the sparse
    /// store, bypassing the write path that indexes text. This proves
    /// `restore_text_index` catches the row up: the text an old index entry
    /// named is gone, and the row's restored text is findable with no
    /// manual `REINDEX`.
    #[test]
    fn restore_text_index_replaces_postings_a_direct_row_install_left_stale() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let surrogate = nodedb_types::Surrogate::new(9);
        let key = StorageKey::for_surrogate(surrogate);

        // Index the row's OLD text through the normal indexing path, as a
        // live write would have before the snapshot install below replaced
        // the row underneath that index.
        let txn = core.sparse.begin_write().expect("begin write");
        core.inverted
            .index_document_in_txn(
                &txn,
                IndexDocScope {
                    database_id: DB,
                    tid: TenantId::new(TID),
                    collection: COLL,
                    surrogate,
                },
                &crate::data::executor::fts_text::extract_fts_fields(
                    &serde_json::json!({ "body": "alpha original" }),
                ),
            )
            .expect("index old text");
        txn.commit().expect("commit old index");
        assert!(searchable(&core, "alpha"), "the old text must be indexed");

        // The snapshot install writes the restored body straight into the
        // store, exactly as `restore_sparse` does, with no FTS side effect.
        let restored = serde_json::json!({"body": "beta replacement"});
        core.sparse
            .put(
                DB,
                TID,
                COLL,
                &key,
                &doc_format::encode_to_msgpack(&restored),
            )
            .expect("install the restored row");

        let snap = crate::types::TenantDataSnapshot {
            documents: vec![(format!("{DB}:{TID}:{COLL}:{key}"), Vec::new())],
            ..Default::default()
        };
        let reindexed = core.restore_text_index(&snap).expect("restore text index");
        assert_eq!(reindexed, 1, "the one restored row must be reindexed");

        assert!(
            !searchable(&core, "alpha"),
            "the pre-restore text must no longer match"
        );
        assert!(
            searchable(&core, "beta"),
            "the restored row's text must be findable with no manual REINDEX"
        );
    }
}
