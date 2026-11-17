// SPDX-License-Identifier: BUSL-1.1

//! Full-text postings for restored documents.
//!
//! A snapshot carries document versions but no full-text postings, and the
//! raw version install bypasses the write path that indexes text. After the
//! versions land, every restored document's versions are replayed into the
//! inverted index in system-time order, each at its own ordinal. A live
//! version indexes its body, and a tombstone removes the text. The index
//! then holds the text history the source held: a read at any cut the
//! snapshot's versions cover matches the words the row held there, and a row
//! the restore replaced no longer matches its old words.
//!
//! Each collection replays in one write transaction. Each version goes
//! through the per-version replay a text-index rebuild also uses.
//!
//! The snapshot also carries every stored sync-frame text version. They
//! install as stored rows. Each document the local store holds no version
//! of after the install replays its frames through the rebuild's frame
//! replay.

use std::collections::BTreeMap;

use nodedb_types::{StorageKey, Surrogate};
use roaring::RoaringBitmap;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::text_rebuild::replay::TextVersion;
use crate::engine::sparse::btree_versioned::{decode_value, parse_sys_from};
use crate::engine::sparse::inverted::IndexDocScope;
use crate::types::{DatabaseId, SyncedTextEntry, TenantId};

/// `(database_id, tenant_id, collection)` of a restored document key.
type CollectionOf = (u64, u64, String);

/// One document version to replay: its row, its ordinal, its stored value.
type VersionToReplay<'a> = (Surrogate, i64, &'a [u8]);

/// `(database_id, tenant_id, collection, doc_id)` of a restored document key.
///
/// Keys are `{db}:{tenant}:{collection}:{doc_id}\x00{sys_from}`. A document
/// id never holds `:`, so the collection is everything between the tenant
/// and the last `:`.
fn document_of(key: &str) -> Option<(CollectionOf, &str)> {
    let mut parts = key.splitn(3, ':');
    let db = parts.next()?.parse().ok()?;
    let tid = parts.next()?.parse().ok()?;
    let rest = parts.next()?.split('\x00').next()?;
    let (collection, doc_id) = rest.rsplit_once(':')?;
    Some(((db, tid, collection.to_string()), doc_id))
}

/// The error for a restored document key the replay cannot read.
fn unreadable_key(key: &str, what: &str) -> crate::Error {
    crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("restore: document key {key:?} names no {what}"),
    }
}

impl CoreLoop {
    /// Replay the text of every document version the snapshot restored,
    /// then install and replay its sync-frame versions. Returns the
    /// versions replayed. The first failure fails the restore, as a failed
    /// row install does.
    pub(super) fn restore_text_index(
        &mut self,
        snap: &crate::types::TenantDataSnapshot,
    ) -> crate::Result<u64> {
        let mut collections: BTreeMap<CollectionOf, Vec<VersionToReplay<'_>>> = BTreeMap::new();
        for (key, value) in &snap.documents_versioned {
            let (collection, doc_id) =
                document_of(key).ok_or_else(|| unreadable_key(key, "collection"))?;
            let surrogate = StorageKey::parse(doc_id)
                .ok_or_else(|| unreadable_key(key, "row"))?
                .surrogate();
            let sys_from = parse_sys_from(key).ok_or_else(|| unreadable_key(key, "system time"))?;
            collections.entry(collection).or_default().push((
                surrogate,
                sys_from,
                value.as_slice(),
            ));
        }
        let mut replayed = 0u64;
        for ((db, tid, collection), mut versions) in collections {
            versions.sort_unstable_by_key(|(surrogate, sys_from, _)| (*surrogate, *sys_from));
            replayed += self.replay_collection_text(db, tid, &collection, &versions)?;
        }
        replayed += self.restore_synced_text(&snap.synced_text)?;
        Ok(replayed)
    }

    /// Install every sync-frame version in `frames`, then replay the frames
    /// of each document the local document store holds no version of, each
    /// at its own ordinal. A document with a stored body takes its text
    /// from the body alone, as on the source. The check reads the store
    /// after the install, as [`Self::install_synced_text`] reads it. So a
    /// body the install kept counts as a body the snapshot carried. Returns
    /// the frame versions replayed.
    fn restore_synced_text(&self, frames: &[SyncedTextEntry]) -> crate::Result<u64> {
        let mut restored: BTreeMap<CollectionOf, RoaringBitmap> = BTreeMap::new();
        let txn = self.sparse.begin_write()?;
        for frame in frames {
            self.inverted.put_synced_text_in_txn(
                &txn,
                IndexDocScope {
                    database_id: frame.database_id,
                    tid: TenantId::new(frame.tenant_id),
                    collection: &frame.collection,
                    surrogate: Surrogate::new(frame.surrogate),
                    ordinal: frame.ordinal,
                },
                &frame.fields,
            )?;
            restored
                .entry((frame.database_id, frame.tenant_id, frame.collection.clone()))
                .or_default()
                .insert(frame.surrogate);
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("restore synced text install commit: {e}"),
        })?;
        let mut replayed = 0u64;
        for ((db, tid, collection), framed) in &restored {
            let mut bodiless = RoaringBitmap::new();
            for surrogate in framed {
                let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
                if self
                    .sparse
                    .versioned_latest_sys_from(*db, *tid, collection, &key)?
                    .is_none()
                {
                    bodiless.insert(surrogate);
                }
            }
            replayed += self.replay_synced_text(*db, TenantId::new(*tid), collection, &|s| {
                bodiless.contains(s.as_u32())
            })?;
        }
        Ok(replayed)
    }

    /// Replay `versions`, ordered by row then system time, into the text
    /// index of one collection in one transaction.
    fn replay_collection_text(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        versions: &[VersionToReplay<'_>],
    ) -> crate::Result<u64> {
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let txn = self.sparse.begin_write()?;
        let config = self.doc_configs.get(&config_key);
        let mut replayed = 0u64;
        for &(surrogate, ordinal, value) in versions {
            let decoded = decode_value(value)?;
            self.replay_version_text(
                &txn,
                config,
                TextVersion {
                    database_id,
                    tid,
                    collection,
                    surrogate,
                    ordinal,
                    body: decoded.is_live().then_some(decoded.body),
                },
            )?;
            replayed += 1;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("restore text replay commit ({collection}): {e}"),
        })?;
        Ok(replayed)
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
    fn plain_and_versioned_keys_name_their_document() {
        assert_eq!(
            document_of("0:7:docs:0000002a"),
            Some(((0, 7, "docs".to_string()), "0000002a"))
        );
        assert_eq!(
            document_of("3:7:docs:0000002a\x0000000000000000001234"),
            Some(((3, 7, "docs".to_string()), "0000002a"))
        );
        assert_eq!(document_of("not-a-key"), None);
    }

    /// The stored value of a live version holding `doc`.
    fn live_value(doc: &serde_json::Value) -> Vec<u8> {
        crate::engine::sparse::btree_versioned::encode_value(
            crate::engine::sparse::btree_versioned::TAG_LIVE,
            i64::MIN,
            i64::MAX,
            &doc_format::encode_to_msgpack(doc),
        )
    }

    /// Register `COLL` with a full-text index on `body`, so the replay
    /// indexes that field.
    fn declare_body(core: &mut CoreLoop) {
        let mut config = crate::engine::document::store::CollectionConfig::new(COLL);
        config.text_fields =
            nodedb_physical::physical_plan::TextFieldSet::all_readable(vec!["body".to_string()]);
        core.doc_configs.insert(
            (DatabaseId::new(DB), TenantId::new(TID), COLL.to_string()),
            config,
        );
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
        declare_body(&mut core);
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
                    ordinal: crate::engine::sparse::inverted::UNVERSIONED,
                },
                &crate::data::executor::fts_text::extract_fts_fields(
                    &serde_json::json!({ "body": "alpha original" }),
                    &["body".to_string()],
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
                1,
            )
            .expect("install the restored row");

        let snap = crate::types::TenantDataSnapshot {
            documents_versioned: vec![(
                crate::engine::sparse::btree_versioned::versioned_doc_key(DB, TID, COLL, &key, 1),
                live_value(&restored),
            )],
            ..Default::default()
        };
        let reindexed = core.restore_text_index(&snap).expect("restore text index");
        assert_eq!(reindexed, 1, "the one restored version must be replayed");

        assert!(
            !searchable(&core, "alpha"),
            "the pre-restore text must no longer match"
        );
        assert!(
            searchable(&core, "beta"),
            "the restored row's text must be findable with no manual REINDEX"
        );
    }

    /// The tokens a read of `COLL`'s whole-document index at `at` sees for
    /// row `id`.
    fn tokens_at(core: &CoreLoop, id: u32, at: i64) -> Vec<String> {
        core.inverted
            .text_cut_view(
                DB,
                TenantId::new(TID),
                nodedb_fts::IndexScope::document(COLL),
                at,
            )
            .expect("cut view")
            .docs
            .into_iter()
            .filter(|d| d.doc_id == Surrogate::new(id))
            .flat_map(|d| d.tokens)
            .collect()
    }

    /// Whether a read of `COLL`'s whole-document index at `at` hides the
    /// live text of row `id`.
    fn hidden_at(core: &CoreLoop, id: u32, at: i64) -> bool {
        core.inverted
            .text_cut_view(
                DB,
                TenantId::new(TID),
                nodedb_fts::IndexScope::document(COLL),
                at,
            )
            .expect("cut view")
            .hidden
            .contains(Surrogate::new(id))
    }

    /// A snapshot carries every stored sync-frame version. A restore on
    /// another core installs them and re-indexes the declared fields of a
    /// row with no restored body, each version at its own ordinal. A row
    /// with a restored body takes its text from the body.
    #[test]
    fn snapshot_capture_and_restore_carry_frame_text() {
        let frame = |text: &str| -> Vec<(String, String)> {
            if text.is_empty() {
                Vec::new()
            } else {
                vec![("body".to_string(), text.to_string())]
            }
        };
        let source_dir = tempfile::tempdir().expect("tempdir");
        let (mut source, _req, _resp) = make_core_with_dir(source_dir.path());
        declare_body(&mut source);
        for (id, ordinal, text) in [
            (7, 10, "alpha"),
            (7, 20, "beta"),
            (8, 10, "gamma"),
            (8, 20, ""),
            (9, 10, "zulu"),
        ] {
            source
                .write_synced_text(DB, TID, COLL, Surrogate::new(id), &frame(text), ordinal)
                .expect("write frame");
        }
        let restored_body = serde_json::json!({"body": "delta"});
        let key = StorageKey::for_surrogate(Surrogate::new(9));
        let snap = crate::types::TenantDataSnapshot {
            synced_text: source.capture_synced_text(DB, TID).expect("capture frames"),
            documents_versioned: vec![(
                crate::engine::sparse::btree_versioned::versioned_doc_key(DB, TID, COLL, &key, 5),
                live_value(&restored_body),
            )],
            ..Default::default()
        };
        assert_eq!(snap.synced_text.len(), 5, "every frame version travels");
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot");
        let snap: crate::types::TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).expect("decode snapshot");

        let target_dir = tempfile::tempdir().expect("tempdir");
        let (mut target, _req, _resp) = make_core_with_dir(target_dir.path());
        declare_body(&mut target);
        // The document install lands row 9's body, as `restore_sparse` does.
        target
            .sparse
            .put(
                DB,
                TID,
                COLL,
                &key,
                &doc_format::encode_to_msgpack(&restored_body),
                5,
            )
            .expect("install row 9's body");
        let replayed = target.restore_text_index(&snap).expect("restore");
        assert_eq!(
            replayed,
            1 + 4,
            "one restored version and four body-less frames"
        );

        assert!(
            tokens_at(&target, 7, 15)
                .iter()
                .any(|t| t.contains("alpha"))
        );
        // A cut lists ended versions only. Row 7's version at 20 is live, so
        // a cut at 25 hides nothing of it and the live index answers.
        assert!(tokens_at(&target, 7, 25).is_empty());
        assert!(!hidden_at(&target, 7, 25), "row 7's live text reads at 25");
        assert!(searchable(&target, "beta"));
        assert!(!searchable(&target, "gamma"), "row 8 ended at 20");
        assert!(
            tokens_at(&target, 8, 15)
                .iter()
                .any(|t| t.contains("gamma"))
        );
        assert!(searchable(&target, "delta"), "row 9 takes its body");
        assert!(
            !searchable(&target, "zulu"),
            "a restored body wins over a frame"
        );
        assert_eq!(
            target.capture_synced_text(DB, TID).expect("frames"),
            snap.synced_text,
            "the target stores the frames for its own later rebuild"
        );
    }

    /// A row whose body the local store holds, and the snapshot does not
    /// carry, keeps its body's text. Its snapshot frames are only stored.
    #[test]
    fn a_locally_bodied_row_keeps_its_body_text_over_snapshot_frames() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_body(&mut core);
        let surrogate = Surrogate::new(9);
        let key = StorageKey::for_surrogate(surrogate);
        let local = serde_json::json!({"body": "kilo local"});
        core.sparse
            .put(
                DB,
                TID,
                COLL,
                &key,
                &doc_format::encode_to_msgpack(&local),
                5,
            )
            .expect("store the local body");
        let txn = core.sparse.begin_write().expect("begin write");
        core.inverted
            .index_document_in_txn(
                &txn,
                IndexDocScope {
                    database_id: DB,
                    tid: TenantId::new(TID),
                    collection: COLL,
                    surrogate,
                    ordinal: crate::engine::sparse::inverted::UNVERSIONED,
                },
                &crate::data::executor::fts_text::extract_fts_fields(&local, &["body".to_string()]),
            )
            .expect("index the local body");
        txn.commit().expect("commit local index");

        let snap = crate::types::TenantDataSnapshot {
            synced_text: vec![SyncedTextEntry {
                database_id: DB,
                tenant_id: TID,
                collection: COLL.to_string(),
                surrogate: surrogate.as_u32(),
                ordinal: 10,
                fields: vec![("body".to_string(), "lima frame".to_string())],
            }],
            ..Default::default()
        };
        assert_eq!(core.restore_text_index(&snap).expect("restore"), 0);
        assert!(searchable(&core, "kilo"), "the local body keeps its text");
        assert!(!searchable(&core, "lima"), "the frame is only stored");
        assert_eq!(
            core.capture_synced_text(DB, TID).expect("frames"),
            snap.synced_text,
            "the frame is stored"
        );
    }

    /// The replay keeps the text history: a read at a cut between two
    /// restored versions matches the older text, and a read after the
    /// restored delete matches nothing.
    #[test]
    fn restore_text_index_replays_every_version_at_its_ordinal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_body(&mut core);
        let surrogate = nodedb_types::Surrogate::new(5);
        let key = StorageKey::for_surrogate(surrogate);
        let version = |sys_from: i64| {
            crate::engine::sparse::btree_versioned::versioned_doc_key(DB, TID, COLL, &key, sys_from)
        };
        let tombstone = crate::engine::sparse::btree_versioned::encode_value(
            crate::engine::sparse::btree_versioned::TAG_TOMBSTONE,
            i64::MIN,
            i64::MAX,
            &[],
        );
        let snap = crate::types::TenantDataSnapshot {
            documents_versioned: vec![
                (
                    version(20),
                    live_value(&serde_json::json!({"body": "beta"})),
                ),
                (
                    version(10),
                    live_value(&serde_json::json!({"body": "alpha"})),
                ),
                (version(30), tombstone),
            ],
            ..Default::default()
        };
        assert_eq!(core.restore_text_index(&snap).expect("replay"), 3);

        let view = |at: i64| {
            core.inverted
                .text_cut_view(
                    DB,
                    TenantId::new(TID),
                    nodedb_fts::IndexScope::document(COLL),
                    at,
                )
                .expect("cut view")
        };
        let tokens_at = |at: i64| -> Vec<String> {
            view(at)
                .docs
                .into_iter()
                .filter(|d| d.doc_id == surrogate)
                .flat_map(|d| d.tokens)
                .collect()
        };
        assert!(tokens_at(15).iter().any(|t| t.contains("alpha")));
        assert!(tokens_at(25).iter().any(|t| t.contains("beta")));
        assert!(tokens_at(35).is_empty(), "deleted at 30");
        assert!(
            !searchable(&core, "beta"),
            "the current read sees the delete"
        );
    }
}
