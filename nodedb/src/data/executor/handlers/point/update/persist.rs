// SPDX-License-Identifier: BUSL-1.1

//! Landing the post-update image, with its secondary indexes and everything the
//! collection's constraints derive from it, in one write.
//!
//! Separate from image construction because the concern here is atomicity, not
//! value: which of three mutually exclusive write shapes the collection takes,
//! and what has to travel with the body so no index is left describing the old
//! value. A bitemporal collection appends a version and diffs the VERSIONED
//! index; a plain collection with index paths diffs the secondary btree; a
//! collection with neither writes the body alone. Keeping the three side by
//! side in one file is what makes it visible that only the last one is allowed
//! to skip the diff, and that a body whose index diff cannot be computed must
//! fail rather than write alone.
//!
//! All three run inside ONE transaction this function owns, and image-folding
//! enforcement runs inside that same transaction before it commits. That is why
//! the enforcement lives here rather than in the caller that sequences the
//! statement: a materialized-sum target write is a document write of its own,
//! and running it after this function returned would put it in a SECOND
//! transaction — a crash between the two leaves the row updated and the total
//! it feeds stale, which is exactly the divergence the constraint exists to
//! rule out.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::engine::document::store::IndexPath;
use crate::types::{DatabaseId, Lsn, TenantId};
use nodedb_physical::physical_plan::ResolvedSumTarget;

/// Inputs to [`CoreLoop::persist_point_update`].
pub(in crate::data::executor) struct PointUpdatePersist<'a> {
    pub(in crate::data::executor) config_key: &'a (DatabaseId, TenantId, String),
    pub(in crate::data::executor) database_id: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    /// The same storage key, typed — what every storage call below takes.
    pub(in crate::data::executor) storage_key: &'a crate::engine::document::store::StorageKey,
    /// The row as it was before this update — the old side of the index diff,
    /// and the pre-image every folded constraint subtracts.
    pub(in crate::data::executor) current_bytes: &'a [u8],
    /// The row as it will be stored.
    pub(in crate::data::executor) updated_bytes: &'a [u8],
    pub(in crate::data::executor) bitemporal: bool,
    pub(in crate::data::executor) sys_from_ms: i64,
    pub(in crate::data::executor) wal_lsn: Option<Lsn>,
    /// `(target collection, join-key value)` → target row surrogate for every
    /// materialized-sum target this update may touch — BOTH sides when the
    /// update moves a row between targets by changing its join key. Resolved on
    /// the Control Plane.
    pub(in crate::data::executor) resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Write the post-update body, reconcile the collection's secondary indexes
    /// with it, and apply everything its declared constraints derive from the
    /// change — all in one transaction.
    ///
    /// Returns the redo entries for any derived target rows, which the caller
    /// carries back on its response so each is journalled against its own
    /// collection.
    pub(in crate::data::executor) fn persist_point_update(
        &mut self,
        params: PointUpdatePersist<'_>,
    ) -> crate::Result<Vec<crate::bridge::envelope::WriteSetEntry>> {
        let PointUpdatePersist {
            config_key,
            database_id,
            tid,
            collection,
            storage_key,
            current_bytes,
            updated_bytes,
            bitemporal,
            sys_from_ms,
            wal_lsn,
            resolved_sum_targets,
        } = params;

        // The plain `INDEXES` secondary-index paths for this collection.
        // The non-bitemporal write must reconcile these atomically with
        // the primary body so a changed value can't leave a stale index
        // entry pointing at the old value.
        let index_paths: Vec<IndexPath> = self
            .doc_configs
            .get(config_key)
            .map(|c| c.index_paths.clone())
            .unwrap_or_default();

        // One transaction for the body, its index diff, and every derived write
        // the collection's constraints imply. Dropped un-committed on any error
        // below, so a failure leaves neither a body without its index nor a
        // total without the row that moved it.
        let txn = self.sparse.begin_write()?;

        let write_result = if bitemporal {
            // Bitemporal collections keep secondary-index entries in the
            // versioned index only; the update must tombstone values it
            // dropped and assert current values, atomically with the new
            // body. Decode old/new docs (storage-mode-aware) so the
            // reindex sees the real indexed values for strict + schemaless.
            let index_paths = self
                .doc_configs
                .get(config_key)
                .map(|c| c.index_paths.clone())
                .unwrap_or_default();
            // An unregistered collection has no index paths to maintain,
            // so it still takes the plain versioned put. A REGISTERED
            // one whose stored images will not decode is the separate,
            // non-skippable case: writing the body without the index
            // diff desyncs the versioned index exactly the way the
            // non-bitemporal branch below refuses to.
            let images = match self.doc_configs.get(config_key) {
                Some(cfg) => {
                    let old = self.decode_stored_document(cfg, current_bytes);
                    let new = self.decode_stored_document(cfg, updated_bytes);
                    Some(old.and_then(|o| new.map(|n| (o, n))))
                }
                None => None,
            };
            match images {
                Some(Ok((old_doc, new_doc))) => self.bitemporal_update_reindex(
                    &txn,
                    super::super::update_reindex::BitemporalUpdateReindex {
                        database_id,
                        tid,
                        collection,
                        doc_id: storage_key,
                        sys_from_ms,
                        valid_from_ms: i64::MIN,
                        valid_until_ms: i64::MAX,
                        new_body: updated_bytes,
                        index_paths: &index_paths,
                        old_doc: Some(&old_doc),
                        new_doc: &new_doc,
                    },
                ),
                Some(Err(e)) => Err(crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!(
                        "bitemporal update: document failed to decode for \
                         versioned-index diff (collection {collection}, id {storage_key}): {e}"
                    ),
                }),
                None => self
                    .sparse
                    .versioned_put_in_txn(
                        &txn,
                        crate::engine::sparse::btree_versioned::VersionedPut {
                            database_id,
                            tenant: tid,
                            coll: collection,
                            doc_id: storage_key,
                            sys_from_ms,
                            valid_from_ms: i64::MIN,
                            valid_until_ms: i64::MAX,
                            body: updated_bytes,
                        },
                    )
                    .map(|()| Vec::new()),
            }
        } else if index_paths.is_empty() {
            // No secondary index to maintain — nothing to diff, and no index
            // tuples to publish, so the body write is the whole write.
            self.sparse
                .put_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    updated_bytes,
                )
                .map(|_prior| Vec::new())
        } else {
            // Reconcile the plain secondary index atomically with the
            // primary body. Decode old/new (storage-mode-aware) so the
            // SET diff drops values the update removed and asserts the
            // new ones in the same redb transaction — otherwise a later
            // lookup on the new value misses the row and a lookup on the
            // old value wrongly returns it. Mirrors the bitemporal branch.
            let images = match self.doc_configs.get(config_key) {
                Some(cfg) => {
                    let old = self.decode_stored_document(cfg, current_bytes);
                    let new = self.decode_stored_document(cfg, updated_bytes);
                    old.and_then(|o| new.map(|n| (o, n)))
                }
                None => Err(crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: "collection has index paths but no registered config".into(),
                }),
            };
            match images {
                Ok((old_doc, new_doc)) => self.nonbitemporal_update_reindex(
                    &txn,
                    super::super::update_reindex::NonbitemporalUpdateReindex {
                        database_id,
                        tid,
                        collection,
                        storage_key,
                        new_body: updated_bytes,
                        index_paths: &index_paths,
                        old_doc: &old_doc,
                        new_doc: &new_doc,
                    },
                ),
                Err(e) => {
                    // Both images are documents we just read / re-encoded.
                    // If one fails to decode we cannot compute the
                    // secondary-index diff, so we must NOT write the
                    // primary alone — that would silently desync the index
                    // (the very bug this path fixes). Fail loud, carrying
                    // the reason the image was unreadable.
                    Err(crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!(
                            "non-bitemporal update: document failed to decode for \
                             secondary-index diff (collection {collection}, id {storage_key}): {e}"
                        ),
                    })
                }
            }
        };
        let touched = write_result?;

        // Image-folding enforcement, inside the transaction the body just landed
        // in. Both images are STORED bytes: `current_bytes` came off the store
        // and `updated_bytes` was re-encoded in the collection's own mode, so a
        // strict collection's Binary Tuples decode as tuples on both sides.
        //
        // A join-key change is an ordinary UPDATE here — the fold derives the
        // two-target split from `old_doc[join] != new_doc[join]` itself, moving
        // the amount off one target and onto the other.
        let hook_ctx = HookCtx {
            database_id,
            tid,
            collection,
            resolved_targets: resolved_sum_targets,
            deferred_sum_targets: &[],
            wal_lsn,
        };
        let enforcement = write_hook::run(
            self,
            &txn,
            &hook_ctx,
            WriteImages::Update {
                old: ImageBody::Stored(current_bytes),
                new: ImageBody::Stored(updated_bytes),
            },
        )?;
        let target_write_set = write_hook::target_write_set(&enforcement.target_writes);

        // An update contributes both legs — the old amount out, the new one in
        // — so a single-row update that moves an amount unbalances its group.
        // Settled before the commit; `?` drops `txn` un-committed.
        self.settle_balanced_entries(database_id, tid, collection, enforcement.balanced_entries)?;

        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("point update commit: {e}"),
        })?;

        // Index write-versions are published only once the write they describe
        // is durable.
        if let Some(lsn) = wal_lsn
            && !touched.is_empty()
        {
            self.note_index_write_values(
                DatabaseId::new(database_id),
                TenantId::new(tid),
                collection,
                &touched,
                lsn,
            );
        }

        Ok(target_write_set)
    }
}
