// SPDX-License-Identifier: BUSL-1.1

//! BASE ∪ OVERLAY constraint checks for staged point writes.
//!
//! Primary-key existence and UNIQUE-index conflicts are evaluated against both
//! the durable engine state (BASE) and the not-yet-committed staged writes of
//! the current transaction (OVERLAY). A prior in-transaction tombstone on a
//! primary key makes it "absent" (so a re-insert after an in-transaction
//! delete succeeds). A unique value is judged on the transaction's post-state
//! so far: a staged put under another surrogate that holds it is a conflict,
//! and a base row the transaction rewrote or deleted no longer owns its
//! values. A staged TRUNCATE hides every base row, so only the overlay is
//! consulted afterwards.

use std::collections::HashSet;

use super::context::{CollKey, StageCtx};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::unique::{PostImage, UniqueScope, check_unique_post_state};
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::document::store::{CollectionConfig, StorageKey};
use crate::types::{DatabaseId, TenantId, TxnId};

/// The transaction and collection one staged statement writes.
pub(in crate::data::executor) struct StagedStatement<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub txn_id: TxnId,
    pub coll_key: &'a (DatabaseId, TenantId, String),
}

/// The overlay's verdict on a primary key within the current transaction.
pub(super) enum OverlayPk {
    /// A put is staged for this key: present regardless of base.
    Present,
    /// A tombstone is staged for this key, or the collection is truncated in
    /// this transaction: absent regardless of base.
    Absent,
    /// Nothing staged for this key: fall back to base.
    Unstaged,
}

impl CoreLoop {
    /// Whether the base rows of `coll_key` are visible inside `txn_id`.
    /// `false` after a staged TRUNCATE of the collection.
    pub(super) fn stage_base_visible(&self, txn_id: TxnId, coll_key: &CollKey) -> bool {
        self.txn_overlays
            .get(&txn_id)
            .is_none_or(|overlay| overlay.base_visible(coll_key))
    }

    /// True when the primary key is present under BASE ∪ OVERLAY semantics.
    pub(super) fn stage_pk_present(
        &self,
        ctx: &StageCtx<'_>,
        storage_key: &StorageKey,
        bitemporal: bool,
        overlay: OverlayPk,
    ) -> crate::Result<bool> {
        match overlay {
            OverlayPk::Present => Ok(true),
            OverlayPk::Absent => Ok(false),
            OverlayPk::Unstaged => {
                // Read-only base existence probe: open a scratch write txn (so
                // the probe is linearizable with the durable apply that COMMIT
                // will perform) and DROP it without committing — no durable
                // write happens here.
                let txn = self.sparse.begin_write()?;
                let exists = if bitemporal {
                    self.sparse.versioned_exists_current_in_txn(
                        &txn,
                        ctx.database_id,
                        ctx.tid,
                        ctx.collection,
                        storage_key,
                    )?
                } else {
                    self.sparse.exists_in_txn(
                        &txn,
                        ctx.database_id,
                        ctx.tid,
                        ctx.collection,
                        storage_key,
                    )?
                };
                drop(txn);
                Ok(exists)
            }
        }
    }

    /// Reject the incoming document if it violates a UNIQUE index under
    /// BASE ∪ OVERLAY.
    pub(super) fn stage_unique_check(
        &self,
        ctx: &StageCtx<'_>,
        config: &CollectionConfig,
        incoming_doc: &serde_json::Value,
    ) -> crate::Result<()> {
        self.stage_statement_unique_check(
            &StagedStatement {
                database_id: ctx.database_id,
                tid: ctx.tid,
                txn_id: ctx.txn_id,
                coll_key: &ctx.coll_key,
            },
            config,
            &[(ctx.surrogate.0, incoming_doc)],
        )
    }

    /// Judge the stored post-images one staged UPDATE statement writes, all
    /// together, under BASE ∪ OVERLAY. A value one row of the statement
    /// releases is free for another. A collection with no UNIQUE index
    /// decodes nothing.
    pub(in crate::data::executor) fn stage_stored_unique_check(
        &self,
        statement: &StagedStatement<'_>,
        rows: &[(u32, &[u8])],
    ) -> crate::Result<()> {
        let collection = statement.coll_key.2.as_str();
        let Some(config) = self.unique_config(statement.database_id, statement.tid, collection)
        else {
            return Ok(());
        };
        let docs = rows
            .iter()
            .map(|(_, body)| self.decode_stored_document(config, body))
            .collect::<crate::Result<Vec<_>>>()?;
        let incoming: Vec<(u32, &serde_json::Value)> = rows
            .iter()
            .zip(&docs)
            .map(|((surrogate, _), doc)| (*surrogate, doc))
            .collect();
        self.stage_statement_unique_check(statement, config, &incoming)
    }

    /// Judge `incoming` against the transaction's post-state so far: the
    /// collection's other staged rows count with their staged images, and
    /// their base images no longer count. COMMIT judges the whole record again,
    /// since a concurrent transaction can claim the value in between.
    fn stage_statement_unique_check(
        &self,
        statement: &StagedStatement<'_>,
        config: &CollectionConfig,
        incoming: &[(u32, &serde_json::Value)],
    ) -> crate::Result<()> {
        let written: HashSet<u32> = incoming.iter().map(|(surrogate, _)| *surrogate).collect();
        let overlay = self.txn_overlays.get(&statement.txn_id);
        // A staged body that will not decode cannot show the value it claims,
        // so the check fails rather than let an incoming row take it.
        let staged: Vec<(u32, Option<serde_json::Value>)> = match overlay {
            Some(overlay) => overlay
                .iter_for_collection(statement.coll_key)
                .filter(|(surrogate, _)| !written.contains(surrogate))
                .map(|(surrogate, staged)| match staged {
                    Staged::Put(body) => self
                        .decode_stored_document(config, body)
                        .map(|doc| (surrogate, Some(doc))),
                    Staged::Tombstone => Ok((surrogate, None)),
                })
                .collect::<crate::Result<_>>()?,
            None => Vec::new(),
        };
        let rows: Vec<PostImage<'_>> = staged
            .iter()
            .map(|(surrogate, doc)| PostImage {
                surrogate: *surrogate,
                doc: doc.as_ref(),
                judged: false,
            })
            .chain(incoming.iter().map(|(surrogate, doc)| PostImage {
                surrogate: *surrogate,
                doc: Some(*doc),
                judged: true,
            }))
            .collect();
        check_unique_post_state(
            &UniqueScope {
                sparse: &self.sparse,
                database_id: statement.database_id,
                tid: statement.tid,
                collection: statement.coll_key.2.as_str(),
                paths: &config.index_paths,
                bitemporal: config.bitemporal,
                base_visible: overlay
                    .is_none_or(|overlay| overlay.base_visible(statement.coll_key)),
                crs: &self.crs_context(),
            },
            &rows,
        )
    }
}
