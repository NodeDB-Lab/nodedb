// SPDX-License-Identifier: BUSL-1.1

//! BASE ∪ OVERLAY constraint checks for staged point writes.
//!
//! Primary-key existence and UNIQUE-index conflicts are evaluated against both
//! the durable engine state (BASE) and the not-yet-committed staged writes of
//! the current transaction (OVERLAY). A prior in-transaction tombstone on a
//! primary key makes it "absent" (so a re-insert after an in-transaction
//! delete succeeds); a prior in-transaction put under a different surrogate
//! sharing a unique value is a conflict.

use super::context::StageCtx;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::unique::{
    UniqueCheck, check_unique_constraints,
};
use crate::engine::document::store::{CollectionConfig, extract_index_values};

/// The overlay's verdict on a primary key within the current transaction.
pub(super) enum OverlayPk {
    /// A put is staged for this key: present regardless of base.
    Present,
    /// A tombstone is staged for this key: absent regardless of base.
    Absent,
    /// Nothing staged for this key: fall back to base.
    Unstaged,
}

impl CoreLoop {
    /// True when the primary key is present under BASE ∪ OVERLAY semantics.
    pub(super) fn stage_pk_present(
        &self,
        ctx: &StageCtx<'_>,
        row_key: &str,
        storage_key: &crate::engine::document::store::StorageKey,
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
                        row_key,
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
    /// BASE ∪ OVERLAY. `staged_others` are the collection's staged put bodies
    /// under surrogates OTHER than the incoming one (tombstones excluded).
    pub(super) fn stage_unique_check(
        &self,
        ctx: &StageCtx<'_>,
        config: &CollectionConfig,
        incoming_doc: &serde_json::Value,
        staged_others: &[Vec<u8>],
    ) -> crate::Result<()> {
        let collection = ctx.collection;
        // BASE: another durable row already owning one of the unique values.
        check_unique_constraints(UniqueCheck {
            sparse: &self.sparse,
            database_id: ctx.database_id,
            tid: ctx.tid,
            collection,
            doc: incoming_doc,
            document_id: &ctx.document_id,
            paths: &config.index_paths,
            bitemporal: config.bitemporal,
        })?;

        // OVERLAY: a staged put under a different surrogate sharing a value.
        for path in &config.index_paths {
            if !path.unique {
                continue;
            }
            if let Some(ref pred) = path.predicate
                && !pred.evaluate_json(incoming_doc)
            {
                continue;
            }
            let incoming: std::collections::HashSet<String> =
                extract_index_values(incoming_doc, &path.path, path.is_array)
                    .into_iter()
                    .map(|raw| {
                        if path.case_insensitive {
                            raw.to_lowercase()
                        } else {
                            raw
                        }
                    })
                    .collect();
            if incoming.is_empty() {
                continue;
            }
            for body in staged_others {
                // A staged body that will not decode cannot be checked for the
                // unique value it might already own, so skipping it would let
                // the incoming row take a value another staged row in the same
                // transaction is claiming.
                let staged_doc = self.decode_stored_document(config, body)?;
                if let Some(ref pred) = path.predicate
                    && !pred.evaluate_json(&staged_doc)
                {
                    continue;
                }
                for raw in extract_index_values(&staged_doc, &path.path, path.is_array) {
                    let needle = if path.case_insensitive {
                        raw.to_lowercase()
                    } else {
                        raw
                    };
                    if incoming.contains(&needle) {
                        return Err(crate::Error::RejectedConstraint {
                            collection: collection.to_string(),
                            constraint: "unique".to_string(),
                            detail: format!(
                                "unique index '{}' violation on field '{}' (value '{}')",
                                path.name, path.path, needle
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}
