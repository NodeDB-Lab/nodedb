// SPDX-License-Identifier: BUSL-1.1

//! Commit-boundary checks a committed redo record passes before anything of it
//! is written.
//!
//! A redo record applies as absolute post-images, so every check runs first,
//! against this core's current state, and a refusal writes nothing. Every
//! replica applies the record at the same log position against the same
//! state, and judges retention at the instant the boundary names, so each
//! one reaches the same verdict.
//!
//! A Calvin slice cannot abort once its transaction commits. Its stage runs
//! [`CoreLoop::check_commit_boundary`] on its staged images, under the
//! slice's locks and at its epoch instant, so a refusal there is an abort
//! vote. The install then judges the same writes against the same state.
//!
//! * BALANCED — the signed entries of every document write, judged per
//!   collection across the whole record.
//! * UNIQUE — the record's post-state: a unique value may have one owner among
//!   the rows the record leaves untouched and the rows it writes.
//! * Materialized-sum folds — every fold the install runs on this core
//!   finds its resolution and its target row, and leaves each target's
//!   UNIQUE values with one owner (`sum_fold_check`).
//! * Stateless PUT / DELETE enforcement — append-only, period lock, state
//!   transitions, transition checks, retention and legal hold, each against
//!   the stored pre-image. A put to a `HASH_CHAIN` collection that sets a
//!   chain field is refused.
//!
//! A [`RedoOrigin::Restore`] record re-installs rows a backup captured. Each
//! row passed BALANCED and the stateless rules when it was first written, and
//! a bitemporal row's earlier versions are history. So a restore runs UNIQUE
//! only. UNIQUE judges the post-state: the last write to each row.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{RedoOrigin, RedoSumTargets, ResolvedSumTarget};
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::balanced::{self, BalancedEntry};
use crate::data::executor::enforcement::hash_chain;
use crate::data::executor::enforcement::images::RowImages;
use crate::data::executor::enforcement::unique::{PostImage, UniqueScope, check_unique_post_state};
use crate::data::executor::handlers::point::apply_delete::{
    DeleteEnforcement, run_delete_enforcement,
};
use crate::data::executor::handlers::point::apply_put::PutEnforcement;
use crate::engine::document::store::{CollectionConfig, StorageKey};
use crate::types::{DatabaseId, TenantId};

use super::sub_ops::{RedoDocOp, RedoKvOp};

/// What one commit boundary judges: the document and KV writes of one
/// transaction slice, and what their checks read beside the stored state.
///
/// The Calvin stage builds it from the slice's staged images, and the redo
/// install builds it from the slice's committed record. Both judge it with
/// [`CoreLoop::check_commit_boundary`], so the stage refuses every slice the
/// install would refuse, as an abort vote.
pub(in crate::data::executor) struct CommitBoundary<'a> {
    pub database_id: u64,
    pub tid: u64,
    /// The document writes, without the clone copies the install skips.
    pub documents: &'a [RedoDocOp],
    pub kv: &'a [RedoKvOp],
    /// The sum targets each written collection folds on this core. A
    /// period lock reads its reference row from them.
    pub sum_targets: &'a [RedoSumTargets],
    pub origin: RedoOrigin,
    /// Roles of the identity that commits the writes.
    pub user_roles: &'a [String],
    /// The instant retention is judged at, in ms since the Unix epoch.
    pub judged_at_ms: i64,
}

impl CoreLoop {
    /// Refuse `boundary` when applying it would break a constraint the
    /// transaction checks at commit: the row image limits, the stateless
    /// PUT and DELETE rules, BALANCED and UNIQUE. Reads only.
    pub(in crate::data::executor) fn check_commit_boundary(
        &self,
        boundary: &CommitBoundary<'_>,
    ) -> crate::Result<()> {
        self.validate_redo_document_ops(boundary)?;
        check_redo_kv_row_images(boundary.kv)
    }

    /// Refuse the document writes of `boundary` when applying them would
    /// break a constraint. A role-guarded transition is judged against
    /// `boundary.user_roles`. Reads only.
    fn validate_redo_document_ops(&self, boundary: &CommitBoundary<'_>) -> crate::Result<()> {
        let CommitBoundary {
            database_id,
            tid,
            documents: ops,
            origin,
            user_roles,
            judged_at_ms,
            ..
        } = *boundary;
        self.check_redo_row_images(database_id, tid, ops)?;
        let mut by_collection: BTreeMap<&str, Vec<&RedoDocOp>> = BTreeMap::new();
        for op in ops {
            by_collection.entry(op.collection()).or_default().push(op);
        }
        for (collection, collection_ops) in by_collection {
            let config_key = (
                DatabaseId::new(database_id),
                TenantId::new(tid),
                collection.to_string(),
            );
            // An unregistered collection declares no constraint to check.
            let Some(config) = self.doc_configs.get(&config_key) else {
                continue;
            };
            let bitemporal = self.is_bitemporal(database_id, tid, collection);
            let resolved = sum_targets_for(boundary.sum_targets, collection);
            let scope = CollectionScope {
                database_id,
                tid,
                collection,
                config_key: &config_key,
                config,
                bitemporal,
                resolved: &resolved,
                user_roles,
                judged_at_ms,
            };
            if origin == RedoOrigin::Commit {
                self.check_collection_ops(&scope, &collection_ops)?;
            }
            self.check_record_unique(&scope, &collection_ops)?;
            if origin == RedoOrigin::Commit {
                self.check_sum_folds(&scope, &collection_ops)?;
            }
        }
        Ok(())
    }

    /// Stateless enforcement per write, then BALANCED across the collection.
    fn check_collection_ops(
        &self,
        scope: &CollectionScope<'_>,
        ops: &[&RedoDocOp],
    ) -> crate::Result<()> {
        let mut balanced_entries: Vec<BalancedEntry> = Vec::new();
        for op in ops {
            balanced_entries.extend(self.check_op_enforcement(scope, op)?);
        }
        if let Some(def) = &scope.config.enforcement.balanced {
            balanced::check_balanced(scope.collection, def, &balanced_entries)?;
        }
        Ok(())
    }

    /// Judge `op` by the stateless rules against the row stored now, and
    /// answer the BALANCED entries it contributes to its collection.
    pub(super) fn check_op_enforcement(
        &self,
        scope: &CollectionScope<'_>,
        op: &RedoDocOp,
    ) -> crate::Result<Vec<BalancedEntry>> {
        let enforcement = &scope.config.enforcement;
        let needs_prior = enforcement.has_put_checks()
            || enforcement.balanced.is_some()
            || enforcement.retention.is_some()
            || enforcement.has_legal_hold;
        let prior = if needs_prior {
            self.stored_prior(scope, op.surrogate())?
        } else {
            None
        };
        match op {
            RedoDocOp::Put { value, .. } => self.check_put_enforcement(scope, value, &prior)?,
            RedoDocOp::Delete { .. } => self.check_delete_enforcement(scope, prior.as_deref())?,
        }
        self.balanced_op_entries(scope, op, prior.as_deref())
    }

    /// The stateless PUT rules for the post-image `value` over the stored
    /// row `prior`: a supplied hash-chain link, append-only, period lock,
    /// state transitions and transition checks.
    pub(super) fn check_put_enforcement(
        &self,
        scope: &CollectionScope<'_>,
        value: &[u8],
        prior: &Option<Vec<u8>>,
    ) -> crate::Result<()> {
        // The install writes the chain fields; a submitted value for either
        // is refused before anything is written.
        if scope.config.enforcement.hash_chain {
            let doc = doc_format::decode_document(value)?;
            hash_chain::refuse_supplied_link(scope.collection, &doc)?;
        }
        self.check_stateless_put_enforcement(
            true,
            PutEnforcement {
                config_key: scope.config_key,
                database_id: scope.database_id,
                tid: scope.tid,
                collection: scope.collection,
                value,
                old_value: prior,
                user_roles: scope.user_roles,
                resolved_targets: scope.resolved,
            },
        )
    }

    /// The DELETE rules, retention and legal hold, for the stored row
    /// `prior`. A bitemporal delete judges only a row that exists. A plain
    /// delete judges whatever is stored, as the delete path does.
    pub(super) fn check_delete_enforcement(
        &self,
        scope: &CollectionScope<'_>,
        prior: Option<&[u8]>,
    ) -> crate::Result<()> {
        if scope.bitemporal && prior.is_none() {
            return Ok(());
        }
        run_delete_enforcement(
            self,
            DeleteEnforcement {
                database_id: scope.database_id,
                tid: scope.tid,
                collection: scope.collection,
                config: scope.config,
                old_value: prior,
                resolved_targets: scope.resolved,
                judged_at_ms: scope.judged_at_ms,
            },
        )
    }

    /// The signed BALANCED entries `op` contributes over the stored row
    /// `prior`. None when the collection declares no BALANCED constraint.
    pub(super) fn balanced_op_entries(
        &self,
        scope: &CollectionScope<'_>,
        op: &RedoDocOp,
        prior: Option<&[u8]>,
    ) -> crate::Result<Vec<BalancedEntry>> {
        let Some(def) = &scope.config.enforcement.balanced else {
            return Ok(Vec::new());
        };
        match op {
            RedoDocOp::Put { value, .. } => {
                let new_doc = doc_format::decode_document(value).ok();
                let old_doc = match prior {
                    Some(bytes) => Some(self.decode_stored_document(scope.config, bytes)?),
                    None => None,
                };
                let images = match (old_doc.as_ref(), new_doc.as_ref()) {
                    (Some(old_doc), Some(new_doc)) => RowImages::Update { old_doc, new_doc },
                    (None, Some(new_doc)) => RowImages::Insert { new_doc },
                    // A post-image with no readable document carries no
                    // column the definition can read.
                    (_, None) => return Ok(Vec::new()),
                };
                Ok(balanced::entries_for(def, &images))
            }
            RedoDocOp::Delete { .. } => {
                let Some(bytes) = prior else {
                    return Ok(Vec::new());
                };
                let old_doc = self.decode_stored_document(scope.config, bytes)?;
                Ok(balanced::entries_for(
                    def,
                    &RowImages::Delete { old_doc: &old_doc },
                ))
            }
        }
    }

    /// Every unique index value the record's post-state holds has one owner.
    /// The install writes row by row, so its puts do not judge UNIQUE.
    fn check_record_unique(
        &self,
        scope: &CollectionScope<'_>,
        ops: &[&RedoDocOp],
    ) -> crate::Result<()> {
        if !scope.config.index_paths.iter().any(|path| path.unique) {
            return Ok(());
        }
        // The install re-evaluates generated columns before it indexes a row,
        // so the judge reads the same image: a UNIQUE index can cover one.
        let docs = ops
            .iter()
            .map(|op| match op {
                RedoDocOp::Put { value, .. } => self.unique_image(scope.config, value).map(Some),
                RedoDocOp::Delete { .. } => Ok(None),
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let rows: Vec<PostImage<'_>> = ops
            .iter()
            .zip(&docs)
            .map(|(op, doc)| PostImage {
                surrogate: op.surrogate(),
                doc: doc.as_ref(),
                judged: true,
            })
            .collect();
        check_unique_post_state(
            &UniqueScope {
                sparse: &self.sparse,
                database_id: scope.database_id,
                tid: scope.tid,
                collection: scope.collection,
                paths: &scope.config.index_paths,
                base_visible: true,
                crs: &self.crs_context(),
            },
            &rows,
        )
    }

    /// The row as stored now, in the store this collection writes.
    fn stored_prior(
        &self,
        scope: &CollectionScope<'_>,
        surrogate: u32,
    ) -> crate::Result<Option<Vec<u8>>> {
        let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
        self.current_row_body(scope.database_id, scope.tid, scope.collection, &key)
    }

    /// Refuse the record when a post-image it puts is over the row image
    /// limit, before any of its rows is written.
    fn check_redo_row_images(
        &self,
        database_id: u64,
        tid: u64,
        ops: &[RedoDocOp],
    ) -> crate::Result<()> {
        for op in ops {
            self.check_doc_row_image(database_id, tid, op)?;
        }
        Ok(())
    }

    /// Refuse a document put whose post-image is over the row image limit.
    pub(super) fn check_doc_row_image(
        &self,
        database_id: u64,
        tid: u64,
        op: &RedoDocOp,
    ) -> crate::Result<()> {
        let RedoDocOp::Put {
            collection,
            value,
            surrogate,
            ..
        } = op
        else {
            return Ok(());
        };
        if value.len() <= nodedb_types::MAX_ROW_IMAGE_BYTES {
            return Ok(());
        }
        let column = self.identity_column(database_id, tid, collection);
        let identity = nodedb_types::RowIdentity::of_stored_row(
            value,
            Some(column.as_str()),
            StorageKey::for_surrogate(Surrogate::new(*surrogate)),
        );
        Err(crate::Error::DataPlane(
            crate::data::executor::row_image_limit::row_image_too_large(
                collection,
                identity.as_str(),
                value.len(),
            ),
        ))
    }
}

/// Refuse the record when a KV row it puts holds a key and value over the
/// row image limit, before any of its rows is written.
fn check_redo_kv_row_images(ops: &[RedoKvOp]) -> crate::Result<()> {
    ops.iter().try_for_each(check_kv_row_image)
}

/// Refuse a KV put whose key and value are over the row image limit.
pub(super) fn check_kv_row_image(op: &RedoKvOp) -> crate::Result<()> {
    if let RedoKvOp::Put {
        collection,
        key,
        value,
    } = op
    {
        crate::engine::kv::KvRowTooLarge::check(collection, key, value)?;
    }
    Ok(())
}

/// One collection's slice of a record, with what its checks read.
pub(super) struct CollectionScope<'a> {
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) collection: &'a str,
    pub(super) config_key: &'a (DatabaseId, TenantId, String),
    pub(super) config: &'a CollectionConfig,
    pub(super) bitemporal: bool,
    pub(super) resolved: &'a [ResolvedSumTarget],
    /// Roles of the identity that committed the record.
    pub(super) user_roles: &'a [String],
    /// The instant retention is judged at, in ms since the Unix epoch.
    pub(super) judged_at_ms: i64,
}

/// The sum targets writes to `collection` fold on this core.
pub(super) fn sum_targets_for(
    sum_targets: &[RedoSumTargets],
    collection: &str,
) -> Vec<ResolvedSumTarget> {
    sum_targets
        .iter()
        .filter(|targets| targets.collection == collection)
        .flat_map(|targets| targets.resolved.iter().cloned())
        .collect()
}
