// SPDX-License-Identifier: BUSL-1.1

//! One document op's collection checks, run as the stream reaches it, and
//! each collection's whole-record checks once the stream ended.
//!
//! `validate_redo_document_ops` judges each collection in name order: the
//! stateless rules op by op, then BALANCED, then UNIQUE, then the folds op
//! by op. A record from `RedoOrigin::Restore` runs UNIQUE only. The stream
//! runs the op-by-op parts at each op and folds the rest into the
//! post-state, and every error enters the verdict at its collection, phase
//! and op.

use nodedb_physical::physical_plan::RedoOrigin;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::unique::claimed_values;
use crate::data::executor::enforcement::unique_paths::{UniqueTarget, judged_unique_paths};
use crate::engine::document::store::DocumentEngine;
use crate::types::{DatabaseId, TenantId};

use super::super::sub_ops::RedoDocOp;
use super::super::validate::{CollectionScope, sum_targets_for};
use super::folds::SpilledFolds;
use super::unique::UniqueFinal;
use super::verdict::{Phase, Rank};
use super::walk::{CollectionTrack, StreamValidateInput, StreamWalk};

/// The refusal of a record whose spilled post-state failed on this node.
/// The failure is no verdict on the record, so the refusal is retryable.
pub(super) fn post_state_failed(detail: impl std::fmt::Display) -> ErrorCode {
    ErrorCode::RetryableRefusal {
        reason: format!(
            "the streamed validate of a committed redo failed to keep its post-state on this \
             node: {detail}"
        ),
    }
}

impl CoreLoop {
    /// Run the collection checks of document op `op`, which a clone copy
    /// skip left in.
    pub(super) fn check_stream_collection_op(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        op: u64,
        doc: &RedoDocOp,
    ) {
        let collection = doc.collection();
        if !walk
            .verdict
            .admits_collection(collection, Phase::Stateless, op)
        {
            return;
        }
        let database_id = input.task.request.database_id.as_u64();
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(input.tid),
            collection.to_string(),
        );
        // An unregistered collection declares no constraint to check.
        let Some(config) = self.judged_doc_config(&config_key) else {
            return;
        };
        let Some(header) = &walk.header else {
            return;
        };
        if !walk.collections.contains_key(collection) {
            let unique = judged_unique_paths(
                &self.maintained_indexes,
                UniqueTarget {
                    database_id,
                    tid: input.tid,
                    collection,
                },
                &config.index_paths,
            )
            .map(|paths| paths.into_iter().cloned().collect::<Vec<_>>())
            .map_err(ErrorCode::from);
            let track = CollectionTrack {
                bitemporal: self.is_bitemporal(database_id, input.tid, collection),
                resolved: sum_targets_for(&header.sum_targets, collection),
                unique,
            };
            walk.collections.insert(collection.to_string(), track);
        }
        let Some(track) = walk.collections.get(collection) else {
            return;
        };
        let scope = CollectionScope {
            database_id,
            tid: input.tid,
            collection,
            config_key: &config_key,
            config,
            bitemporal: track.bitemporal,
            resolved: &track.resolved,
            user_roles: &header.user_roles,
            judged_at_ms: input.judged_at_ms,
        };
        let commit = input.origin == RedoOrigin::Commit;
        let verdict = &mut walk.verdict;
        let post = &mut walk.post;

        // The entry check above found the stateless rank admitted, and no
        // offer has changed the verdict since.
        if commit {
            match self.check_op_enforcement(&scope, doc) {
                Err(error) => verdict.offer(
                    Rank::Collection(collection.to_string(), Phase::Stateless, op),
                    ErrorCode::from(error),
                ),
                Ok(entries) => {
                    if let Some(def) = &config.enforcement.balanced
                        && verdict.admits_collection(collection, Phase::Balanced, 0)
                        && let Err(error) = post.add_balanced(collection, def, &entries)
                    {
                        verdict.offer(Rank::Local, post_state_failed(error));
                    }
                }
            }
        }

        if config.index_paths.iter().any(|path| path.unique)
            && verdict.admits_collection(collection, Phase::UniqueImage, op)
        {
            post.note_written(collection, doc.surrogate(), op);
            if let RedoDocOp::Put { value, .. } = doc {
                match self.unique_image(config, value) {
                    Err(error) => verdict.offer(
                        Rank::Collection(collection.to_string(), Phase::UniqueImage, op),
                        ErrorCode::from(error),
                    ),
                    // A stale pinned path refuses the collection at its
                    // UNIQUE finalize, so its claims are not kept.
                    Ok(image) => {
                        if let Ok(paths) = &track.unique {
                            let crs = self.crs_context();
                            for (position, path) in (0u32..).zip(paths) {
                                for value in claimed_values(&image, path, &crs) {
                                    post.claim(collection, position, value, op, doc.surrogate());
                                }
                            }
                        }
                    }
                }
            }
        }

        if commit
            && !config.enforcement.materialized_sum_sources.is_empty()
            && verdict.admits_collection(collection, Phase::Fold, op)
        {
            let mut folds = SpilledFolds::new(collection, post);
            if let Err(error) = self.check_op_folds(&scope, &mut folds, doc) {
                match folds.take_local() {
                    Some(detail) => verdict.offer(Rank::Local, post_state_failed(detail)),
                    None => verdict.offer(
                        Rank::Collection(collection.to_string(), Phase::Fold, op),
                        ErrorCode::from(error),
                    ),
                }
            }
        }
    }

    /// Judge BALANCED and UNIQUE of every collection the stream wrote, in
    /// name order, on the post-state.
    pub(super) fn finish_stream_collections(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
    ) {
        let database_id = input.task.request.database_id.as_u64();
        let commit = input.origin == RedoOrigin::Commit;
        for (collection, track) in &walk.collections {
            let config_key = (
                DatabaseId::new(database_id),
                TenantId::new(input.tid),
                collection.clone(),
            );
            let Some(config) = self.judged_doc_config(&config_key) else {
                continue;
            };
            if commit
                && config.enforcement.balanced.is_some()
                && walk
                    .verdict
                    .admits_collection(collection, Phase::Balanced, 0)
            {
                match walk.post.balanced_violation(collection) {
                    Ok(Some(error)) => walk.verdict.offer(
                        Rank::Collection(collection.clone(), Phase::Balanced, 0),
                        ErrorCode::from(error),
                    ),
                    Ok(None) => {}
                    Err(error) => walk.verdict.offer(Rank::Local, post_state_failed(error)),
                }
            }
            if config.index_paths.iter().any(|path| path.unique)
                && walk.verdict.admits_collection(collection, Phase::Unique, 0)
            {
                let paths = match &track.unique {
                    Ok(paths) => paths,
                    Err(refusal) => {
                        walk.verdict.offer(
                            Rank::Collection(collection.clone(), Phase::Unique, 0),
                            refusal.clone(),
                        );
                        continue;
                    }
                };
                let scope = UniqueFinal {
                    collection,
                    paths,
                    doc_engine: DocumentEngine::new(&self.sparse, database_id, input.tid),
                };
                match walk.post.unique_violation(&scope) {
                    Ok(Some(error)) => walk.verdict.offer(
                        Rank::Collection(collection.clone(), Phase::Unique, 0),
                        ErrorCode::from(error),
                    ),
                    Ok(None) => {}
                    Err(error) => walk.verdict.offer(Rank::Local, post_state_failed(error)),
                }
            }
        }
    }
}
