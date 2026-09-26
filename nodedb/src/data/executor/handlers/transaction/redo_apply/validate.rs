// SPDX-License-Identifier: BUSL-1.1

//! Commit-boundary checks a committed redo record passes before anything of it
//! is written.
//!
//! The transaction batch judged these at its commit and rolled the whole
//! transaction back on a refusal. A redo record applies as absolute
//! post-images with no undo log, so every check runs first, against this
//! core's current state, and a refusal writes nothing. Every replica applies
//! the record at the same log position against the same state, so each one
//! reaches the same verdict.
//!
//! * BALANCED — the signed entries of every document write, judged per
//!   collection across the whole record.
//! * UNIQUE — the record's post-state: a unique value may have one owner among
//!   the rows the record leaves untouched and the rows it writes.
//! * Stateless PUT / DELETE enforcement — append-only, period lock, state
//!   transitions, transition checks, retention and legal hold, each against
//!   the stored pre-image.
//!
//! A [`RedoOrigin::Restore`] record re-installs rows a backup captured. Each
//! row passed BALANCED and the stateless rules when it was first written, and
//! a bitemporal row's earlier versions are history. So a restore runs UNIQUE
//! only. UNIQUE judges the post-state: the last write to each row.

use std::collections::{BTreeMap, HashMap, HashSet};

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::balanced::{self, BalancedEntry};
use crate::data::executor::enforcement::images::RowImages;
use crate::data::executor::handlers::point::apply_delete::run_delete_enforcement;
use crate::data::executor::handlers::point::apply_put::PutEnforcement;
use crate::engine::document::store::{
    CollectionConfig, DocumentEngine, StorageKey, extract_index_values,
};
use crate::types::{DatabaseId, TenantId};

use super::sub_ops::RedoDocOp;

impl CoreLoop {
    /// Refuse `ops` when applying them would break a constraint the
    /// transaction batch checks at commit. Reads only.
    pub(super) fn validate_redo_document_ops(
        &self,
        database_id: u64,
        tid: u64,
        ops: &[RedoDocOp],
        apply_scope: &super::state::RedoApplyScope,
        origin: RedoOrigin,
    ) -> crate::Result<()> {
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
            let (resolved, _) = apply_scope.sum_targets_for(collection);
            let scope = CollectionScope {
                database_id,
                tid,
                collection,
                config_key: &config_key,
                config,
                bitemporal,
                resolved: &resolved,
            };
            if origin == RedoOrigin::Commit {
                self.check_collection_ops(&scope, &collection_ops)?;
            }
            self.check_unique_post_state(&scope, &collection_ops)?;
        }
        Ok(())
    }

    /// Stateless enforcement per write, then BALANCED across the collection.
    fn check_collection_ops(
        &self,
        scope: &CollectionScope<'_>,
        ops: &[&RedoDocOp],
    ) -> crate::Result<()> {
        let enforcement = &scope.config.enforcement;
        let needs_prior = enforcement.has_put_checks()
            || enforcement.balanced.is_some()
            || enforcement.retention.is_some()
            || enforcement.has_legal_hold;
        let mut balanced_entries: Vec<BalancedEntry> = Vec::new();
        for op in ops {
            let prior = if needs_prior {
                self.stored_prior(scope, op.surrogate())?
            } else {
                None
            };
            match op {
                RedoDocOp::Put { value, .. } => {
                    self.check_stateless_put_enforcement(
                        true,
                        PutEnforcement {
                            config_key: scope.config_key,
                            database_id: scope.database_id,
                            tid: scope.tid,
                            collection: scope.collection,
                            value,
                            old_value: &prior,
                            user_roles: &[],
                            resolved_targets: scope.resolved,
                        },
                    )?;
                    if let Some(def) = &enforcement.balanced {
                        let new_doc = doc_format::decode_document(value).ok();
                        let old_doc = match &prior {
                            Some(bytes) => Some(self.decode_stored_document(scope.config, bytes)?),
                            None => None,
                        };
                        let images = match (old_doc.as_ref(), new_doc.as_ref()) {
                            (Some(old_doc), Some(new_doc)) => {
                                Some(RowImages::Update { old_doc, new_doc })
                            }
                            (None, Some(new_doc)) => Some(RowImages::Insert { new_doc }),
                            // A post-image with no readable document carries no
                            // column the definition can read.
                            (_, None) => None,
                        };
                        if let Some(images) = images {
                            balanced_entries.extend(balanced::entries_for(def, &images));
                        }
                    }
                }
                RedoDocOp::Delete { .. } => {
                    // A bitemporal delete judges only a row that exists; a plain
                    // delete judges whatever is stored, as the delete path does.
                    if !scope.bitemporal || prior.is_some() {
                        run_delete_enforcement(
                            &self.sparse,
                            scope.database_id,
                            scope.tid,
                            scope.collection,
                            scope.config,
                            prior.as_deref(),
                            scope.resolved,
                        )?;
                    }
                    if let (Some(def), Some(bytes)) = (&enforcement.balanced, &prior) {
                        let old_doc = self.decode_stored_document(scope.config, bytes)?;
                        balanced_entries.extend(balanced::entries_for(
                            def,
                            &RowImages::Delete { old_doc: &old_doc },
                        ));
                    }
                }
            }
        }
        if let Some(def) = &enforcement.balanced {
            balanced::check_balanced(scope.collection, def, &balanced_entries)?;
        }
        Ok(())
    }

    /// Every unique index value the record's post-state holds has one owner.
    fn check_unique_post_state(
        &self,
        scope: &CollectionScope<'_>,
        ops: &[&RedoDocOp],
    ) -> crate::Result<()> {
        let unique_paths: Vec<_> = scope
            .config
            .index_paths
            .iter()
            .filter(|path| path.unique)
            .collect();
        if unique_paths.is_empty() {
            return Ok(());
        }
        // Rows this record rewrites or removes: their stored values do not
        // count, whatever they are.
        let touched: HashSet<u32> = ops.iter().map(|op| op.surrogate()).collect();
        // The post-state holds each row's last write only. A restored
        // bitemporal row writes each of its versions in order, and a value an
        // earlier version held is not in the post-state.
        let last_write: HashMap<u32, usize> = ops
            .iter()
            .enumerate()
            .map(|(index, op)| (op.surrogate(), index))
            .collect();
        let doc_engine = DocumentEngine::new(&self.sparse, scope.database_id, scope.tid);
        for path in unique_paths {
            // Needle → the surrogate of the record's own row holding it.
            let mut claimed: HashMap<String, u32> = HashMap::new();
            for (index, op) in ops.iter().enumerate() {
                let RedoDocOp::Put {
                    value, surrogate, ..
                } = op
                else {
                    continue;
                };
                if last_write.get(surrogate) != Some(&index) {
                    continue;
                }
                let Ok(doc) = doc_format::decode_document(value) else {
                    continue;
                };
                if let Some(predicate) = &path.predicate
                    && !predicate.evaluate_json(&doc)
                {
                    continue;
                }
                for raw in extract_index_values(&doc, &path.path, path.is_array) {
                    let needle = if path.case_insensitive {
                        raw.to_lowercase()
                    } else {
                        raw
                    };
                    let violation = || crate::Error::RejectedConstraint {
                        collection: scope.collection.to_string(),
                        constraint: "unique".to_string(),
                        detail: format!(
                            "unique index '{}' violation on field '{}' (value '{}')",
                            path.name, path.path, needle
                        ),
                    };
                    if let Some(owner) = claimed.insert(needle.clone(), *surrogate)
                        && owner != *surrogate
                    {
                        return Err(violation());
                    }
                    let stored_owners = doc_engine.index_lookup(
                        scope.collection,
                        &path.path,
                        &needle,
                        scope.bitemporal,
                    )?;
                    let foreign_owner = stored_owners.iter().any(|key: &StorageKey| {
                        let owner = key.surrogate().as_u32();
                        owner != *surrogate && !touched.contains(&owner)
                    });
                    if foreign_owner {
                        return Err(violation());
                    }
                }
            }
        }
        Ok(())
    }

    /// The row as stored now, in the store this collection writes.
    fn stored_prior(
        &self,
        scope: &CollectionScope<'_>,
        surrogate: u32,
    ) -> crate::Result<Option<Vec<u8>>> {
        let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
        if scope.bitemporal {
            self.sparse
                .versioned_get_current(scope.database_id, scope.tid, scope.collection, &key)
        } else {
            self.sparse
                .get(scope.database_id, scope.tid, scope.collection, &key)
        }
    }
}

/// One collection's slice of a record, with what its checks read.
struct CollectionScope<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    config_key: &'a (DatabaseId, TenantId, String),
    config: &'a CollectionConfig,
    bitemporal: bool,
    resolved: &'a [nodedb_physical::physical_plan::ResolvedSumTarget],
}
