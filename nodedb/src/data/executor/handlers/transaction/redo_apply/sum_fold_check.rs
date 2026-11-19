// SPDX-License-Identifier: BUSL-1.1

//! The materialized-sum folds a committed record's install runs, judged
//! before any write.
//!
//! The install writes each document row, then folds the row's delta into
//! every target row its bindings name on this core (`apply_materialized_sums`).
//! A fold refuses when:
//!
//! - the record carries no resolution for a join value it folds;
//! - the target collection stores vector-primary sidecars;
//! - the target row is absent, or is no object;
//! - the target row's new images are over the row image limit, measured on
//!   the bytes the install builds;
//! - the target row's new balance gives a UNIQUE value two owners.
//!
//! A refusal there rolls the record back after part of it ran. So the check
//! replays the folds over the record's images, in record order, against the
//! stored target rows and the balances the earlier folds left, and refuses
//! the record first. The Calvin stage runs it through `check_commit_boundary`
//! on the same writes, under the target rows' locks.

use std::borrow::Cow;

use rust_decimal::Decimal;

use crate::bridge::envelope::RowVersion;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::images::RowImages;
use crate::data::executor::enforcement::materialized_sum::apply::coalesce;
use crate::data::executor::enforcement::materialized_sum::delta::{
    fold_sum_deltas, json_to_decimal,
};
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::enforcement::unique::{claimed_values, violation};
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::handlers::point::apply_put::stored_body::StoredBodyInput;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::engine::document::store::{DocumentEngine, StorageKey};
use crate::types::SharedInstalledDocument;
use crate::types::{DatabaseId, TenantId};

use super::folded_store::{FoldedRow, FoldedStore, FoldedTargets};
use super::sub_ops::RedoDocOp;
use super::validate::CollectionScope;

impl CoreLoop {
    /// Refuse `ops` of one source collection when a fold their install runs
    /// on this core would refuse. Reads only.
    pub(super) fn check_sum_folds(
        &self,
        scope: &CollectionScope<'_>,
        ops: &[&RedoDocOp],
    ) -> crate::Result<()> {
        if scope.config.enforcement.materialized_sum_sources.is_empty() {
            return Ok(());
        }
        let mut folded = FoldedTargets::new();
        for op in ops {
            self.check_op_folds(scope, &mut folded, op)?;
        }
        Ok(())
    }

    /// Refuse `op` of one source collection when a fold its install runs on
    /// this core would refuse, against the target rows the record's earlier
    /// folds left in `folded`. Records the rows its folds leave there.
    /// Reads only.
    pub(super) fn check_op_folds(
        &self,
        scope: &CollectionScope<'_>,
        folded: &mut impl FoldedStore,
        op: &RedoDocOp,
    ) -> crate::Result<()> {
        let bindings = &scope.config.enforcement.materialized_sum_sources;
        if bindings.is_empty() {
            return Ok(());
        }
        let source = nodedb_types::CollectionKey::from_qualified_str(
            DatabaseId::new(scope.database_id),
            scope.collection,
        )?;
        let crs = self.crs_context();
        let key = StorageKey::for_surrogate(nodedb_types::Surrogate::new(op.surrogate()));
        let prior = self.current_row_body(scope.database_id, scope.tid, scope.collection, &key)?;
        let old_doc = match &prior {
            Some(bytes) => Some(self.decode_stored_document(scope.config, bytes)?),
            None => None,
        };
        let new_doc = match op {
            RedoDocOp::Put { value, .. } => Some(self.unique_image(scope.config, value)?),
            RedoDocOp::Delete { .. } => None,
        };
        let images = match (old_doc.as_ref(), new_doc.as_ref()) {
            (Some(old_doc), Some(new_doc)) => RowImages::Update { old_doc, new_doc },
            (None, Some(new_doc)) => RowImages::Insert { new_doc },
            (Some(old_doc), None) => RowImages::Delete { old_doc },
            // A delete of a row that is not stored folds nothing.
            (None, None) => return Ok(()),
        };
        for binding in bindings {
            let co_resident =
                crate::query::sum_target_is_co_resident(source, &binding.target_collection);
            for (join_value, delta) in coalesce(fold_sum_deltas(binding, &images, &crs)?) {
                if delta == Decimal::ZERO {
                    continue;
                }
                let resolved = scope.resolved.iter().find(|target| {
                    target.target_collection == binding.target_collection
                        && target.join_value == join_value
                });
                // A cross-shard target with no resolution travels on its own
                // task.
                if !co_resident && resolved.is_none() {
                    continue;
                }
                let Some(target) = resolved else {
                    return Err(crate::Error::MaterializedSumResolutionMissing {
                        target_collection: binding.target_collection.clone(),
                        join_column: binding.join_column.clone(),
                        join_value,
                    });
                };
                self.check_one_fold(
                    scope,
                    folded,
                    FoldStep {
                        target_collection: &binding.target_collection,
                        target_column: &binding.target_column,
                        join_column: &binding.join_column,
                        join_value: &join_value,
                        declared_primary_key: binding.declared_primary_key.as_deref(),
                        surrogate: target.surrogate.as_u32(),
                        delta,
                    },
                )?;
            }
        }
        Ok(())
    }

    /// Judge one fold of `step.delta` into its target row, and record the
    /// row it leaves in `folded`.
    fn check_one_fold(
        &self,
        scope: &CollectionScope<'_>,
        folded: &mut impl FoldedStore,
        step: FoldStep<'_>,
    ) -> crate::Result<()> {
        let database_id = DatabaseId::new(scope.database_id);
        let tenant = TenantId::new(scope.tid);
        let format = self.sparse_body_format(database_id, tenant, step.target_collection);
        if matches!(format, SparseBodyFormat::VectorSidecar) {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target collection '{}' is vector-primary; its rows are metadata \
                     sidecars and cannot carry a materialized sum",
                    step.target_collection
                ),
            });
        }
        let folded_row = folded.folded(step.target_collection, step.surrogate)?;
        let (mut target_doc, prior_stored) = match folded_row.as_deref() {
            Some(row) => (row.doc.clone(), Cow::Borrowed(row.stored.as_slice())),
            None => {
                let key = StorageKey::for_surrogate(nodedb_types::Surrogate::new(step.surrogate));
                let Some(bytes) = self.current_row_body(
                    scope.database_id,
                    scope.tid,
                    step.target_collection,
                    &key,
                )?
                else {
                    return Err(crate::Error::MaterializedSumTargetNotFound {
                        target_collection: step.target_collection.to_string(),
                        join_column: step.join_column.to_string(),
                        join_value: step.join_value.to_string(),
                    });
                };
                (
                    decode_scanned_document(&bytes, format.as_format_ref())?,
                    Cow::Owned(bytes),
                )
            }
        };
        let current = target_doc
            .get(step.target_column)
            .and_then(json_to_decimal)
            .unwrap_or(Decimal::ZERO);
        let Some(object) = target_doc.as_object_mut() else {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target row {}/{} is not an object",
                    step.target_collection,
                    StorageKey::for_surrogate(nodedb_types::Surrogate::new(step.surrogate))
                ),
            });
        };
        object.insert(
            step.target_column.to_string(),
            serde_json::Value::String((current + step.delta).to_string()),
        );
        let stored = self.check_folded_image_size(scope, &step, &target_doc, &prior_stored)?;
        drop(prior_stored);
        drop(folded_row);
        self.check_folded_unique(
            scope,
            &*folded,
            step.target_collection,
            step.surrogate,
            &target_doc,
        )?;
        folded.put(
            step.target_collection,
            step.surrogate,
            FoldedRow {
                doc: target_doc,
                stored,
            },
        )
    }

    /// Refuse a folded target row whose images are over the row image
    /// limit. The images are the ones the install's `apply_balance_delta`
    /// writes: the body `build_stored_body` builds from the folded document,
    /// judged by `apply_point_put`'s own `check_installed_image_size`, and the
    /// row's event images, judged by `prepare_installed_put_event`. Answers
    /// the stored bytes, which a later fold of the row reads as its prior.
    ///
    /// The version stamp is the boundary's ordinal. Every live ordinal
    /// encodes at one width, so the images measure what the install's own
    /// stamp measures.
    fn check_folded_image_size(
        &self,
        scope: &CollectionScope<'_>,
        step: &FoldStep<'_>,
        target_doc: &serde_json::Value,
        prior_stored: &[u8],
    ) -> crate::Result<Vec<u8>> {
        let config_key = (
            DatabaseId::new(scope.database_id),
            TenantId::new(scope.tid),
            step.target_collection.to_string(),
        );
        let surrogate = nodedb_types::Surrogate::new(step.surrogate);
        let storage_key = StorageKey::for_surrogate(surrogate);
        let submitted = doc_format::encode_to_msgpack(target_doc);
        let installed = self.folded_target_image(&config_key, surrogate, &submitted)?;
        self.check_installed_image_size(
            scope.database_id,
            scope.tid,
            step.target_collection,
            storage_key,
            &installed,
        )?;
        let identity = nodedb_types::RowIdentity::of_stored_row(
            &submitted,
            step.declared_primary_key,
            storage_key,
        );
        self.prepare_installed_put_event(
            &WriteSetRow {
                database_id: scope.database_id,
                tid: scope.tid,
                collection: step.target_collection,
                identity,
            },
            &installed,
            Some(prior_stored),
        )?;
        Ok(installed.stored)
    }

    /// The image the install's `apply_point_put` builds for the folded target
    /// body `submitted`: its frozen body, with the guards it injects and the
    /// version stamp of a write that carries none.
    pub(in crate::data::executor) fn folded_target_image(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        surrogate: nodedb_types::Surrogate,
        submitted: &[u8],
    ) -> crate::Result<SharedInstalledDocument> {
        let sys_from = self
            .apply_scope
            .calvin_txn_ordinal
            .or(self.apply_scope.replicated_ordinal)
            .unwrap_or(i64::MAX);
        let version = RowVersion {
            sys_from,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        };
        let body = self.build_stored_body(StoredBodyInput {
            config_key,
            surrogate,
            value: submitted,
            bitemporal: self.is_bitemporal(
                config_key.0.as_u64(),
                config_key.1.as_u64(),
                &config_key.2,
            ),
            sys_from: version.sys_from,
            valid_from_ms: version.valid_from_ms,
            valid_until_ms: version.valid_until_ms,
            guards: GuardScope::InjectOnly,
        })?;
        Ok(SharedInstalledDocument {
            canonical: body.value,
            stored: body.stored,
            version,
            schema: self.capture_storage_schema(config_key)?,
            chain_head: None,
        })
    }

    /// Judge the UNIQUE values of one folded target row, against the stored
    /// rows and the target rows earlier folds of the record left.
    ///
    /// A value of the row is free when no other folded row claims it and
    /// every stored row that holds it is a folded row: the folds replace
    /// those rows' images. The check names the first taken value, path by
    /// path, in value order: first among the folded rows, then among the
    /// stored rows.
    fn check_folded_unique(
        &self,
        scope: &CollectionScope<'_>,
        folded: &impl FoldedStore,
        collection: &str,
        surrogate: u32,
        target_doc: &serde_json::Value,
    ) -> crate::Result<()> {
        let config_key = (
            DatabaseId::new(scope.database_id),
            TenantId::new(scope.tid),
            collection.to_string(),
        );
        let Some(config) = self.judged_doc_config(&config_key) else {
            return Ok(());
        };
        if !config.index_paths.iter().any(|path| path.unique) {
            return Ok(());
        }
        // The install indexes the encoded body, with its generated columns.
        let image = self.unique_image(config, &doc_format::encode_to_msgpack(target_doc))?;
        let crs = self.crs_context();
        let doc_engine = DocumentEngine::new(&self.sparse, scope.database_id, scope.tid);
        for path in config.index_paths.iter().filter(|path| path.unique) {
            let needles = claimed_values(&image, path, &crs);
            if let Some(needle) =
                folded.first_claimed_by_other(collection, surrogate, path, &needles, &crs)?
            {
                return Err(violation(collection, path, &needle));
            }
            for needle in &needles {
                let stored = doc_engine.index_lookup(collection, &path.path, needle, None)?;
                for key in &stored {
                    let holder = key.surrogate().as_u32();
                    if holder != surrogate && !folded.holds(collection, holder)? {
                        return Err(violation(collection, path, needle));
                    }
                }
            }
        }
        Ok(())
    }
}

/// One fold the install runs: `delta` onto the target row `surrogate`.
struct FoldStep<'a> {
    target_collection: &'a str,
    target_column: &'a str,
    join_column: &'a str,
    join_value: &'a str,
    /// The target collection's declared primary key, which names the row
    /// in its event.
    declared_primary_key: Option<&'a str>,
    surrogate: u32,
    delta: Decimal,
}
