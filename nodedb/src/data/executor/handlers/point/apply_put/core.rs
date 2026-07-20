// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointPut inside an externally-owned transaction" helper.
//!
//! This is called by PointPut and by any composite path (triggers, UPSERT)
//! that needs document write + index + stats side-effects atomically.

use redb::WriteTransaction;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::{
    append_only, period_lock, state_transition, transition_check,
};
use crate::data::executor::handlers::generated;
use crate::data::executor::{doc_format, strict_format};

use super::types::{PointPutOutcome, PointPutParams, map_enforcement_error};
use super::unique::{UniqueCheck, check_unique_constraints};

impl CoreLoop {
    /// Apply a PointPut within an externally-owned WriteTransaction.
    ///
    /// Stores the document, auto-indexes text fields, updates column stats,
    /// and populates the document cache. Does NOT commit the transaction.
    ///
    /// `surrogate` is the stable numeric identity for this document, used
    /// to key the inverted index. `document_id` is the hex-encoded form of
    /// the surrogate (the redb storage key).
    ///
    /// Returns a [`PointPutOutcome`] capturing the prior stored bytes (present
    /// when this put replaced an existing row) plus the bitemporal system time
    /// and versioned index tuples written, so a transactional caller can build
    /// a fully-reversible undo entry. Autocommit callers read only
    /// `prior_value` and thread it into `emit_write_event` so the Event Plane's
    /// `WriteOp` tag reflects the actual mutation.
    pub(in crate::data::executor) fn apply_point_put(
        &mut self,
        txn: &WriteTransaction,
        params: PointPutParams<'_>,
    ) -> crate::Result<PointPutOutcome> {
        let PointPutParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            value,
            index_text,
            user_roles,
            enforce,
            wal_lsn,
        } = params;
        // Evaluate generated columns before encoding.
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let value = if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            if let Some(mut doc) = doc_format::decode_document(value) {
                if let Err(e) = generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                ) {
                    return Err(crate::Error::Storage {
                        engine: "generated".into(),
                        detail: format!("generated column evaluation failed: {e:?}"),
                    });
                }
                doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            doc_format::canonicalize_document_for_storage(value)
        };
        let value = &value;

        // A resolve-time stamp carried in `active_bitemporal_stamps` (present on
        // the commit-time base install and on WAL replay of an 8-tuple document
        // redo) forces the versioned branch at the EXACT stamp the redo carries,
        // independent of `doc_configs` — which is empty during WAL replay. This
        // is what keeps a normal restart from writing a SECOND version of the
        // row and a crash-window restart from landing it on the plain table.
        // Absent an override, keep the autocommit behavior: derive bitemporality
        // from config and mint a fresh monotonic stamp.
        let (bitemporal, sys_from_ms, valid_from_ms, valid_until_ms) =
            match self.active_bitemporal_stamps.get(&surrogate.as_u32()) {
                Some(stamp) => (
                    true,
                    stamp.sys_from_ms,
                    stamp.valid_from_ms,
                    stamp.valid_until_ms,
                ),
                None => (
                    self.is_bitemporal(database_id, tid, collection),
                    self.bitemporal_now_ms(),
                    i64::MIN,
                    i64::MAX,
                ),
            };

        // Strict (Binary Tuple) encoding pipeline. Runs in two steps under
        // a single doc-config lookup:
        //   (1) When the schema has an auto-generated `_rowid` primary key
        //       (injected by `build_strict_schema` when no explicit PK is
        //       declared), the client INSERT payload won't contain it.
        //       Inject it from the surrogate before encoding so the NOT NULL
        //       constraint is satisfied.
        //   (2) Encode the (possibly-injected) MessagePack into Binary Tuple.
        // Downstream indexing reads the rebound `value` so it sees the
        // injected `_rowid` alongside the user's fields.
        let value_with_rowid: Vec<u8>;
        let (value, stored): (&[u8], Vec<u8>) = if let Some(config) =
            self.doc_configs.get(&config_key)
            && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                config.storage_mode
        {
            let encoded_input: &[u8] = if schema
                .columns
                .first()
                .is_some_and(|c| c.name == "_rowid" && !c.nullable)
                && let Ok(mut decoded) = nodedb_types::json_from_msgpack(value)
                && let serde_json::Value::Object(ref mut obj) = decoded
                && !obj.contains_key("_rowid")
            {
                obj.insert(
                    "_rowid".to_string(),
                    serde_json::Value::Number((surrogate.0 as i64).into()),
                );
                value_with_rowid =
                    nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.to_vec());
                &value_with_rowid
            } else {
                value
            };

            let stored = if bitemporal && schema.bitemporal {
                strict_format::bytes_to_binary_tuple_bitemporal(
                    encoded_input,
                    schema,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                )
            } else {
                strict_format::bytes_to_binary_tuple(encoded_input, schema)
            }
            .map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e.to_string(),
            })?;

            (encoded_input, stored)
        } else {
            (value, value.to_vec())
        };

        // Read the prior stored value before the write lands, but only when
        // something downstream actually needs it: bitemporal collections
        // always need the current version (it becomes `prior` below), and
        // enforcement-configured collections need it to feed the stateless
        // PUT checks. The common case (non-bitemporal, no put-enforcement
        // configured) skips this read entirely — `prior` for that case
        // comes solely from `put_in_txn`'s own return value.
        //
        // The plain (non-bitemporal) secondary-index diff also needs the old
        // bytes: an UPDATE that changes an indexed field must drop the stale
        // index entry, which requires knowing the prior value. So read the old
        // value whenever the collection has index paths — exactly the case that
        // would otherwise leak stale entries.
        let need_old = bitemporal
            || (enforce
                && self
                    .doc_configs
                    .get(&config_key)
                    .is_some_and(|config| config.enforcement.has_put_checks()))
            || self
                .doc_configs
                .get(&config_key)
                .is_some_and(|config| !config.index_paths.is_empty());
        let old_value = if bitemporal {
            self.sparse
                .versioned_get_current(database_id, tid, collection, document_id)?
        } else if need_old {
            self.sparse.get(database_id, tid, collection, document_id)?
        } else {
            None
        };
        // Decode the pre-write document for the non-bitemporal secondary-index
        // SET diff. Borrowed here (before `old_value` may be moved into `prior`
        // on the bitemporal branch below); bitemporal reverses via versioned
        // index tuples instead, so it needs no old-doc diff.
        // Strict collections store the old row as a Binary Tuple, which
        // `doc_format::decode_document` cannot decode without the schema —
        // route through the storage-mode-aware helper so strict UPDATEs also
        // compute their real old index values (and thus drop stale entries).
        let old_doc_for_index: Option<serde_json::Value> = if bitemporal {
            None
        } else {
            match (old_value.as_ref(), self.doc_configs.get(&config_key)) {
                (Some(b), Some(config)) => self.decode_stored_document(config, b),
                _ => None,
            }
        };

        // Stateless PUT enforcement, unified across the autocommit
        // (`apply_point_put`) and transactional (`tx_point_put`) paths.
        // These checks have no persistent side effect, so a violation here
        // simply aborts before the write — safe even though the caller
        // owns a single redb write transaction. Reuses `config_key` from
        // the generated-columns lookup above.
        //
        // Skipped entirely for CRDT-sync materialization (`enforce ==
        // false`): those deltas already passed admission on their origin
        // replica at Raft commit time.
        if enforce && let Some(config) = self.doc_configs.get(&config_key) {
            append_only::check_point_put(collection, &config.enforcement, &old_value)
                .map_err(map_enforcement_error)?;
            if let Some(ref pl) = config.enforcement.period_lock {
                period_lock::check_period_lock(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    value,
                    pl,
                )
                .map_err(map_enforcement_error)?;
            }
            if old_value.is_some() {
                let old_json = old_value
                    .as_ref()
                    .and_then(|b| doc_format::decode_document(b));
                let new_json = doc_format::decode_document(value);
                if let (Some(old_doc), Some(new_doc)) = (&old_json, &new_json) {
                    if !config.enforcement.state_constraints.is_empty() {
                        state_transition::check_state_transitions(
                            collection,
                            &config.enforcement.state_constraints,
                            old_doc,
                            new_doc,
                            user_roles,
                        )
                        .map_err(map_enforcement_error)?;
                    }
                    if !config.enforcement.transition_checks.is_empty() {
                        transition_check::check_transition_predicates(
                            collection,
                            &config.enforcement.transition_checks,
                            old_doc,
                            new_doc,
                        )
                        .map_err(map_enforcement_error)?;
                    }
                }
            }
        }

        // Bitemporal collections version every write: append a new version
        // at `sys_from = now()`, returning the current (pre-write) version
        // read above as the `prior` slot. Non-bitemporal collections use
        // the legacy overwrite path, returning the old bytes redb replaced.
        let prior = if bitemporal {
            self.sparse.versioned_put_in_txn(
                txn,
                crate::engine::sparse::btree_versioned::VersionedPut {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    doc_id: document_id,
                    sys_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                    body: &stored,
                },
            )?;
            old_value
        } else {
            self.sparse
                .put_in_txn(txn, database_id, tid, collection, document_id, &stored)?
        };

        // Pre-image capture for the column-stats read-modify-write, so a
        // transactional caller can restore the exact prior stats on rollback.
        let mut stats_prior: Vec<crate::engine::sparse::stats::StatsPreImage> = Vec::new();

        // Text indexing and stats use the original JSON input, not the stored
        // bytes — Binary Tuple requires a schema to decode, and the input JSON
        // is already available here regardless of storage mode.
        if let Some(doc) = doc_format::decode_document(value) {
            // Shared extraction: the DELETE-rollback re-index path recomputes
            // the exact same text from the restored body via this helper.
            let text_content = crate::data::executor::fts_text::extract_fts_text(&doc);
            if index_text
                && !text_content.is_empty()
                && let Err(e) = self.inverted.index_document_in_txn(
                    txn,
                    crate::engine::sparse::inverted::IndexDocScope {
                        database_id,
                        tid: crate::types::TenantId::new(tid),
                        collection,
                        surrogate,
                    },
                    &text_content,
                )
            {
                warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index update failed");
            }

            match self
                .stats_store
                .observe_document_in_txn(txn, database_id, tid, collection, &doc)
            {
                Ok(pre) => stats_prior = pre,
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "column stats update failed");
                }
            }

            self.invalidate_aggregate_cache_for_collection(database_id, tid, collection);
        }

        self.doc_cache
            .put(database_id, tid, collection, document_id, &stored);

        // Secondary index extraction: if this collection has registered
        // index paths, extract values and write them into the INDEXES redb
        // B-Tree inside the CALLER'S write txn. Using the non-_in_txn
        // variant here would deadlock — `execute_point_put` already owns
        // the only writer.
        //
        // UNIQUE enforcement runs first: for every `unique: true` path we
        // check whether the incoming value already belongs to a different
        // document and reject with a typed constraint error. The check
        // uses the sparse engine's read API, which opens a separate read
        // transaction (redb MVCC) — the read view won't see our outer
        // write txn but that's precisely the semantics we want for the
        // "does another row already hold this value" question.
        let mut bitemporal_index_tuples: Vec<(String, String)> = Vec::new();
        let mut secondary_index_added: Vec<(String, String)> = Vec::new();
        let mut secondary_index_removed: Vec<(String, String)> = Vec::new();
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if let Some(config) = self.doc_configs.get(&config_key)
            && let Some(doc) = doc_format::decode_document(value)
        {
            let paths = config.index_paths.clone();
            // UNIQUE enforcement is a CORE side-effect: it must run in both the
            // autocommit and transactional paths (a violation rejects the write
            // before commit).
            check_unique_constraints(UniqueCheck {
                sparse: &self.sparse,
                database_id,
                tid,
                collection,
                doc: &doc,
                document_id,
                paths: &paths,
                bitemporal,
            })?;
            if bitemporal {
                // Versioned index entries are keyed at the SAME system time as
                // the primary version row written above (`sys_from_ms`), so a
                // single `bitemporal_sys_from_ms` in the undo entry reverses
                // both together. These are CORE (undoable via the captured
                // tuples).
                for path in &paths {
                    if let Some(ref pred) = path.predicate
                        && !pred.evaluate_json(&doc)
                    {
                        continue;
                    }
                    for v in crate::engine::document::store::extract_index_values(
                        &doc,
                        &path.path,
                        path.is_array,
                    ) {
                        let value = if path.case_insensitive {
                            v.to_lowercase()
                        } else {
                            v
                        };
                        self.sparse.versioned_index_put_in_txn(
                            txn,
                            crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                database_id,
                                tenant: tid,
                                coll: collection,
                                field: &path.path,
                                value: &value,
                                doc_id: document_id,
                                sys_from_ms,
                            },
                        )?;
                        bitemporal_index_tuples.push((path.path.clone(), value));
                    }
                }
            } else {
                // Non-bitemporal secondary index write. The
                // SET diff against `old_doc_for_index` inserts new values and
                // removes stale ones (fixing the leaked-entry-on-UPDATE bug).
                // The (added, removed) tuples are captured so a transactional
                // caller can reverse them on rollback.
                let (added, removed) = self.apply_secondary_indexes_in_txn(
                    txn,
                    crate::data::executor::core_loop::maintenance::SecondaryIndexInputs {
                        database_id,
                        tid,
                        collection,
                        old_doc: old_doc_for_index.as_ref(),
                        new_doc: &doc,
                        doc_id: document_id,
                        index_paths: &paths,
                    },
                );
                secondary_index_added = added;
                secondary_index_removed = removed;
            }
        }

        let spatial_inserts =
            self.apply_point_put_spatial(database_id, tid, collection, document_id, value);
        let vector_inserts = self.apply_point_put_vector_indexes(
            crate::data::executor::handlers::point::apply_put::VectorIndexPutParams {
                database_id,
                tid,
                collection,
                document_id,
                surrogate,
                value,
                wal_lsn: wal_lsn.map(|l| l.as_u64()).unwrap_or(0),
            },
        );
        // Sparse inverted-index maintenance mirrors the dense-vector side-effect
        // above: a no-op unless the strict schema declares a `SparseVector`
        // column, so non-sparse collections are byte-identical to before.
        self.apply_point_put_sparse_indexes(database_id, tid, collection, document_id, value);

        Ok(PointPutOutcome {
            prior_value: prior,
            bitemporal_sys_from_ms: if bitemporal { Some(sys_from_ms) } else { None },
            bitemporal_index_tuples,
            secondary_index_added,
            secondary_index_removed,
            vector_inserts,
            spatial_inserts,
            stats_prior,
        })
    }
}
