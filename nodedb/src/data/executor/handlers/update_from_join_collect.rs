// SPDX-License-Identifier: BUSL-1.1

//! Shared match-and-resolve pass for `DocumentOp::UpdateFromJoin`.
//!
//! Split out of `update_from_join.rs` to keep each file within the size limit.
//! Scans the target collection, joins each row against the pre-built source
//! join-map, evaluates the `SET` assignments against the merged document,
//! recomputes generated columns, and encodes each matched row's post-image —
//! WITHOUT touching storage. Both the write path and the COMMIT-time RESOLVE
//! pass consume the resulting [`ResolvedUpdateRow`]s, so the two can never
//! diverge on which rows match or what post-image each carries (mirrors how
//! `collect_merge_plan` is shared between the MERGE resolve and apply passes).

use nodedb_types::columnar::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::join_source_map::JoinSource;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::UpdateValue;

use super::update_from_join::ResolvedUpdateRow;

/// Borrowed inputs for [`CoreLoop::collect_update_from_join_rows`], bundled to
/// keep the shared classifier's signature within argument limits.
pub(in crate::data::executor) struct CollectUpdateRows<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub target_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub source_map: &'a JoinSource,
    pub target_filters: &'a [ScanFilter],
    pub strict_schema: Option<&'a StrictSchema>,
    pub config_key: &'a (DatabaseId, TenantId, String),
    /// Declared `PRIMARY KEY` column of a schemaless target, `None`
    /// otherwise. `Some` makes the post-image guard below run.
    pub declared_primary_key: Option<&'a str>,
    /// The system time the statement's new versions land at on a bitemporal
    /// target, `None` on any other. A bitemporal target is read at its
    /// current versions, and a strict one encodes the time into each tuple.
    pub bitemporal_sys_from: Option<i64>,
}

impl CoreLoop {
    /// Resolve every target row matched by the join into its post-image without
    /// writing. Shared by the write path and the RESOLVE pass.
    pub(in crate::data::executor) fn collect_update_from_join_rows(
        &self,
        ctx: CollectUpdateRows<'_>,
    ) -> crate::Result<Vec<ResolvedUpdateRow>> {
        let CollectUpdateRows {
            task,
            tid,
            target_collection,
            source_alias,
            target_join_col,
            updates,
            source_map,
            target_filters,
            strict_schema,
            config_key,
            declared_primary_key,
            bitemporal_sys_from,
        } = ctx;
        let database_id = task.request.database_id.as_u64();
        // Read the TARGET as the transaction's CURRENT view = base ∪ overlay:
        // `None` (autocommit write path) is base-only; `Some(txn)` (COMMIT-time
        // RESOLVE) folds rows staged earlier in the same transaction.
        let txn_id = task.request.txn_id;
        let target_coll_key: (DatabaseId, TenantId, String) = (
            task.request.database_id,
            TenantId::new(tid),
            target_collection.to_string(),
        );

        // Scan the target collection for rows passing the target-only filters,
        // folded with the transaction's staging overlay. The scan already yields
        // each matched row's CURRENT body (overlay put superseding base, staged
        // insert appended), so the body is used directly — a base `sparse.get`
        // would miss a row this transaction only staged.
        // Each row is at its current version, so a bitemporal target matches.
        let target_rows = match txn_id {
            Some(txn_id) => self.txn_matching_rows(txn_id, &target_coll_key, target_filters)?,
            None => {
                self.current_matching_rows(database_id, tid, target_collection, target_filters)?
            }
        };

        let mut rows: Vec<ResolvedUpdateRow> = Vec::new();
        let crs = self.crs_context();
        for (key, current_bytes) in target_rows {
            // A row the statement matched but cannot decode fails the
            // statement. Skipping it leaves the row untouched under a smaller
            // affected count that reports success.
            let mut target_doc = if let Some(schema) = strict_schema {
                super::super::strict_format::binary_tuple_to_json(&current_bytes, schema)
                    .ok_or_else(|| {
                        crate::diag::strict_row_undecodable(
                            target_collection,
                            &key.to_string(),
                            "update_from_join_collect",
                        );
                        let identity = key.to_identity();
                        super::super::strict_format::undecodable_strict_row(
                            target_collection,
                            identity.as_str(),
                        )
                    })?
            } else {
                doc_format::decode_document(&current_bytes)?
            };

            // Extract the join key from the target document.
            // The source row whose join key a WHERE `=` would match. A
            // target row with no such row is not updated.
            let source_doc = match source_map.probe(target_doc.get(target_join_col)) {
                Some(row) => &row.doc,
                None => continue,
            };

            // Build a merged document for expression evaluation:
            // target fields are bare; source fields are qualified as "alias.field".
            let mut merged = target_doc.clone();
            if let (Some(merged_obj), Some(src_obj)) =
                (merged.as_object_mut(), source_doc.as_object())
            {
                for (k, v) in src_obj {
                    merged_obj.insert(format!("{source_alias}.{k}"), v.clone());
                }
            }
            let merged_ndb: nodedb_types::Value = merged.clone().into();
            let identity = super::identity_guard::IdentitySnapshot::capture(
                strict_schema,
                declared_primary_key,
                updates,
                &target_doc,
            );

            // Apply SET assignments evaluated against the merged document.
            if let Some(target_obj) = target_doc.as_object_mut() {
                for (field, update_val) in updates {
                    let val: serde_json::Value = match update_val {
                        UpdateValue::Literal(bytes) => nodedb_types::json_from_msgpack(bytes)
                            .map_err(|e| crate::Error::Serialization {
                                format: "msgpack".into(),
                                detail: format!(
                                    "literal assigned to \"{field}\" for document \"{key}\" \
                                     of collection \"{target_collection}\" does not decode: {e}"
                                ),
                            })?,
                        // Division or modulo by zero fails the statement.
                        UpdateValue::Expr(expr) => expr
                            .eval(&merged_ndb, &crs)
                            .map_err(crate::Error::from)?
                            .into(),
                    };
                    target_obj.insert(field.clone(), val);
                }
            }

            // Only schemaless needs this check, and only here does a computed
            // RHS resolve to NULL — a strict collection already refuses one
            // at encode time.
            if strict_schema.is_none() {
                super::merge_helpers::check_declared_pk_not_null(
                    target_collection,
                    &target_doc,
                    declared_primary_key,
                )?;
            }
            identity.check_unchanged(target_collection, &target_doc)?;

            // Apply typeguards to the post-image, judging the assigned and
            // injected fields. Then recompute generated columns if any
            // dependency changed. A column the engine cannot recompute fails
            // the statement.
            if let Some(config) = self.doc_configs.get(config_key) {
                if !config.crdt_enabled && !config.type_guards.is_empty() {
                    let assigned = super::write_body::assigned_names(updates);
                    crate::data::executor::enforcement::typeguard::enforce_type_guards(
                        &config.type_guards,
                        &mut target_doc,
                        crate::data::executor::enforcement::typeguard::GuardScope::Assigned(
                            &assigned,
                        ),
                        &crs,
                    )
                    .map_err(crate::Error::DataPlane)?;
                }
                if !config.enforcement.generated_columns.is_empty()
                    && super::generated::needs_recomputation(
                        updates,
                        &config.enforcement.generated_columns,
                    )
                {
                    super::generated::evaluate_generated_columns(
                        &mut target_doc,
                        &config.enforcement.generated_columns,
                        &crs,
                    )
                    .map_err(crate::Error::DataPlane)?;
                }
            }

            // A declared numeric column holds the value its type stores,
            // whether the assignment was a literal or computed.
            super::super::strict_format::coerce_declared_doc(
                &mut target_doc,
                self.declared_columns(config_key),
            )?;

            // Re-encode the post-image (strict Binary Tuple or MessagePack).
            // An encode error carries its own typed cause, such as a field the
            // strict schema does not declare.
            let updated_bytes = if let Some(schema) = strict_schema {
                let ndb_val: nodedb_types::Value = target_doc.clone().into();
                match bitemporal_sys_from {
                    // The version's system time lives in a bitemporal tuple,
                    // valid for all time, as a point update encodes it.
                    Some(sys_from) if schema.bitemporal => {
                        super::super::strict_format::value_to_binary_tuple_bitemporal(
                            &ndb_val,
                            schema,
                            sys_from,
                            i64::MIN,
                            i64::MAX,
                            target_collection,
                        )?
                    }
                    _ => super::super::strict_format::value_to_binary_tuple(
                        &ndb_val,
                        schema,
                        target_collection,
                    )?,
                }
            } else {
                doc_format::encode_to_msgpack(&target_doc)
            };

            // The storage key came typed off the target scan; the write-set
            // reindex and the expanded `PointPut`'s identity (RESOLVE path)
            // both need it, so it's carried through rather than re-parsed.
            rows.push(ResolvedUpdateRow {
                key,
                body: updated_bytes,
                old_body: current_bytes,
                doc: target_doc,
            });
        }
        Ok(rows)
    }
}
