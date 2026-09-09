// SPDX-License-Identifier: BUSL-1.1

//! Turning a bulk UPDATE's matched row set into the post-images it will store.
//!
//! Split from the apply loop so a statement-wide constraint can be judged
//! before the first row is written — the apply loop commits one transaction
//! per row, so a check running during iteration could only catch a violation
//! after earlier rows were already durable. Projecting a row is pure: it
//! reads the stored body and computes the new one, writing nothing.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::columnar::StrictSchema;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::types::{DatabaseId, TenantId};

/// One matched row and everything the apply loop needs to land it.
pub(in crate::data::executor) struct ProjectedUpdateRow {
    /// Storage key (the surrogate hex).
    pub(in crate::data::executor) doc_id: String,
    /// The row as stored before the update — the `old_value` of the emitted
    /// event and the old side of the secondary-index diff.
    pub(in crate::data::executor) current_bytes: Vec<u8>,
    /// Pre-mutation image, captured before any field changed.
    pub(in crate::data::executor) old_doc: serde_json::Value,
    /// Post-update image, with assignments and regenerated columns applied.
    pub(in crate::data::executor) doc: serde_json::Value,
    /// The post-update image encoded in the collection's storage mode.
    pub(in crate::data::executor) updated_bytes: Vec<u8>,
}

/// Inputs to [`CoreLoop::project_bulk_update_rows`].
pub(in crate::data::executor) struct ProjectUpdateRows<'a> {
    pub(in crate::data::executor) database_id: u64,
    pub(in crate::data::executor) tid: u64,
    pub(in crate::data::executor) collection: &'a str,
    /// The settled apply set, in statement order.
    pub(in crate::data::executor) doc_ids: &'a [String],
    pub(in crate::data::executor) updates: &'a [(String, UpdateValue)],
    /// `Some` for a strict collection, whose bodies are Binary Tuples.
    pub(in crate::data::executor) strict_schema: Option<&'a StrictSchema>,
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise. `Some` makes the post-image guard below run.
    pub(in crate::data::executor) declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Compute the post-image of every matched row. A row deleted between the
    /// match and this pass is skipped — it is no longer in the update set. A
    /// row the engine cannot decode, re-encode, or evaluate is an error.
    pub(in crate::data::executor) fn project_bulk_update_rows(
        &self,
        p: ProjectUpdateRows<'_>,
    ) -> crate::Result<Vec<ProjectedUpdateRow>> {
        let ProjectUpdateRows {
            database_id,
            tid,
            collection,
            doc_ids,
            updates,
            strict_schema,
            declared_primary_key,
        } = p;
        let config_key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );

        let mut projected = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            let Some(current_bytes) = self.sparse.get(database_id, tid, collection, doc_id)? else {
                continue;
            };

            // Decode current value — format depends on storage mode, with the
            // storage key attached as `id` for a schemaless row whose body
            // carries none, so this image matches the one DELETE's
            // write-gate judges. A row the statement matched but cannot
            // decode fails the statement rather than under-reporting the
            // affected count.
            let mut doc = match strict_schema {
                Some(schema) => crate::data::executor::strict_format::binary_tuple_to_json(
                    &current_bytes,
                    schema,
                )
                .ok_or_else(|| {
                    crate::diag::strict_row_undecodable(collection, doc_id, "bulk_update_project");
                    crate::data::executor::strict_format::undecodable_strict_row(collection, doc_id)
                })?,
                None => crate::data::executor::handlers::returning_doc::from_stored(
                    &current_bytes,
                    doc_id,
                    None,
                )?,
            };

            // Feeds the secondary-index SET diff for values the UPDATE drops.
            let old_doc = doc.clone();
            // All assignments see this pre-update snapshot — they don't
            // observe each other, matching PostgreSQL semantics.
            let eval_doc: nodedb_types::Value = doc.clone().into();
            if let Some(obj) = doc.as_object_mut() {
                for (field, update_val) in updates {
                    let val: serde_json::Value = match update_val {
                        UpdateValue::Literal(bytes) => nodedb_types::json_from_msgpack(bytes)
                            .map_err(|e| crate::Error::Serialization {
                                format: "msgpack".into(),
                                detail: format!(
                                    "literal assigned to \"{field}\" for document \"{doc_id}\" \
                                     of collection \"{collection}\" does not decode: {e}"
                                ),
                            })?,
                        // Division or modulo by zero fails the statement.
                        UpdateValue::Expr(expr) => {
                            let result: nodedb_types::Value = expr.eval(&eval_doc)?;
                            result.into()
                        }
                    };
                    obj.insert(field.clone(), val);
                }
            }

            // Only schemaless needs this check, and only here does a computed
            // RHS resolve to NULL — a strict collection already refuses one
            // at encode time.
            if strict_schema.is_none() {
                super::super::merge_helpers::check_declared_pk_not_null(
                    collection,
                    &doc,
                    declared_primary_key,
                )?;
            }

            // Recompute generated columns if any dependency changed. A column
            // the engine cannot recompute fails the statement.
            if let Some(config) = self.doc_configs.get(&config_key)
                && !config.enforcement.generated_columns.is_empty()
                && super::super::generated::needs_recomputation(
                    updates,
                    &config.enforcement.generated_columns,
                )
            {
                super::super::generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                )
                .map_err(crate::Error::DataPlane)?;
            }

            // Re-encode — format depends on storage mode. An encode error
            // carries its own typed cause, such as a field the strict schema
            // does not declare.
            let updated_bytes = match strict_schema {
                Some(schema) => {
                    let ndb_val: nodedb_types::Value = doc.clone().into();
                    crate::data::executor::strict_format::value_to_binary_tuple(
                        &ndb_val, schema, collection,
                    )?
                }
                None => doc_format::encode_to_msgpack(&doc),
            };

            projected.push(ProjectedUpdateRow {
                doc_id: doc_id.clone(),
                current_bytes,
                old_doc,
                doc,
                updated_bytes,
            });
        }
        Ok(projected)
    }
}
