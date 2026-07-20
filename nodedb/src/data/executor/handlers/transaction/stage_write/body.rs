// SPDX-License-Identifier: BUSL-1.1

//! Body encoding for staged point writes.
//!
//! Produces the stored-form bytes an in-transaction point write records in the
//! overlay, using the SAME encoders the durable apply path uses so a
//! same-transaction read-modify-write (and the eventual COMMIT replay)
//! observe a consistent representation. Schemaless documents are canonicalized
//! MessagePack; strict documents are Binary Tuples (with the auto `_rowid`
//! primary key injected from the surrogate and the bitemporal slot layout when
//! configured).

use nodedb_physical::physical_plan::{StorageMode, UpdateValue};
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::generated;
use crate::data::executor::{doc_format, strict_format};
use crate::types::TenantId;

impl CoreLoop {
    /// Encode a PointPut / PointInsert body into its stored form, mirroring
    /// `apply_point_put`'s encoding pipeline (generated columns, `_rowid`
    /// injection, schemaless canonicalization, strict Binary Tuple, bitemporal
    /// slots).
    pub(super) fn stage_encode_put_body(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        value: &[u8],
    ) -> crate::Result<Vec<u8>> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );

        // Evaluate generated columns before encoding, matching the durable path.
        let value: Vec<u8> = if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            if let Some(mut doc) = doc_format::decode_document(value) {
                generated::evaluate_generated_columns(
                    &mut doc,
                    &config.enforcement.generated_columns,
                )
                .map_err(crate::Error::DataPlane)?;
                doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            doc_format::canonicalize_document_for_storage(value)
        };

        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let sys_from_ms = self.bitemporal_now_ms();

        if let Some(config) = self.doc_configs.get(&config_key)
            && let StorageMode::Strict { ref schema } = config.storage_mode
        {
            // Inject the auto-generated `_rowid` primary key from the surrogate
            // when the schema declares one and the client payload omits it.
            let encoded_input: Vec<u8> = if schema
                .columns
                .first()
                .is_some_and(|c| c.name == "_rowid" && !c.nullable)
                && let Ok(mut decoded) = nodedb_types::json_from_msgpack(&value)
                && let serde_json::Value::Object(ref mut obj) = decoded
                && !obj.contains_key("_rowid")
            {
                obj.insert(
                    "_rowid".to_string(),
                    serde_json::Value::Number((surrogate.0 as i64).into()),
                );
                nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.clone())
            } else {
                value.clone()
            };

            let stored = if bitemporal && schema.bitemporal {
                strict_format::bytes_to_binary_tuple_bitemporal(
                    &encoded_input,
                    schema,
                    sys_from_ms,
                    i64::MIN,
                    i64::MAX,
                )
            } else {
                strict_format::bytes_to_binary_tuple(&encoded_input, schema)
            }
            .map_err(|e| crate::Error::Serialization {
                format: "binary_tuple".into(),
                detail: e.to_string(),
            })?;
            Ok(stored)
        } else {
            Ok(value)
        }
    }

    /// Apply an update patch to a current stored body and re-encode it,
    /// mirroring `execute_point_update`'s decode → mutate → re-encode path.
    ///
    /// `current_bytes` is the overlay-or-base current body; expressions in the
    /// patch are evaluated against it, so a same-transaction prior update is
    /// observed by a later `col = col + 1`.
    pub(in crate::data::executor) fn stage_apply_update(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        current_bytes: &[u8],
        updates: &[(String, UpdateValue)],
    ) -> crate::Result<Vec<u8>> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        let bitemporal = self.is_bitemporal(database_id, tid, collection);
        let sys_from_ms = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };

        let strict_schema = self.doc_configs.get(&config_key).and_then(|c| {
            if let StorageMode::Strict { ref schema } = c.storage_mode {
                Some(schema.clone())
            } else {
                None
            }
        });

        let mut doc =
            match strict_schema.as_ref() {
                Some(schema) => strict_format::binary_tuple_to_json(current_bytes, schema)
                    .ok_or_else(|| crate::Error::Storage {
                        engine: "binary_tuple".into(),
                        detail: "failed to decode Binary Tuple for staged update".into(),
                    })?,
                None => doc_format::decode_document(current_bytes).ok_or_else(|| {
                    crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: "failed to decode document for staged update".into(),
                    }
                })?,
            };

        // Expressions evaluate against the pre-update snapshot (PostgreSQL
        // semantics): a later assignment observing a column updated earlier in
        // the same statement still sees the pre-statement value.
        let eval_doc: nodedb_types::Value = doc.clone().into();
        if let Some(obj) = doc.as_object_mut() {
            for (field, update_val) in updates {
                let val =
                    match update_val {
                        UpdateValue::Literal(bytes) => nodedb_types::json_from_msgpack(bytes)
                            .map_err(|e| crate::Error::Serialization {
                                format: "msgpack".into(),
                                detail: format!("staged update field '{field}': {e}"),
                            })?,
                        UpdateValue::Expr(expr) => {
                            let result: nodedb_types::Value = expr.eval(&eval_doc);
                            result.into()
                        }
                    };
                obj.insert(field.clone(), val);
            }
        }

        // Recompute generated columns after the patch.
        if let Some(config) = self.doc_configs.get(&config_key)
            && !config.enforcement.generated_columns.is_empty()
        {
            generated::evaluate_generated_columns(&mut doc, &config.enforcement.generated_columns)
                .map_err(crate::Error::DataPlane)?;
        }

        match strict_schema.as_ref() {
            Some(schema) => {
                let ndb_val: nodedb_types::Value = doc.into();
                let bytes = if bitemporal && schema.bitemporal {
                    strict_format::value_to_binary_tuple_bitemporal(
                        &ndb_val,
                        schema,
                        sys_from_ms,
                        i64::MIN,
                        i64::MAX,
                    )
                } else {
                    strict_format::value_to_binary_tuple(&ndb_val, schema)
                }
                .map_err(|e| crate::Error::Serialization {
                    format: "binary_tuple".into(),
                    detail: e.to_string(),
                })?;
                Ok(bytes)
            }
            None => Ok(doc_format::encode_to_msgpack(&doc)),
        }
    }
}
