// SPDX-License-Identifier: BUSL-1.1

//! Producing the post-update row image from the current one. Pure value
//! computation — reads the stored body, applies assignments, recomputes
//! generated columns, re-encodes — touching no store, index, or transaction.
//! Nothing here has a side effect, so a failure aborts the statement before
//! anything is written.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::handlers::identity_guard::IdentitySnapshot;
use crate::data::executor::strict_format;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::UpdateValue;

/// The post-update row, before it is committed to a storage encoding. Its
/// own type because the stored body and the resolved-write MessagePack body
/// must come from ONE computation — recomputing per encoding is how resolve
/// and apply disagree.
pub(in crate::data::executor) enum PointUpdateBody {
    /// Native tagged document with assignments and generated fields materialized.
    Msgpack(Vec<u8>),
}

/// Inputs to [`CoreLoop::build_point_update_image`].
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct PointUpdateImage<'a> {
    pub(in crate::data::executor) config_key: &'a (DatabaseId, TenantId, String),
    /// The row as currently stored (Binary Tuple for a strict collection,
    /// MessagePack/JSON for a schemaless one).
    pub(in crate::data::executor) current_bytes: &'a [u8],
    pub(in crate::data::executor) updates: &'a [(String, UpdateValue)],
    pub(in crate::data::executor) is_strict: bool,
    pub(in crate::data::executor) bitemporal: bool,
    /// System time stamped into a bitemporal strict tuple; `0` otherwise.
    pub(in crate::data::executor) sys_from: i64,
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise. `Some` makes the post-image guard below run.
    pub(in crate::data::executor) declared_primary_key: Option<&'a str>,
}

/// MessagePack encoding of `null`.
const MSGPACK_NIL: u8 = 0xC0;

impl CoreLoop {
    /// Build the bytes this update will store, in the collection's storage mode.
    pub(in crate::data::executor) fn build_point_update_image(
        &self,
        params: PointUpdateImage<'_>,
    ) -> Result<Vec<u8>, ErrorCode> {
        let body = self.compute_point_update_body(params)?;
        self.encode_point_update_body(params, &body)
    }

    /// Encode `body` in the collection's storage mode.
    pub(in crate::data::executor) fn encode_point_update_body(
        &self,
        params: PointUpdateImage<'_>,
        body: &PointUpdateBody,
    ) -> Result<Vec<u8>, ErrorCode> {
        let PointUpdateImage {
            config_key,
            is_strict,
            bitemporal,
            sys_from,
            ..
        } = params;
        let PointUpdateBody::Msgpack(bytes) = body;
        if !is_strict {
            return Ok(bytes.clone());
        }
        let ndb_val = doc_format::decode_document_value(bytes).map_err(ErrorCode::from)?;
        let Some(config) = self.doc_configs.get(config_key) else {
            return Err(ErrorCode::Internal {
                detail: "strict config missing during re-encode".into(),
            });
        };
        let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        else {
            return Err(ErrorCode::Internal {
                detail: "strict config missing during re-encode".into(),
            });
        };
        let collection = &config_key.2;
        let result = if bitemporal && schema.bitemporal {
            strict_format::value_to_binary_tuple_bitemporal(
                &ndb_val,
                schema,
                sys_from,
                i64::MIN,
                i64::MAX,
                collection,
            )
        } else {
            strict_format::value_to_binary_tuple(&ndb_val, schema, collection)
        };
        result.map_err(ErrorCode::from)
    }

    /// Apply the assignments and recompute generated columns, touching no
    /// store, no index, and no transaction.
    pub(in crate::data::executor) fn compute_point_update_body(
        &self,
        params: PointUpdateImage<'_>,
    ) -> Result<PointUpdateBody, ErrorCode> {
        let PointUpdateImage {
            config_key,
            current_bytes,
            updates,
            is_strict,
            bitemporal: _,
            sys_from: _,
            declared_primary_key,
        } = params;

        // A literal NULL key receives the same constraint error as a computed NULL.
        if let Some(pk) = declared_primary_key {
            for (field, value) in updates {
                if field == pk
                    && let UpdateValue::Literal(bytes) = value
                    && matches!(bytes.first(), Some(&MSGPACK_NIL))
                {
                    return Err(ErrorCode::RejectedConstraint {
                        constraint: "not_null".to_string(),
                        detail: format!("primary key '{pk}' cannot be NULL or omitted"),
                    });
                }
            }
        }

        // Decode native fields before assignments, preserving values unrelated to the update.
        let strict_schema = if is_strict {
            match self.doc_configs.get(config_key).map(|c| &c.storage_mode) {
                Some(nodedb_physical::physical_plan::StorageMode::Strict { schema }) => {
                    Some(schema)
                }
                _ => {
                    return Err(ErrorCode::Internal {
                        detail: "strict config missing during update".into(),
                    });
                }
            }
        } else {
            None
        };
        let mut doc = if let Some(schema) = strict_schema {
            match strict_format::binary_tuple_to_row_value(current_bytes, schema) {
                Some(v) => v,
                None => {
                    return Err(ErrorCode::Internal {
                        detail: "failed to decode Binary Tuple for update".into(),
                    });
                }
            }
        } else {
            match doc_format::decode_document_value(current_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ErrorCode::Internal {
                        detail: format!("failed to parse document for update: {e}"),
                    });
                }
            }
        };

        let identity = IdentitySnapshot::capture(
            strict_schema,
            declared_primary_key,
            updates,
            &serde_json::Value::from(doc.clone()),
        );

        // Expressions evaluate against the pre-update snapshot, so later
        // assignments don't observe earlier ones — matches PostgreSQL.
        let eval_doc = doc.clone();
        let crs = self.crs_context();
        if let nodedb_types::Value::Object(obj) = &mut doc {
            for (field, update_val) in updates {
                let val = match update_val {
                    UpdateValue::Literal(bytes) => match nodedb_types::value_from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(ErrorCode::Internal {
                                detail: format!("update field '{field}': msgpack decode: {e}"),
                            });
                        }
                    },
                    UpdateValue::Expr(expr) => {
                        let result: nodedb_types::Value = match expr.eval(&eval_doc, &crs) {
                            Ok(v) => v,
                            // An evaluation error fails the statement.
                            Err(e) => return Err(ErrorCode::from(e)),
                        };
                        result
                    }
                };
                obj.insert(field.clone(), val);
            }
        }

        // A declared PRIMARY KEY implies NOT NULL. The plan-time check only
        // catches a literal NULL; a computed RHS is only known here.
        if let Some(pk) = declared_primary_key
            && matches!(doc.get(pk), None | Some(nodedb_types::Value::Null))
        {
            return Err(ErrorCode::RejectedConstraint {
                constraint: "not_null".into(),
                detail: format!("primary key '{pk}' cannot be NULL or omitted"),
            });
        }
        identity
            .check_unchanged(&config_key.2, &serde_json::Value::from(doc.clone()))
            .map_err(ErrorCode::from)?;

        if let Some(config) = self.doc_configs.get(config_key) {
            let assigned = crate::data::executor::handlers::write_body::assigned_names(updates);
            crate::data::executor::handlers::point::apply_put::materialize_native_write_body(
                config,
                &mut doc,
                crate::data::executor::enforcement::typeguard::GuardScope::Assigned(&assigned),
                &crs,
            )?;
        }
        if let nodedb_types::Value::Object(fields) = &mut doc {
            strict_format::coerce_declared_row(fields, self.declared_columns(config_key))
                .map_err(ErrorCode::from)?;
        }
        let bytes = zerompk::to_msgpack_vec(&doc).map_err(|error| ErrorCode::Internal {
            detail: format!("post-update document cannot encode: {error}"),
        })?;
        Ok(PointUpdateBody::Msgpack(bytes))
    }
}

/// The post-update row as pre-encode MessagePack, for both storage modes — a
/// resolved write ships this one shape and apply encodes the strict Binary
/// Tuple exactly as a direct write does.
pub(in crate::data::executor) fn point_update_body_to_msgpack(body: &PointUpdateBody) -> Vec<u8> {
    match body {
        PointUpdateBody::Msgpack(bytes) => bytes.clone(),
    }
}
