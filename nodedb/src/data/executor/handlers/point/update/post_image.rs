// SPDX-License-Identifier: BUSL-1.1

//! Producing the post-update row image from the current one. Pure value
//! computation — reads the stored body, applies assignments, recomputes
//! generated columns, re-encodes — touching no store, index, or transaction.
//! Nothing here has a side effect, so a failure aborts the statement before
//! anything is written.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::strict_format;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::UpdateValue;

/// The post-update row, before it is committed to a storage encoding. Its
/// own type because the stored body and the resolved-write MessagePack body
/// must come from ONE computation — recomputing per encoding is how resolve
/// and apply disagree.
pub(in crate::data::executor) enum PointUpdateBody {
    /// Schemaless fast path: fields merged at the binary level, already the
    /// MessagePack the collection stores.
    Msgpack(Vec<u8>),
    /// Decoded, mutated, generated-columns-recomputed document.
    Document(serde_json::Value),
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
    pub(in crate::data::executor) has_generated: bool,
    pub(in crate::data::executor) has_expr: bool,
    pub(in crate::data::executor) bitemporal: bool,
    /// System time stamped into a bitemporal strict tuple; `0` otherwise.
    pub(in crate::data::executor) sys_from_ms: i64,
    /// Declared `PRIMARY KEY` column of a schemaless collection, `None`
    /// otherwise. `Some` makes the post-image guard below run.
    pub(in crate::data::executor) declared_primary_key: Option<&'a str>,
}

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
            sys_from_ms,
            ..
        } = params;
        let doc = match body {
            PointUpdateBody::Msgpack(bytes) => return Ok(bytes.clone()),
            PointUpdateBody::Document(doc) => doc,
        };
        if !is_strict {
            return Ok(doc_format::encode_to_msgpack(doc));
        }
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
        let ndb_val: nodedb_types::Value = doc.clone().into();
        let collection = &config_key.2;
        let result = if bitemporal && schema.bitemporal {
            strict_format::value_to_binary_tuple_bitemporal(
                &ndb_val,
                schema,
                sys_from_ms,
                i64::MIN,
                i64::MAX,
                collection,
            )
        } else {
            strict_format::value_to_binary_tuple(&ndb_val, schema, collection)
        };
        result.map_err(|e| match e {
            crate::Error::UnknownStrictField { column, .. } => {
                ErrorCode::UndefinedColumn { column }
            }
            other => ErrorCode::Internal {
                detail: format!("strict re-encode: {other}"),
            },
        })
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
            has_generated,
            has_expr,
            bitemporal: _,
            sys_from_ms: _,
            declared_primary_key,
        } = params;

        // Fast path: non-strict, no generated columns, all literal — merge at binary level.
        if !is_strict && !has_generated && !has_expr {
            let base_mp = doc_format::json_to_msgpack(current_bytes);
            let update_pairs: Vec<(&str, &[u8])> = updates
                .iter()
                .filter_map(|(field, v)| match v {
                    UpdateValue::Literal(bytes) => Some((field.as_str(), bytes.as_slice())),
                    UpdateValue::Expr(_) => None,
                })
                .collect();
            return Ok(PointUpdateBody::Msgpack(
                nodedb_query::msgpack_scan::merge_fields(&base_mp, &update_pairs),
            ));
        }

        // Strict, generated, or expression RHS: decode → mutate → re-encode.
        let mut doc = if is_strict {
            if let Some(config) = self.doc_configs.get(config_key)
                && let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
            {
                match strict_format::binary_tuple_to_json(current_bytes, schema) {
                    Some(v) => v,
                    None => {
                        return Err(ErrorCode::Internal {
                            detail: "failed to decode Binary Tuple for update".into(),
                        });
                    }
                }
            } else {
                return Err(ErrorCode::Internal {
                    detail: "strict config missing during update".into(),
                });
            }
        } else {
            match doc_format::decode_document(current_bytes) {
                Ok(v) => v,
                Err(e) => {
                    return Err(ErrorCode::Internal {
                        detail: format!("failed to parse document for update: {e}"),
                    });
                }
            }
        };

        // Expressions evaluate against the pre-update snapshot, so later
        // assignments don't observe earlier ones — matches PostgreSQL.
        let eval_doc: nodedb_types::Value = doc.clone().into();
        if let Some(obj) = doc.as_object_mut() {
            for (field, update_val) in updates {
                let val = match update_val {
                    UpdateValue::Literal(bytes) => match nodedb_types::json_from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(ErrorCode::Internal {
                                detail: format!("update field '{field}': msgpack decode: {e}"),
                            });
                        }
                    },
                    UpdateValue::Expr(expr) => {
                        let result: nodedb_types::Value = match expr.eval(&eval_doc) {
                            Ok(v) => v,
                            // Division/modulo by zero fails the statement.
                            Err(_e) => return Err(ErrorCode::DivisionByZero),
                        };
                        let json: serde_json::Value = result.into();
                        json
                    }
                };
                obj.insert(field.clone(), val);
            }
        }

        // A declared PRIMARY KEY implies NOT NULL. The plan-time check only
        // catches a literal NULL; a computed RHS is only known here.
        if let Some(pk) = declared_primary_key
            && matches!(doc.get(pk), None | Some(serde_json::Value::Null))
        {
            return Err(ErrorCode::RejectedConstraint {
                constraint: "not_null".into(),
                detail: format!("primary key '{pk}' cannot be NULL or omitted"),
            });
        }

        // Recompute generated columns.
        if has_generated
            && let Some(config) = self.doc_configs.get(config_key)
            && let Err(e) = crate::data::executor::handlers::generated::evaluate_generated_columns(
                &mut doc,
                &config.enforcement.generated_columns,
            )
        {
            return Err(e);
        }

        Ok(PointUpdateBody::Document(doc))
    }
}

/// The post-update row as pre-encode MessagePack, for both storage modes — a
/// resolved write ships this one shape and apply encodes the strict Binary
/// Tuple exactly as a direct write does.
pub(in crate::data::executor) fn point_update_body_to_msgpack(body: &PointUpdateBody) -> Vec<u8> {
    match body {
        PointUpdateBody::Msgpack(bytes) => bytes.clone(),
        PointUpdateBody::Document(doc) => doc_format::encode_to_msgpack(doc),
    }
}
