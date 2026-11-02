// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::OpenArray` handler.

use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::ArrayId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn handle_array_open(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        schema_msgpack: &[u8],
        schema_hash: u64,
        prefix_bits: u8,
    ) -> Response {
        // Verify the Control-Plane-owned catalog when an authorized DDL has
        // installed an entry. The Data Plane never registers entries itself:
        // planning and raw engine opening must not create an in-memory catalog
        // side effect.
        {
            let cat = match self.array_catalog.read() {
                Ok(g) => g,
                Err(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "array catalog lock poisoned".to_string(),
                        },
                    );
                }
            };
            if let Some(existing) = cat.lookup_by_id(array_id) {
                if existing.schema_hash != schema_hash || existing.prefix_bits != prefix_bits {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: format!(
                                "array '{}' catalog identity differs (schema {:#x}/{:#x}, prefix {}/{})",
                                array_id.name,
                                existing.schema_hash,
                                schema_hash,
                                existing.prefix_bits,
                                prefix_bits
                            ),
                        },
                    );
                }
            } else {
                // The Data Plane may reconstruct its in-memory mirror during
                // WAL recovery and direct already-admitted execution. It never
                // persists catalog state here; client authorization and the
                // durable transition remain Control-Plane responsibilities.
                drop(cat);
                let entry = ArrayCatalogEntry {
                    array_id: array_id.clone(),
                    name: array_id.name.clone(),
                    schema_msgpack: schema_msgpack.to_vec(),
                    schema_hash,
                    created_at_ms: now_epoch_ms(),
                    prefix_bits,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                    modification_hlc: nodedb_types::Hlc::ZERO,
                    incarnation: nodedb_types::Hlc::ZERO,
                };
                let mut cat = match self.array_catalog.write() {
                    Ok(guard) => guard,
                    Err(_) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: "array catalog lock poisoned".to_string(),
                            },
                        );
                    }
                };
                if cat.lookup_by_id(array_id).is_none()
                    && let Err(error) = cat.register(entry)
                {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array catalog register: {error}"),
                        },
                    );
                }
            }
        }

        // Decode the schema and open the engine side. zerompk-encoded
        // (matches the wire contract documented on
        // `ArrayOp::OpenArray::schema_msgpack`).
        let schema: ArraySchema = match zerompk::from_msgpack(schema_msgpack) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array schema decode: {e}"),
                    },
                );
            }
        };

        // A `PutArray` is proposed only for an identity the committed catalog
        // lacks, and its `DeleteArray` removed the row before staging the
        // drop. A tombstone here is therefore the unpurged residue of a prior
        // incarnation; remove it before opening the replacement store.
        if let Err(e) = self.array_engine.purge_finalized_drop_before_open(array_id) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array finalized-drop purge: {e}"),
                },
            );
        }

        if let Err(e) =
            self.array_engine
                .open_array(array_id.clone(), Arc::new(schema), schema_hash)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array engine open: {e}"),
                },
            );
        }
        self.fold_array_versions(array_id);

        match super::super::super::response_codec::encode_count("opened", 1) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
