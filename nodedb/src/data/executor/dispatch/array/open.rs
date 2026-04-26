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
        // Verify-or-register against the in-memory catalog. Persistence
        // into `_system.arrays` is owned by the Control-Plane DDL path
        // (`SystemCatalog::put_array`), not the Data Plane — redb is
        // Control-Plane-only.
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
            if let Some(existing) = cat.lookup_by_name(&array_id.name) {
                if existing.schema_hash != schema_hash {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: format!(
                                "array '{}' already registered with different schema hash ({:#x} != {:#x})",
                                array_id.name, existing.schema_hash, schema_hash
                            ),
                        },
                    );
                }
            } else {
                drop(cat);
                let entry = ArrayCatalogEntry {
                    array_id: array_id.clone(),
                    name: array_id.name.clone(),
                    schema_msgpack: schema_msgpack.to_vec(),
                    schema_hash,
                    created_at_ms: now_epoch_ms(),
                    prefix_bits,
                    audit_retain_ms: None,
                };
                let mut cat = match self.array_catalog.write() {
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
                // Double-check under the write lock.
                if cat.lookup_by_name(&array_id.name).is_none()
                    && let Err(e) = cat.register(entry)
                {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array catalog register: {e}"),
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

        match super::super::super::response_codec::encode_count("opened", 1) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
