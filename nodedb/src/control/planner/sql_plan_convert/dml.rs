//! DML plan conversions (INSERT, UPDATE, DELETE).

use nodedb_sql::types::{EngineType, Filter, SqlExpr, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::filter::serialize_filters;
use super::value::{
    assignments_to_bytes, row_to_msgpack, rows_to_msgpack_array, sql_value_to_bytes,
    sql_value_to_msgpack, sql_value_to_string, write_msgpack_map_header, write_msgpack_str,
    write_msgpack_value,
};

pub(super) fn convert_insert(
    collection: &str,
    engine: &EngineType,
    rows: &[Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    for row in rows {
        let doc_id = row
            .iter()
            .find(|(k, _)| k == "id" || k == "document_id" || k == "key")
            .map(|(_, v)| sql_value_to_string(v))
            .unwrap_or_default();

        match engine {
            EngineType::KeyValue => {
                return Err(crate::Error::PlanError {
                    detail: "KV INSERT must use SqlPlan::KvInsert path".into(),
                });
            }
            EngineType::Columnar | EngineType::Spatial => {
                // Columnar/Spatial INSERT: batch into a single ColumnarOp::Insert.
                // Accumulate rows and emit after the loop.
                columnar_rows.push(row);
            }
            EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
                let value_bytes = row_to_msgpack(row)?;
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Document(DocumentOp::PointPut {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                    }),
                    post_set_op: PostSetOp::None,
                });
            }
            other => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "INSERT into '{collection}': unexpected engine type {other:?}, expected Document/Columnar/Spatial"
                    ),
                });
            }
        }
    }

    // Emit batched ColumnarOp::Insert for columnar/spatial collections.
    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows, column_defaults)?;
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.into(),
                payload,
                format: "msgpack".into(),
            }),
            post_set_op: PostSetOp::None,
        });
    }

    Ok(tasks)
}

pub(super) fn convert_kv_insert(
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let mut tasks = Vec::with_capacity(entries.len());
    for (key_val, value_cols) in entries {
        let key = sql_value_to_bytes(key_val);
        // Value is the payload columns as msgpack map.
        let value = if value_cols.len() == 1 && value_cols[0].0 == "value" {
            // Simple (key, value) form — value is raw bytes.
            sql_value_to_bytes(&value_cols[0].1)
        } else {
            // Typed columns form — write standard msgpack map directly.
            let mut buf = Vec::with_capacity(value_cols.len() * 32);
            write_msgpack_map_header(&mut buf, value_cols.len());
            for (col, val) in value_cols {
                write_msgpack_str(&mut buf, col);
                write_msgpack_value(&mut buf, val);
            }
            buf
        };
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Kv(KvOp::Put {
                collection: collection.into(),
                key,
                value,
                ttl_ms: 0,
            }),
            post_set_op: PostSetOp::None,
        });
    }
    Ok(tasks)
}

pub(super) fn convert_update(
    collection: &str,
    engine: &EngineType,
    assignments: &[(String, SqlExpr)],
    filters: &[Filter],
    target_keys: &[SqlValue],
    returning: bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    let filter_bytes = serialize_filters(filters)?;
    let updates = assignments_to_bytes(assignments)?;

    // KV engine: route to FieldSet for point updates.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            let field_updates: Vec<(String, Vec<u8>)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let SqlExpr::Literal(val) = expr {
                        Some((field.clone(), sql_value_to_msgpack(val)))
                    } else {
                        None
                    }
                })
                .collect();
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::FieldSet {
                    collection: collection.into(),
                    key: sql_value_to_bytes(key),
                    updates: field_updates,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        return Ok(tasks);
    }

    if !target_keys.is_empty() {
        // Point updates (document engine).
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointUpdate {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                    updates: updates.clone(),
                    returning,
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkUpdate {
                collection: collection.into(),
                filters: filter_bytes,
                updates,
                returning,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}

pub(super) fn convert_delete(
    collection: &str,
    engine: &EngineType,
    filters: &[Filter],
    target_keys: &[SqlValue],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);

    // KV engine: route to KvOp::Delete.
    if matches!(engine, EngineType::KeyValue) && !target_keys.is_empty() {
        let keys: Vec<Vec<u8>> = target_keys.iter().map(sql_value_to_bytes).collect();
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Kv(KvOp::Delete {
                collection: collection.into(),
                keys,
            }),
            post_set_op: PostSetOp::None,
        }]);
    }

    if !target_keys.is_empty() {
        let mut tasks = Vec::new();
        for key in target_keys {
            tasks.push(PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: collection.into(),
                    document_id: sql_value_to_string(key),
                }),
                post_set_op: PostSetOp::None,
            });
        }
        Ok(tasks)
    } else {
        let filter_bytes = serialize_filters(filters)?;
        Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: collection.into(),
                filters: filter_bytes,
            }),
            post_set_op: PostSetOp::None,
        }])
    }
}
