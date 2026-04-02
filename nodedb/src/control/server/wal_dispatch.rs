//! WAL append logic for write operations.
//!
//! Serializes write plans as MessagePack and appends to the appropriate
//! WAL record type. Read operations are no-ops.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, TimeseriesOp, VectorOp};
use crate::control::security::credential::CredentialStore;
use crate::types::{TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    wal_append_if_write_with_creds(wal, tenant_id, vshard_id, plan, None)
}

/// WAL append with optional credential store for timeseries WAL bypass check.
pub fn wal_append_if_write_with_creds(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
    credentials: Option<&CredentialStore>,
) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            value,
        }) => {
            let entry = rmp_serde::to_vec(&(collection, document_id, value)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
        }) => {
            let entry = rmp_serde::to_vec(&(collection, document_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name: _,
            doc_id: _,
        }) => {
            let entry = rmp_serde::to_vec(&(collection, vector, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
        }) => {
            let entry = rmp_serde::to_vec(&(collection, vectors, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector batch insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::Delete {
            collection,
            vector_id,
        }) => {
            let entry = rmp_serde::to_vec(&(collection, vector_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector delete: {e}"),
                }
            })?;
            wal.append_vector_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Crdt(CrdtOp::Apply { delta, .. }) => {
            wal.append_crdt_delta(tenant_id, vshard_id, delta)?;
        }
        PhysicalPlan::Graph(GraphOp::EdgePut {
            src_id,
            label,
            dst_id,
            properties,
        }) => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id, properties)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge put: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            src_id,
            label,
            dst_id,
        }) => {
            let entry = rmp_serde::to_vec(&(src_id, label, dst_id)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal edge delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        }) => {
            let entry = rmp_serde::to_vec(&(
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            wal.append_vector_params(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Columnar(crate::bridge::physical_plan::ColumnarOp::Insert {
            collection: _,
            payload,
            format: _,
        }) => {
            wal.append_timeseries_batch(tenant_id, vshard_id, payload)?;
        }
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format: _,
            ..
        }) => {
            // WAL bypass: skip WAL if collection has wal=false in timeseries_config.
            if let Some(creds) = credentials
                && let Some(catalog) = creds.catalog()
                && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
                && let Some(config) = coll.get_timeseries_config()
                && config.get("wal").and_then(|v| v.as_str()) == Some("false")
            {
                // WAL bypassed — acceptable data loss of last flush interval on crash.
                return Ok(());
            }

            let wal_payload = rmp_serde::to_vec(&(collection, payload)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal timeseries batch: {e}"),
                }
            })?;
            wal.append_timeseries_batch(tenant_id, vshard_id, &wal_payload)?;
        }
        // KV write operations.
        PhysicalPlan::Kv(KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
        }) => {
            let entry =
                rmp_serde::to_vec(&("kv_put", collection, key, value, ttl_ms)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv put: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => {
            let entry = rmp_serde::to_vec(&("kv_delete", collection, keys)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
        }) => {
            let entry =
                rmp_serde::to_vec(&("kv_batch_put", collection, entries, ttl_ms)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv batch put: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::Expire {
            collection,
            key,
            ttl_ms,
        }) => {
            let entry =
                rmp_serde::to_vec(&("kv_expire", collection, key, ttl_ms)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv expire: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::Persist { collection, key }) => {
            let entry = rmp_serde::to_vec(&("kv_persist", collection, key)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv persist: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::RegisterIndex {
            collection,
            field,
            field_position,
            backfill: _,
        }) => {
            let entry =
                rmp_serde::to_vec(&("kv_register_index", collection, field, field_position))
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv register index: {e}"),
                    })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::DropIndex { collection, field }) => {
            let entry = rmp_serde::to_vec(&("kv_drop_index", collection, field)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv drop index: {e}"),
                }
            })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection,
            key,
            updates,
        }) => {
            let entry =
                rmp_serde::to_vec(&("kv_field_set", collection, key, updates)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal kv field set: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, &entry)?;
        }
        // Truncate uses append_delete to mark the collection as cleared in the WAL.
        // On recovery, replaying this entry drops all hash table state for the collection.
        PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
            let entry = rmp_serde::to_vec(&("kv_truncate", collection)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal kv truncate: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, &entry)?;
        }
        // Read operations and control commands: no WAL needed.
        _ => {}
    }
    Ok(())
}

/// Append a timeseries batch to WAL and return the assigned LSN.
///
/// Used by the ILP listener to propagate the WAL LSN to the Data Plane
/// for proper dedup tracking and `flush_wal_lsn` in partition metadata.
/// Returns `None` if WAL is bypassed for this collection.
pub fn wal_append_timeseries(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    collection: &str,
    payload: &[u8],
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    // WAL bypass check.
    if let Some(creds) = credentials
        && let Some(catalog) = creds.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), collection)
        && let Some(config) = coll.get_timeseries_config()
        && config.get("wal").and_then(|v| v.as_str()) == Some("false")
    {
        return Ok(None);
    }

    let wal_payload =
        rmp_serde::to_vec(&(collection, payload)).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch: {e}"),
        })?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, &wal_payload)?;
    Ok(Some(lsn))
}
