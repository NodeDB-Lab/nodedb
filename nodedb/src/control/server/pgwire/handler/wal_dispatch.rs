//! WAL append logic: serialize write operations for single-node durability.

use crate::types::{TenantId, VShardId};

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Append a write operation to the WAL for single-node durability.
    ///
    /// Serializes the write as MessagePack and appends to the appropriate
    /// WAL record type. Read operations are no-ops (return Ok immediately).
    pub(super) fn wal_append_if_write(
        &self,
        tenant_id: TenantId,
        vshard_id: VShardId,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<()> {
        use crate::bridge::envelope::PhysicalPlan;

        match plan {
            PhysicalPlan::PointPut {
                collection,
                document_id,
                value,
            } => {
                let entry = rmp_serde::to_vec(&(collection, document_id, value)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point put: {e}"),
                    }
                })?;
                self.state.wal.append_put(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::PointDelete {
                collection,
                document_id,
            } => {
                let entry = rmp_serde::to_vec(&(collection, document_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point delete: {e}"),
                    }
                })?;
                self.state.wal.append_delete(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::VectorInsert {
                collection,
                vector,
                dim,
                field_name: _,
            } => {
                let entry = rmp_serde::to_vec(&(collection, vector, dim)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector insert: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_put(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::VectorBatchInsert {
                collection,
                vectors,
                dim,
            } => {
                let entry = rmp_serde::to_vec(&(collection, vectors, dim)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector batch insert: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_put(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::VectorDelete {
                collection,
                vector_id,
            } => {
                let entry = rmp_serde::to_vec(&(collection, vector_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal vector delete: {e}"),
                    }
                })?;
                self.state
                    .wal
                    .append_vector_delete(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::CrdtApply { delta, .. } => {
                self.state
                    .wal
                    .append_crdt_delta(tenant_id, vshard_id, delta)?;
            }
            PhysicalPlan::EdgePut {
                src_id,
                label,
                dst_id,
                properties,
            } => {
                let entry =
                    rmp_serde::to_vec(&(src_id, label, dst_id, properties)).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("wal edge put: {e}"),
                        }
                    })?;
                self.state.wal.append_put(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::EdgeDelete {
                src_id,
                label,
                dst_id,
            } => {
                let entry = rmp_serde::to_vec(&(src_id, label, dst_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal edge delete: {e}"),
                    }
                })?;
                self.state.wal.append_delete(tenant_id, vshard_id, &entry)?;
            }
            PhysicalPlan::SetVectorParams {
                collection,
                m,
                ef_construction,
                metric,
            } => {
                let entry =
                    rmp_serde::to_vec(&(collection, m, ef_construction, metric)).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("wal set vector params: {e}"),
                        }
                    })?;
                self.state
                    .wal
                    .append_vector_params(tenant_id, vshard_id, &entry)?;
            }
            // Read operations and control commands: no WAL needed.
            _ => {}
        }
        Ok(())
    }
}
