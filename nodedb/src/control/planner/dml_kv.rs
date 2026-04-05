//! DML plan conversion for Key-Value collections.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::KvOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::{extract_insert_values, extract_point_targets, extract_update_assignments};

impl PlanConverter {
    /// Convert DML for a KV collection.
    ///
    /// Routes INSERT → KvPut, DELETE → KvDelete, UPDATE → KvFieldSet.
    /// Truncate uses KvOp::Truncate.
    pub(super) fn convert_kv_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
        collection: &str,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                // Extract inserted values: each (doc_id, value_bytes) maps to a KV PUT.
                // The doc_id serves as the primary key, value_bytes are the serialized row.
                match extract_insert_values(&dml.input) {
                    Ok(values) if !values.is_empty() => {
                        let mut tasks = Vec::with_capacity(values.len());
                        for (key_str, value_bytes) in values {
                            tasks.push(PhysicalTask {
                                tenant_id,
                                vshard_id: vshard,
                                plan: PhysicalPlan::Kv(KvOp::Put {
                                    collection: collection.to_string(),
                                    key: key_str.into_bytes(),
                                    value: value_bytes,
                                    ttl_ms: 0, // Collection-default TTL applied by engine.
                                }),
                            });
                        }
                        return Ok(tasks);
                    }
                    _ => {}
                }

                Err(crate::Error::PlanError {
                    detail: "KV INSERT requires VALUES clause".into(),
                })
            }
            WriteOp::Delete => {
                let doc_ids = extract_point_targets(&dml.input, collection).unwrap_or_default();
                if !doc_ids.is_empty() {
                    let keys: Vec<Vec<u8>> =
                        doc_ids.into_iter().map(|id| id.into_bytes()).collect();
                    return Ok(vec![PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Kv(KvOp::Delete {
                            collection: collection.to_string(),
                            keys,
                        }),
                    }]);
                }

                // Bulk KV delete not yet supported — fall back to error.
                Err(crate::Error::PlanError {
                    detail: "KV DELETE requires WHERE with primary key(s)".into(),
                })
            }
            WriteOp::Update => {
                let updates = extract_update_assignments(&dml.input)?;
                if updates.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "KV UPDATE requires at least one SET assignment".into(),
                    });
                }

                // Point UPDATE: WHERE key = 'x' (or WHERE id = 'x').
                let key_ids = extract_point_targets(&dml.input, collection).unwrap_or_default();
                if key_ids.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "KV UPDATE requires WHERE with primary key \
                                 (e.g., WHERE key = 'mykey')"
                            .into(),
                    });
                }

                let mut tasks = Vec::with_capacity(key_ids.len());
                for key_str in key_ids {
                    tasks.push(PhysicalTask {
                        tenant_id,
                        vshard_id: vshard,
                        plan: PhysicalPlan::Kv(KvOp::FieldSet {
                            collection: collection.to_string(),
                            key: key_str.into_bytes(),
                            updates: updates.clone(),
                        }),
                    });
                }
                Ok(tasks)
            }
            WriteOp::Truncate => Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                plan: PhysicalPlan::Kv(KvOp::Truncate {
                    collection: collection.to_string(),
                }),
            }]),
        }
    }
}
