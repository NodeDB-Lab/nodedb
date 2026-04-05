//! DML plan conversion for timeseries collections.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TimeseriesOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::extract_insert_values;

impl PlanConverter {
    /// Convert DML for a timeseries collection.
    ///
    /// Routes INSERT → TimeseriesOp::Ingest (converts SQL values to ILP format).
    /// DELETE and UPDATE are not supported on timeseries (append-only semantics).
    pub(super) fn convert_timeseries_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        tenant_id: TenantId,
        collection: &str,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values =
                    extract_insert_values(&dml.input).map_err(|_| crate::Error::PlanError {
                        detail: "timeseries INSERT requires VALUES clause".into(),
                    })?;

                if values.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "timeseries INSERT requires at least one row".into(),
                    });
                }

                // Convert SQL row values to ILP lines for the timeseries ingest handler.
                // Each row is a MessagePack map: {"col1": val1, "col2": val2, ...}
                // Convert to ILP: `collection field1=val1,field2=val2 timestamp_ns`
                let mut ilp_batch = String::new();
                for (_doc_id, value_bytes) in &values {
                    let row: serde_json::Value =
                        rmp_serde::from_slice(value_bytes).unwrap_or_default();
                    if let serde_json::Value::Object(map) = row {
                        // Extract timestamp (look for common timestamp field names).
                        let ts_ns = map
                            .get("ts")
                            .or_else(|| map.get("timestamp"))
                            .or_else(|| map.get("time"))
                            .or_else(|| map.get("created_at"))
                            .and_then(|v| v.as_i64())
                            .map(|ms| ms * 1_000_000) // ms → ns
                            .unwrap_or_else(|| {
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_nanos() as i64)
                                    .unwrap_or(0)
                            });

                        // Build ILP fields from remaining columns.
                        let mut fields = Vec::new();
                        for (k, v) in &map {
                            if k == "ts"
                                || k == "timestamp"
                                || k == "time"
                                || k == "created_at"
                                || k == "id"
                                || k == "document_id"
                            {
                                continue;
                            }
                            match v {
                                serde_json::Value::Number(n) => {
                                    fields.push(format!("{k}={n}"));
                                }
                                serde_json::Value::String(s) => {
                                    fields.push(format!("{k}=\"{s}\""));
                                }
                                serde_json::Value::Bool(b) => {
                                    fields.push(format!("{k}={b}"));
                                }
                                _ => {}
                            }
                        }

                        if !fields.is_empty() {
                            ilp_batch.push_str(collection);
                            ilp_batch.push(' ');
                            ilp_batch.push_str(&fields.join(","));
                            ilp_batch.push(' ');
                            ilp_batch.push_str(&ts_ns.to_string());
                            ilp_batch.push('\n');
                        }
                    }
                }

                if ilp_batch.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "timeseries INSERT: no valid field values extracted".into(),
                    });
                }

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                        collection: collection.to_string(),
                        payload: ilp_batch.into_bytes(),
                        format: "ilp".to_string(),
                        wal_lsn: None,
                    }),
                }])
            }
            WriteOp::Update => Err(crate::Error::PlanError {
                detail: "UPDATE not supported on timeseries collections (append-only)".into(),
            }),
            WriteOp::Delete => Err(crate::Error::PlanError {
                detail: "DELETE not supported on timeseries collections (use retention policies)"
                    .into(),
            }),
            WriteOp::Truncate => Err(crate::Error::PlanError {
                detail: "TRUNCATE not supported on timeseries collections (use retention policies)"
                    .into(),
            }),
        }
    }
}
