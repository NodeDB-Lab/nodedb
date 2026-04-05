//! DML plan conversion for plain and spatial columnar collections.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::ColumnarOp;
use crate::control::planner::physical::PhysicalTask;
use crate::types::{TenantId, VShardId};

use super::converter::PlanConverter;
use super::extract::extract_insert_values;

impl PlanConverter {
    /// Convert DML for a plain or spatial columnar collection.
    ///
    /// Routes INSERT → ColumnarOp::Insert (JSON payload).
    pub(super) fn convert_columnar_dml(
        &self,
        dml: &datafusion::logical_expr::DmlStatement,
        collection: &str,
        tenant_id: TenantId,
        vshard: VShardId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        use datafusion::logical_expr::WriteOp;

        match &dml.op {
            WriteOp::Insert(_) | WriteOp::Ctas => {
                let values =
                    extract_insert_values(&dml.input).map_err(|_| crate::Error::PlanError {
                        detail: "columnar INSERT requires VALUES clause".into(),
                    })?;

                if values.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: "columnar INSERT requires at least one row".into(),
                    });
                }

                // Convert SQL row values to JSON array for columnar insert handler.
                let mut json_rows = Vec::with_capacity(values.len());
                for (_doc_id, value_bytes) in &values {
                    let row: serde_json::Value =
                        rmp_serde::from_slice(value_bytes).unwrap_or_default();
                    json_rows.push(row);
                }
                let payload = serde_json::to_vec(&json_rows).unwrap_or_default();

                Ok(vec![PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                        collection: collection.to_string(),
                        payload,
                        format: "json".to_string(),
                    }),
                }])
            }
            _ => Err(crate::Error::PlanError {
                detail: format!(
                    "{:?} not supported on columnar collections (append-only)",
                    dml.op
                ),
            }),
        }
    }
}
