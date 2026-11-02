// SPDX-License-Identifier: BUSL-1.1

//! Conversion of planned SQL to physical tasks for [`QueryContext`], with
//! every surrogate key resolved at its home first.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::types::SqlPlan;

use super::QueryContext;
use crate::control::planner::sql_plan_convert::{ConvertContext, PlanningPurpose, convert_bound};

impl QueryContext {
    /// The conversion context for one plan batch, read from this context's
    /// wiring and per-request knobs.
    fn convert_context(
        &self,
        purpose: PlanningPurpose,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
    ) -> ConvertContext {
        ConvertContext {
            purpose,
            retention_registry: self.retention_registry.clone(),
            array_catalog: self.array_catalog.clone(),
            credentials: Some(Arc::clone(&self.catalog_inputs.credentials)),
            surrogate_assigner: self.surrogate_assigner.clone(),
            bitemporal_retention_registry: self.bitemporal_retention_registry.clone(),
            max_vector_dim: self.max_vector_dim.load(Ordering::Relaxed),
            force_shuffle_join: self.force_shuffle_join.load(Ordering::Relaxed),
            shuffle_num_parts: self.shuffle_num_parts.load(Ordering::Relaxed) as usize,
            force_shuffle_agg: self.force_shuffle_agg.load(Ordering::Relaxed),
            shuffle_agg_num_parts: self.shuffle_agg_num_parts.load(Ordering::Relaxed) as usize,
            broadcast_threshold_bytes: self.broadcast_threshold_bytes.load(Ordering::Relaxed),
            shuffle_agg_threshold: self.shuffle_agg_threshold.load(Ordering::Relaxed),
            database_id,
            tenant_id,
            prefetched: Default::default(),
            reads_txn_snapshot: self.txn_snapshot().is_some(),
        }
    }

    /// Convert `plans` to physical tasks. Every surrogate the conversion uses
    /// is drawn or answered at its collection home first, awaited, so the
    /// synchronous conversion finds each answer in hand and never parks a
    /// runtime worker.
    pub(super) async fn convert_prefetched(
        &self,
        plans: &[SqlPlan],
        purpose: PlanningPurpose,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
    ) -> crate::Result<Vec<PhysicalTask>> {
        let mut ctx = self.convert_context(purpose, database_id, tenant_id);
        convert_bound(plans, tenant_id, &mut ctx).await
    }
}
