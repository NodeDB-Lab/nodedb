//! [`OriginApplyEngine`] — Control-Plane side of array op application on Origin.
//!
//! On Origin, array engine state (`ArrayEngine`) lives in the Data Plane
//! (`!Send`, TPC thread). The Control Plane cannot own or mutate it directly.
//!
//! This struct provides the **Control-Plane-facing checks** that mirror the
//! [`nodedb_array::sync::ApplyEngine`] contract:
//!
//! - Schema HLC gating (`schema_hlc`)
//! - Idempotency (`already_seen`)
//!
//! The actual cell-level writes (`apply_put`, `apply_delete`, `apply_erase`)
//! are dispatched to the Data Plane via `PhysicalPlan::Array` in
//! [`super::inbound`] — the same pattern used by the Timeseries ingest path.
//! `OriginApplyEngine` does **not** implement `ApplyEngine` directly because
//! the trait's `&mut self` dispatch methods would require Data Plane access
//! that is unavailable on the Control Plane.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_log::OpLog;

use super::op_log::OriginOpLog;
use super::schema_registry::OriginSchemaRegistry;

/// Control-Plane facing checks for Origin array op application.
///
/// Wraps the per-node schema registry and op-log. Used by `OriginArrayInbound`
/// to gate ops before dispatching them to the Data Plane.
pub struct OriginApplyEngine {
    pub(super) schemas: Arc<OriginSchemaRegistry>,
    pub(super) op_log: Arc<OriginOpLog>,
}

impl OriginApplyEngine {
    /// Construct from component parts.
    pub fn new(schemas: Arc<OriginSchemaRegistry>, op_log: Arc<OriginOpLog>) -> Self {
        Self { schemas, op_log }
    }

    /// Return the current schema HLC for `array`, or `None` if the array is
    /// not known to this replica.
    pub fn schema_hlc(&self, array: &str) -> Option<Hlc> {
        self.schemas.schema_hlc(array)
    }

    /// Return `true` if an op with `hlc` has already been applied to `array`.
    ///
    /// Performs a point scan on the op-log: if the key `(array, hlc)` exists,
    /// the op was previously applied.
    pub fn already_seen(&self, array: &str, hlc: Hlc) -> bool {
        match self.op_log.scan_range(array, hlc, hlc) {
            Ok(mut iter) => iter.next().is_some(),
            Err(_) => false,
        }
    }

    /// Record that an op has been applied by appending it to the durable op-log.
    ///
    /// Called by `OriginArrayInbound` after a successful Data Plane dispatch so
    /// future `already_seen` calls return `true` for the same `(array, hlc)`.
    pub fn record_applied(
        &self,
        op: &nodedb_array::sync::op::ArrayOp,
    ) -> nodedb_array::error::ArrayResult<()> {
        self.op_log.append(op)
    }
}
