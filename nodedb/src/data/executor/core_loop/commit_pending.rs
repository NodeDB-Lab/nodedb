// SPDX-License-Identifier: BUSL-1.1

//! Staged Calvin write plans awaiting the local commit verdict.

use crate::data::executor::handlers::control::calvin_reply::CalvinReply;
use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;

/// A Calvin transaction staged for commit, held between the
/// validate-and-stage step and the verdict-driven flush-or-drop.
///
/// `CalvinExecuteStatic` and `CalvinExecuteActive` stage the plans into the
/// synthetic overlay and insert this entry WITHOUT mutating base or firing
/// side effects. `CalvinResolve` resolves the overlay into the transaction's
/// redo record. A verdict-driven `CalvinFlush` installs that record and
/// answers with `reply`. A `CalvinDrop` discards the entry.
/// Nothing here is observable in the base engines until a flush.
pub(in crate::data::executor) struct PendingCommit {
    /// The staged write plans `CalvinResolve` resolves against the overlay.
    pub plans: Vec<PhysicalPlan>,
    /// Tenant scope for `plans`.
    pub tenant_id: TenantId,
    /// Deterministic epoch timestamp anchor. Resolve restores it so the
    /// stamps it assigns are identical across replicas, and the flush
    /// advances the core clock to it.
    pub epoch_system_ms: i64,
    /// The reply the transaction's plans decided when they staged: the last
    /// `RETURNING` plan's rows, else the last plan's affected count. The
    /// flush answers with it.
    pub reply: CalvinReply,
}
