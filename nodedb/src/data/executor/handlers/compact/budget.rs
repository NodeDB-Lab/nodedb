// SPDX-License-Identifier: BUSL-1.1

//! Per-database maintenance CPU budget gating.
//!
//! Compaction work is gated against the per-database CPU budget tracker
//! installed via `CoreLoop::set_maintenance_budget`. Each granted lease
//! records actual elapsed wall-clock time on drop into a 60-second sliding
//! window; when the cap is exhausted the next acquire returns `None` and
//! the caller defers the work to a future cycle.

use crate::control::maintenance::MaintenanceLease;
use crate::data::executor::core_loop::CoreLoop;
use nodedb_types::DatabaseId;

/// Outcome of a budget gate for a single maintenance unit.
///
/// The `Granted` variant carries an `Option<MaintenanceLease>`:
/// - `Some(lease)` — caller MUST hold the lease for the duration of the work;
///   on drop, actual elapsed wall-clock time is recorded into the per-database
///   sliding window. Dropping the lease before the work runs records ~0 and
///   silently disables the budget — see the regression test
///   `lease_is_held_across_work` in `tests`.
/// - `None` — no tracker installed or `force` set; no recording is needed.
pub(in crate::data::executor::handlers) enum BudgetGate {
    Granted(Option<MaintenanceLease>),
    Deferred,
}

impl CoreLoop {
    /// Acquire a maintenance lease for `db`, returning a [`BudgetGate`].
    ///
    /// Callers MUST bind the returned lease to a `let` whose scope spans the
    /// actual maintenance work. The lease's `Drop` impl is what records
    /// elapsed wall-clock time into the per-database budget window.
    pub(in crate::data::executor::handlers) fn acquire_maintenance_lease(
        &self,
        db: DatabaseId,
        force: bool,
    ) -> BudgetGate {
        if force {
            return BudgetGate::Granted(None);
        }
        match self.maintenance.maintenance_budget.as_ref() {
            None => BudgetGate::Granted(None),
            Some(tracker) => match tracker.try_acquire(db, 0.0) {
                Some(lease) => BudgetGate::Granted(Some(lease)),
                None => BudgetGate::Deferred,
            },
        }
    }
}
