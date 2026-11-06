// SPDX-License-Identifier: BUSL-1.1

//! How far back a historical read of a non-bitemporal collection reaches.
//!
//! Every collection keeps superseded versions, but only a `bitemporal=true`
//! collection keeps them for an audit window. Any other collection keeps
//! them until the cleanup horizon passes them, and the horizon trails the
//! clock by `snapshot_max_hold` (`super::horizon`). A historical read at a
//! cut older than `txn_snapshot_max_age_secs` sees versions the purge
//! removed mid-statement, so a read of such a collection at such a cut is
//! refused. A cut within the limit stays above every purge for the whole
//! statement: the hold covers the age limit, the longest statement
//! deadline, and the clock skew bound.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::SystemTimeScope;

use crate::control::gateway::version_set::{catalog_name, touched_collections};
use crate::control::planner::plan_children::children;
use crate::control::planner::system_cut::plan_scopes;
use crate::control::state::SharedState;

/// Refuse `tasks` when one reads a collection that is not bitemporal at a
/// pinned cut older than the node's snapshot age limit. A collection the
/// catalog does not hold, such as an array, is not refused here.
///
/// Returns true when a task reads such a collection at any pinned cut: the
/// read then depends on when it runs, so its plan never enters a plan
/// cache.
///
/// A malformed plan name is an error. A fallback to the raw name would miss
/// the catalog row, and read as a collection the catalog does not hold, with
/// no refusal.
pub fn check_history_window(state: &SharedState, tasks: &[PhysicalTask]) -> crate::Result<bool> {
    let limit = std::time::Duration::from_secs(state.tuning.network.txn_snapshot_max_age_secs);
    let limit_ns = window_nanos(limit)?;
    let mut time_bound = false;
    for task in tasks {
        let Some(oldest) = oldest_pinned_cut(&task.plan) else {
            continue;
        };
        let now_ns = crate::control::read_cut::history_clock_ns(state, task.database_id)?;
        let floor = now_ns.saturating_sub(limit_ns);
        let mut names = Vec::new();
        collect_collections(&task.plan, &mut names);
        names.sort();
        names.dedup();
        for name in names {
            let (owner, bare) = catalog_name(task.database_id, &name)?;
            let Some(collection) =
                state
                    .credentials
                    .catalog()
                    .get_collection(owner, task.tenant_id.as_u64(), bare)?
            else {
                continue;
            };
            if collection.bitemporal {
                continue;
            }
            if oldest < floor {
                return Err(refusal(bare, now_ns.saturating_sub(oldest), limit));
            }
            time_bound = true;
        }
    }
    Ok(time_bound)
}

/// The edge collections a graph walk reads: one collection, or every
/// collection of the tenant for a walk that names none.
#[derive(Debug, Clone, Copy)]
pub struct GraphHistory<'a> {
    pub database_id: crate::types::DatabaseId,
    pub tenant_id: crate::types::TenantId,
    pub collection: Option<&'a str>,
}

/// Refuse a graph walk at `cut` when the cut is older than the snapshot age
/// limit and a collection `history` names is not bitemporal. A walk that
/// names no collection reads every collection of its tenant.
pub fn check_graph_history_window(
    state: &SharedState,
    history: GraphHistory<'_>,
    cut: i64,
) -> crate::Result<()> {
    let limit = std::time::Duration::from_secs(state.tuning.network.txn_snapshot_max_age_secs);
    let limit_ns = window_nanos(limit)?;
    let now_ns = crate::control::read_cut::history_clock_ns(state, history.database_id)?;
    if cut >= now_ns.saturating_sub(limit_ns) {
        return Ok(());
    }
    let catalog = state.credentials.catalog();
    let tenant = history.tenant_id.as_u64();
    let plain = match history.collection {
        Some(collection) => catalog
            .get_collection(history.database_id, tenant, collection)?
            .filter(|stored| !stored.bitemporal)
            .map(|stored| stored.name),
        None => catalog
            .load_all_collections_across_databases()?
            .into_iter()
            .find(|stored| {
                stored.is_active
                    && !stored.bitemporal
                    && stored.database_id == history.database_id
                    && stored.tenant_id == tenant
            })
            .map(|stored| stored.name),
    };
    match plain {
        Some(name) => Err(refusal(&name, now_ns.saturating_sub(cut), limit)),
        None => Ok(()),
    }
}

/// The snapshot age limit in ordinal units. A limit past the ordinal range is
/// an error: a saturated limit would refuse no read at any age.
fn window_nanos(limit: std::time::Duration) -> crate::Result<i64> {
    i64::try_from(limit.as_nanos()).map_err(|_| crate::Error::Config {
        detail: format!(
            "txn_snapshot_max_age_secs {} exceeds the system-time ordinal range; set a lower \
             value",
            limit.as_secs()
        ),
    })
}

/// The oldest pinned historical cut `plan` reads at, `None` for none.
fn oldest_pinned_cut(plan: &nodedb_physical::physical_plan::PhysicalPlan) -> Option<i64> {
    plan_scopes(plan)
        .into_iter()
        .filter_map(|scope| match scope {
            SystemTimeScope::Pinned { ordinal, .. } => Some(ordinal),
            SystemTimeScope::AsOf(ms) => Some(nodedb_types::ms_to_ordinal_upper(ms)),
            SystemTimeScope::Current
            | SystemTimeScope::TxnSnapshot { .. }
            | SystemTimeScope::AllVersions => None,
        })
        .min()
}

/// Every collection `plan` and its child plans touch.
fn collect_collections(plan: &nodedb_physical::physical_plan::PhysicalPlan, out: &mut Vec<String>) {
    out.extend(touched_collections(plan));
    for child in children(plan) {
        collect_collections(child, out);
    }
}

/// The refusal of a read `age_ns` back on `collection`.
fn refusal(collection: &str, age_ns: i64, limit: std::time::Duration) -> crate::Error {
    let age_ms = nodedb_types::ordinal_to_ms(age_ns);
    crate::Error::BadRequest {
        detail: format!(
            "AS OF SYSTEM TIME reaches {age_ms}ms back on collection '{collection}', past the \
             {}ms history of a collection without bitemporal=true \
             (txn_snapshot_max_age_secs); read a later time, or create the collection \
             WITH (bitemporal=true) to keep its history",
            limit.as_millis()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_limit_past_the_ordinal_range_is_an_error_and_never_saturates() {
        assert_eq!(
            window_nanos(std::time::Duration::from_secs(5)).expect("in range"),
            5_000_000_000
        );
        let refused = window_nanos(std::time::Duration::from_secs(u64::MAX));
        assert!(matches!(refused, Err(crate::Error::Config { .. })));
    }

    /// A malformed name is an error here. It never reads as a collection the
    /// catalog does not hold, which skips the refusal.
    #[test]
    fn a_malformed_plan_name_is_an_error() {
        let database = crate::types::DatabaseId::new(7);
        assert!(catalog_name(database, "x/orders").is_err());
    }
}
