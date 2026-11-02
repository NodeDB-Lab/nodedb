// SPDX-License-Identifier: BUSL-1.1

//! The collections whose history the cleanup horizon does not govern.
//!
//! A `bitemporal=true` collection keeps its history for `audit_retain_ms`.
//! Its history purges at the last ordinal of `now - audit_retain_ms`, capped
//! at the cleanup horizon, by the same rule as every other collection. One
//! with no audit window keeps its whole history.

use nodedb_physical::physical_plan::RetainedHistory;

use crate::control::state::SharedState;

/// Every active bitemporal collection of the local catalog, each with the
/// horizon its history purges below under `horizon`.
pub fn retained_histories(
    state: &SharedState,
    horizon: i64,
) -> crate::Result<Vec<RetainedHistory>> {
    let now_ms = nodedb_types::ordinal_to_ms(nodedb_types::wall_now_ns_i64());
    let windows = state.bitemporal_retention_registry.snapshot();
    let retained = state
        .credentials
        .catalog()
        .load_all_collections_across_databases()?
        .into_iter()
        .filter(|collection| collection.is_active && collection.bitemporal)
        .map(|collection| {
            let audit_ms = windows
                .iter()
                .find(|entry| {
                    entry.database_id == collection.database_id
                        && entry.tenant_id.as_u64() == collection.tenant_id
                        && entry.collection == collection.name
                })
                .map_or(0, |entry| entry.retention.audit_retain_ms);
            RetainedHistory {
                database_id: collection.database_id.as_u64(),
                tenant_id: collection.tenant_id,
                collection: collection.name,
                horizon: audit_horizon(now_ms, audit_ms, horizon),
            }
        })
        .collect();
    Ok(retained)
}

/// The horizon of a collection with audit window `audit_ms` at `now_ms`,
/// capped at the cleanup horizon `horizon`. `None` for no window.
fn audit_horizon(now_ms: i64, audit_ms: u64, horizon: i64) -> Option<i64> {
    if audit_ms == 0 {
        return None;
    }
    let audit_ms = i64::try_from(audit_ms).unwrap_or(i64::MAX);
    let cutoff = nodedb_types::ms_to_ordinal_upper(now_ms.saturating_sub(audit_ms));
    Some(cutoff.min(horizon))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_audit_window_is_capped_at_the_horizon() {
        let now_ms = 1_000_000;
        assert_eq!(audit_horizon(now_ms, 0, 9), None, "no window keeps all");
        assert_eq!(
            audit_horizon(now_ms, 1_000, i64::MAX),
            Some(nodedb_types::ms_to_ordinal_upper(999_000))
        );
        assert_eq!(audit_horizon(now_ms, 1_000, 5), Some(5));
    }
}
