// SPDX-License-Identifier: BUSL-1.1

//! Distributed maintenance operations: ANALYZE/COMPACT/REINDEX across shards.
//!
//! These operations are dispatched to each shard leader independently.
//! Results are merged on the coordinator node. On a one-node cluster every
//! leader is this node, so they run on the local Data Plane.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

/// Budget for one background COMPACT / REINDEX pass.
///
/// Deliberately not `tuning.network.default_deadline_secs`: that bounds a
/// client statement, and a maintenance pass rewrites whole segments at
/// `Priority::Background`. Bounding it by a query deadline will abandon the
/// rewrite partway on every collection large enough to need it.
const MAINTENANCE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);

/// Dispatch a maintenance operation (COMPACT/REINDEX) to all Data Plane cores.
///
/// In single-node: dispatches to core 0 (the only core in most test configs).
/// In cluster: will dispatch to each shard leader. Currently dispatches locally.
pub fn dispatch_maintenance_to_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: MetaOp,
) {
    let request = Request {
        request_id: RequestId::new(0),
        tenant_id,
        database_id,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Meta(op),
        deadline: std::time::Instant::now() + MAINTENANCE_DEADLINE,
        priority: Priority::Background,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };

    match state.dispatcher.lock() {
        Ok(mut d) => {
            let _ = d.dispatch(request);
        }
        Err(p) => {
            let _ = p.into_inner().dispatch(request);
        }
    }
}

/// Merge per-shard column statistics into global statistics.
///
/// For distributed ANALYZE, each shard leader collects local stats.
/// The coordinator calls this to merge them:
/// - row_count: sum across shards
/// - null_count: sum across shards
/// - distinct_count: max across shards (HLL merge will be more accurate)
/// - min_value: min of all shard mins
/// - max_value: max of all shard maxes
pub fn merge_column_stats(
    shards: &[crate::control::security::catalog::column_stats::StoredColumnStats],
) -> Option<crate::control::security::catalog::column_stats::StoredColumnStats> {
    if shards.is_empty() {
        return None;
    }

    let first = &shards[0];
    let mut merged = first.clone();

    for shard in &shards[1..] {
        merged.row_count += shard.row_count;
        merged.null_count += shard.null_count;
        // Approximate: take max distinct count (HLL merge will be better).
        merged.distinct_count = merged.distinct_count.max(shard.distinct_count);

        // Merge min/max.
        match (&merged.min_value, &shard.min_value) {
            (Some(cur), Some(new)) if new < cur => {
                merged.min_value = shard.min_value.clone();
            }
            (None, Some(_)) => {
                merged.min_value = shard.min_value.clone();
            }
            _ => {}
        }

        match (&merged.max_value, &shard.max_value) {
            (Some(cur), Some(new)) if new > cur => {
                merged.max_value = shard.max_value.clone();
            }
            (None, Some(_)) => {
                merged.max_value = shard.max_value.clone();
            }
            _ => {}
        }
    }

    Some(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::column_stats::StoredColumnStats;

    #[test]
    fn merge_two_shards() {
        let a = StoredColumnStats {
            database_id: 4,
            tenant_id: 1,
            collection: "t".into(),
            column: "c".into(),
            row_count: 500,
            null_count: 10,
            distinct_count: 200,
            min_value: Some("10".into()),
            max_value: Some("500".into()),
            avg_value_len: None,
            analyzed_at: 0,
        };
        let b = StoredColumnStats {
            database_id: 4,
            tenant_id: 1,
            collection: "t".into(),
            column: "c".into(),
            row_count: 300,
            null_count: 5,
            distinct_count: 150,
            min_value: Some("5".into()),
            max_value: Some("800".into()),
            avg_value_len: None,
            analyzed_at: 0,
        };

        let merged = merge_column_stats(&[a, b]).unwrap();
        assert_eq!(merged.row_count, 800);
        assert_eq!(merged.null_count, 15);
        assert_eq!(merged.distinct_count, 200);
        assert_eq!(merged.min_value, Some("10".into())); // "10" < "5" lexically
        assert_eq!(merged.max_value, Some("800".into()));
    }
}
