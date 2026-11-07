// SPDX-License-Identifier: BUSL-1.1

//! One version cleanup pass: compute the horizon, then purge every Data
//! Plane core below it in batches.
//!
//! Each round sends `MetaOp::PurgeVersionsBelow` to every core. A core
//! purges at most `batch_units` stores per round and answers how many stores
//! it still has due. Other requests run between the rounds, so no reactor
//! stalls on a long purge. The pass ends once every core answers `0`. A
//! failed step records a `version_cleanup_failed` report, and the next pass
//! retries at a fresh horizon.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{MetaOp, PURGE_REMAINING_KEY, PhysicalPlan};

use super::horizon::{cleanup_horizon, mirror_floors};
use super::retained::retained_histories;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// How long every core gets to answer one round.
const ROUND_DEADLINE: Duration = Duration::from_secs(60);

/// What one pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassOutcome {
    /// The horizon every core purged below.
    pub horizon: i64,
    /// The rounds it took until every core answered `0`.
    pub rounds: u64,
}

/// Run one cleanup pass with `batch_units` stores per core per round.
/// `Ok(None)` when the node does not serve reads yet: a pass closes cuts on
/// the leaders of the groups it hosts, and needs Raft running for that.
pub async fn run_cleanup_pass(
    state: &SharedState,
    batch_units: u32,
) -> crate::Result<Option<PassOutcome>> {
    if state.cluster_routing.is_some() && state.raft_read_gate.get().is_none() {
        return Ok(None);
    }
    let horizon = match cleanup_horizon(state).await {
        Ok(computed) => computed.horizon,
        Err(e) => {
            crate::diag::version_cleanup_failed(&e, "horizon", 0);
            return Err(e);
        }
    };
    if horizon <= 0 {
        return Ok(None);
    }
    let retained = match retained_histories(state, horizon) {
        Ok(retained) => retained,
        Err(e) => {
            crate::diag::version_cleanup_failed(&e, "retained collections", horizon);
            return Err(e);
        }
    };
    let mirror_floors = match mirror_floors(state) {
        Ok(floors) => floors,
        Err(e) => {
            crate::diag::version_cleanup_failed(&e, "mirror floors", horizon);
            return Err(e);
        }
    };
    let plan = PhysicalPlan::Meta(MetaOp::PurgeVersionsBelow {
        horizon,
        batch_units,
        retained,
        mirror_floors,
    });
    let mut rounds = 0u64;
    loop {
        rounds += 1;
        let remaining = match purge_round(state, &plan).await {
            Ok(remaining) => remaining,
            Err(e) => {
                crate::diag::version_cleanup_failed(&e, "core purge", horizon);
                return Err(e);
            }
        };
        if remaining == 0 {
            return Ok(Some(PassOutcome { horizon, rounds }));
        }
    }
}

/// One round on every core. Returns the stores still due, summed.
async fn purge_round(state: &SharedState, plan: &PhysicalPlan) -> crate::Result<u64> {
    let response = crate::control::server::broadcast::broadcast_count_to_all_cores_until(
        state,
        TenantId::new(0),
        DatabaseId::DEFAULT,
        plan.clone(),
        TraceId::generate(),
        PURGE_REMAINING_KEY,
        Instant::now() + ROUND_DEADLINE,
    )
    .await?;
    let counts: std::collections::BTreeMap<String, u64> =
        zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("version purge count: {e}"),
            }
        })?;
    counts
        .get(PURGE_REMAINING_KEY)
        .copied()
        .ok_or(crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "version purge count: the response carries no `{PURGE_REMAINING_KEY}` key"
            ),
        })
}
