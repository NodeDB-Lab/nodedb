// SPDX-License-Identifier: BUSL-1.1

//! Fixtures shared by the intake-throttle cases.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::bridge::dispatch::BridgeRequest;
use nodedb::bridge::envelope::Request;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::RequestId;
use nodedb_bridge::buffer::Producer;
use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
use nodedb_types::{DatabaseId, TenantId};

/// Per-engine budget every test governor is built with.
const TEST_BUDGET_BYTES: usize = 10_000;

/// Response-ring capacity, matching the rings `make_core` wires up.
const RESPONSE_RING_CAPACITY: usize = 64;

/// Responses that put the ring above the 85% throttle-enter threshold.
pub(super) const RESPONSES_ABOVE_THROTTLE: usize = RESPONSE_RING_CAPACITY * 90 / 100;

/// Responses that put the ring above the 95% suspend-enter threshold.
pub(super) const RESPONSES_ABOVE_SUSPEND: usize = RESPONSE_RING_CAPACITY * 97 / 100;

/// Read depth reported while intake is suspended.
pub(super) const SUSPENDED_READ_DEPTH: usize = 1;

/// Baseline SPSC read depth from the `CoreLoop` accessor, so cases never
/// hard-code it.
pub(super) fn normal_depth() -> usize {
    CoreLoop::spsc_read_depth_normal()
}

/// A `MemoryGovernor` with `TEST_BUDGET_BYTES` per engine, with `engine`
/// pre-filled to `utilization_percent` of its budget.
///
/// Every other engine sits at 0%, so `engine_pressure(engine)` reflects the
/// supplied utilization and the rest stay Normal.
pub(super) fn make_governor_at(engine: EngineId, utilization_percent: u8) -> Arc<MemoryGovernor> {
    let global_ceiling = TEST_BUDGET_BYTES * EngineId::ALL.len() * 2;
    let gov = MemoryGovernor::new(GovernorConfig {
        global_ceiling,
        engine_limits: EngineLimits::uniform(TEST_BUDGET_BYTES),
    })
    .expect("governor");
    let fill = (TEST_BUDGET_BYTES as u64 * utilization_percent as u64 / 100) as usize;
    if fill > 0
        && let Ok(token) = gov.try_reserve(DatabaseId::DEFAULT, TenantId::new(1), engine, fill)
    {
        // `ReservationToken` is RAII: dropping it releases the reservation and
        // resets the engine to 0%. Leak it so the budget stays charged for the
        // lifetime of this governor.
        std::mem::forget(token);
    }
    Arc::new(gov)
}

/// Park `count` responses in the outbound ring without consuming them.
///
/// Each request carries an expired deadline, so the core answers it with
/// `DEADLINE_EXCEEDED` without touching an engine. The ring fills, the
/// governor stays calm, and the response ring is the only pressure input.
pub(super) fn fill_response_ring(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    count: usize,
) {
    for id in 0..count as u64 {
        let plan = PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: RequestId::new(u64::MAX),
        });
        let inner = Request {
            deadline: Instant::now() - Duration::from_secs(1),
            ..crate::cases::core_loop::helpers::make_request_with_id(id, plan)
        };
        tx.try_push(BridgeRequest::unfloored(inner))
            .expect("request ring has room");
    }
    core.tick();
}
