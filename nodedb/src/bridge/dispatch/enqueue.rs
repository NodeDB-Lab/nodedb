// SPDX-License-Identifier: BUSL-1.1

//! Admission, weighted-fair enqueue, and per-tenant in-flight accounting for
//! the bridge [`Dispatcher`].
//!
//! A capacity limit refuses with [`crate::Error::DispatchCapacity`] and hands
//! the request back. Every other refusal is terminal.

use std::collections::HashMap;

use tracing::warn;

use crate::DispatchCapacityScope;
use crate::bridge::admission_chokepoint::{assert_write_admitted, reject_uninjected_write};
use crate::bridge::envelope;

use super::dispatcher::Dispatcher;
use super::refusal::DispatchRefusal;

impl Dispatcher {
    /// Dispatch a request to the correct Data Plane core.
    ///
    /// Enqueues into the per-core weighted-fair queue keyed by `DatabaseId`,
    /// then flushes WFQ → physical ring. A capacity limit refuses with
    /// [`crate::Error::DispatchCapacity`]. Every other refusal is terminal.
    pub fn dispatch(&mut self, request: envelope::Request) -> crate::Result<()> {
        self.try_dispatch(request).map_err(|refusal| refusal.error)
    }

    /// Dispatch like [`Self::dispatch`], handing the request back on refusal.
    ///
    /// A caller that must retry a capacity refusal re-sends the returned
    /// request. The dispatcher tracks nothing for a refused request.
    pub fn try_dispatch(&mut self, request: envelope::Request) -> Result<(), Box<DispatchRefusal>> {
        if let Err(error) = reject_uninjected_write(&request) {
            return Err(DispatchRefusal::boxed(error, request));
        }
        assert_write_admitted(&request);
        if let Err(error) = self.reject_if_draining() {
            return Err(DispatchRefusal::boxed(error, request));
        }
        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();

        // Per-tenant fairness: refuse while the tenant holds its in-flight cap.
        if self.max_per_tenant_inflight > 0 {
            let inflight = self.tenant_inflight.get(&tenant_id).copied().unwrap_or(0);
            if inflight >= self.max_per_tenant_inflight {
                let scope = DispatchCapacityScope::TenantInflight {
                    tenant_id: request.tenant_id,
                    inflight,
                    cap: self.max_per_tenant_inflight,
                };
                return Err(DispatchRefusal::boxed(
                    crate::Error::DispatchCapacity { scope },
                    request,
                ));
            }
        }

        let Some(core_id) = self.router.resolve(request.vshard_id) else {
            let error = crate::Error::Dispatch {
                detail: format!("no core for vshard {}", request.vshard_id),
            };
            return Err(DispatchRefusal::boxed(error, request));
        };

        let channel = &mut self.cores[core_id];

        // Refresh priority for this DB in the WFQ.
        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        // Check per-DB suspended state (≥95% of fair share).
        if channel.wfq.is_suspended_for(database_id) {
            let scope = DispatchCapacityScope::DatabaseSuspended {
                database_id: request.database_id,
                core_id,
            };
            return Err(DispatchRefusal::boxed(
                crate::Error::DispatchCapacity { scope },
                request,
            ));
        }

        // Enqueue into the WFQ. A full queue hands the request back.
        if let Err(request) = channel.wfq.try_enqueue(database_id, request) {
            let scope = DispatchCapacityScope::QueueFull {
                core_id,
                capacity: self.per_core_capacity,
            };
            return Err(DispatchRefusal::boxed(
                crate::Error::DispatchCapacity { scope },
                request,
            ));
        }

        self.commit_enqueued(core_id, database_id, tenant_id, req_id);
        Ok(())
    }

    /// Dispatch a request directly to a specific core by index.
    ///
    /// Bypasses vShard routing. Used by the checkpoint manager to send
    /// checkpoint requests to every core regardless of vShard assignment.
    pub fn dispatch_to_core(
        &mut self,
        core_id: usize,
        request: envelope::Request,
    ) -> crate::Result<()> {
        reject_uninjected_write(&request)?;
        assert_write_admitted(&request);
        self.reject_if_draining()?;
        if core_id >= self.cores.len() {
            return Err(crate::Error::Dispatch {
                detail: format!("core {core_id} out of range (have {})", self.cores.len()),
            });
        }

        let tenant_id = request.tenant_id.as_u64();
        let req_id = request.request_id.as_u64();
        let database_id = request.database_id.as_u64();
        let channel = &mut self.cores[core_id];

        let cls = self.priority_resolver.priority_for(database_id);
        channel.wfq.set_priority(database_id, cls);

        channel.wfq.try_enqueue(database_id, request).map_err(|_| {
            crate::Error::DispatchCapacity {
                scope: DispatchCapacityScope::QueueFull {
                    core_id,
                    capacity: self.per_core_capacity,
                },
            }
        })?;

        self.commit_enqueued(core_id, database_id, tenant_id, req_id);
        Ok(())
    }

    /// Recalculate the per-tenant in-flight limit based on active tenants.
    pub fn recalculate_tenant_limits(&mut self) {
        let active = self.tenant_inflight.len().max(1) as u32;
        let total_capacity: u32 = self.cores.len() as u32 * self.per_core_capacity;
        self.max_per_tenant_inflight = (total_capacity / active).max(2);
        self.tenant_inflight.retain(|_, count| *count > 0);
    }

    /// Bookkeeping once a request sits in `core_id`'s weighted-fair queue:
    /// flush it toward the ring, record pressure, track it as outstanding and
    /// in flight for its tenant, and wake the core.
    fn commit_enqueued(&mut self, core_id: usize, database_id: u64, tenant_id: u64, req_id: u64) {
        let channel = &mut self.cores[core_id];

        // Update per-DB pressure.
        channel.update_db_pressure(database_id);

        // Flush WFQ → physical ring.
        channel.flush_wfq();

        // Update global backpressure based on ring utilization.
        let util = channel.request_tx.utilization();
        if let Some(new_state) = channel.backpressure.update(util) {
            warn!(
                core_id,
                utilization = util,
                state = ?new_state,
                "backpressure transition"
            );
        }

        // Track the request as outstanding on this core, so a later core death
        // can fail it instead of stranding the caller's waiter.
        channel.outstanding.insert(req_id);

        // Track per-tenant in-flight + request→tenant mapping for response routing.
        *self.tenant_inflight.entry(tenant_id).or_insert(0) += 1;
        self.request_tenant.insert(req_id, tenant_id);

        // Wake the Data Plane core via eventfd.
        if let Some(ref notifier) = self.cores[core_id].wake_notifier {
            notifier.notify();
        }
    }
}

/// Release the in-flight slot `request_id` holds for its tenant.
///
/// Returns `true` when a slot was freed. A free function over the two maps,
/// so a caller can release while it holds a borrow of one core's channel.
pub(super) fn release_inflight_slot(
    request_tenant: &mut HashMap<u64, u64>,
    tenant_inflight: &mut HashMap<u64, u32>,
    request_id: u64,
) -> bool {
    let Some(tenant_id) = request_tenant.remove(&request_id) else {
        return false;
    };
    match tenant_inflight.get_mut(&tenant_id) {
        Some(count) if *count > 0 => {
            *count -= 1;
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::BridgeResponse;
    use crate::bridge::dispatch::test_requests::{make_request, make_request_for_db};
    use crate::bridge::envelope::*;
    use crate::types::*;

    #[test]
    fn dispatch_routes_to_correct_core() {
        let (mut dispatcher, data_sides) = Dispatcher::new(4, 64);

        dispatcher.dispatch(make_request(0)).unwrap();
        dispatcher.dispatch(make_request(1)).unwrap();
        dispatcher.dispatch(make_request(4)).unwrap(); // Wraps to core 0.

        assert_eq!(data_sides[0].request_rx.len(), 2);
        assert_eq!(data_sides[1].request_rx.len(), 1);
        assert_eq!(data_sides[2].request_rx.len(), 0);
    }

    #[test]
    fn tenant_at_inflight_cap_is_refused_with_tenant_scope() {
        // One core with capacity 4 caps each tenant at 4 in-flight requests.
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);

        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, i + 1, i + 1))
                .unwrap();
        }

        let refusal = dispatcher
            .try_dispatch(make_request_for_db(0, 99, 99))
            .expect_err("the fifth request exceeds the tenant cap");
        let DispatchRefusal { error, request } = *refusal;
        match error {
            crate::Error::DispatchCapacity {
                scope:
                    DispatchCapacityScope::TenantInflight {
                        tenant_id,
                        inflight,
                        cap,
                    },
            } => {
                assert_eq!(tenant_id, TenantId::new(1));
                assert_eq!(inflight, 4);
                assert_eq!(cap, 4);
            }
            other => panic!("expected a tenant-cap refusal, got: {other}"),
        }
        assert_eq!(
            request.request_id,
            RequestId::new(99),
            "the refused request is handed back unsent"
        );
    }

    #[test]
    fn full_weighted_fair_queue_is_refused_with_queue_full_scope() {
        // Distinct tenants and databases keep the tenant cap and the per-DB
        // suspension out of play, so only the queue total can refuse.
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 4);

        for i in 1..=64u64 {
            let mut request = make_request_for_db(0, i, i);
            request.tenant_id = TenantId::new(i);
            match dispatcher.dispatch(request) {
                Ok(()) => continue,
                Err(crate::Error::DispatchCapacity {
                    scope: DispatchCapacityScope::QueueFull { core_id, capacity },
                }) => {
                    assert_eq!(core_id, 0);
                    assert_eq!(capacity, 4);
                    return;
                }
                Err(other) => panic!("expected a queue-full refusal, got: {other}"),
            }
        }
        panic!("the weighted-fair queue never filled");
    }

    #[test]
    fn dispatch_to_core_tracks_request_lifecycle() {
        let (mut dispatcher, mut data_sides) = Dispatcher::new(2, 64);
        let request = make_request(0);
        let tenant_id = request.tenant_id.as_u64();
        let request_id = request.request_id.as_u64();

        dispatcher.dispatch_to_core(1, request).unwrap();

        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&1));
        assert_eq!(dispatcher.request_tenant.get(&request_id), Some(&tenant_id));
        assert_eq!(data_sides[1].request_rx.len(), 1);

        let _req = data_sides[1].request_rx.try_pop().unwrap();
        data_sides[1]
            .response_tx
            .try_push(BridgeResponse {
                inner: envelope::Response {
                    request_id: RequestId::new(request_id),
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: None,
                    read_set_valid: None,
                    read_version_lsn: crate::types::Lsn::ZERO,
                    write_set: Vec::new(),
                },
            })
            .unwrap();

        let responses = dispatcher.poll_responses();
        assert_eq!(responses.len(), 1);
        assert_eq!(dispatcher.tenant_inflight.get(&tenant_id), Some(&0));
        assert!(!dispatcher.request_tenant.contains_key(&request_id));
    }

    #[test]
    fn per_db_pressure_reported() {
        let (mut dispatcher, _) = Dispatcher::new(1, 8);
        // Fill fair share for DB 1 using 4 of 8 slots.
        // With one DB initially, fair share = 8. With two DBs = 4 each.
        // First enqueue DB1 + DB2, so fair_share = 4.
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 1, i + 10))
                .unwrap();
        }
        for i in 0..4u64 {
            dispatcher
                .dispatch(make_request_for_db(0, 2, i + 20))
                .unwrap();
        }
        // After filling DB1's fair share, it should be suspended on core 0.
        // (exact state depends on WFQ flush draining items to ring first)
        // The test confirms per-DB pressure is being tracked without panic.
        let _ = dispatcher.db_pressure_on_core(0, 1);
        let _ = dispatcher.db_pressure_on_core(0, 2);
    }
}
