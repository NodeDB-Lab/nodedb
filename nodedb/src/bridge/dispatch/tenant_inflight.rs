// SPDX-License-Identifier: BUSL-1.1

//! Per-tenant in-flight accounting and the fair-share cap it derives.
//!
//! Each tenant with a request in flight is active. The cap is the dispatcher's
//! total capacity divided by the active tenant count, at least two. The cap
//! moves only when a tenant turns active or idle. A move gates new admissions
//! only: a request already admitted keeps its slot until it is answered.

use std::collections::HashMap;

/// Smallest per-tenant cap. One slot will serialize every request a tenant
/// sends, so the floor keeps one request in flight while the next one queues.
const MIN_TENANT_CAP: u32 = 2;

/// A tenant held at its in-flight cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TenantAtCap {
    pub(super) inflight: u32,
    pub(super) cap: u32,
}

/// In-flight counts per tenant, the request-to-tenant map, and the cap.
#[derive(Debug)]
pub(super) struct TenantInflight {
    /// In-flight count of every active tenant. A tenant at zero has no entry.
    by_tenant: HashMap<u64, u32>,
    /// The tenant of every admitted request not yet answered.
    request_tenant: HashMap<u64, u64>,
    /// Requests the dispatcher holds across every core.
    total_capacity: u32,
    /// Most requests one tenant can hold in flight. Zero means no cap, which
    /// holds only for a dispatcher with no capacity.
    cap: u32,
}

impl TenantInflight {
    /// Accounting for a dispatcher that holds `total_capacity` requests.
    pub(super) fn new(total_capacity: u32) -> Self {
        Self {
            by_tenant: HashMap::new(),
            request_tenant: HashMap::new(),
            total_capacity,
            cap: total_capacity,
        }
    }

    /// The refusal for one more request of `tenant_id`, when it is at its cap.
    pub(super) fn at_cap(&self, tenant_id: u64) -> Option<TenantAtCap> {
        if self.cap == 0 {
            return None;
        }
        let inflight = self.inflight(tenant_id);
        (inflight >= self.cap).then_some(TenantAtCap {
            inflight,
            cap: self.cap,
        })
    }

    /// Count `request_id` in flight for `tenant_id`. A tenant that turns
    /// active lowers the cap of every tenant.
    pub(super) fn admit(&mut self, tenant_id: u64, request_id: u64) {
        let count = self.by_tenant.entry(tenant_id).or_insert(0);
        *count += 1;
        let turned_active = *count == 1;
        self.request_tenant.insert(request_id, tenant_id);
        if turned_active {
            self.recalculate_cap();
        }
    }

    /// Release the slot `request_id` holds. Returns `true` when a slot was
    /// freed. A tenant that turns idle raises the cap of every other tenant.
    pub(super) fn release(&mut self, request_id: u64) -> bool {
        let Some(tenant_id) = self.request_tenant.remove(&request_id) else {
            return false;
        };
        let Some(count) = self.by_tenant.get_mut(&tenant_id) else {
            return false;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.by_tenant.remove(&tenant_id);
            self.recalculate_cap();
        }
        true
    }

    /// Requests `tenant_id` holds in flight.
    pub(super) fn inflight(&self, tenant_id: u64) -> u32 {
        self.by_tenant.get(&tenant_id).copied().unwrap_or(0)
    }

    /// The tenant of `request_id`, while it is in flight.
    #[cfg(test)]
    pub(super) fn tenant_of(&self, request_id: u64) -> Option<u64> {
        self.request_tenant.get(&request_id).copied()
    }

    /// The current per-tenant cap.
    #[cfg(test)]
    pub(super) fn cap(&self) -> u32 {
        self.cap
    }

    /// Set the cap to the total capacity split across the active tenants.
    fn recalculate_cap(&mut self) {
        if self.total_capacity == 0 {
            return;
        }
        let active = u32::try_from(self.by_tenant.len())
            .unwrap_or(u32::MAX)
            .max(1);
        self.cap = (self.total_capacity / active).max(MIN_TENANT_CAP);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_active_tenant_holds_the_whole_capacity() {
        let mut tenants = TenantInflight::new(8);
        tenants.admit(1, 10);
        assert_eq!(tenants.cap(), 8);
        assert_eq!(tenants.at_cap(1), None);
    }

    #[test]
    fn a_second_active_tenant_halves_the_cap_and_its_exit_restores_it() {
        let mut tenants = TenantInflight::new(8);
        tenants.admit(1, 10);
        tenants.admit(2, 20);
        assert_eq!(tenants.cap(), 4);

        assert!(tenants.release(20));
        assert_eq!(tenants.cap(), 8);
        assert_eq!(tenants.inflight(2), 0);
    }

    #[test]
    fn a_lower_cap_keeps_every_admitted_slot() {
        let mut tenants = TenantInflight::new(8);
        for request_id in 0..6 {
            tenants.admit(1, request_id);
        }
        tenants.admit(2, 100);
        assert_eq!(tenants.cap(), 4);
        assert_eq!(tenants.inflight(1), 6, "admitted requests stay in flight");
        assert_eq!(
            tenants.at_cap(1),
            Some(TenantAtCap {
                inflight: 6,
                cap: 4
            })
        );
        for request_id in 0..6 {
            assert!(tenants.release(request_id));
        }
        assert_eq!(tenants.inflight(1), 0);
    }

    #[test]
    fn the_cap_never_falls_below_the_floor() {
        let mut tenants = TenantInflight::new(4);
        for tenant_id in 0..10 {
            tenants.admit(tenant_id, tenant_id);
        }
        assert_eq!(tenants.cap(), MIN_TENANT_CAP);
    }

    #[test]
    fn releasing_an_unknown_request_frees_nothing() {
        let mut tenants = TenantInflight::new(8);
        assert!(!tenants.release(42));
        tenants.admit(1, 10);
        assert!(tenants.release(10));
        assert!(!tenants.release(10));
        assert_eq!(tenants.tenant_of(10), None);
    }

    #[test]
    fn a_dispatcher_with_no_capacity_has_no_cap() {
        let mut tenants = TenantInflight::new(0);
        tenants.admit(1, 10);
        assert_eq!(tenants.at_cap(1), None);
    }
}
