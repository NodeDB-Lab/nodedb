// SPDX-License-Identifier: Apache-2.0

//! RAII unwind scope for a multi-layer budget reservation.
//!
//! [`ReserveScope`] owns the rollback for a reservation spanning any subset
//! of the global, database, tenant, and engine counters. A caller credits
//! each layer as it succeeds and calls [`ReserveScope::commit`] once every
//! layer needed is credited. Dropping the scope before `commit` releases
//! every credited layer, in reverse order, so a caller never hand-writes a
//! rollback block for a failed layer.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{MemError, Result};
use crate::governor::GlobalCounter;
use crate::over_release::{Layer, ReleaseIdentity, release_layer};

/// Owns the unwind for a reservation across the global, database, tenant,
/// and engine counters.
///
/// Every layer credited before `commit` is released on `Drop` if the scope
/// was never committed. `commit` consumes the scope and hands back the
/// credited layers as a [`ReservedLayers`] for the caller to hold.
pub(crate) struct ReserveScope {
    global: Arc<GlobalCounter>,
    global_credited: bool,
    database: Option<Arc<AtomicUsize>>,
    tenant: Option<Arc<AtomicUsize>>,
    engine: Option<Arc<AtomicUsize>>,
    size: usize,
    committed: bool,
    /// Names the reservation in an unwind-time over-release warning.
    identity: ReleaseIdentity,
}

/// The counters a committed [`ReserveScope`] credited, handed to the caller
/// so it can build a [`crate::reservation_token::ReservationToken`] on top
/// of them.
pub(crate) struct ReservedLayers {
    pub global: Arc<GlobalCounter>,
    pub database: Option<Arc<AtomicUsize>>,
    pub tenant: Option<Arc<AtomicUsize>>,
    pub engine: Option<Arc<AtomicUsize>>,
}

impl ReserveScope {
    /// Start a scope for a `size`-byte reservation. Credits nothing yet.
    pub(crate) fn new(global: Arc<GlobalCounter>, size: usize, identity: ReleaseIdentity) -> Self {
        Self {
            global,
            global_credited: false,
            database: None,
            tenant: None,
            engine: None,
            size,
            committed: false,
            identity,
        }
    }

    /// Credit the global ceiling via a CAS loop.
    ///
    /// A `size` of zero credits nothing and always succeeds. Returns
    /// [`MemError::GlobalCeilingExceeded`] without crediting anything when
    /// the ceiling would be exceeded.
    pub(crate) fn try_credit_global(&mut self) -> Result<()> {
        if self.size == 0 {
            self.global_credited = true;
            return Ok(());
        }
        // The headroom saturates at zero, so the check cannot wrap.
        let mut current = self.global.allocated.load(Ordering::Relaxed);
        loop {
            if self.size > self.global.ceiling.saturating_sub(current) {
                return Err(MemError::GlobalCeilingExceeded {
                    allocated: current,
                    ceiling: self.global.ceiling,
                    requested: self.size,
                });
            }
            let new_val = current + self.size;
            match self.global.allocated.compare_exchange_weak(
                current,
                new_val,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.global_credited = true;
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Credit the global ceiling unconditionally, ignoring the limit.
    ///
    /// The caller already holds this memory — a denial here would only
    /// hide it from the ceiling, not free it. Always succeeds, even past
    /// the ceiling.
    pub(crate) fn credit_global_unchecked(&mut self) {
        self.global.allocated.fetch_add(self.size, Ordering::AcqRel);
        self.global_credited = true;
    }

    /// Record an already-credited database counter so `Drop` releases it.
    pub(crate) fn credit_database(&mut self, counter: Arc<AtomicUsize>) {
        self.database = Some(counter);
    }

    /// Record an already-credited tenant counter so `Drop` releases it.
    pub(crate) fn credit_tenant(&mut self, counter: Arc<AtomicUsize>) {
        self.tenant = Some(counter);
    }

    /// Record an already-credited engine counter so `Drop` releases it.
    pub(crate) fn credit_engine(&mut self, counter: Arc<AtomicUsize>) {
        self.engine = Some(counter);
    }

    /// Finish the scope successfully. Disarms `Drop` and hands back every
    /// credited layer.
    ///
    /// Takes each layer out of `self` rather than cloning it: `Drop` is a
    /// no-op once `committed` is set, so a clone here would only add an
    /// atomic refcount round-trip on a value nobody else holds.
    pub(crate) fn commit(mut self) -> ReservedLayers {
        self.committed = true;
        ReservedLayers {
            global: Arc::clone(&self.global),
            database: self.database.take(),
            tenant: self.tenant.take(),
            engine: self.engine.take(),
        }
    }
}

impl Drop for ReserveScope {
    fn drop(&mut self) {
        if self.committed || self.size == 0 {
            return;
        }
        // Release in reverse order: engine → tenant → database → global.
        // A shortfall here means an earlier layer's rollback unwound a
        // counter this scope never actually credited that much of — the
        // same clamp-hides-drift case `ReservationToken::drop` guards
        // against. `release_layer` counts and warns for both paths, so an
        // unwind-time drift is as visible as a drop-time one.
        if let Some(ref counter) = self.engine {
            release_layer(
                counter,
                self.size,
                Layer::Engine,
                &self.global.over_release,
                self.identity,
            );
        }
        if let Some(ref counter) = self.tenant {
            release_layer(
                counter,
                self.size,
                Layer::Tenant,
                &self.global.over_release,
                self.identity,
            );
        }
        if let Some(ref counter) = self.database {
            release_layer(
                counter,
                self.size,
                Layer::Database,
                &self.global.over_release,
                self.identity,
            );
        }
        if self.global_credited {
            release_layer(
                &self.global.allocated,
                self.size,
                Layer::Global,
                &self.global.over_release,
                self.identity,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineId;
    use nodedb_types::{DatabaseId, TenantId};

    #[test]
    fn try_credit_global_rejects_when_sum_overflows_usize() {
        let global = Arc::new(GlobalCounter::new(usize::MAX));
        global.allocated.store(usize::MAX - 1, Ordering::Relaxed);
        let identity = ReleaseIdentity {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            engine: EngineId::Vector,
        };
        let mut scope = ReserveScope::new(Arc::clone(&global), 2, identity);

        let err = scope.try_credit_global().unwrap_err();
        assert!(matches!(err, MemError::GlobalCeilingExceeded { .. }));
        assert_eq!(global.allocated.load(Ordering::Relaxed), usize::MAX - 1);
        assert!(!scope.global_credited);
    }
}
