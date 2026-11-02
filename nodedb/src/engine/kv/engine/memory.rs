// SPDX-License-Identifier: BUSL-1.1

//! KV memory accounting against the node's memory governor.
//!
//! Each table's resident bytes (slot arrays, overflow pool, live entries,
//! and ended versions) are charged to the governor's KV budget, under the
//! table's database and tenant. The charge follows the table in steps of
//! [`KV_CHARGE_STEP_BYTES`], so a write pays for a recharge only when the
//! table drifts a full step from its charge. The governor's KV limit is the
//! budget a write is refused against.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_mem::{EngineId, MemoryGovernor, ReservationToken};
use nodedb_types::TenantId;

use super::KvEngine;

/// Drift between a table's resident bytes and its charge that triggers a
/// recharge.
const KV_CHARGE_STEP_BYTES: usize = 64 * 1024;

/// One table's live charge.
struct TableCharge {
    charged: usize,
    _token: ReservationToken,
}

/// Governor handle and per-table charges.
#[derive(Default)]
pub(in crate::engine::kv) struct KvMemory {
    governor: Option<Arc<MemoryGovernor>>,
    charges: HashMap<u64, TableCharge>,
}

impl KvEngine {
    /// Charge every table's resident bytes to `governor`'s KV budget, and
    /// refuse a write once that budget is exhausted.
    pub fn attach_governor(&mut self, governor: Arc<MemoryGovernor>) {
        self.memory.charges.clear();
        self.memory.governor = Some(governor);
        let tables: Vec<u64> = self.tables.keys().copied().collect();
        for tkey in tables {
            self.recharge_table(tkey);
        }
    }

    /// Whether the governor's KV budget is exhausted. A write handler refuses
    /// a new write with a retriable error while it is. An engine with no
    /// governor has no budget.
    pub fn is_over_budget(&self) -> bool {
        self.memory.governor.as_ref().is_some_and(|governor| {
            let budget = governor.budget(EngineId::Kv);
            budget.allocated() > budget.limit()
        })
    }

    /// Bring `tkey`'s charge in line with its resident bytes. A table that
    /// no longer exists releases its charge.
    pub(in crate::engine::kv) fn recharge_table(&mut self, tkey: u64) {
        let Some(governor) = self.memory.governor.clone() else {
            return;
        };
        let bytes = self.tables.get(&tkey).map_or(0, |table| table.mem_usage());
        if bytes == 0 {
            self.memory.charges.remove(&tkey);
            return;
        }
        if let Some(current) = self.memory.charges.get(&tkey)
            && current.charged.abs_diff(bytes) < KV_CHARGE_STEP_BYTES
        {
            return;
        }
        let (Some(tenant), Some(database)) = (
            self.hash_to_tenant.get(&tkey).copied(),
            self.hash_to_database.get(&tkey).copied(),
        ) else {
            return;
        };
        // Release the prior charge first, so a recharge never counts the
        // table twice.
        self.memory.charges.remove(&tkey);
        let token = governor.charge(database, TenantId::new(tenant), EngineId::Kv, bytes);
        self.memory.charges.insert(
            tkey,
            TableCharge {
                charged: bytes,
                _token: token,
            },
        );
    }

    /// Release `tkey`'s charge. Called when the table is removed.
    pub(in crate::engine::kv) fn release_table(&mut self, tkey: u64) {
        self.memory.charges.remove(&tkey);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_mem::{EngineLimits, GovernorConfig};

    use super::*;
    use crate::engine::kv::KvPutParams;
    use crate::engine::kv::test_support::row_surrogate;

    fn governor(limit: usize) -> Arc<MemoryGovernor> {
        Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: limit * EngineId::ALL.len(),
                engine_limits: EngineLimits::uniform(limit),
            })
            .expect("valid governor config"),
        )
    }

    fn put(e: &mut KvEngine, key: &[u8], value: &[u8]) {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key,
            value,
            ttl_ms: 0,
            now_ms: 1_000,
            surrogate: row_surrogate(key),
        })
        .expect("a bound row writes");
    }

    #[test]
    fn writes_charge_the_governor_and_a_dropped_table_releases() {
        let gov = governor(1 << 30);
        let mut e = KvEngine::new(1_000, 16, 0.75, 4, 64, 1_000, 1_024);
        e.attach_governor(Arc::clone(&gov));
        assert_eq!(gov.budget(EngineId::Kv).allocated(), 0);
        put(&mut e, b"k", b"v");
        assert!(gov.budget(EngineId::Kv).allocated() > 0);
        e.purge_collection(0, 1, "c");
        assert_eq!(
            gov.budget(EngineId::Kv).allocated(),
            0,
            "a dropped table releases"
        );
    }

    #[test]
    fn an_exhausted_budget_refuses_writes() {
        let gov = governor(1);
        let mut e = KvEngine::new(1_000, 16, 0.75, 4, 64, 1_000, 1_024);
        e.attach_governor(gov);
        assert!(!e.is_over_budget());
        put(&mut e, b"k", b"v");
        assert!(e.is_over_budget());
    }
}
