// SPDX-License-Identifier: BUSL-1.1

//! Per-session record of the last `nextval` each sequence handed this session.
//!
//! SQL `currval` returns the last value THIS SESSION obtained from `nextval`,
//! never the node-wide counter that `SequenceRegistry::node_current_value`
//! reads. Keys match `SequenceRegistry`'s: `"{database_id}:{tenant_id}:{name}"`.

use std::collections::HashMap;
use std::sync::Mutex;

use super::registry::registry_key;

/// One connection's `currval` state, shared with the plan-time catalog adapter.
#[derive(Default)]
pub struct SessionSequenceValues {
    values: Mutex<HashMap<String, i64>>,
}

impl SessionSequenceValues {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the value `nextval` just handed this session.
    pub fn record(&self, database_id: u64, tenant_id: u64, name: &str, value: i64) {
        let key = registry_key(database_id, tenant_id, name);
        self.values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, value);
    }

    /// The last value this session obtained for `name`, or `None` when this
    /// session never called `nextval` on it.
    pub fn last(&self, database_id: u64, tenant_id: u64, name: &str) -> Option<i64> {
        let key = registry_key(database_id, tenant_id, name);
        self.values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .copied()
    }

    /// Drop every recorded value. Called on a full session reset.
    pub fn clear(&self) {
        self.values
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}
