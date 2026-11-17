// SPDX-License-Identifier: BUSL-1.1

//! Per-database DML audit mode cache.
//!
//! Stores the `AuditDmlMode` for each database in memory so the Event Plane
//! consumer can check it without a catalog round-trip on every write event.
//! Updated synchronously in the `ALTER DATABASE SET AUDIT_DML` handler.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_types::{AuditDmlMode, DatabaseId};

use crate::control::security::catalog::SystemCatalog;
use crate::event::interest::{Interest, InterestSlice};

/// In-memory cache of per-database DML audit modes.
///
/// Control Plane only (`Send + Sync`). Backed by a `RwLock<HashMap>` for
/// concurrent reads with infrequent writes.
pub struct AuditDmlCache {
    inner: RwLock<HashMap<DatabaseId, AuditDmlMode>>,
    /// Every collection of each audited database, republished after every
    /// change.
    interest: Arc<InterestSlice>,
}

impl AuditDmlCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            interest: InterestSlice::new(),
        }
    }

    /// The collections whose write events the DML audit reads: every
    /// collection of an audited database.
    pub fn interest(&self) -> Arc<InterestSlice> {
        Arc::clone(&self.interest)
    }

    fn publish_interest(&self, map: &HashMap<DatabaseId, AuditDmlMode>) {
        let mut interest = Interest::default();
        for (database_id, mode) in map {
            if *mode != AuditDmlMode::None {
                interest.insert_database(*database_id);
            }
        }
        self.interest.publish(interest);
    }

    /// Return the current DML audit mode for `db_id`.
    ///
    /// Returns `AuditDmlMode::None` on cache miss (safe default: no extra auditing).
    pub fn get(&self, db_id: DatabaseId) -> AuditDmlMode {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&db_id)
            .copied()
            .unwrap_or(AuditDmlMode::None)
    }

    /// Update the DML audit mode for `db_id`.
    pub fn set(&self, db_id: DatabaseId, mode: AuditDmlMode) {
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        map.insert(db_id, mode);
        self.publish_interest(&map);
    }

    /// Replace the cache with every descriptor in the catalog.
    ///
    /// Called at startup. Only databases with non-None `audit_dml` are inserted;
    /// the `get` method returns `None` for unknown databases (same semantics).
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let databases = catalog.list_databases()?;
        let mut map = self.inner.write().unwrap_or_else(|p| p.into_inner());
        map.clear();
        for descriptor in databases {
            if descriptor.audit_dml != AuditDmlMode::None {
                map.insert(descriptor.id, descriptor.audit_dml);
            }
        }
        self.publish_interest(&map);
        Ok(())
    }
}

impl Default for AuditDmlCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_audited_database_consumes_every_collection() {
        let cache = AuditDmlCache::new();
        let interest = cache.interest();
        let db = DatabaseId::new(3);
        cache.set(db, AuditDmlMode::Writes);
        assert!(interest.contains(db, "3/metrics"));
        assert!(!interest.contains(DatabaseId::DEFAULT, "metrics"));
        cache.set(db, AuditDmlMode::None);
        assert!(!interest.contains(db, "3/metrics"));
    }
}
