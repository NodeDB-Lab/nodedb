// SPDX-License-Identifier: BUSL-1.1

//! Per-collection count of the sessions with a shape on it.
//!
//! The packager asks for every user write whether any session wants the
//! collection. The count answers that with one map lookup and no allocation,
//! so a write to a collection no session subscribes to costs nothing more.

use std::collections::HashMap;

use super::subscriptions::CollectionChange;
use crate::types::DatabaseId;

/// Session counts keyed by `(tenant, database)`, then by bare collection name.
#[derive(Debug, Default)]
pub(super) struct CollectionInterest {
    counts: HashMap<(u64, DatabaseId), HashMap<String, usize>>,
}

impl CollectionInterest {
    /// Whether any session has a shape on `collection` in this scope.
    pub(super) fn any(&self, tenant_id: u64, database_id: DatabaseId, collection: &str) -> bool {
        self.counts
            .get(&(tenant_id, database_id))
            .is_some_and(|collections| collections.contains_key(collection))
    }

    /// Apply one session's collection change: count a gained collection,
    /// release a lost one. Constant work per change.
    pub(super) fn apply(
        &mut self,
        tenant_id: u64,
        database_id: DatabaseId,
        change: &CollectionChange,
    ) {
        if let Some(gained) = change.gained.as_deref() {
            self.add(tenant_id, database_id, gained);
        }
        if let Some(lost) = change.lost.as_deref() {
            self.remove(tenant_id, database_id, lost);
        }
    }

    fn add(&mut self, tenant_id: u64, database_id: DatabaseId, collection: &str) {
        *self
            .counts
            .entry((tenant_id, database_id))
            .or_default()
            .entry(collection.to_string())
            .or_insert(0) += 1;
    }

    pub(super) fn remove(&mut self, tenant_id: u64, database_id: DatabaseId, collection: &str) {
        let scope = (tenant_id, database_id);
        let Some(collections) = self.counts.get_mut(&scope) else {
            return;
        };
        if let Some(count) = collections.get_mut(collection) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                collections.remove(collection);
            }
        }
        if collections.is_empty() {
            self.counts.remove(&scope);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gained(name: &str) -> CollectionChange {
        CollectionChange {
            gained: Some(name.into()),
            lost: None,
        }
    }

    fn lost(name: &str) -> CollectionChange {
        CollectionChange {
            gained: None,
            lost: Some(name.into()),
        }
    }

    #[test]
    fn interest_counts_sessions_per_collection() {
        let mut interest = CollectionInterest::default();
        let db = DatabaseId::new(7);

        interest.apply(1, db, &gained("orders"));
        interest.apply(1, db, &gained("orders"));
        assert!(interest.any(1, db, "orders"));
        assert!(!interest.any(1, db, "users"));
        assert!(!interest.any(2, db, "orders"));
        assert!(!interest.any(1, DatabaseId::new(8), "orders"));

        interest.apply(1, db, &lost("orders"));
        assert!(interest.any(1, db, "orders"));
        interest.apply(1, db, &lost("orders"));
        assert!(!interest.any(1, db, "orders"));
        assert!(interest.counts.is_empty());
    }

    #[test]
    fn moving_interest_releases_the_old_collection() {
        let mut interest = CollectionInterest::default();
        let db = DatabaseId::new(7);

        interest.apply(1, db, &gained("orders"));
        interest.apply(
            1,
            db,
            &CollectionChange {
                gained: Some("users".into()),
                lost: Some("orders".into()),
            },
        );

        assert!(!interest.any(1, db, "orders"));
        assert!(interest.any(1, db, "users"));
    }
}
