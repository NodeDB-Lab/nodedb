// SPDX-License-Identifier: BUSL-1.1

//! The collections whose write events some Event Plane consumer reads.
//!
//! A Data Plane core reads this set before it builds the write events of an
//! engine that emits them on demand. A timeseries ingest emits one event per
//! stored row, so a collection nothing consumes pays nothing for them.
//!
//! Each Control Plane registry that feeds an Event Plane consumer publishes
//! its own [`InterestSlice`] after every change:
//! - the trigger registry: AFTER triggers the Event Plane fires;
//! - the change-stream registry: every stream, and a `*` stream names every
//!   collection of its database;
//! - the event-definition index: collections with DEFINE EVENT definitions;
//! - the DML audit cache: every collection of an audited database.
//!
//! The set ignores tenants. A collection one tenant's consumer reads counts
//! for every tenant, so the set holds every consumed collection and can hold
//! more. A core reads each slice lock-free.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::types::DatabaseId;

/// The collection name that stands for every collection of a database.
pub const EVERY_COLLECTION: &str = "*";

/// The consumed collections of one database.
#[derive(Debug, Default, Clone)]
struct DatabaseInterest {
    every: bool,
    names: HashSet<String>,
}

/// A set of consumed collections, by database.
#[derive(Debug, Default, Clone)]
pub struct Interest {
    by_database: HashMap<DatabaseId, DatabaseInterest>,
}

impl Interest {
    /// Add `collection` of `database_id`. [`EVERY_COLLECTION`] adds every
    /// collection of the database.
    pub fn insert(&mut self, database_id: DatabaseId, collection: &str) {
        let entry = self.by_database.entry(database_id).or_default();
        if collection == EVERY_COLLECTION {
            entry.every = true;
        } else {
            entry.names.insert(collection.to_owned());
        }
    }

    /// Add every collection of `database_id`.
    pub fn insert_database(&mut self, database_id: DatabaseId) {
        self.by_database.entry(database_id).or_default().every = true;
    }

    /// Whether the set holds `collection` of `database_id`.
    ///
    /// The registries add bare catalog names. Callers ask with the name as
    /// storage keys it, the form a plan and a write event carry: qualified
    /// outside the default database. The qualifier is checked against
    /// `database_id` and dropped. A name not qualified for `database_id` is
    /// never held.
    pub fn contains(&self, database_id: DatabaseId, collection: &str) -> bool {
        let Ok(key) = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)
        else {
            return false;
        };
        self.by_database
            .get(&database_id)
            .is_some_and(|database| database.every || database.names.contains(key.name()))
    }
}

/// One registry's consumed collections. The registry replaces the whole set
/// after each change. Readers never block the registry.
#[derive(Debug)]
pub struct InterestSlice {
    current: ArcSwap<Interest>,
}

impl Default for InterestSlice {
    fn default() -> Self {
        Self {
            current: ArcSwap::from_pointee(Interest::default()),
        }
    }
}

impl InterestSlice {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Replace the registry's consumed collections with `interest`.
    pub fn publish(&self, interest: Interest) {
        self.current.store(Arc::new(interest));
    }

    /// Whether the registry consumes `collection` of `database_id`, named as
    /// storage keys it.
    pub fn contains(&self, database_id: DatabaseId, collection: &str) -> bool {
        self.current.load().contains(database_id, collection)
    }
}

/// The slices of every registry that feeds an Event Plane consumer.
#[derive(Debug, Default)]
pub struct InterestSources {
    pub triggers: Arc<InterestSlice>,
    pub change_streams: Arc<InterestSlice>,
    pub event_definitions: Arc<InterestSlice>,
    pub dml_audit: Arc<InterestSlice>,
}

impl InterestSources {
    fn contains(&self, database_id: DatabaseId, collection: &str) -> bool {
        [
            &self.triggers,
            &self.change_streams,
            &self.event_definitions,
            &self.dml_audit,
        ]
        .iter()
        .any(|slice| slice.contains(database_id, collection))
    }
}

/// The set every Data Plane core reads. Boot creates it before the cores
/// start and installs the registries' slices once the Control Plane state
/// exists. Until then it holds no collection: a core replaying its WAL emits
/// no event.
#[derive(Debug)]
pub struct EventInterest {
    sources: ArcSwap<InterestSources>,
}

impl Default for EventInterest {
    fn default() -> Self {
        Self {
            sources: ArcSwap::from_pointee(InterestSources::default()),
        }
    }
}

impl EventInterest {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Read the consumed collections from `sources` from now on.
    pub fn install(&self, sources: InterestSources) {
        self.sources.store(Arc::new(sources));
    }

    /// Whether some Event Plane consumer reads the write events of
    /// `collection` in `database_id`.
    pub fn consumes(&self, database_id: DatabaseId, collection: &str) -> bool {
        self.sources.load().contains(database_id, collection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn slice_with(database_id: DatabaseId, collection: &str) -> Arc<InterestSlice> {
        let slice = InterestSlice::new();
        let mut interest = Interest::default();
        interest.insert(database_id, collection);
        slice.publish(interest);
        slice
    }

    #[test]
    fn an_uninstalled_set_consumes_nothing() {
        assert!(!EventInterest::new().consumes(DB, "metrics"));
    }

    #[test]
    fn a_named_collection_is_consumed_in_its_database_only() {
        let interest = EventInterest::new();
        interest.install(InterestSources {
            triggers: slice_with(DB, "metrics"),
            ..InterestSources::default()
        });
        assert!(interest.consumes(DB, "metrics"));
        assert!(!interest.consumes(DB, "other"));
        assert!(!interest.consumes(DatabaseId::new(7), "metrics"));
    }

    /// Outside the default database a core asks with the qualified name, as
    /// its plan carries it, and the registry holds the bare catalog name.
    #[test]
    fn a_qualified_name_matches_its_bare_registration() {
        let db = DatabaseId::new(7);
        let interest = EventInterest::new();
        interest.install(InterestSources {
            triggers: slice_with(db, "metrics"),
            ..InterestSources::default()
        });
        assert!(interest.consumes(db, "7/metrics"));
        assert!(!interest.consumes(db, "metrics"));
        assert!(!interest.consumes(db, "8/metrics"));
    }

    #[test]
    fn a_wildcard_consumes_every_collection_of_its_database() {
        let interest = EventInterest::new();
        interest.install(InterestSources {
            change_streams: slice_with(DB, EVERY_COLLECTION),
            ..InterestSources::default()
        });
        assert!(interest.consumes(DB, "metrics"));
        assert!(!interest.consumes(DatabaseId::new(7), "metrics"));
    }

    #[test]
    fn a_republished_slice_replaces_the_previous_set() {
        let interest = EventInterest::new();
        let audit = slice_with(DB, "metrics");
        interest.install(InterestSources {
            dml_audit: Arc::clone(&audit),
            ..InterestSources::default()
        });
        assert!(interest.consumes(DB, "metrics"));
        audit.publish(Interest::default());
        assert!(!interest.consumes(DB, "metrics"));
    }
}
