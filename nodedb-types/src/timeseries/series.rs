// SPDX-License-Identifier: Apache-2.0

//! Series identity and catalog.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Unique identifier for a timeseries (hash of metric name + sorted tag set).
pub type SeriesId = u64;

/// The canonical key for a series — used for collision detection in the
/// series catalog. Two `SeriesKey`s that hash to the same `SeriesId` are
/// a collision; the catalog rehashes until it finds a free slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

impl SeriesKey {
    pub fn new(metric: impl Into<String>, mut tags: Vec<(String, String)>) -> Self {
        tags.sort();
        Self {
            metric: metric.into(),
            tags,
        }
    }

    /// Compute the SeriesId for this key at a given rehash attempt.
    ///
    /// `rehash_attempt` is a plain collision-resolution counter — attempt 0 is
    /// the natural hash, and `SeriesCatalog::resolve` increments it until it
    /// lands on a free slot. It is NOT a cryptographic salt: the hash is
    /// `DefaultHasher`, the value is stored in the clear beside the key, and it
    /// carries no secrecy or uniqueness guarantee.
    pub fn to_series_id(&self, rehash_attempt: u64) -> SeriesId {
        let mut hasher = DefaultHasher::new();
        rehash_attempt.hash(&mut hasher);
        self.metric.hash(&mut hasher);
        for (k, v) in &self.tags {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Persistent catalog that maps SeriesId → SeriesKey with collision detection.
///
/// On insert, if the SeriesId already maps to a *different* SeriesKey, the
/// catalog rehashes with an incrementing attempt counter until it finds a free
/// slot. This is one lookup per new series (not per row).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesCatalog {
    /// SeriesId → (SeriesKey, rehash attempt that produced this ID).
    entries: HashMap<SeriesId, (SeriesKey, u64)>,
}

impl SeriesCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a SeriesKey to its SeriesId, registering it if new.
    ///
    /// Returns the SeriesId (potentially rehashed if the natural hash collided).
    pub fn resolve(&mut self, key: &SeriesKey) -> SeriesId {
        self.resolve_detailed(key).id
    }

    /// Resolve a SeriesKey, also reporting how the ID was arrived at.
    ///
    /// Callers that want to log or meter collisions need `rehash_attempt`
    /// together with `newly_registered`: a key that collided keeps its
    /// non-zero attempt for its whole lifetime, so reporting on the attempt
    /// alone would fire on every subsequent row for that series.
    pub fn resolve_detailed(&mut self, key: &SeriesKey) -> ResolvedSeries {
        let mut rehash_attempt = 0u64;
        loop {
            let id = key.to_series_id(rehash_attempt);
            match self.entries.get(&id) {
                None => {
                    self.entries.insert(id, (key.clone(), rehash_attempt));
                    return ResolvedSeries {
                        id,
                        rehash_attempt,
                        newly_registered: true,
                    };
                }
                Some((existing_key, attempt)) if existing_key == key => {
                    return ResolvedSeries {
                        id,
                        rehash_attempt: *attempt,
                        newly_registered: false,
                    };
                }
                Some(_) => {
                    rehash_attempt += 1;
                }
            }
        }
    }

    /// Drop a series registration, freeing its ID slot.
    ///
    /// Used when the owning structures (memtable stats, last-value cache) evict
    /// the same series, so the catalog does not grow without bound over the
    /// lifetime of a high-churn collection. Re-resolving a forgotten key is
    /// safe: it re-derives the same ID as long as the keys it collided with are
    /// still registered.
    pub fn forget(&mut self, id: SeriesId) -> bool {
        self.entries.remove(&id).is_some()
    }

    /// Look up a SeriesId to get its canonical key.
    pub fn get(&self, id: SeriesId) -> Option<&SeriesKey> {
        self.entries.get(&id).map(|(k, _)| k)
    }

    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|(_, (key, _))| {
                std::mem::size_of::<SeriesId>()
                    + std::mem::size_of::<(SeriesKey, u64)>()
                    + key.metric.len()
                    + key
                        .tags
                        .iter()
                        .map(|(k, v)| k.len() + v.len() + 48)
                        .sum::<usize>()
                    + 24
            })
            .sum()
    }
}

/// Outcome of [`SeriesCatalog::resolve_detailed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedSeries {
    /// The ID this key is registered under.
    pub id: SeriesId,
    /// Collision-resolution attempt that produced `id`. 0 = natural hash.
    pub rehash_attempt: u64,
    /// Whether this call created the registration (vs. finding an existing one).
    pub newly_registered: bool,
}

#[cfg(test)]
mod catalog_tests {
    use super::*;

    #[test]
    fn series_key_sorted_tags() {
        let k1 = SeriesKey::new(
            "cpu",
            vec![("host".into(), "a".into()), ("dc".into(), "us".into())],
        );
        let k2 = SeriesKey::new(
            "cpu",
            vec![("dc".into(), "us".into()), ("host".into(), "a".into())],
        );
        assert_eq!(k1, k2);
        assert_eq!(k1.to_series_id(0), k2.to_series_id(0));
    }

    #[test]
    fn series_catalog_resolve() {
        let mut catalog = SeriesCatalog::new();
        let k = SeriesKey::new("cpu", vec![("host".into(), "prod-1".into())]);
        let id1 = catalog.resolve(&k);
        let id2 = catalog.resolve(&k);
        assert_eq!(id1, id2);
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn series_catalog_different_keys() {
        let mut catalog = SeriesCatalog::new();
        let k1 = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let k2 = SeriesKey::new("mem", vec![("host".into(), "a".into())]);
        let id1 = catalog.resolve(&k1);
        let id2 = catalog.resolve(&k2);
        assert_ne!(id1, id2);
        assert_eq!(catalog.len(), 2);
    }

    /// A real 64-bit collision cannot be searched for, so the colliding slot is
    /// planted directly — `entries` is reachable from inside this module, which
    /// is the only way to exercise the rehash branch deterministically.
    fn plant(catalog: &mut SeriesCatalog, id: SeriesId, key: SeriesKey, attempt: u64) {
        catalog.entries.insert(id, (key, attempt));
    }

    #[test]
    fn colliding_key_is_rehashed_to_a_distinct_id() {
        let victim = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let squatter = SeriesKey::new("unrelated", vec![]);
        let natural = victim.to_series_id(0);

        let mut catalog = SeriesCatalog::new();
        plant(&mut catalog, natural, squatter.clone(), 0);

        let resolved = catalog.resolve_detailed(&victim);
        assert_ne!(
            resolved.id, natural,
            "a collision must not reuse the occupied ID"
        );
        assert_eq!(resolved.rehash_attempt, 1);
        assert!(resolved.newly_registered);
        assert_eq!(catalog.get(resolved.id), Some(&victim));
        assert_eq!(catalog.get(natural), Some(&squatter));
    }

    #[test]
    fn rehashed_key_keeps_its_id_and_reports_not_newly_registered() {
        let victim = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let mut catalog = SeriesCatalog::new();
        plant(
            &mut catalog,
            victim.to_series_id(0),
            SeriesKey::new("unrelated", vec![]),
            0,
        );

        let first = catalog.resolve_detailed(&victim);
        let second = catalog.resolve_detailed(&victim);
        assert_eq!(first.id, second.id);
        assert_eq!(second.rehash_attempt, 1);
        assert!(
            !second.newly_registered,
            "a collision must be reported once, not on every row"
        );
    }

    #[test]
    fn forget_frees_the_slot_and_resolve_reproduces_the_id() {
        let key = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let mut catalog = SeriesCatalog::new();
        let id = catalog.resolve(&key);

        assert!(catalog.forget(id));
        assert!(catalog.is_empty());
        assert!(!catalog.forget(id), "forgetting twice is not an error");
        assert_eq!(catalog.resolve(&key), id);
    }
}

/// Persistent identity of a NodeDB-Lite database instance.
///
/// Generated as a CUID2 on first `open()`, stored in redb metadata.
/// Scope = one database file. Not a device ID, user ID, or app ID.
pub type LiteId = String;

/// Battery state reported by the host application for battery-aware flushing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BatteryState {
    /// Battery level is sufficient (>50%) or device is on AC power.
    Normal,
    /// Battery is low (<20%) and not charging. Defer non-critical I/O.
    Low,
    /// Device is currently charging. Safe to flush.
    Charging,
    /// Battery state unknown (desktop, non-mobile). Treat as Normal.
    #[default]
    Unknown,
}

impl BatteryState {
    /// Whether flushing should be deferred in battery-aware mode.
    pub fn should_defer_flush(&self) -> bool {
        matches!(self, Self::Low)
    }
}
