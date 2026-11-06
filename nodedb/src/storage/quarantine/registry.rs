// SPDX-License-Identifier: BUSL-1.1

//! In-process quarantine registry for segments that fail CRC.
//!
//! # Lifetime
//!
//! Quarantine state lives for one process run. The corrupt bytes are the
//! durable record: a restart re-reads them and quarantines the segment again
//! after two failing reads. No file is renamed and no marker is persisted.
//! A persisted marker would outlive the segment it names, because columnar
//! segment ids restart at 1 after a truncate or a drop-and-recreate.
//!
//! # Concurrency
//!
//! `QuarantineRegistry` is `Send + Sync`. The hot-path check is one
//! `RwLock::read` plus one `HashMap` lookup. Writes happen at most twice per
//! segment: the first strike inserts, the second strike marks it quarantined.
//!
//! # Strike accounting
//!
//! Two consecutive CRC failures quarantine a segment. A successful read drops
//! its pending strikes.
//!
//! # Release
//!
//! A key names a segment, and segment ids come back after a truncate, a drop
//! and recreate, or a snapshot restore. The owner of the segment releases its
//! key when the segment is removed or replaced, so a new segment starts with
//! no strikes.
//!
//! # Bound
//!
//! The registry holds at most [`MAX_RECORDS`] records. At the bound a new key
//! evicts a record with pending strikes first, else the oldest quarantine. The
//! corrupt bytes stay on disk, so the next reads quarantine an evicted segment
//! again. An eviction files one report.

use std::collections::HashMap;
use std::sync::RwLock;

use super::error::QuarantineError;
use super::key::{CollectionScope, QuarantineEngine, SegmentKey};

/// Records the registry holds. A healthy node holds almost none.
pub const MAX_RECORDS: usize = 4096;

/// In-memory record for a segment observed to fail CRC.
#[derive(Debug, Clone)]
struct QuarantineRecord {
    /// Consecutive CRC failures since the last successful read.
    strikes: u32,
    /// Error message from the most recent failure.
    last_error: String,
    /// Unix ms of the second strike. `None` while the segment is not quarantined.
    quarantined_at_ms: Option<u64>,
}

/// Groups quarantined segments for the metrics gauge: one series per engine
/// and owning collection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuarantineCountKey {
    pub engine: QuarantineEngine,
    /// `None` for a Raft snapshot chunk.
    pub scope: Option<CollectionScope>,
}

/// Thread-safe quarantine registry.
///
/// Stored as `Arc<QuarantineRegistry>` in `SharedState` and injected into
/// engine read wrappers. All public methods take `&self`.
#[derive(Debug, Default)]
pub struct QuarantineRegistry {
    entries: RwLock<HashMap<SegmentKey, QuarantineRecord>>,
}

impl QuarantineRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one CRC-class failure of `key`.
    ///
    /// - Already quarantined: returns `Err(SegmentQuarantined)`.
    /// - First strike: returns `Ok(())`. The caller surfaces the read error.
    /// - Second strike: marks the segment quarantined and returns
    ///   `Err(SegmentQuarantined)`.
    pub fn record_failure(
        &self,
        key: SegmentKey,
        error_summary: &str,
    ) -> Result<(), QuarantineError> {
        {
            let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
            if let Some(at) = read.get(&key).and_then(|rec| rec.quarantined_at_ms) {
                return Err(QuarantineError::SegmentQuarantined {
                    segment: key,
                    quarantined_at_unix_ms: at,
                });
            }
        }

        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
        if write.len() >= MAX_RECORDS && !write.contains_key(&key) {
            evict_one(&mut write);
        }
        let rec = write.entry(key.clone()).or_insert(QuarantineRecord {
            strikes: 0,
            last_error: String::new(),
            quarantined_at_ms: None,
        });

        // Another thread can quarantine the segment between the read check
        // and the write lock.
        if let Some(at) = rec.quarantined_at_ms {
            return Err(QuarantineError::SegmentQuarantined {
                segment: key,
                quarantined_at_unix_ms: at,
            });
        }

        rec.strikes += 1;
        rec.last_error = error_summary.to_string();
        if rec.strikes < 2 {
            return Ok(());
        }

        let at = unix_ms_now();
        rec.quarantined_at_ms = Some(at);
        tracing::error!(
            segment = %key,
            "segment quarantined after two consecutive CRC failures; \
             other segments of the collection remain readable"
        );
        Err(QuarantineError::SegmentQuarantined {
            segment: key,
            quarantined_at_unix_ms: at,
        })
    }

    /// Returns `true` if `key` is quarantined. Takes a read lock only.
    pub fn is_quarantined(&self, key: &SegmentKey) -> bool {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        read.get(key)
            .is_some_and(|rec| rec.quarantined_at_ms.is_some())
    }

    /// Drops pending strikes of `key` after a successful read. A key with no
    /// record takes the read lock only, so a clean read never serialises
    /// other cores.
    ///
    /// A quarantined segment stays quarantined until its key is released.
    pub fn record_success(&self, key: &SegmentKey) {
        {
            let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
            if !read.contains_key(key) {
                return;
            }
        }
        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
        if write
            .get(key)
            .is_some_and(|rec| rec.quarantined_at_ms.is_none())
        {
            write.remove(key);
        }
    }

    /// Forgets `key`. The segment owner calls it when the segment is removed
    /// or replaced.
    pub fn release(&self, key: &SegmentKey) {
        self.release_where(|candidate| candidate == key);
    }

    /// Forgets every key of `engine` under `scope`.
    pub fn release_collection_engine(&self, engine: QuarantineEngine, scope: &CollectionScope) {
        self.release_where(|key| key.engine() == engine && key.scope() == Some(scope));
    }

    /// Forgets every key of every engine under `scope`.
    pub fn release_collection(&self, scope: &CollectionScope) {
        self.release_where(|key| key.scope() == Some(scope));
    }

    /// Forgets every key of the tenant in the database.
    pub fn release_tenant(&self, database_id: u64, tenant_id: u64) {
        self.release_where(|key| {
            key.scope()
                .is_some_and(|s| s.database_id == database_id && s.tenant_id == tenant_id)
        });
    }

    /// Forgets the snapshot chunks of `group_id` at or below `index`. A group
    /// that adopted a snapshot at `index` never installs an older one.
    pub fn release_raft_through(&self, group_id: u64, index: u64) {
        self.release_where(|key| {
            matches!(
                key,
                SegmentKey::Raft { group_id: g, last_included_index: i }
                    if *g == group_id && *i <= index
            )
        });
    }

    /// Takes the write lock only when a key matches. A release runs on every
    /// truncate and drop, and the registry is almost always empty.
    fn release_where(&self, matches_key: impl Fn(&SegmentKey) -> bool) {
        {
            let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
            if !read.keys().any(&matches_key) {
                return;
            }
        }
        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
        write.retain(|key, _| !matches_key(key));
    }

    /// Every quarantined segment, for the HTTP debug listing.
    pub fn quarantined_snapshot(&self) -> Vec<QuarantineSnapshot> {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        read.iter()
            .filter_map(|(key, rec)| {
                rec.quarantined_at_ms.map(|at| QuarantineSnapshot {
                    key: key.clone(),
                    quarantined_at_unix_ms: at,
                    last_error_summary: rec.last_error.clone(),
                    strikes: rec.strikes,
                })
            })
            .collect()
    }

    /// Count of quarantined segments per engine and owning collection. A
    /// release lowers it.
    pub fn active_counts(&self) -> HashMap<QuarantineCountKey, u64> {
        let read = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let mut counts: HashMap<QuarantineCountKey, u64> = HashMap::new();
        for (key, rec) in read.iter() {
            if rec.quarantined_at_ms.is_some() {
                let count_key = QuarantineCountKey {
                    engine: key.engine(),
                    scope: key.scope().cloned(),
                };
                *counts.entry(count_key).or_insert(0) += 1;
            }
        }
        counts
    }
}

/// One quarantined segment at a point in time.
#[derive(Debug, Clone)]
pub struct QuarantineSnapshot {
    pub key: SegmentKey,
    pub quarantined_at_unix_ms: u64,
    pub last_error_summary: String,
    pub strikes: u32,
}

/// Remove one record to make room: one with pending strikes first, else the
/// oldest quarantine. Files one report for what the eviction forgets.
fn evict_one(entries: &mut HashMap<SegmentKey, QuarantineRecord>) {
    let victim = entries
        .iter()
        .min_by_key(|(_, rec)| rec.quarantined_at_ms.unwrap_or(0))
        .map(|(key, rec)| (key.clone(), rec.quarantined_at_ms.is_some()));
    if let Some((key, was_quarantined)) = victim {
        entries.remove(&key);
        crate::diag::quarantine_record_evicted(
            &key.to_string(),
            key.engine().as_str(),
            was_quarantined,
        );
    }
}

fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn columnar(tenant_id: u64, collection: &str, segment_id: u64) -> SegmentKey {
        SegmentKey::Columnar {
            scope: CollectionScope::new(1, tenant_id, collection),
            segment_id,
        }
    }

    #[test]
    fn first_strike_returns_ok() {
        let reg = QuarantineRegistry::new();
        assert!(
            reg.record_failure(columnar(1, "coll", 1), "crc error")
                .is_ok()
        );
    }

    #[test]
    fn second_strike_returns_quarantined() {
        let reg = QuarantineRegistry::new();
        let k = columnar(1, "coll", 1);
        reg.record_failure(k.clone(), "crc error").unwrap();
        let err = reg.record_failure(k, "crc error").unwrap_err();
        assert!(matches!(err, QuarantineError::SegmentQuarantined { .. }));
    }

    #[test]
    fn success_clears_strikes() {
        let reg = QuarantineRegistry::new();
        let k = columnar(1, "coll", 2);
        reg.record_failure(k.clone(), "crc error").unwrap();
        reg.record_success(&k);
        assert!(reg.record_failure(k, "crc error").is_ok());
    }

    #[test]
    fn quarantined_stays_quarantined_after_success_attempt() {
        let reg = QuarantineRegistry::new();
        let k = columnar(1, "coll", 3);
        reg.record_failure(k.clone(), "crc").unwrap();
        reg.record_failure(k.clone(), "crc").unwrap_err();
        reg.record_success(&k);
        let err = reg.record_failure(k, "crc").unwrap_err();
        assert!(matches!(err, QuarantineError::SegmentQuarantined { .. }));
    }

    #[test]
    fn concurrent_claims_only_one_quarantine() {
        let reg = Arc::new(QuarantineRegistry::new());
        let k = SegmentKey::Raft {
            group_id: 1,
            last_included_index: 4,
        };
        reg.record_failure(k.clone(), "first").unwrap();

        let reg1 = Arc::clone(&reg);
        let k1 = k.clone();
        let reg2 = Arc::clone(&reg);
        let k2 = k.clone();
        let t1 = std::thread::spawn(move || reg1.record_failure(k1, "second"));
        let t2 = std::thread::spawn(move || reg2.record_failure(k2, "second"));
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert!(r1.is_err() && r2.is_err());
        assert_eq!(reg.quarantined_snapshot().len(), 1);
    }

    #[test]
    fn tenants_with_same_collection_and_segment_are_separate() {
        let reg = QuarantineRegistry::new();
        let a = columnar(10, "orders", 1);
        let b = columnar(11, "orders", 1);
        reg.record_failure(a.clone(), "crc").unwrap();
        reg.record_failure(a.clone(), "crc").unwrap_err();

        assert!(reg.is_quarantined(&a));
        assert!(!reg.is_quarantined(&b));
        // Tenant B's first failure is its own first strike.
        assert!(reg.record_failure(b, "crc").is_ok());
        assert_eq!(reg.active_counts().len(), 1);
    }

    fn quarantine(reg: &QuarantineRegistry, key: &SegmentKey) {
        reg.record_failure(key.clone(), "crc").unwrap();
        reg.record_failure(key.clone(), "crc").unwrap_err();
        assert!(reg.is_quarantined(key));
    }

    /// A recreated segment id starts with no strikes once its key is released.
    #[test]
    fn a_released_key_starts_clean() {
        let reg = QuarantineRegistry::new();
        let k = columnar(1, "coll", 1);
        quarantine(&reg, &k);

        reg.release(&k);

        assert!(!reg.is_quarantined(&k));
        assert!(reg.quarantined_snapshot().is_empty());
        assert!(reg.active_counts().is_empty());
        assert!(reg.record_failure(k, "crc").is_ok(), "a fresh first strike");
    }

    #[test]
    fn a_collection_engine_release_keeps_other_engines_and_collections() {
        let reg = QuarantineRegistry::new();
        let scope = CollectionScope::new(1, 7, "orders");
        let segment = columnar(7, "orders", 1);
        let partition = SegmentKey::Timeseries {
            scope: scope.clone(),
            partition: "ts-1".to_string(),
        };
        let other = columnar(7, "other", 1);
        quarantine(&reg, &segment);
        quarantine(&reg, &partition);
        quarantine(&reg, &other);

        reg.release_collection_engine(QuarantineEngine::Columnar, &scope);

        assert!(!reg.is_quarantined(&segment));
        assert!(reg.is_quarantined(&partition));
        assert!(reg.is_quarantined(&other));

        reg.release_collection(&scope);

        assert!(!reg.is_quarantined(&partition));
        assert!(reg.is_quarantined(&other));
    }

    #[test]
    fn a_tenant_release_keeps_other_tenants() {
        let reg = QuarantineRegistry::new();
        let released = columnar(7, "a", 1);
        let kept = columnar(8, "a", 1);
        quarantine(&reg, &released);
        quarantine(&reg, &kept);

        reg.release_tenant(1, 7);

        assert!(!reg.is_quarantined(&released));
        assert!(reg.is_quarantined(&kept));
    }

    /// Adopting a snapshot at an index unblocks older chunks of the group, and
    /// no chunk of a newer index or another group.
    #[test]
    fn a_raft_release_stops_at_the_index_and_the_group() {
        let reg = QuarantineRegistry::new();
        let chunk = |group_id, last_included_index| SegmentKey::Raft {
            group_id,
            last_included_index,
        };
        for key in [chunk(1, 4), chunk(1, 9), chunk(2, 4)] {
            quarantine(&reg, &key);
        }

        reg.release_raft_through(1, 5);

        assert!(!reg.is_quarantined(&chunk(1, 4)));
        assert!(reg.is_quarantined(&chunk(1, 9)));
        assert!(reg.is_quarantined(&chunk(2, 4)));
    }

    /// A clean read forgets pending strikes, so one transient failure per
    /// segment does not fill the registry.
    #[test]
    fn a_clean_read_drops_the_record_of_a_pending_strike() {
        let reg = QuarantineRegistry::new();
        let k = columnar(1, "coll", 1);
        reg.record_failure(k.clone(), "crc").unwrap();
        reg.record_success(&k);
        assert!(reg.entries.read().unwrap().is_empty());
    }

    /// The registry never holds more than its bound. A new key evicts a
    /// pending strike first, so every quarantine survives while one exists.
    #[test]
    fn the_registry_is_bounded_and_evicts_pending_strikes_first() {
        let reg = QuarantineRegistry::new();
        let held = columnar(1, "held", 1);
        quarantine(&reg, &held);
        for id in 0..(MAX_RECORDS as u64 + 16) {
            reg.record_failure(columnar(1, "pending", id), "crc")
                .unwrap();
            assert!(reg.entries.read().unwrap().len() <= MAX_RECORDS);
        }
        assert!(reg.is_quarantined(&held));
    }

    /// With only quarantines held, the oldest one leaves.
    #[test]
    fn at_the_bound_the_oldest_quarantine_leaves() {
        let reg = QuarantineRegistry::new();
        {
            let mut entries = reg.entries.write().unwrap();
            for id in 0..MAX_RECORDS as u64 {
                entries.insert(
                    columnar(1, "held", id),
                    QuarantineRecord {
                        strikes: 2,
                        last_error: String::new(),
                        quarantined_at_ms: Some(100 + id),
                    },
                );
            }
        }
        reg.record_failure(columnar(1, "new", 1), "crc").unwrap();

        assert!(!reg.is_quarantined(&columnar(1, "held", 0)));
        assert!(reg.is_quarantined(&columnar(1, "held", 1)));
        assert_eq!(reg.entries.read().unwrap().len(), MAX_RECORDS);
    }
}
