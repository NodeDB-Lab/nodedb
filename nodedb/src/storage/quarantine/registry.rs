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
//! Two consecutive CRC failures quarantine a segment. A successful read resets
//! the strike counter.

use std::collections::HashMap;
use std::sync::RwLock;

use super::error::QuarantineError;
use super::key::{CollectionScope, QuarantineEngine, SegmentKey};

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

    /// Clears pending strikes of `key` after a successful read.
    ///
    /// A quarantined segment stays quarantined for the rest of the process run.
    pub fn record_success(&self, key: &SegmentKey) {
        let mut write = self.entries.write().unwrap_or_else(|p| p.into_inner());
        if let Some(rec) = write.get_mut(key)
            && rec.quarantined_at_ms.is_none()
        {
            rec.strikes = 0;
        }
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

    /// Count of quarantined segments per engine and owning collection.
    ///
    /// Quarantine is permanent within a run, so this count is also the
    /// cumulative total for the run.
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
}
