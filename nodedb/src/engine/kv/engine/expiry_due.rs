// SPDX-License-Identifier: BUSL-1.1

//! The expired keys a core holds for its data group's leader to reap.
//!
//! A core never reaps a key on its own. A local reap runs at each replica's
//! wall time, unordered against the writes of the log: one replica can reap
//! before a write of the key and another after it. Their version histories
//! and read-set verdicts then differ. Instead:
//!
//! 1. The maintenance tick moves every entry the wheel hands out into the
//!    due set ([`KvEngine::collect_due_expiries`]).
//! 2. The data group's leader reads its due set ([`KvEngine::due_expiries`])
//!    and proposes one `KvResolvedMutation::Reap` per key, through write
//!    admission like every other write.
//! 3. Every replica applies the reap at the same log position
//!    ([`KvEngine::reap_due`]): it reaps the key only while the key holds the
//!    named expiry, ends the version at that expiry's last ordinal, and drops
//!    the entry from its wheel and its due set.
//!
//! A stale due entry, one whose key a write gave another TTL, stays until
//! the leader proposes it. Its reap is a no-op that drops it everywhere.

use super::KvEngine;
use crate::engine::kv::ExpiredKey;
use crate::engine::kv::engine_helpers::{expiry_key, parse_expiry_key};

/// One expired key a core holds for a reap.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DueExpiry {
    pub database_id: u64,
    pub tenant_id: u64,
    /// The collection as the table stores it.
    pub collection: String,
    pub key: Vec<u8>,
    /// The expiry instant the key held when the wheel handed it out.
    pub expire_at_ms: u64,
}

impl KvEngine {
    /// Advance the expiry wheel to `now_ms` and move every entry it hands
    /// out into the due set. Reaps nothing. Returns the entries moved.
    pub fn collect_due_expiries(&mut self, now_ms: u64) -> usize {
        let batch = self.expiry.tick(now_ms);
        let moved = batch.expired.len();
        self.due.extend(batch.expired);
        moved
    }

    /// At most `limit` due entries, in key order.
    pub fn due_expiries(&self, limit: usize) -> Vec<DueExpiry> {
        self.due
            .iter()
            .filter_map(|(composite, expire_at_ms)| {
                let (database_id, tenant_id, collection, key) = parse_expiry_key(composite)?;
                Some(DueExpiry {
                    database_id,
                    tenant_id,
                    collection,
                    key,
                    expire_at_ms: *expire_at_ms,
                })
            })
            .take(limit)
            .collect()
    }

    /// The number of due entries.
    pub fn due_len(&self) -> usize {
        self.due.len()
    }

    /// Apply a replicated reap of `key` at `expire_at_ms`: reap it while it
    /// holds exactly that expiry, ending the version at the expiry's last
    /// ordinal, and drop the entry from the wheel and the due set either
    /// way. `None` when the key holds another expiry or none.
    pub fn reap_due(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        expire_at_ms: u64,
    ) -> Option<ExpiredKey> {
        let composite = expiry_key(database_id, tenant_id, collection, key);
        // A replica whose wheel has not handed the entry out yet still
        // holds it there.
        self.expiry.cancel(&composite, expire_at_ms);
        self.due.remove(&(composite.clone(), expire_at_ms));
        self.reap_composite(&composite, expire_at_ms)
    }

    /// Hand out every due entry and reap each one, as a replicated reap of
    /// each does. Tests drive expiry with it.
    #[cfg(test)]
    pub fn tick_expiry(&mut self, now_ms: u64) -> Vec<ExpiredKey> {
        self.collect_due_expiries(now_ms);
        let due: Vec<DueExpiry> = self.due_expiries(usize::MAX);
        due.into_iter()
            .filter_map(|entry| {
                self.reap_due(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.collection,
                    &entry.key,
                    entry.expire_at_ms,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::kv::test_support::row_surrogate;
    use crate::engine::kv::{KvEngine, KvPutParams};

    fn engine() -> KvEngine {
        KvEngine::new(0, 16, 0.75, 4, 64, 1000, 1024)
    }

    /// Whether `s` holds a value, judged before any expiry.
    fn stored(e: &KvEngine) -> bool {
        e.get(0, 1, "c", b"s", 0).is_some()
    }

    fn put(e: &mut KvEngine, key: &[u8], ttl_ms: u64) {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key,
            value: b"v",
            ttl_ms,
            now_ms: 1_000,
            surrogate: row_surrogate(key),
        })
        .expect("a bound row writes");
    }

    /// The tick reaps nothing. It only hands the entry to the due set, and
    /// the key stays stored until a replicated reap applies.
    #[test]
    fn the_tick_reaps_nothing_and_holds_the_entry_due() {
        let mut e = engine();
        put(&mut e, b"s", 500);
        assert_eq!(e.collect_due_expiries(10_000), 1);
        let due = e.due_expiries(16);
        assert_eq!(due.len(), 1);
        assert_eq!(
            (due[0].key.as_slice(), due[0].expire_at_ms),
            (&b"s"[..], 1_500)
        );
        assert!(stored(&e), "the key stays until a reap applies");

        assert!(e.reap_due(0, 1, "c", b"s", 1_500).is_some());
        assert_eq!(e.due_len(), 0);
        assert!(!stored(&e));
    }

    /// A reap of an expiry the key no longer holds drops the stale entry
    /// and keeps the key.
    #[test]
    fn a_stale_reap_drops_the_entry_and_keeps_the_key() {
        let mut e = engine();
        put(&mut e, b"s", 500);
        e.collect_due_expiries(10_000);
        // A write gives the key a new TTL after the entry fell due.
        put(&mut e, b"s", 1_000_000);
        assert!(e.reap_due(0, 1, "c", b"s", 1_500).is_none());
        assert_eq!(e.due_len(), 0);
        assert!(stored(&e));
    }

    /// Two replicas that hand the entry out at different times reap the
    /// same version at the same ordinal when the reap applies.
    #[test]
    fn replicas_that_tick_apart_converge_on_the_reap() {
        let mut ahead = engine();
        let mut behind = engine();
        put(&mut ahead, b"s", 500);
        put(&mut behind, b"s", 500);
        ahead.collect_due_expiries(10_000);
        // `behind` has not ticked: its wheel still holds the entry.
        assert!(ahead.reap_due(0, 1, "c", b"s", 1_500).is_some());
        assert!(behind.reap_due(0, 1, "c", b"s", 1_500).is_some());
        assert_eq!(
            behind.collect_due_expiries(10_000),
            0,
            "the wheel entry is gone"
        );
        assert_eq!(ahead.due_len(), behind.due_len());
    }
}
