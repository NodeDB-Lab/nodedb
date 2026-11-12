// SPDX-License-Identifier: BUSL-1.1

//! Spilling a staging overlay's hot tier.
//!
//! `[tuning.query] max_txn_overlay_bytes` is the hot-tier threshold of one
//! overlay. A staged mutation that lifts the hot weight past it writes every
//! hot tier as a sorted run, moves the row-sized journal entries to a
//! journal file, and empties the hot tier. The runs keep a key bloom and a
//! sparse index in RAM.

use super::staged::TxnOverlay;

impl TxnOverlay {
    /// Weight of every hot tier and of the in-RAM journal entries.
    pub fn hot_bytes(&self) -> usize {
        self.collections
            .values()
            .map(super::staged::CollectionOverlay::hot_bytes)
            .sum::<usize>()
            + self.journal.tail_bytes()
    }

    /// RAM the spilled tiers keep: blooms, run indexes and the journal index.
    pub fn spilled_heap_bytes(&self) -> usize {
        self.collections
            .values()
            .map(|o| {
                o.by_surrogate.run_heap_bytes()
                    + o.doc_id_to_surrogate.run_heap_bytes()
                    + o.ttl_by_surrogate.run_heap_bytes()
                    + o.bitemporal_by_surrogate.run_heap_bytes()
                    + o.base_pk_by_surrogate.run_heap_bytes()
            })
            .sum::<usize>()
            + self.journal.spilled_heap_bytes()
    }

    /// Spill when the hot weight passed the threshold. Called by every
    /// mutator after it staged.
    pub(super) fn spill_past_threshold(&mut self) -> crate::Result<()> {
        if self.hot_bytes() <= self.space.threshold() {
            return Ok(());
        }
        self.spill()
    }

    /// Write every hot tier and the row-sized journal entries to the spill
    /// directory. An error part way leaves the overlay unusable.
    pub(super) fn spill(&mut self) -> crate::Result<()> {
        self.guard()?;
        let result = self.spill_tiers();
        if let Err(e) = &result {
            self.poisoned = Some(e.to_string());
        }
        result
    }

    fn spill_tiers(&mut self) -> crate::Result<()> {
        for overlay in self.collections.values_mut() {
            overlay.spill(&mut self.space)?;
        }
        self.journal.spill(&mut self.space)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use nodedb_types::RowIdentity;

    use super::super::staged::{Staged, TouchedSlot};
    use super::super::staged_sidecar::{BitemporalStamp, StagedTtl};
    use super::*;
    use crate::data::executor::handlers::spill::tiered::SpillSpace;
    use crate::types::{DatabaseId, TenantId};

    type CollKey = (DatabaseId, TenantId, String);

    fn key(coll: &str) -> CollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    fn id(n: u32) -> RowIdentity {
        RowIdentity::from_user_key(format!("row-{n}"))
    }

    /// An overlay that spills after every mutation.
    fn spilling(dir: &std::path::Path) -> TxnOverlay {
        TxnOverlay::with_spill(SpillSpace::new(dir.join("txn-1"), "doc", 0))
    }

    fn value(overlay: &TxnOverlay, coll: &CollKey, surrogate: u32) -> Option<Staged> {
        overlay.get(coll, surrogate).unwrap().map(Cow::into_owned)
    }

    fn put(byte: u8) -> Option<Staged> {
        Some(Staged::Put(vec![byte]))
    }

    #[test]
    fn spilled_rows_are_read_back_by_surrogate_and_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut overlay = spilling(dir.path());
        let users = key("users");
        for n in 0..50u32 {
            overlay
                .insert_put(users.clone(), n, &id(n), vec![n as u8])
                .unwrap();
        }
        assert!(overlay.spilled_heap_bytes() > 0);
        assert!(dir.path().join("txn-1").exists());

        for n in 0..50u32 {
            assert_eq!(value(&overlay, &users, n), put(n as u8));
            assert_eq!(
                overlay
                    .get_by_doc_id(&users, &id(n))
                    .unwrap()
                    .map(Cow::into_owned),
                put(n as u8)
            );
            assert_eq!(
                overlay.surrogate_for_doc_id(&users, &id(n)).unwrap(),
                Some(n)
            );
        }
        assert_eq!(overlay.len().unwrap(), 50);
        let scanned: Vec<(u32, Staged)> = overlay
            .iter_for_collection(&users)
            .map(|item| item.map(|(s, v)| (s, v.into_owned())))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(scanned.len(), 50);
        let by_identity = overlay.iter_doc_entries_for_collection(&users).count();
        assert_eq!(by_identity, 50);
    }

    #[test]
    fn a_tombstone_hides_a_spilled_put() {
        let dir = tempfile::tempdir().unwrap();
        let mut overlay = spilling(dir.path());
        let users = key("users");
        overlay
            .insert_put(users.clone(), 1, &id(1), vec![1])
            .unwrap();
        overlay
            .insert_put(users.clone(), 2, &id(2), vec![2])
            .unwrap();
        overlay.insert_tombstone(users.clone(), 1, &id(1)).unwrap();

        assert_eq!(value(&overlay, &users, 1), Some(Staged::Tombstone));
        assert_eq!(value(&overlay, &users, 2), put(2));
        let scanned: Vec<(u32, Staged)> = overlay
            .iter_for_collection(&users)
            .map(|item| item.map(|(s, v)| (s, v.into_owned())))
            .collect::<crate::Result<_>>()
            .unwrap();
        assert!(scanned.contains(&(1, Staged::Tombstone)));
        assert!(scanned.contains(&(2, Staged::Put(vec![2]))));
    }

    #[test]
    fn savepoint_rollback_across_a_spill_boundary_restores_the_marker_state() {
        let dir = tempfile::tempdir().unwrap();
        // A threshold that holds a few rows, so rows written before and
        // after the marker share runs and the hot tier.
        let mut overlay = TxnOverlay::with_spill(SpillSpace::new(dir.path().join("t"), "doc", 600));
        let users = key("users");
        for n in 0..20u32 {
            overlay
                .insert_put(users.clone(), n, &id(n), vec![n as u8])
                .unwrap();
        }
        // Overwrite rows in the hot tier and in runs before the marker.
        overlay
            .insert_put(users.clone(), 3, &id(3), vec![103])
            .unwrap();
        overlay
            .insert_put(users.clone(), 19, &id(19), vec![119])
            .unwrap();
        let marker = overlay.journal_len();
        let before: Vec<Option<Staged>> = (0..25u32).map(|n| value(&overlay, &users, n)).collect();

        for n in 0..25u32 {
            overlay
                .insert_put(users.clone(), n, &id(n), vec![200])
                .unwrap();
        }
        overlay.insert_tombstone(users.clone(), 5, &id(5)).unwrap();
        overlay
            .insert_put(users.clone(), 19, &id(19), vec![201])
            .unwrap();
        let touched = overlay.slots_touched_since(marker, &users).unwrap();
        assert_eq!(touched.len(), 25);
        assert_eq!(
            touched.iter().find(|s| s.surrogate == 3),
            Some(&TouchedSlot {
                surrogate: 3,
                doc_id: id(3),
                before: put(103),
                after: put(200),
            })
        );

        overlay.rollback_to(marker).unwrap();

        assert_eq!(overlay.journal_len(), marker);
        for n in 0..25u32 {
            assert_eq!(value(&overlay, &users, n), before[n as usize], "row {n}");
        }
        assert_eq!(overlay.surrogate_for_doc_id(&users, &id(22)).unwrap(), None);
        assert_eq!(overlay.len().unwrap(), 20);

        // Staging after the rollback, and rolling back again, still works.
        overlay
            .insert_put(users.clone(), 7, &id(7), vec![77])
            .unwrap();
        assert_eq!(value(&overlay, &users, 7), put(77));
        overlay.rollback_to(marker).unwrap();
        assert_eq!(value(&overlay, &users, 7), put(7));
        overlay.rollback_to(0).unwrap();
        assert!(overlay.is_empty());
    }

    #[test]
    fn ttl_and_sidecars_spill_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut overlay = spilling(dir.path());
        let cache = key("cache");
        overlay
            .set_ttl(cache.clone(), 4, &id(4), StagedTtl::ExpireAt(9))
            .unwrap();
        let stamp = BitemporalStamp {
            sys_from: 1,
            valid_from_ms: 2,
            valid_until_ms: 3,
        };
        overlay.set_bitemporal(&cache, 4, stamp).unwrap();
        overlay.note_base_pk(&cache, 4, vec![0xa1, 0x61]).unwrap();
        overlay.note_base_pk(&cache, 4, vec![0xa1, 0x62]).unwrap();

        assert_eq!(
            overlay.get_ttl_by_doc_id(&cache, &id(4)).unwrap(),
            Some(StagedTtl::ExpireAt(9))
        );
        assert_eq!(overlay.get_bitemporal(&cache, 4).unwrap(), Some(stamp));
        assert_eq!(overlay.base_pk(&cache, 4).unwrap(), Some(vec![0xa1, 0x61]));
        let ttl_only: Vec<_> = overlay
            .iter_ttl_only_for_collection(&cache)
            .collect::<crate::Result<_>>()
            .unwrap();
        assert_eq!(ttl_only, vec![(id(4), StagedTtl::ExpireAt(9))]);
    }

    #[test]
    fn a_spill_error_poisons_the_overlay() {
        let dir = tempfile::tempdir().unwrap();
        // A file where the spill directory must go makes every spill fail.
        let blocker = dir.path().join("blocked");
        std::fs::write(&blocker, b"x").unwrap();
        let mut overlay = TxnOverlay::with_spill(SpillSpace::new(blocker.join("t"), "doc", 0));
        let users = key("users");

        assert!(
            overlay
                .insert_put(users.clone(), 1, &id(1), vec![1])
                .is_err()
        );
        assert!(overlay.get(&users, 1).is_err());
        assert!(overlay.insert_put(users, 2, &id(2), vec![2]).is_err());
    }
}
