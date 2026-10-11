// SPDX-License-Identifier: BUSL-1.1

//! The data groups whose replica on this node owes a snapshot install.
//!
//! A node that leaves a group drops the group's streams (see
//! [`super::leave`]). A remount resumes the group's old log. That log can
//! hold a later chunk or the final entry of a dropped stream. Here it
//! refuses, while every other replica installs the redo. So the drop marks
//! the group owed, durably, before the streams release their WAL floor
//! holds:
//!
//! - The group's snapshot requirement holds on this node while it is owed.
//!   Its replica refuses log entries, and its leader sends a snapshot. The
//!   snapshot carries the group's open streams.
//! - A refusal of a chunk or final entry of an owed group holds the group's
//!   durable floor instead of passing the entry.
//! - A snapshot install of the group settles the debt.
//!
//! The set is a leaf lock: no other lock is taken while it is held.

use std::collections::BTreeSet;
use std::sync::MutexGuard;

use crate::control::security::catalog::SystemCatalog;

use super::store::RedoChunkStore;

impl RedoChunkStore {
    fn owed_set(&self) -> MutexGuard<'_, BTreeSet<u64>> {
        self.owed.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Load the owed groups the catalog saved, at boot.
    pub fn load_owed(&self, groups: Vec<u64>) {
        self.owed_set().extend(groups);
    }

    /// Whether this node's replica of `group_id` owes a snapshot install.
    pub fn owes_snapshot(&self, group_id: u64) -> bool {
        self.owed_set().contains(&group_id)
    }

    /// Every group whose replica here owes a snapshot install.
    pub fn owed_groups(&self) -> Vec<u64> {
        self.owed_set().iter().copied().collect()
    }

    /// Record that the replicas of `groups` here owe a snapshot install.
    /// Durable before it takes effect. A group owed already is not written
    /// again.
    pub fn record_owed(
        &self,
        catalog: &SystemCatalog,
        groups: &BTreeSet<u64>,
    ) -> crate::Result<()> {
        let fresh: Vec<u64> = {
            let owed = self.owed_set();
            groups
                .iter()
                .copied()
                .filter(|group_id| !owed.contains(group_id))
                .collect()
        };
        catalog.save_redo_snapshot_owed(&fresh)?;
        self.owed_set().extend(fresh);
        Ok(())
    }

    /// Settle the debt of `group_id`: a snapshot of the group installed
    /// here, with the group's open streams.
    pub fn settle_owed(&self, catalog: &SystemCatalog, group_id: u64) -> crate::Result<()> {
        if !self.owes_snapshot(group_id) {
            return Ok(());
        }
        catalog.remove_redo_snapshot_owed(group_id)?;
        self.owed_set().remove(&group_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::wal;
    use super::super::store::{RedoChunkLimits, RedoChunkStore};
    use super::*;

    fn limits() -> RedoChunkLimits {
        RedoChunkLimits {
            max_entry_bytes: 64 * 1024,
            max_open_bytes: 1 << 20,
        }
    }

    #[test]
    fn an_owed_group_survives_a_restart_until_its_snapshot_installs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let store = RedoChunkStore::new(wal(&dir), limits());

        store
            .record_owed(&catalog, &BTreeSet::from([3, 8]))
            .expect("record");
        assert!(store.owes_snapshot(3));
        assert!(!store.owes_snapshot(4));

        let restarted_dir = tempfile::tempdir().expect("tempdir");
        let restarted = RedoChunkStore::new(wal(&restarted_dir), limits());
        restarted.load_owed(catalog.load_redo_snapshot_owed().expect("load"));
        assert_eq!(restarted.owed_groups(), vec![3, 8]);

        restarted.settle_owed(&catalog, 3).expect("settle");
        restarted
            .settle_owed(&catalog, 4)
            .expect("settle a group never owed");
        assert_eq!(restarted.owed_groups(), vec![8]);
        assert_eq!(catalog.load_redo_snapshot_owed().expect("load"), vec![8]);
    }
}
