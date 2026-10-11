// SPDX-License-Identifier: BUSL-1.1

//! Drop the streams of the data groups this node stops hosting.
//!
//! A node that leaves a group applies no later entry of it. No final entry,
//! abandon, or later term then closes the group's streams, so they close
//! here. Their floor holds release with them.
//!
//! A remount of the group resumes its old log, which can still name a
//! dropped stream. So each group that loses a stream owes a snapshot
//! install first (see [`super::owed`]). The debt is durable before any
//! floor hold releases: a checkpoint can truncate the stream's records once
//! the hold is gone.
//!
//! No WAL record marks the drop. A boot can rebuild a dropped stream from
//! its records, and the next membership pass drops it again.

use std::collections::BTreeSet;

use crate::control::security::catalog::SystemCatalog;

use super::store::RedoChunkStore;

impl RedoChunkStore {
    /// Drop every open and parked stream of a data group `hosted` rejects,
    /// once each such group owes a snapshot install in `catalog`. Returns
    /// how many open streams dropped.
    ///
    /// The caller holds the lock that orders group mounts, so a group that
    /// mounts concurrently keeps its streams, and a remount sees the debt.
    /// When the debt does not persist, every stream stays.
    pub fn drop_unhosted_groups(
        &self,
        hosted: impl Fn(u64) -> bool,
        catalog: &SystemCatalog,
    ) -> crate::Result<usize> {
        let leaving: BTreeSet<u64> = {
            let state = self.lock();
            state
                .streams
                .values()
                .map(|open| open.group_id)
                .chain(state.parked.iter().map(|parked| parked.group_id))
                .filter(|&group_id| !hosted(group_id))
                .collect()
        };
        if leaving.is_empty() {
            return Ok(0);
        }
        self.record_owed(catalog, &leaving)?;
        // A stream of a group outside `leaving` opened after the scan. It
        // drops on the next pass, once its group owes a snapshot.
        let drops = |group_id: u64| leaving.contains(&group_id) && !hosted(group_id);
        let (dropped, released) = {
            let mut state = self.lock();
            let streams = std::mem::take(&mut state.streams);
            let (dropped, kept): (Vec<_>, Vec<_>) = streams
                .into_iter()
                .partition(|(_, open)| drops(open.group_id));
            state.streams = kept.into_iter().collect();
            let parked = std::mem::take(&mut state.parked);
            let (released, kept): (Vec<_>, Vec<_>) = parked
                .into_iter()
                .partition(|parked| drops(parked.group_id));
            state.parked = kept;
            self.note_open(&state);
            (dropped, released)
        };
        let count = dropped.len();
        // The floor holds release outside the store lock.
        drop(dropped);
        drop(released);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{chunk, session, wal};
    use super::super::store::{RedoChunkLimits, RedoChunkStore};
    use super::*;

    fn limits() -> RedoChunkLimits {
        RedoChunkLimits {
            max_entry_bytes: 64 * 1024,
            max_open_bytes: 1 << 20,
        }
    }

    fn catalog(dir: &tempfile::TempDir) -> SystemCatalog {
        SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog")
    }

    #[tokio::test]
    async fn leaving_a_group_drops_its_streams_and_releases_their_floor_holds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(&dir);
        let store = RedoChunkStore::new(wal(&dir), limits());
        // Group 1: one open stream and one parked stream.
        store
            .apply_chunk(chunk(session(1), 0, 4, b"ab"))
            .await
            .expect("chunk");
        store
            .apply_chunk(chunk(session(2), 0, 2, b"cd"))
            .await
            .expect("chunk");
        let failed = store.take_for_final(&session(2)).expect("open stream");
        store.park(failed);
        // Group 9 stays hosted.
        let mut kept = chunk(session(3), 0, 4, b"ef");
        kept.group_id = 9;
        store.apply_chunk(kept).await.expect("chunk");
        assert_eq!(store.open_streams(), 2);

        let dropped = store
            .drop_unhosted_groups(|group_id| group_id == 9, &catalog)
            .expect("drop");
        assert_eq!(dropped, 1);
        assert_eq!(store.open_streams(), 1);
        assert!(store.take_for_final(&session(1)).is_none());
        // Only the hosted group's stream holds the floor.
        assert!(store.wal.floor_holds().lowest().is_some());
        drop(
            store
                .take_for_final(&session(3))
                .expect("the hosted stream stays"),
        );
        assert_eq!(store.wal.floor_holds().lowest(), None);
    }

    /// A remount of a group that lost a stream resumes a log that can name
    /// the stream. The group owes a snapshot install, durably, and a hosted
    /// group owes nothing.
    #[tokio::test]
    async fn dropping_a_groups_streams_records_its_snapshot_requirement() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(&dir);
        let store = RedoChunkStore::new(wal(&dir), limits());
        assert_eq!(
            store
                .drop_unhosted_groups(|_| false, &catalog)
                .expect("drop"),
            0
        );
        assert!(
            store.owed_groups().is_empty(),
            "a group with no stream owes nothing"
        );

        store
            .apply_chunk(chunk(session(1), 0, 4, b"ab"))
            .await
            .expect("chunk");
        let failed = store.take_for_final(&session(1)).expect("open stream");
        store.park(failed);
        let mut kept = chunk(session(3), 0, 4, b"ef");
        kept.group_id = 9;
        store.apply_chunk(kept).await.expect("chunk");

        store
            .drop_unhosted_groups(|group_id| group_id == 9, &catalog)
            .expect("drop");
        assert!(store.owes_snapshot(1), "a parked stream's group owes one");
        assert!(!store.owes_snapshot(9));
        assert_eq!(catalog.load_redo_snapshot_owed().expect("load"), vec![1]);
    }
}
