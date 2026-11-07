// SPDX-License-Identifier: Apache-2.0

//! The purge of text history no retained cut reads.

use crate::backend::FtsHistoryWrite;

/// Drop every history row a cut at or above its index's horizon cannot
/// read: the versions ended at or below the horizon, and the live starts at
/// or below it. The version of each document visible at the horizon stays.
/// `horizon_of(database, tenant, collection)` names the horizon, `None` to
/// keep the collection's whole history. Returns the number of ended
/// versions dropped.
pub fn purge_below<S: FtsHistoryWrite + ?Sized>(
    store: &mut S,
    horizon_of: &dyn Fn(u64, u64, &str) -> Option<i64>,
) -> Result<usize, S::Error> {
    let at_or_below = |database_id: u64, tid: u64, collection: &str, ordinal: i64| {
        horizon_of(database_id, tid, collection).is_some_and(|horizon| ordinal <= horizon)
    };
    let mut ended = Vec::new();
    store.scan_ended(&mut |row| {
        if at_or_below(row.database_id, row.tid, row.index.collection(), row.sys_to) {
            ended.push(row.to_key());
        }
    })?;
    store.remove_ended(&ended)?;

    let mut live = Vec::new();
    store.scan_live(&mut |row| {
        if at_or_below(row.database_id, row.tid, row.index.collection(), row.sys_from) {
            live.push(row.to_key());
        }
    })?;
    store.remove_live(&live)?;
    Ok(ended.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::FtsHistoryRead;
    use crate::backend::memory::MemoryBackend;
    use crate::history::test_rows::{end, end_in, start, start_in};
    use crate::history::{TextCut, cut_view};
    use crate::scope::IndexScope;

    fn docs_at(store: &MemoryBackend, collection: &str, at: i64) -> usize {
        cut_view(store, 0, 1, IndexScope::document(collection), TextCut::at(at))
            .expect("cut view")
            .docs
            .len()
    }

    /// Versions `[10, 20)`, `[20, 30)`, and live from 30. A purge at 25
    /// drops the version ended at 20 and keeps the one a read at 25 sees.
    #[test]
    fn a_purge_drops_the_versions_ended_below_the_horizon() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        start(&mut store, 1, 20);
        end(&mut store, 1, 30, &["beta"]);
        start(&mut store, 1, 30);
        assert_eq!(docs_at(&store, "docs", 15), 1);

        assert_eq!(purge_below(&mut store, &|_, _, _| Some(25)).expect("purge"), 1);
        assert_eq!(docs_at(&store, "docs", 15), 0, "purged below 25");
        assert_eq!(docs_at(&store, "docs", 25), 1, "the version at 25 stays");
    }

    /// A purge at the live start drops it: every retained cut reads the
    /// live version.
    #[test]
    fn a_purge_settles_the_live_starts_at_or_below_the_horizon() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        start(&mut store, 2, 40);
        purge_below(&mut store, &|_, _, _| Some(10)).expect("purge");
        let mut live = Vec::new();
        store.scan_live(&mut |row| live.push(row.surrogate.as_u32())).expect("scan");
        assert_eq!(live, vec![2], "only the start above the horizon stays");
    }

    /// A collection whose horizon is `None` keeps its whole history.
    #[test]
    fn a_collection_without_a_horizon_keeps_its_history() {
        let mut store = MemoryBackend::new();
        for collection in ["docs", "audit"] {
            start_in(&mut store, collection, 1, 10);
            end_in(&mut store, collection, 1, 20, &["alpha"]);
        }
        let dropped = purge_below(&mut store, &|_, _, collection| {
            (collection != "audit").then_some(25)
        })
        .expect("purge");
        assert_eq!(dropped, 1);
        assert_eq!(docs_at(&store, "docs", 15), 0);
        assert_eq!(docs_at(&store, "audit", 15), 1);
    }
}
