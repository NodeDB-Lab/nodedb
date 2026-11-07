// SPDX-License-Identifier: BUSL-1.1

//! The read cut of a statement on a mirror database.
//!
//! A mirror's newest consistent point is the newest source cut every group
//! applied. A later row can sit in one group and not yet in another, so a
//! historical read past that point mixes two cuts. The cap of such a read is
//! the cut's `cap_ordinal`, whatever the local clock reads.
//!
//! The local clock and the local Calvin markers order nothing on a mirror, so
//! the cap carries no marker and closes no local cut.

use nodedb_types::DatabaseId;

use crate::control::state::SharedState;

use super::statement_cap::StatementCap;

/// The cap of a statement reading `database`, or `None` for a database that is
/// not a read-only mirror. A mirror with no applied cut caps at `0`, which
/// reads before every version.
pub fn mirror_cap(
    state: &SharedState,
    database: DatabaseId,
) -> crate::Result<Option<StatementCap>> {
    if !state.mirror.fence.is_read_only(database) {
        return Ok(None);
    }
    let cap = state
        .credentials
        .catalog()
        .get_mirror_cut_state(database)?
        .map_or(0, |cut| cut.cap_ordinal);
    Ok(Some(StatementCap { cap, marker: 0 }))
}

/// The ordinal a history window on `database` measures its age from: a mirror's
/// cap, or the wall clock. Cleanup on a mirror holds history below the cap, so
/// a read at the cap keeps its window however far the mirror lags.
pub fn history_clock_ns(state: &SharedState, database: DatabaseId) -> crate::Result<i64> {
    let now_ns = nodedb_types::wall_now_ns_i64();
    Ok(match mirror_cap(state, database)? {
        Some(cap) => now_ns.min(cap.cap),
        None => now_ns,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::{MirrorCutState, MirrorStatus};

    use super::*;
    use crate::control::mirror::test_fixture::mirror_descriptor;

    const DB: u64 = 1200;

    fn shared(dir: &tempfile::TempDir) -> std::sync::Arc<SharedState> {
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("state")
    }

    fn hold(state: &SharedState, status: MirrorStatus, cap_ordinal: i64) {
        let descriptor = mirror_descriptor(DB, "mirror_db", status);
        let catalog = state.credentials.catalog();
        catalog.put_database(&descriptor).expect("descriptor");
        catalog
            .raise_mirror_cut_state(
                DatabaseId::new(DB),
                &MirrorCutState {
                    ready_cut: 2,
                    applied_cut: 2,
                    cap_ordinal,
                    ..MirrorCutState::default()
                },
            )
            .expect("cut state");
        state.mirror.fence.apply_descriptor(&descriptor);
    }

    #[test]
    fn a_read_on_a_mirror_caps_at_its_applied_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = shared(&dir);
        hold(&state, MirrorStatus::Following, 42);
        let cap = mirror_cap(&state, DatabaseId::new(DB))
            .expect("read")
            .expect("a mirror has a cap");
        assert_eq!(cap, StatementCap { cap: 42, marker: 0 });
        assert_eq!(
            history_clock_ns(&state, DatabaseId::new(DB)).expect("read"),
            42
        );
    }

    #[test]
    fn a_promoted_mirror_and_a_plain_database_have_no_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = shared(&dir);
        hold(&state, MirrorStatus::Promoted, 42);
        assert_eq!(
            mirror_cap(&state, DatabaseId::new(DB)).expect("read"),
            None,
            "a promoted mirror reads under the local clock"
        );
        assert_eq!(
            mirror_cap(&state, DatabaseId::new(DB + 1)).expect("read"),
            None
        );
    }
}
