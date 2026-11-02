// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::PurgeVersionsBelow`: one batch of this core's history purge
//! below the cleanup horizon.
//!
//! A request at a horizon this core holds no pass for starts a pass: it lists
//! every store to visit (`version_purge_units`) and drops every historical
//! CSR below the horizon. Each request then purges at most `batch_units`
//! stores, so other requests run between the batches, and answers how many
//! stores are still due. A repeated request at the same horizon continues
//! the pass.
//!
//! The purge removes only versions no read at or above the horizon reaches,
//! so every such read answers as before on this core. The newest-ordinal
//! cache forgets every value afterwards (`dispatch_meta`): a row whose
//! tombstone left lowers its slice's newest ordinal, and only to a value at
//! or below the horizon.

use nodedb_physical::physical_plan::{PURGE_REMAINING_KEY, RetainedHistory};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::version_purge::PurgePass;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Run one batch of the pass at `horizon`. Answers `{"remaining": n}`.
    pub(in crate::data::executor::dispatch) fn meta_purge_versions_below(
        &mut self,
        task: &ExecutionTask,
        horizon: i64,
        batch_units: u32,
        retained: &[RetainedHistory],
    ) -> Response {
        let state = &mut self.maintenance.version_purge;
        state.safe_horizon = state.safe_horizon.max(horizon);
        let current = state
            .pass
            .take()
            .filter(|pass| pass.horizon == horizon && pass.retained == retained);
        let mut pass = match current {
            Some(pass) => pass,
            None => match self.version_purge_units() {
                Ok(pending) => {
                    self.historical_csr.drop_below(horizon);
                    PurgePass {
                        horizon,
                        retained: retained.to_vec(),
                        pending,
                    }
                }
                Err(e) => return self.response_error(task, ErrorCode::from(e)),
            },
        };

        let mut purged = 0usize;
        let mut failure = None;
        for _ in 0..batch_units.max(1) {
            let Some(unit) = pass.pending.pop_front() else {
                break;
            };
            match self.run_version_purge_unit(&pass, &unit) {
                Ok(count) => purged += count,
                Err(e) => {
                    // The failed store leaves the pass. The next pass, at a
                    // fresh horizon, visits it again.
                    failure = Some((unit, e));
                    break;
                }
            }
        }
        let remaining = pass.pending.len();
        self.maintenance.version_purge.pass = Some(pass);

        if let Some((unit, e)) = failure {
            tracing::warn!(
                core = self.core_id,
                horizon,
                unit = ?unit,
                error = %e,
                "version purge: a store did not purge"
            );
            return self.response_error(task, ErrorCode::from(e));
        }
        if purged > 0 {
            tracing::debug!(
                core = self.core_id,
                horizon,
                purged,
                remaining,
                "version purge batch"
            );
        }
        match response_codec::encode_count(PURGE_REMAINING_KEY, remaining) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_types::{StorageKey, Surrogate};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
    use crate::engine::sparse::btree_versioned::VersionedPut;
    use crate::types::VShardId;

    fn key() -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(7))
    }

    fn put(core: &CoreLoop, coll: &str, ordinal: i64) {
        let body = ordinal.to_le_bytes();
        core.sparse
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 1,
                coll,
                doc_id: &key(),
                sys_from: ordinal,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &body,
            })
            .expect("put");
    }

    /// The `sys_from` of the version a read of `coll` at `cut` sees.
    fn read_at(core: &CoreLoop, coll: &str, cut: i64) -> Option<i64> {
        core.sparse
            .versioned_get_as_of(0, 1, coll, &key(), Some(cut), None)
            .expect("read")
            .map(|body| i64::from_le_bytes(body.try_into().expect("8-byte body")))
    }

    fn remaining(response: &Response) -> usize {
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let counts: BTreeMap<String, usize> =
            zerompk::from_msgpack(response.payload.as_ref()).expect("count map");
        counts[PURGE_REMAINING_KEY]
    }

    /// Run the pass at `horizon` with one store per request. Returns the
    /// number of requests it took.
    fn run_pass(core: &mut CoreLoop, horizon: i64, retained: &[RetainedHistory]) -> usize {
        let task = task_with_vshard(VShardId::new(0));
        for round in 1..=64 {
            let response = core.meta_purge_versions_below(&task, horizon, 1, retained);
            if remaining(&response) == 0 {
                return round;
            }
        }
        panic!("the pass did not finish in 64 requests");
    }

    /// One store per request: the pass takes one request per store, and
    /// every collection keeps the version a read at the horizon sees.
    #[test]
    fn a_pass_purges_store_by_store_and_keeps_the_version_at_the_horizon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        for coll in ["a", "b", "c"] {
            for ordinal in [10, 20, 30] {
                put(&core, coll, ordinal);
            }
        }

        let rounds = run_pass(&mut core, 25, &[]);

        assert!(
            rounds >= 4,
            "three document stores and the engine-wide ones"
        );
        for coll in ["a", "b", "c"] {
            assert_eq!(
                read_at(&core, coll, 25),
                Some(20),
                "{coll}: the horizon's version"
            );
            assert_eq!(
                read_at(&core, coll, i64::MAX),
                Some(30),
                "{coll}: the current one"
            );
            assert_eq!(
                read_at(&core, coll, 15),
                None,
                "{coll}: the superseded one left"
            );
        }
        assert_eq!(core.maintenance.version_purge.safe_horizon, 25);
        assert_eq!(
            remaining(&core.meta_purge_versions_below(
                &task_with_vshard(VShardId::new(0)),
                25,
                1,
                &[]
            )),
            0,
            "a repeated request at the same horizon finds nothing due"
        );
    }

    /// A retained collection with no audit window keeps its history. One
    /// with a window purges at its own horizon.
    #[test]
    fn a_retained_collection_purges_at_its_own_horizon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        for coll in ["keep", "audit"] {
            for ordinal in [10, 20, 30] {
                put(&core, coll, ordinal);
            }
        }
        let retained = |collection: &str, horizon| RetainedHistory {
            database_id: 0,
            tenant_id: 1,
            collection: collection.into(),
            horizon,
        };

        run_pass(
            &mut core,
            35,
            &[retained("keep", None), retained("audit", Some(15))],
        );

        assert_eq!(read_at(&core, "keep", 15), Some(10), "no window keeps all");
        assert_eq!(
            read_at(&core, "audit", 15),
            Some(10),
            "its own horizon is 15"
        );
        assert_eq!(read_at(&core, "audit", 25), Some(20));
    }

    /// A commit whose snapshot trails its ordinal past the window is
    /// refused, and one with no ordinal of its own is not.
    #[test]
    fn the_commit_window_bounds_a_snapshot_below_its_commit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.maintenance.version_purge.commit_window_ns = 100;
        assert!(core.snapshot_within_window(50, Some(120)));
        assert!(
            core.snapshot_within_window(20, Some(120)),
            "at the window edge"
        );
        assert!(!core.snapshot_within_window(19, Some(120)));
        assert!(core.snapshot_within_window(19, None));
    }
}
