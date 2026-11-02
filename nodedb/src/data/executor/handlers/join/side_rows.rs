// SPDX-License-Identifier: BUSL-1.1

//! The rows of one side of a nested-loop or sort-merge join: the side's
//! resolved input when the coordinator set one, else a local scan of the
//! side's collection.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// One join side as the plan names it.
pub(super) struct JoinSideSource<'a> {
    pub collection: &'a str,
    /// The coordinator-resolved rows, set inside a transaction so the side
    /// reads its staged writes. The child carries the side's read policy.
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    /// The side's read policy, applied to locally scanned rows.
    pub rls_filters: &'a [u8],
    /// The scope the join reads its by-name sides at.
    pub system_time: &'a nodedb_types::SystemTimeScope,
}

impl CoreLoop {
    /// Materialize one join side, up to `scan_limit` locally scanned rows.
    ///
    /// A local scan reads the current rows of every engine the collection
    /// can live in. A by-name side at any other scope is refused: the
    /// coordinator resolves such a side to an input that carries its scope,
    /// so this read never answers current rows for a snapshot or a cut.
    ///
    /// A child error response is returned unchanged.
    pub(super) fn join_side_rows(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        side: JoinSideSource<'_>,
        scan_limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, Box<Response>> {
        if let Some(input) = side.input {
            let docs = self.input_docs(task, input)?;
            return self
                .retain_rows_matching(docs, side.rls_filters, "RLS filter (join side)")
                .map_err(|e| Box::new(self.response_error(task, ErrorCode::from(e))));
        }
        if !side.system_time.is_current() {
            return Err(Box::new(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "join side '{}' at {:?} reached the Data Plane by name; the \
                         coordinator resolves a side read at a scope other than the current \
                         state to an input",
                        side.collection, side.system_time
                    ),
                },
            )));
        }
        let did = task.request.database_id.as_u64();
        // A timeseries side reads its flushed partitions through a registry
        // that loads lazily.
        if let Err(e) = self.prepare_collection_scan(did, tid, side.collection) {
            return Err(Box::new(self.response_error(task, ErrorCode::from(e))));
        }
        self.scan_collection_with_rls(did, tid, side.collection, scan_limit, side.rls_filters)
            .map_err(|e| Box::new(self.response_error(task, ErrorCode::from(e))))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::SystemTimeScope;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

    /// A by-name side reads the current rows. A side at a block's snapshot
    /// never falls back to a current read: the coordinator resolves it to an
    /// input, so a by-name one is refused.
    #[test]
    fn a_by_name_side_at_a_snapshot_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let current = SystemTimeScope::Current;
        let rows = core.join_side_rows(
            &task,
            1,
            JoinSideSource {
                collection: "absent",
                input: None,
                rls_filters: &[],
                system_time: &current,
            },
            16,
        );
        assert!(rows.is_ok(), "a current by-name side scans");

        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 42,
            cut_marker: 0,
        };
        let refused = core.join_side_rows(
            &task,
            1,
            JoinSideSource {
                collection: "absent",
                input: None,
                rls_filters: &[],
                system_time: &snapshot,
            },
            16,
        );
        let Err(response) = refused else {
            panic!("a by-name side at a snapshot must be refused");
        };
        assert_eq!(response.status, Status::Error);
    }
}
