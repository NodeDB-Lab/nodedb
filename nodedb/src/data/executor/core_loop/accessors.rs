//! Small accessor impls on `CoreLoop`: scan-quiesce install + acquire,
//! test-only inspection helpers. Extracted from `mod.rs` to keep the
//! main file under the 500-LOC ceiling while leaving the struct
//! definition in one place.

#[cfg(test)]
use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Install the shared scan-quiesce registry. Called once by the
    /// server bootstrap in `main.rs` after `SharedState::open`.
    pub fn set_quiesce(
        &mut self,
        quiesce: std::sync::Arc<crate::bridge::quiesce::CollectionQuiesce>,
    ) {
        self.quiesce = Some(quiesce);
    }

    /// Acquire a scan guard for `(tid, collection)`. Returns `Ok(None)`
    /// if no quiesce registry is installed (e.g. in tests) — callers
    /// treat that as "scan unconditionally". Returns `Err(Response)`
    /// carrying a `NodeDbError::collection_draining` error code when
    /// a drain is in progress against the collection.
    pub(in crate::data::executor) fn acquire_scan_guard(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: u32,
        collection: &str,
    ) -> Result<Option<crate::bridge::quiesce::ScanGuard>, crate::bridge::envelope::Response> {
        let Some(q) = self.quiesce.as_ref() else {
            return Ok(None);
        };
        match q.try_start_scan(tid, collection) {
            Ok(g) => Ok(Some(g)),
            Err(_) => Err(self.response_error(
                task,
                crate::bridge::envelope::ErrorCode::CollectionDraining {
                    collection: collection.to_string(),
                },
            )),
        }
    }

    /// Set the last timeseries ingest timestamp (for testing idle flush).
    pub fn set_last_ts_ingest(&mut self, value: Option<std::time::Instant>) {
        self.last_ts_ingest = value;
    }

    /// Test accessor: row count in a columnar memtable.
    #[cfg(test)]
    pub fn columnar_memtable_row_count(&self, tid: u32, collection: &str) -> u64 {
        let key = (TenantId::new(tid), collection.to_string());
        self.columnar_memtables
            .get(&key)
            .map(|mt| mt.row_count())
            .unwrap_or(0)
    }

    /// Test accessor: total row count across all partitions in a timeseries registry.
    #[cfg(test)]
    pub fn ts_registry_row_count(&self, tid: u32, collection: &str) -> u64 {
        let key = (TenantId::new(tid), collection.to_string());
        self.ts_registries
            .get(&key)
            .map(|reg| {
                let range = nodedb_types::timeseries::TimeRange::new(0, i64::MAX);
                reg.query_partitions(&range)
                    .iter()
                    .map(|e| e.meta.row_count)
                    .sum()
            })
            .unwrap_or(0)
    }
}
