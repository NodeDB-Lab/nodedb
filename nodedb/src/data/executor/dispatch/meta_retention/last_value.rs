// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::QueryLastValues` / `MetaOp::QueryLastValue`: last-value cache
//! reads.
//!
//! A current read answers from the newest entry of each series. A read at
//! a system-time cut `S` answers from the version of each series current at
//! `S` (`LastValueCache::get_at`). The cache cannot answer such a read when
//! a TRUNCATE after `S` cleared it, when retention evicted series after
//! `S`, or when the version current at `S` was dropped below the cleanup
//! horizon. The read is then refused as a serialization conflict, and the
//! transaction retries at a later snapshot.

use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::last_value_cache::{LastValueCache, LastValueGap};

/// The ordinal a last-value read sees: `None` for the current state.
fn read_ordinal(collection: &str, system_time: SystemTimeScope) -> crate::Result<Option<i64>> {
    match system_time {
        SystemTimeScope::Current => Ok(None),
        SystemTimeScope::AsOf(ms) => Ok(Some(nodedb_types::ms_to_ordinal_upper(ms))),
        SystemTimeScope::Pinned { ordinal, .. } | SystemTimeScope::TxnSnapshot { ordinal, .. } => {
            Ok(Some(ordinal))
        }
        SystemTimeScope::AllVersions => Err(crate::Error::BadRequest {
            detail: format!(
                "the last-value cache of '{collection}' keeps no version history: \
                 AS OF SYSTEM TIME NULL cannot read it"
            ),
        }),
    }
}

/// The refusal of a read at `at` the cache cannot answer.
fn conflict(collection: &str, at: i64, why: String) -> crate::Error {
    crate::Error::ConflictRetry {
        collection: collection.to_string(),
        document_id: format!(
            "the last values of '{collection}' at snapshot {at} are gone: {why}; \
             retry the transaction"
        ),
    }
}

fn gap_reason(gap: LastValueGap) -> String {
    match gap {
        LastValueGap::Pruned { below } => {
            format!("the cleanup horizon {below} dropped the value current then")
        }
        LastValueGap::Evicted { at } => format!("retention evicted series at {at}"),
    }
}

impl CoreLoop {
    /// The collection's cache as a read at `at` sees it, or the refusal of
    /// a read at `at` a TRUNCATE after it cleared.
    fn last_value_cache_at(
        &self,
        task: &ExecutionTask,
        collection: &str,
        at: Option<i64>,
    ) -> crate::Result<Option<&LastValueCache>> {
        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        if let Some(at) = at
            && let Some(truncate) = self
                .ts_newest_truncate_of(&key)
                .filter(|ordinal| *ordinal > at)
        {
            return Err(conflict(
                collection,
                at,
                format!("a TRUNCATE at {truncate} cleared the cache"),
            ));
        }
        Ok(self.ts_last_value_caches.get(&key))
    }

    /// `MetaOp::QueryLastValues`.
    pub(in crate::data::executor::dispatch) fn meta_query_last_values(
        &self,
        task: &ExecutionTask,
        collection: &str,
        system_time: SystemTimeScope,
    ) -> Response {
        let entries = read_ordinal(collection, system_time).and_then(|at| {
            let Some(cache) = self.last_value_cache_at(task, collection, at)? else {
                return Ok(Vec::new());
            };
            let entries: Vec<(u64, i64, f64)> = match at {
                None => cache.all().map(|(id, e)| (id, e.ts, e.value)).collect(),
                Some(at) => cache
                    .all_at(at)
                    .map_err(|gap| conflict(collection, at, gap_reason(gap)))?
                    .into_iter()
                    .map(|(id, e)| (id, e.ts, e.value))
                    .collect(),
            };
            Ok(entries)
        });
        let entries = match entries {
            Ok(entries) => entries,
            Err(e) => return self.response_error(task, e),
        };
        match response_codec::encode(&entries) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// `MetaOp::QueryLastValue`.
    pub(in crate::data::executor::dispatch) fn meta_query_last_value(
        &self,
        task: &ExecutionTask,
        collection: &str,
        series_id: u64,
        system_time: SystemTimeScope,
    ) -> Response {
        let entry = read_ordinal(collection, system_time).and_then(|at| {
            let Some(cache) = self.last_value_cache_at(task, collection, at)? else {
                return Ok(None);
            };
            let entry = match at {
                None => cache.get(series_id),
                Some(at) => cache
                    .get_at(series_id, at)
                    .map_err(|gap| conflict(collection, at, gap_reason(gap)))?,
            };
            Ok(entry.map(|e| (e.ts, e.value)))
        });
        let entry: Option<(i64, f64)> = match entry {
            Ok(entry) => entry,
            Err(e) => return self.response_error(task, e),
        };
        match response_codec::encode(&entry) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::SystemTimeScope;

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::cagg_test_support::{
        COLLECTION, TENANT, ingest, task_at,
    };
    use crate::engine::timeseries::last_value_cache::LastValueCache;
    use crate::types::{DatabaseId, TenantId};

    const ROW_1: &str = "metrics,host=a value=1.5 1700000000000000000\n";
    const ROW_2: &str = "metrics,host=a value=2.5 1700000001000000000\n";

    fn decode(resp: &crate::bridge::envelope::Response) -> Vec<(u64, i64, f64)> {
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        crate::data::executor::response_codec::decode_payload(resp.payload.as_ref())
            .expect("decode")
    }

    /// A read at a snapshot answers with the value current at the
    /// snapshot. A value written after it stays invisible, and the current
    /// read sees it.
    #[test]
    fn a_snapshot_read_sees_the_value_current_at_the_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        core.ts_last_value_caches.insert(key, LastValueCache::new());
        let first = 7_000 * nodedb_types::NANOS_PER_MS;
        core.apply_scope.replicated_ordinal = Some(first);
        assert_eq!(ingest(&mut core, ROW_1, 1), Status::Ok);
        core.apply_scope.replicated_ordinal = Some(first + 1_000);
        assert_eq!(ingest(&mut core, ROW_2, 2), Status::Ok);
        core.apply_scope.replicated_ordinal = None;

        let at = |ordinal: i64| SystemTimeScope::TxnSnapshot {
            ordinal,
            cut_marker: 0,
        };
        let task = task_at(20);
        let snapshot = decode(&core.meta_query_last_values(&task, COLLECTION, at(first)));
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].2, 1.5, "the value current at the snapshot");
        let current =
            decode(&core.meta_query_last_values(&task, COLLECTION, SystemTimeScope::Current));
        assert_eq!(current[0].2, 2.5);
        let before = decode(&core.meta_query_last_values(&task, COLLECTION, at(first - 1)));
        assert!(before.is_empty(), "no value before the first write");
    }
}
