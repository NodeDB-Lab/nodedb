// SPDX-License-Identifier: BUSL-1.1

//! The engine scan a per-shard aggregate folds when it reads at a system-time
//! scope other than `Current`.
//!
//! The current-state aggregate scan reads the newest version of every row and
//! has its own fast paths (result cache, index-backed COUNT). A read at a
//! transaction snapshot or at a historical cut reads the versions visible at
//! the scope's ordinal instead. Each engine's own scan op already serves every
//! scope, and a transaction snapshot scan folds the transaction's staged
//! writes. The aggregate runs over that scan's rows, so each engine answers the
//! scope exactly as its plain scan does.

use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp, UNBOUNDED_TIME_RANGE,
};
use nodedb_types::{QualifiedCollection, SystemTimeScope};

use crate::data::executor::core_loop::CoreLoop;

/// The scan reads every row: the aggregate folds all of them, and the scan
/// byte budget bounds the read.
const EVERY_ROW: usize = usize::MAX;

impl CoreLoop {
    /// The engine scan of `collection` on this core at `system_time`. The
    /// engine is the one [`CoreLoop::scan_collection`] reads: timeseries,
    /// then KV, then columnar, then the document store.
    pub(in crate::data::executor) fn scoped_aggregate_scan(
        &self,
        did: u64,
        tid: u64,
        collection: &str,
        system_time: SystemTimeScope,
    ) -> PhysicalPlan {
        let qualified = QualifiedCollection::from_stored(collection.to_string());
        if self.is_timeseries_collection(did, tid, collection) {
            return PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: qualified,
                time_range: UNBOUNDED_TIME_RANGE,
                projection: Vec::new(),
                limit: EVERY_ROW,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time,
                valid_at_ms: None,
                having: Vec::new(),
                tiering: None,
                output_columns: Vec::new(),
            });
        }
        if self.kv_engine.has_collection(did, tid, collection) {
            return PhysicalPlan::Kv(KvOp::Scan {
                collection: qualified,
                cursor: Vec::new(),
                count: EVERY_ROW,
                filters: Vec::new(),
                projection: Vec::new(),
                computed_columns: Vec::new(),
                sort_keys: Vec::new(),
                match_pattern: None,
                surrogate_ceiling: None,
                system_time,
            });
        }
        let columnar_key = (
            crate::types::DatabaseId::new(did),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        if self.columnar_engines.contains_key(&columnar_key) {
            return PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: qualified,
                projection: Vec::new(),
                limit: EVERY_ROW,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                rls_filters: Vec::new(),
                system_time,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            });
        }
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: qualified,
            limit: EVERY_ROW,
            offset: 0,
            filters: Vec::new(),
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time,
            valid_at_ms: None,
            prefilter: None,
        })
    }
}
