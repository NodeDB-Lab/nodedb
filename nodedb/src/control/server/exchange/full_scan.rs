// SPDX-License-Identifier: BUSL-1.1

//! Build a full-collection scan `PhysicalPlan` for a catalog-resolved
//! collection, carrying that collection's row-level-security predicate.
//!
//! Shared by the cross-node join paths that need a complete by-name scan of a
//! user collection on the coordinator:
//!
//! - `resolve::exchange::gather_join_build_side` — gathers a broadcast-join
//!   build side across all vShards and inlines it as a `ProviderScan`.
//! - `resolve::shuffle` — encodes each side's scan as `plan_bytes` for the
//!   distributed shuffle producers.
//!
//! Both need the SAME "scan everything for this engine, no projection" plan,
//! so it lives here once. The plan is fabricated on the coordinator AFTER the
//! RLS pass has run over the query, so it can never be reached by that pass:
//! the caller hands it the side's already-compiled policy through
//! [`ScanSide`], which pairs a collection with its filters so one side's rows
//! can never be scanned under another side's policy.
//!
//! The match over `CollectionType` is EXHAUSTIVE — every catalog-creatable
//! engine is handled and there is no name-scan fallback for an "unsupported
//! engine". The Array engine is intentionally absent: it is not a
//! `CollectionType` variant (Array uses its own `CREATE ARRAY` DDL and never
//! appears as a `StoredCollection`), so it cannot reach this path.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp};
use nodedb_types::{CollectionType, ColumnarProfile, DocumentMode, SystemTimeScope};

use crate::bridge::scan_filter::decode_scan_filters;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// The build/scan side of a join must be COMPLETE — every row is needed for
/// correct match output, so the scan is unbounded (no row cap). A fixed cap
/// would silently drop join matches for larger collections. This is
/// allocation-safe: the scan path sizes its buffer as `with_capacity(limit
/// .min(256))` and bounds output with `take(limit)`, and `fetch_limit` uses
/// `saturating_mul`, so `usize::MAX` returns all rows without pre-allocating or
/// overflowing.
const COMPLETE_SCAN: usize = usize::MAX;

/// A collection to scan, paired with the predicates that apply to it: the
/// row-level-security filters for the requesting identity and the side's own
/// `WHERE` predicates. The pairing is the point: no caller can scan one join
/// side's rows under the other side's policy or predicate, whichever side the
/// planner drives from.
pub struct ScanSide<'a> {
    collection: &'a str,
    rls_filters: &'a [u8],
    scan_filters: &'a [u8],
}

impl<'a> ScanSide<'a> {
    /// One side of a join, under the compiled read filters the RLS pass
    /// injected into that side's slot plus that side's own `WHERE` predicates.
    /// Empty `rls_filters` means no policy restricts this identity on the
    /// collection; empty `scan_filters` means the side has no local predicate.
    pub fn join_side(collection: &'a str, rls_filters: &'a [u8], scan_filters: &'a [u8]) -> Self {
        Self {
            collection,
            rls_filters,
            scan_filters,
        }
    }

    /// A scan whose rows never reach the caller: a read-set capture plan names
    /// the collection and its engine for commit-time validation, so it carries
    /// no policy of its own.
    pub fn read_set_only(collection: &'a str) -> Self {
        Self {
            collection,
            rls_filters: &[],
            scan_filters: &[],
        }
    }

    /// The collection name this scan reads.
    pub fn collection(&self) -> &'a str {
        self.collection
    }
}

/// AND the two predicate sets a scanned join side carries into one
/// MessagePack `Vec<ScanFilter>`.
///
/// A set that fails to decode is an error, never an empty set: dropping either
/// returns rows the caller excluded.
fn combine_side_filters(rls_filters: &[u8], scan_filters: &[u8]) -> crate::Result<Vec<u8>> {
    if scan_filters.is_empty() {
        return Ok(rls_filters.to_vec());
    }
    if rls_filters.is_empty() {
        return Ok(scan_filters.to_vec());
    }
    let mut combined = decode_scan_filters(rls_filters, "join side filter")?;
    combined.extend(decode_scan_filters(scan_filters, "join side filter")?);
    zerompk::to_msgpack_vec(&combined).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("join side filter serialization: {e}"),
    })
}

/// Build a full-collection scan plan for `side`, or `Ok(None)` when the
/// catalog has no record for it on this node.
///
/// `Ok(None)` is a graceful "fall back to a by-name scan on the executing
/// node" signal for callers that have a name-scan fallback — it is never an
/// error. Callers that REQUIRE a scan plan (the shuffle producer cannot scan
/// by name across nodes) treat `None` as a typed error themselves.
pub fn full_scan_plan_for_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
) -> crate::Result<Option<PhysicalPlan>> {
    let collection = side.collection;
    // Both predicate sets travel in the slot the RLS filters already use: that
    // slot is evaluated per row before any join match, which is what a local
    // `WHERE` predicate needs too.
    let side_filters = combine_side_filters(side.rls_filters, side.scan_filters)?;
    let catalog = state.credentials.catalog();
    let stored = match catalog.get_collection(database_id, tenant_id.as_u64(), collection)? {
        Some(s) => s,
        None => return Ok(None),
    };

    let plan = match &stored.collection_type {
        CollectionType::Document(DocumentMode::Schemaless)
        | CollectionType::Document(DocumentMode::Strict(_)) => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                limit: COMPLETE_SCAN,
                offset: 0,
                filters: side_filters,
                sort_keys: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            })
        }
        CollectionType::KeyValue(_) => PhysicalPlan::Kv(KvOp::Scan {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
            cursor: Vec::new(),
            count: COMPLETE_SCAN,
            filters: side_filters,
            sort_keys: Vec::new(),
            match_pattern: None,
            surrogate_ceiling: None,
            projection: Vec::new(),
            computed_columns: Vec::new(),
        }),
        CollectionType::Columnar(ColumnarProfile::Plain)
        | CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => {
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                projection: Vec::new(),
                limit: COMPLETE_SCAN,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                rls_filters: side_filters,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            })
        }
        CollectionType::Columnar(ColumnarProfile::Timeseries { .. }) => {
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                // (0, i64::MAX) = no time filter — scan all rows.
                time_range: (0, i64::MAX),
                projection: Vec::new(),
                limit: COMPLETE_SCAN,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: side_filters,
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            })
        }
    };

    Ok(Some(plan))
}
