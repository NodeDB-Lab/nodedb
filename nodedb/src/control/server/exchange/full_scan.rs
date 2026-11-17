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
//! Both resolve through [`require_full_scan_plan`]: a collection the catalog
//! does not hold is a typed error, never a by-name scan.
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
    /// The system-time scope the scan reads at. `Current` unless the read
    /// it stands for names one ([`ScanSide::at`]).
    system_time: SystemTimeScope,
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
            system_time: SystemTimeScope::Current,
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
            system_time: SystemTimeScope::Current,
        }
    }

    /// This side, read at `system_time`: the scope of the read it stands
    /// for, so a transaction snapshot or a historical cut survives the
    /// rewrite to a full scan.
    pub fn at(self, system_time: SystemTimeScope) -> Self {
        Self {
            system_time,
            ..self
        }
    }

    /// The collection name this scan reads.
    pub fn collection(&self) -> &'a str {
        self.collection
    }

    /// The system-time scope this scan reads at.
    pub fn system_time(&self) -> SystemTimeScope {
        self.system_time
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
/// A read-set capture uses [`read_set_scan_plan`], which records an absence
/// read for a missing collection. A join side that must read rows uses
/// [`require_full_scan_plan`], which answers a missing collection with a
/// typed error.
pub fn full_scan_plan_for_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
) -> crate::Result<Option<PhysicalPlan>> {
    // `side.collection` is the plan's database-qualified name. The catalog
    // keys the collection by its bare name.
    let key = nodedb_types::CollectionKey::from_qualified_str(database_id, side.collection)?;
    let qualified =
        nodedb_types::QualifiedCollection::from_stored(database_id, side.collection.to_string())?;
    let system_time = side.system_time;
    // Both predicate sets travel in the slot the RLS filters already use: that
    // slot is evaluated per row before any join match, which is what a local
    // `WHERE` predicate needs too.
    let side_filters = combine_side_filters(side.rls_filters, side.scan_filters)?;
    let catalog = state.credentials.catalog();
    let stored = match catalog.get_collection(database_id, tenant_id.as_u64(), key.name())? {
        Some(s) => s,
        None => return Ok(None),
    };
    Ok(Some(scan_plan_for_stored(
        &stored,
        qualified,
        side_filters,
        system_time,
    )))
}

/// The scan plan a transaction read-set capture records for `side`: the
/// collection's engine scan, or an absence read when the catalog holds no
/// collection by that name.
///
/// An absence read is a document scan of the bare name. The read-set records
/// it as a collection-scoped predicate on the name's vShard, which commit
/// validates against the newest stored version of that name's slice in
/// every engine. A conflicting transaction that creates and populates the
/// collection after the snapshot writes a version above it, so the commit
/// fails. A create that writes no row changes nothing a reader observed:
/// "absent" and "empty" return the same rows, so the reader still orders
/// first.
pub fn read_set_scan_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
) -> crate::Result<PhysicalPlan> {
    let collection = side.collection;
    match full_scan_plan_for_collection(state, database_id, tenant_id, side)? {
        Some(plan) => Ok(plan),
        None => absence_read_plan(database_id, collection),
    }
}

/// The predicate read of a collection name the catalog does not hold.
pub(crate) fn absence_read_plan(
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::from_stored(
            database_id,
            collection.to_string(),
        )?,
        limit: COMPLETE_SCAN,
        offset: 0,
        filters: Vec::new(),
        sort_keys: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    }))
}

/// Build the full-collection scan plan a join build side MUST read: every
/// row, under the side's policy, gathered from every vShard.
///
/// The collection is resolved through the node's `SystemCatalog`, the
/// authoritative store the planner resolved the same name through (cluster
/// followers apply every replicated catalog record to it before serving).
/// A miss here means the collection does not exist for this tenant: it was
/// never created, or it was dropped and purged after the statement was
/// planned. A soft-deleted row means it was dropped after planning. Both
/// are typed errors, never a by-name scan: a by-name scan on one core reads
/// only that core's part of a sharded collection and joins silently wrong.
pub fn require_full_scan_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    side: ScanSide<'_>,
) -> crate::Result<PhysicalPlan> {
    let qualified =
        nodedb_types::QualifiedCollection::from_stored(database_id, side.collection.to_string())?;
    let side_filters = combine_side_filters(side.rls_filters, side.scan_filters)?;
    let stored = require_active_collection(state, database_id, tenant_id, side.collection)?;
    Ok(scan_plan_for_stored(
        &stored,
        qualified,
        side_filters,
        side.system_time,
    ))
}

/// The catalog row of the stored `collection` name, which a by-name read
/// executed after planning must find active. A miss is `CollectionNotFound`
/// and a soft-deleted row is `CollectionDeactivated`.
pub fn require_active_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<crate::control::security::catalog::StoredCollection> {
    let key = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?;
    let catalog = state.credentials.catalog();
    let Some(stored) = catalog.get_collection(database_id, tenant_id.as_u64(), key.name())? else {
        return Err(crate::Error::CollectionNotFound {
            tenant_id,
            collection: key.name().to_owned(),
        });
    };
    if !stored.is_active {
        return Err(crate::Error::CollectionDeactivated {
            tenant_id,
            collection: key.name().to_owned(),
            retention_expires_at_ns: deactivated_retention_expires_at_ns(&stored),
        });
    }
    Ok(stored)
}

/// When a soft-deleted collection's UNDROP window closes, under the default
/// retention. A drop time of `0` is unknown (a row the GC sweeper has not yet
/// adopted a time for, which it never purges), so the window reads as open
/// forever rather than already past.
pub(crate) fn deactivated_retention_expires_at_ns(
    stored: &crate::control::security::catalog::StoredCollection,
) -> u64 {
    if stored.deactivated_at_ns == 0 {
        return u64::MAX;
    }
    let retention = crate::config::server::RetentionSettings::default()
        .retention_window()
        .as_nanos() as u64;
    stored.deactivated_at_ns.saturating_add(retention)
}

/// The engine scan of every row of `stored`, under `side_filters`, read at
/// `system_time`. Exhaustive over `CollectionType`.
fn scan_plan_for_stored(
    stored: &crate::control::security::catalog::StoredCollection,
    qualified: nodedb_types::QualifiedCollection,
    side_filters: Vec<u8>,
    system_time: SystemTimeScope,
) -> PhysicalPlan {
    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Schemaless)
        | CollectionType::Document(DocumentMode::Strict(_)) => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: qualified,
                limit: COMPLETE_SCAN,
                offset: 0,
                filters: side_filters,
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
        CollectionType::KeyValue(_) => PhysicalPlan::Kv(KvOp::Scan {
            collection: qualified,
            cursor: Vec::new(),
            count: COMPLETE_SCAN,
            filters: side_filters,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            sort_keys: Vec::new(),
            match_pattern: None,
            surrogate_ceiling: None,
            system_time,
        }),
        CollectionType::Columnar(ColumnarProfile::Plain)
        | CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => {
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: qualified,
                projection: Vec::new(),
                limit: COMPLETE_SCAN,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                rls_filters: side_filters,
                system_time,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            })
        }
        CollectionType::Columnar(ColumnarProfile::Timeseries { .. }) => {
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: qualified,
                // No time filter: every row, pre-epoch ones included.
                time_range: nodedb_physical::physical_plan::UNBOUNDED_TIME_RANGE,
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
                system_time,
                valid_at_ms: None,
                having: Vec::new(),
                tiering: None,
                output_columns: Vec::new(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::StoredCollection;
    use crate::wal::WalManager;

    const TENANT: TenantId = TenantId::new(7);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create full-scan test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("full_scan.wal"))
                .expect("open full-scan test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(2, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct full-scan state");
        (state, directory)
    }

    /// A build side resolves straight from the authoritative catalog row, with
    /// no planner or cache in between, to an unbounded scan of every row under
    /// the side's filters: the gather then reads every vShard.
    #[test]
    fn build_side_resolves_from_the_catalog_to_a_complete_scan() {
        let (state, _dir) = test_state();
        state
            .credentials
            .catalog()
            .put_collection(
                DB,
                &StoredCollection::stamped_for_test(TENANT.as_u64(), "orders", "tester"),
            )
            .expect("persist collection");
        let plan =
            require_full_scan_plan(&state, DB, TENANT, ScanSide::join_side("orders", &[], &[]))
                .expect("an existing collection resolves");
        let PhysicalPlan::Document(DocumentOp::Scan {
            collection, limit, ..
        }) = plan
        else {
            panic!("expected a document scan, got {plan:?}");
        };
        assert_eq!(collection.as_str(), "orders");
        assert_eq!(limit, COMPLETE_SCAN);
    }

    /// A collection the catalog does not hold is a typed error, never a
    /// by-name scan of one core's part of it.
    #[test]
    fn nonexistent_build_side_is_collection_not_found() {
        let (state, _dir) = test_state();
        let error =
            require_full_scan_plan(&state, DB, TENANT, ScanSide::join_side("missing", &[], &[]))
                .expect_err("a missing collection must not resolve");
        assert!(
            matches!(&error, crate::Error::CollectionNotFound { collection, .. } if collection == "missing"),
            "{error:?}"
        );
        // The lenient builder the read-set capture uses still answers `None`.
        assert!(
            full_scan_plan_for_collection(&state, DB, TENANT, ScanSide::read_set_only("missing"))
                .expect("lookup")
                .is_none()
        );
    }

    /// A read-set capture of a collection the catalog does not hold records
    /// an absence read: a document scan of the bare name, never nothing.
    #[test]
    fn read_set_capture_of_a_missing_collection_is_an_absence_read() {
        let (state, _dir) = test_state();
        let plan = read_set_scan_plan(&state, DB, TENANT, ScanSide::read_set_only("missing"))
            .expect("an absent collection still yields a capture");
        let PhysicalPlan::Document(DocumentOp::Scan { collection, .. }) = &plan else {
            panic!("expected a document scan, got {plan:?}");
        };
        assert_eq!(collection.as_str(), "missing");

        state
            .credentials
            .catalog()
            .put_collection(
                DB,
                &StoredCollection::stamped_for_test(TENANT.as_u64(), "present", "tester"),
            )
            .expect("persist collection");
        let present = read_set_scan_plan(&state, DB, TENANT, ScanSide::read_set_only("present"))
            .expect("an existing collection resolves");
        assert!(matches!(
            present,
            PhysicalPlan::Document(DocumentOp::Scan { .. })
        ));
    }

    /// A collection dropped after planning is `CollectionDeactivated`, never a
    /// scan of its soft-deleted rows.
    #[test]
    fn soft_deleted_build_side_is_collection_deactivated() {
        let (state, _dir) = test_state();
        let mut dropped = StoredCollection::stamped_for_test(TENANT.as_u64(), "gone", "tester");
        dropped.is_active = false;
        state
            .credentials
            .catalog()
            .put_collection(DB, &dropped)
            .expect("persist collection");
        let error =
            require_full_scan_plan(&state, DB, TENANT, ScanSide::join_side("gone", &[], &[]))
                .expect_err("a dropped collection must not resolve");
        assert!(
            matches!(
                error,
                crate::Error::CollectionDeactivated {
                    retention_expires_at_ns: u64::MAX,
                    ..
                }
            ),
            "{error:?}"
        );
    }
}
