// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine (Plain / Timeseries / Spatial) source-to-target row copy.
//!
//! Drives the source `ColumnarOp::MaterializeScan` cursor to completion, and
//! for each non-tombstoned, not-yet-copied row dispatches a
//! `ColumnarOp::Insert { intent: InsertIfAbsent }` against target with a
//! fresh surrogate. Calls the reaper at the end to flip the collection status
//! to `Materialized` and clear `cloned_from`.
//!
//! ## Idempotency / restart-safety
//!
//! Resume after a crash works because every step is observable:
//!   - Tombstones in `clone_tombstones` filter deleted source rows (keyed on
//!     the synthetic surrogate produced by the Data Plane scan).
//!   - Copy-up entries in `clone_copyups` filter rows already written by the
//!     CoW write path.
//!   - `InsertIfAbsent` intent silently skips rows already written by a prior
//!     materializer pass.
//!
//! ## Profile coverage
//!
//! Plain columnar, Timeseries, and Spatial all share the same `MutationEngine`
//! storage layer. A single `ColumnarOp::MaterializeScan` handler (and this
//! Control Plane loop) serves all three profiles.

use std::collections::HashSet;

use nodedb_types::{DatabaseId, Lsn, RlsWriteCheck, Surrogate, TenantId};

use super::dispatch::dispatch_to_owner;
use super::reaper::{ReapParams, reap_materialized_collection};
use super::status::{check_bound_surrogates, checkpoint_progress, mark_materializing};
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::document::UpdateValue;
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, PhysicalPlan, TimeseriesOp,
};

/// Rows fetched per scan round-trip. Matches the KV / Document page size.
const SCAN_PAGE: usize = 4_096;

/// Materialize one columnar clone collection (Plain / Timeseries / Spatial).
pub(super) async fn materialize_columnar_collection(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let Some(ref origin) = coll.cloned_from else {
        return Ok(());
    };
    mark_materializing(state, coll).await?;

    let target_qualified = db_qualified(db_id, &coll.name);
    // Tombstones: synthetic source surrogates deleted from the clone before
    // materialization. The Data Plane scan encodes a unique u32 per row as the
    // surrogate (segment_id in upper 16 bits, row_idx in lower 16 bits).
    let tombstoned = catalog.list_clone_tombstones(&target_qualified)?;
    // Convert as_of_lsn to milliseconds for the source-side scan.
    let system_as_of_ms = crate::control::clone::lsn_resolve::source_as_of_ms(
        state,
        coll.bitemporal,
        origin.as_of_lsn,
    );

    ColumnarCopy {
        state,
        catalog,
        db_id,
        coll,
        tenant_id: TenantId::new(coll.tenant_id),
        source_db_id: origin.source_database,
        source_qualified: db_qualified(origin.source_database, &origin.source_collection),
        target_qualified: &target_qualified,
        tombstoned: &tombstoned,
        system_as_of_ms,
        as_of_lsn: origin.as_of_lsn,
    }
    .run()
    .await?;

    reap_materialized_collection(ReapParams {
        db_id,
        tenant_id: coll.tenant_id,
        name: &coll.name,
        state,
        catalog,
    })
    .await?;
    Ok(())
}

/// The row copy of one columnar clone collection.
struct ColumnarCopy<'a> {
    state: &'a SharedState,
    catalog: &'a SystemCatalog,
    db_id: DatabaseId,
    coll: &'a StoredCollection,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: String,
    target_qualified: &'a str,
    tombstoned: &'a HashSet<u32>,
    system_as_of_ms: Option<i64>,
    as_of_lsn: Lsn,
}

impl ColumnarCopy<'_> {
    /// Copy every source page, with a progress checkpoint after each one.
    async fn run(&self) -> crate::Result<()> {
        let mut cursor: Vec<u8> = Vec::new();
        let mut copied: u64 = 0;
        let mut total_seen: u64 = 0;
        loop {
            let (entries, next_cursor) = scan_source_page(
                self.state,
                self.tenant_id,
                self.source_db_id,
                &self.source_qualified,
                &cursor,
                self.system_as_of_ms,
            )
            .await?;
            total_seen += entries.len() as u64;
            let pending = self.pending_rows(entries)?;
            copied += self.copy_rows(pending).await?;
            checkpoint_progress(self.state, self.coll, self.as_of_lsn, copied, total_seen).await?;
            if next_cursor.is_empty() {
                break;
            }
            cursor = next_cursor;
        }
        tracing::info!(
            db_id = self.db_id.as_u64(),
            collection = %self.coll.name,
            copied,
            skipped_tombstoned = self.tombstoned.len(),
            source_total = total_seen,
            "columnar materialize: source rows copied to target",
        );
        Ok(())
    }

    /// The rows of one page still to copy. A row deleted from the clone (CoW
    /// tombstone) or already copied up by the CoW write path is skipped.
    fn pending_rows(&self, entries: Vec<(u32, Vec<u8>)>) -> crate::Result<Vec<(u32, Vec<u8>)>> {
        let mut pending = Vec::with_capacity(entries.len());
        for (source_surrogate, value_bytes) in entries {
            if self.tombstoned.contains(&source_surrogate)
                || self
                    .catalog
                    .get_clone_copyup(self.target_qualified, source_surrogate)?
                    .is_some()
            {
                continue;
            }
            pending.push((source_surrogate, value_bytes));
        }
        Ok(pending)
    }

    /// Bind target surrogates for `pending` and insert each row into the
    /// target. Returns the rows copied.
    async fn copy_rows(&self, pending: Vec<(u32, Vec<u8>)>) -> crate::Result<u64> {
        // Target surrogates for the page in one batch at the target
        // collection's home. Each keys on the source surrogate's bytes, so the
        // allocation is deterministic across retries: the source surrogate
        // encodes (segment_id, row_idx) and is unique per source row.
        let keys: Vec<[u8; 4]> = pending.iter().map(|(s, _)| s.to_be_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let target_surrogates =
            crate::control::server::surrogate_exchange::assign_surrogates_routed(
                self.state,
                nodedb_types::CollectionKey::from_bare(self.db_id, &self.coll.name),
                self.tenant_id,
                &key_refs,
                crate::types::TraceId::ZERO,
            )
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "clone_materializer".into(),
                detail: format!(
                    "surrogate assign failed for a page of '{}': {e}",
                    self.target_qualified
                ),
            })?;
        check_bound_surrogates(
            self.target_qualified,
            target_surrogates.len(),
            pending.len(),
        )?;
        let mut copied = 0;
        for ((_, value_bytes), target_surrogate) in pending.into_iter().zip(target_surrogates) {
            let plan = self.insert_plan(value_bytes, target_surrogate);
            dispatch_to_owner(
                self.state,
                self.tenant_id,
                self.db_id,
                self.target_qualified,
                plan,
            )
            .await?;
            copied += 1;
        }
        Ok(copied)
    }

    /// The plan that inserts one row into the target unless it is present.
    ///
    /// A timeseries target uses `TimeseriesOp::Ingest` (msgpack array format)
    /// so rows land in `columnar_memtables`, which the timeseries scan path
    /// reads. Plain / Spatial use `ColumnarOp::Insert` into
    /// `columnar_engines`.
    fn insert_plan(&self, value_bytes: Vec<u8>, target_surrogate: Surrogate) -> PhysicalPlan {
        // Wrap the value_bytes (msgpack Value::Object) in a msgpack array
        // so the Insert / Ingest handler can decode it as a row sequence.
        let payload = wrap_in_array(value_bytes);
        let collection = nodedb_types::QualifiedCollection::new(self.db_id, &self.coll.name);
        if self.coll.collection_type.is_timeseries() {
            // Format "msgpack" = msgpack array-of-maps (same layout as SQL
            // VALUES ingest produced by the planner).
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection,
                payload,
                format: "msgpack".into(),
                wal_lsn: None,
                surrogates: vec![target_surrogate],
                provenance: None,
                // `materialize_one` (walker.rs) already refused this
                // materialization if either side carried an RLS policy,
                // so no policy applies to source or target here — this
                // reflects a check that ran, not an assumption.
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            })
        } else {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection,
                payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::InsertIfAbsent,
                on_conflict_updates: Vec::<(String, UpdateValue)>::new(),
                surrogates: vec![target_surrogate],
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
                system_from: None,
                // `materialize_one` (walker.rs) already refused this
                // materialization if either side carried an RLS policy,
                // so no policy applies to source or target here — this
                // reflects a check that ran, not an assumption.
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
                // Internal row copy — nothing is projected back and no
                // caller identity's reads are being gated.
                returning: None,
                rls_filters: Vec::new(),
            })
        }
    }
}

/// `(source_surrogate_u32, value_bytes)` returned by one scan page.
type ScanPage = (Vec<(u32, Vec<u8>)>, Vec<u8>);

/// Run one source-side `MaterializeScan` round-trip.
async fn scan_source_page(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
    system_as_of_ms: Option<i64>,
) -> crate::Result<ScanPage> {
    let plan = PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        system_as_of_ms,
    });
    let payload = dispatch_to_owner(state, tenant_id, source_db_id, source_qualified, plan).await?;
    parse_materialize_scan_payload(&payload)
}

/// Parse the msgpack payload emitted by `execute_columnar_materialize_scan`:
///   `[next_cursor: bin, entries: [[surrogate: u32, value_bytes: bin], ...]]`.
fn parse_materialize_scan_payload(payload: &[u8]) -> crate::Result<ScanPage> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let bad = || crate::Error::Serialization {
        format: "msgpack".into(),
        detail: "columnar materialize-scan response: malformed payload".into(),
    };

    let (outer_len, mut off) = msgpack_scan::array_header(payload, 0).ok_or_else(bad)?;
    if outer_len != 2 {
        return Err(bad());
    }

    let next_cursor = msgpack_scan::read_bin_advance(payload, &mut off)
        .ok_or_else(bad)?
        .to_vec();

    let (entry_count, mut entry_off) = msgpack_scan::array_header(payload, off).ok_or_else(bad)?;

    // `entry_count` is attacker-controlled msgpack metadata. Grow only after
    // each complete entry has been structurally consumed from `payload`.
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        let (pair_len, mut pair_off) =
            msgpack_scan::array_header(payload, entry_off).ok_or_else(bad)?;
        if pair_len != 2 {
            return Err(bad());
        }
        let surrogate = msgpack_scan::read_u32_advance(payload, &mut pair_off).ok_or_else(bad)?;
        let value = msgpack_scan::read_bin_advance(payload, &mut pair_off)
            .ok_or_else(bad)?
            .to_vec();
        entries.push((surrogate, value));
        entry_off = pair_off;
    }

    Ok((entries, next_cursor))
}

/// Wrap a single msgpack Value::Object blob in a msgpack fixarray of length 1
/// so the columnar insert handler can decode it as `Vec<Value>`.
fn wrap_in_array(value_bytes: Vec<u8>) -> Vec<u8> {
    // fixarray header for 1 element: 0x91
    let mut out = Vec::with_capacity(1 + value_bytes.len());
    out.push(0x91);
    out.extend_from_slice(&value_bytes);
    out
}
