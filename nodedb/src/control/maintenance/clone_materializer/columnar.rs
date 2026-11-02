// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine (Plain / Timeseries / Spatial) source-to-target row copy.
//!
//! Drives the source `ColumnarOp::MaterializeScan` cursor to completion, and
//! copies each non-tombstoned, not-yet-copied row into the target under a
//! bound surrogate. Calls the reaper at the end to flip the collection status
//! to `Materialized` and clear `cloned_from`.
//!
//! Every copy keeps its source version's system-time ordinal: the clone holds
//! the same logical version the source holds, so a snapshot that read the row
//! through the source reads the same row from the clone.
//!
//! - A plain or spatial target takes each page as one `ColumnarOp::Insert`
//!   of intent `Restore`. Each row's `restored_versions` entry carries its
//!   source ordinal and the clone-copy marker: the row installs only where
//!   its key holds no version.
//! - A timeseries target takes each page as one `TimeseriesOp::Ingest` of
//!   format `msgpack-clone-copy`. Each row carries its source ordinal in
//!   `_ts_system`, and the install keeps it.
//!
//! ## Idempotency / restart-safety
//!
//! A clone of a columnar-family collection refuses every client write until
//! it is materialized, so the materializer is the target's only writer.
//!   - Tombstones in `clone_tombstones` filter deleted source rows (keyed on
//!     the synthetic surrogate produced by the Data Plane scan).
//!   - Copy-up entries in `clone_copyups` filter rows already written by the
//!     CoW write path.
//!   - A plain or spatial row already copied keeps its key bound, so a
//!     re-run installs nothing for it.
//!   - A timeseries row has no key to probe. A run truncates the target
//!     before its first page, so a re-run copies every row once.
//!
//! ## Profile coverage
//!
//! Plain columnar, Timeseries, and Spatial all share the same `MutationEngine`
//! storage layer. A single `ColumnarOp::MaterializeScan` handler (and this
//! Control Plane loop) serves all three profiles.

use std::collections::{HashMap, HashSet};

use nodedb_types::columnar::RestoredVersion;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::value::Value;
use nodedb_types::{DatabaseId, Lsn, RlsWriteCheck, Surrogate, TenantId};

use super::dispatch::dispatch_to_owner;
use super::reaper::{ReapParams, reap_materialized_collection};
use super::status::{check_bound_surrogates, checkpoint_progress, mark_materializing};
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::engine::timeseries::resolved_ingest::CLONE_COPY_INGEST_FORMAT;
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
        if self.coll.collection_type.is_timeseries() {
            self.truncate_target().await?;
        }
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

    /// Clear the timeseries target, so a re-run after a crash copies every
    /// source row once. The target holds only rows an earlier run copied.
    async fn truncate_target(&self) -> crate::Result<()> {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(self.db_id, &self.coll.name),
            restart_identity: false,
        });
        dispatch_to_owner(
            self.state,
            self.tenant_id,
            self.db_id,
            self.target_qualified,
            plan,
        )
        .await?;
        Ok(())
    }

    /// The rows of one page still to copy. A row deleted from the clone (CoW
    /// tombstone) or already copied up by the CoW write path is skipped.
    fn pending_rows(&self, entries: Vec<ScannedRow>) -> crate::Result<Vec<ScannedRow>> {
        let mut pending = Vec::with_capacity(entries.len());
        for row in entries {
            if self.tombstoned.contains(&row.source_surrogate)
                || self
                    .catalog
                    .is_clone_tombstoned(self.target_qualified, row.source_surrogate)?
                || self
                    .catalog
                    .get_clone_copyup(self.target_qualified, row.source_surrogate)?
                    .is_some()
            {
                continue;
            }
            pending.push(row);
        }
        Ok(pending)
    }

    /// Bind target surrogates for `pending` and copy the page into the
    /// target. Returns the rows copied.
    async fn copy_rows(&self, mut pending: Vec<ScannedRow>) -> crate::Result<u64> {
        if pending.is_empty() {
            return Ok(0);
        }
        // Target surrogates for the page in one batch at the target
        // collection's home. Each keys on the source surrogate's bytes, so the
        // allocation is deterministic across retries: the source surrogate
        // encodes (segment_id, row_idx) and is unique per source row.
        let keys: Vec<[u8; 4]> = pending
            .iter()
            .map(|row| row.source_surrogate.to_be_bytes())
            .collect();
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
        let mut rows: Vec<(ScannedRow, Surrogate)> =
            pending.drain(..).zip(target_surrogates).collect();
        // The versions install ascending by their ordinals.
        rows.sort_by_key(|(row, _)| row.sys_from);
        let copied = rows.len() as u64;
        let plan = if self.coll.collection_type.is_timeseries() {
            self.ingest_plan(rows)?
        } else {
            self.restore_plan(rows)?
        };
        dispatch_to_owner(
            self.state,
            self.tenant_id,
            self.db_id,
            self.target_qualified,
            plan,
        )
        .await?;
        Ok(copied)
    }

    /// One page as a plain or spatial `Restore` insert: each row at its
    /// source ordinal, open, and installed only where its key holds no
    /// version.
    fn restore_plan(&self, rows: Vec<(ScannedRow, Surrogate)>) -> crate::Result<PhysicalPlan> {
        let mut values = Vec::with_capacity(rows.len());
        let mut surrogates = Vec::with_capacity(rows.len());
        let mut versions = Vec::with_capacity(rows.len());
        for (row, target_surrogate) in rows {
            let sys_from = row
                .sys_from
                .ok_or_else(|| self.unstamped(row.source_surrogate))?;
            values.push(row.value_bytes);
            surrogates.push(target_surrogate);
            versions.push(RestoredVersion {
                sys_from,
                sys_to: None,
                clone_copy: true,
            });
        }
        Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(self.db_id, &self.coll.name),
            payload: msgpack_array(values),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Restore,
            on_conflict_updates: Vec::<(String, UpdateValue)>::new(),
            surrogates,
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            restored_versions: versions,
            // `materialize_one` (walker.rs) already refused this
            // materialization if either side carried an RLS policy, so no
            // policy applies to source or target here — this reflects a check
            // that ran, not an assumption.
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            // Internal row copy — nothing is projected back and no caller
            // identity's reads are being gated.
            returning: None,
            rls_filters: Vec::new(),
        }))
    }

    /// One page as a timeseries clone-copy ingest: each row carries its
    /// source ordinal in `_ts_system`, which the install keeps.
    fn ingest_plan(&self, rows: Vec<(ScannedRow, Surrogate)>) -> crate::Result<PhysicalPlan> {
        let mut values = Vec::with_capacity(rows.len());
        let mut surrogates = Vec::with_capacity(rows.len());
        for (row, target_surrogate) in rows {
            let sys_from = row
                .sys_from
                .ok_or_else(|| self.unstamped(row.source_surrogate))?;
            values.push(with_system_ordinal(&row.value_bytes, sys_from)?);
            surrogates.push(target_surrogate);
        }
        Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(self.db_id, &self.coll.name),
            payload: msgpack_array(values),
            format: CLONE_COPY_INGEST_FORMAT.into(),
            wal_lsn: None,
            surrogates,
            provenance: None,
            // `materialize_one` (walker.rs) already refused this
            // materialization if either side carried an RLS policy, so no
            // policy applies to source or target here — this reflects a check
            // that ran, not an assumption.
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }))
    }

    fn unstamped(&self, source_surrogate: u32) -> crate::Error {
        crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "source row {source_surrogate} of '{}' carries no version stamp",
                self.source_qualified
            ),
        }
    }
}

/// One row a source scan page returned.
struct ScannedRow {
    source_surrogate: u32,
    value_bytes: Vec<u8>,
    /// The system-time ordinal of the source version read. `None` for a
    /// timeseries row that stores no `_ts_system`.
    sys_from: Option<i64>,
}

/// `value_bytes`, a MessagePack row map, with `_ts_system` set to `ordinal`.
fn with_system_ordinal(value_bytes: &[u8], ordinal: i64) -> crate::Result<Vec<u8>> {
    let mut fields: HashMap<String, Value> = match nodedb_types::value_from_msgpack(value_bytes) {
        Ok(Value::Object(fields)) => fields,
        Ok(_) => {
            return Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: "timeseries clone copy: a scanned row is not a map".into(),
            });
        }
        Err(e) => {
            return Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("timeseries clone copy: a scanned row does not decode: {e}"),
            });
        }
    };
    fields.insert(TS_SYSTEM.to_string(), Value::Integer(ordinal));
    nodedb_types::value_to_msgpack(&Value::Object(fields)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("timeseries clone copy: encode a row: {e}"),
        }
    })
}

/// MessagePack rows, each already an encoded map, as one MessagePack array.
fn msgpack_array(rows: Vec<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.iter().map(Vec::len).sum::<usize>() + 5);
    nodedb_query::msgpack_scan::write_array_header(&mut out, rows.len());
    for row in rows {
        out.extend_from_slice(&row);
    }
    out
}

/// The rows of one scan page and the cursor of the next page.
type ScanPage = (Vec<ScannedRow>, Vec<u8>);

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
///   `[next_cursor: bin, entries: [[surrogate: u32, value_bytes: bin, sys_from: int | nil], ...]]`.
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
        let (entry_len, mut field_off) =
            msgpack_scan::array_header(payload, entry_off).ok_or_else(bad)?;
        if entry_len != 3 {
            return Err(bad());
        }
        let source_surrogate =
            msgpack_scan::read_u32_advance(payload, &mut field_off).ok_or_else(bad)?;
        let value_bytes = msgpack_scan::read_bin_advance(payload, &mut field_off)
            .ok_or_else(bad)?
            .to_vec();
        let sys_from = if msgpack_scan::read_null(payload, field_off) {
            None
        } else {
            Some(msgpack_scan::read_i64(payload, field_off).ok_or_else(bad)?)
        };
        field_off = msgpack_scan::skip_value(payload, field_off).ok_or_else(bad)?;
        entries.push(ScannedRow {
            source_surrogate,
            value_bytes,
            sys_from,
        });
        entry_off = field_off;
    }

    Ok((entries, next_cursor))
}
