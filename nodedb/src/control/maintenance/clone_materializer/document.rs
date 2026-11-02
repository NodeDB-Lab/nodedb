// SPDX-License-Identifier: BUSL-1.1

//! Document engine source-to-target row copy.
//!
//! Drives the source `MaterializeScan` cursor to completion; for each
//! non-tombstoned, not-yet-copied surrogate, dispatches a fresh-surrogate
//! `PointInsert { if_absent: true }` against target. Calls the reaper at the
//! end to flip status to `Materialized` and clear `cloned_from`.
//!
//! Crash-restart-safe: tombstones filter deleted source rows, `clone_copyups`
//! filters CoW-copied rows, and `if_absent` skips already-written rows — the
//! per-page checkpoint is best-effort but the per-key probes cover it.

use nodedb_types::{DatabaseId, SystemTimeScope, TenantId};

use crate::types::{DocVersionStamp, TxnId};

use super::dispatch::{dispatch_local, dispatch_to_owner};
use super::document_copy::RowCopy;
use super::reaper::{ReapParams, reap_materialized_collection};
use super::status::{checkpoint_progress, mark_materializing};
use crate::bridge::envelope::Status;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// Rows fetched per scan round-trip. Matches the KV page size.
const SCAN_PAGE: usize = 4_096;

/// Materialize one Document clone collection.
pub(super) async fn materialize_document_collection(
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
    // Tombstones: source surrogates deleted from the clone before materialization.
    let tombstoned = catalog.list_clone_tombstones(&target_qualified)?;
    // Convert as_of_lsn to milliseconds for the source-side scan.
    let system_as_of_ms = crate::control::clone::lsn_resolve::source_as_of_ms(
        state,
        coll.bitemporal,
        origin.as_of_lsn,
    );

    if coll.hash_chain {
        let copied = super::chained::materialize_chained_collection(
            state,
            catalog,
            db_id,
            coll,
            &tombstoned,
            system_as_of_ms,
        )
        .await?;
        checkpoint_progress(state, coll, origin.as_of_lsn, copied, copied).await?;
    } else {
        RowCopy {
            state,
            catalog,
            db_id,
            coll,
            tenant_id: TenantId::new(coll.tenant_id),
            source_db_id: origin.source_database,
            source_collection: &origin.source_collection,
            source_qualified: db_qualified(origin.source_database, &origin.source_collection),
            target_qualified: &target_qualified,
            tombstoned: &tombstoned,
            system_as_of_ms,
            as_of_lsn: origin.as_of_lsn,
        }
        .run()
        .await?;
    }

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

/// One row a source scan page returned.
pub(crate) struct SourceRow {
    /// The row's storage key, its surrogate in hex.
    pub doc_id_hex: String,
    pub source_surrogate: u32,
    pub body: Vec<u8>,
    /// The stamps of the source version read. `None` for a row a
    /// transaction overlay supplied.
    pub stamp: Option<DocVersionStamp>,
}

/// The rows of one scan page and the cursor of the next page.
pub(crate) type ScanPage = (Vec<SourceRow>, Vec<u8>);

/// Run one source-side `MaterializeScan` round-trip.
///
/// `txn_id`, when set (COMMIT-time MERGE / `UPDATE ... FROM` expanders),
/// makes the source handler fold the transaction's staging overlay in.
/// Autocommit callers pass `None` (base-only).
pub(crate) async fn scan_source_page(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
    system_time: SystemTimeScope,
    txn_id: Option<TxnId>,
) -> crate::Result<ScanPage> {
    let page = SourcePage {
        tenant_id,
        source_db_id,
        source_qualified,
        cursor,
        system_time,
        raw_bodies: false,
    };
    scan_page(state, page, txn_id).await
}

/// Read the one source row `surrogate` names, as the version a read at
/// `system_time` sees, with its stamps. `None` when that read sees no row.
/// The read runs on the source shard's owner.
pub(crate) async fn read_source_row(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    surrogate: nodedb_types::Surrogate,
    system_time: SystemTimeScope,
) -> crate::Result<Option<SourceRow>> {
    let plan = PhysicalPlan::Document(DocumentOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: Vec::new(),
        count: 1,
        system_time,
        raw_bodies: true,
        only_row: Some(surrogate),
    });
    let payload = dispatch_to_owner(state, tenant_id, source_db_id, source_qualified, plan).await?;
    let (rows, _) = parse_materialize_scan_payload(&payload)?;
    Ok(rows
        .into_iter()
        .find(|row| row.source_surrogate == surrogate.as_u32()))
}

/// The scope a clone copy reads its source at: `AsOf` the clone's source
/// time, or `Current` when the clone carries none.
pub(crate) fn clone_scope(system_as_of_ms: Option<i64>) -> SystemTimeScope {
    system_as_of_ms.map_or(SystemTimeScope::Current, SystemTimeScope::AsOf)
}

/// One source-side scan page request.
pub(super) struct SourcePage<'a> {
    pub tenant_id: TenantId,
    pub source_db_id: DatabaseId,
    pub source_qualified: &'a str,
    pub cursor: &'a [u8],
    /// `Current`, or the point-in-time cut every page reads at.
    pub system_time: SystemTimeScope,
    /// Bodies as stored, with no `id` added.
    pub raw_bodies: bool,
}

/// Run one `MaterializeScan` round-trip for `page`.
pub(super) async fn scan_page(
    state: &SharedState,
    page: SourcePage<'_>,
    txn_id: Option<TxnId>,
) -> crate::Result<ScanPage> {
    let SourcePage {
        tenant_id,
        source_db_id,
        source_qualified,
        cursor,
        system_time,
        raw_bodies,
    } = page;
    let plan = PhysicalPlan::Document(DocumentOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        system_time,
        raw_bodies,
        only_row: None,
    });
    // A transaction's staging overlay lives on the source shard's leader, so
    // a staged read runs there (`dispatch_local` routes it). Every other read
    // goes to the source shard's owner.
    if txn_id.is_none() {
        let payload =
            dispatch_to_owner(state, tenant_id, source_db_id, source_qualified, plan).await?;
        return parse_materialize_scan_payload(&payload);
    }
    let resp = dispatch_local(
        state,
        tenant_id,
        source_db_id,
        source_qualified,
        plan,
        txn_id,
    )
    .await?;
    if resp.status != Status::Ok {
        return Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "document materialize-scan on source '{source_qualified}' returned status {:?}",
                resp.status
            ),
        });
    }
    parse_materialize_scan_payload(resp.payload.as_ref())
}

/// Parse the msgpack payload emitted by `execute_document_materialize_scan`:
///   `[next_cursor: bin, entries: [[doc_id: str, surrogate: u32, value_bytes: bin, stamp], ...]]`.
fn parse_materialize_scan_payload(payload: &[u8]) -> crate::Result<ScanPage> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let bad = || crate::Error::Serialization {
        format: "msgpack".into(),
        detail: "document materialize-scan response: malformed payload".into(),
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
        if entry_len != 4 {
            return Err(bad());
        }
        let doc_id_hex = msgpack_scan::read_str_advance(payload, &mut field_off)
            .ok_or_else(bad)?
            .to_string();
        let source_surrogate =
            msgpack_scan::read_u32_advance(payload, &mut field_off).ok_or_else(bad)?;
        let body = msgpack_scan::read_bin_advance(payload, &mut field_off)
            .ok_or_else(bad)?
            .to_vec();
        let stamp = DocVersionStamp::read(payload, &mut field_off).ok_or_else(bad)?;
        entries.push(SourceRow {
            doc_id_hex,
            source_surrogate,
            body,
            stamp,
        });
        entry_off = field_off;
    }

    Ok((entries, next_cursor))
}
