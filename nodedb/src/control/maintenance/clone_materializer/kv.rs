// SPDX-License-Identifier: BUSL-1.1

//! KV engine source-to-target row copy.
//!
//! Drives the source `KvOp::MaterializeScan` cursor to completion, and for
//! each non-tombstoned key not yet present in target dispatches a `KvOp::Put`
//! against target with a fresh surrogate. Calls the reaper at the end to flip
//! status to `Materialized` and clear `cloned_from`.
//!
//! ## Idempotency / restart-safety
//!
//! Resume after a crash works because every step is observable via the
//! catalog: the target's per-key `Get` probe filters out keys already
//! copied, tombstones survive in `clone_kv_tombstones`, and the status flip
//! is the atomic Raft proposal at the end of the per-collection pass. The
//! walker re-runs this function on the next sweep until the reaper succeeds.

use nodedb_types::{CloneStatus, DatabaseId, Lsn, TenantId};

use crate::bridge::envelope::Status;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::dispatch::dispatch_local;
use super::reaper::{ReapParams, reap_materialized_collection};

/// Rows fetched per scan round-trip. Larger = fewer round-trips, more memory
/// per response. 4096 is a balance for typical clone sizes; very large
/// collections still complete because the cursor loop drains the source.
const SCAN_PAGE: usize = 4_096;

/// Materialize one KV clone collection.
pub(super) async fn materialize_kv_collection(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let Some(ref origin) = coll.cloned_from else {
        return Ok(());
    };

    let target_qualified = db_qualified(db_id, &coll.name);
    let source_qualified = db_qualified(origin.source_database, &origin.source_collection);
    let tenant_id = TenantId::new(coll.tenant_id);

    // Flip status to `Materializing` if still `Shadowed` so concurrent
    // readers see in-progress state and a crash here resumes from `progress_lsn = 0`.
    if matches!(coll.clone_status, CloneStatus::Shadowed) {
        let mut updated = coll.clone();
        updated.clone_status = CloneStatus::Materializing {
            progress_lsn: Lsn::new(0),
            bytes_done: 0,
            bytes_total: 0,
        };
        let outcome = propose_catalog_entry(
            state,
            &CatalogEntry::PutCollection(Box::new(updated.clone())),
        )?;
        if outcome.needs_local_apply() {
            catalog.put_collection(db_id, &updated)?;
        }
    }

    let tombstoned = catalog.list_kv_clone_tombstones(&target_qualified)?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut copied: u64 = 0;
    let mut total_seen: u64 = 0;

    loop {
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            origin.source_database,
            &source_qualified,
            &cursor,
        )
        .await?;

        for (key, value) in entries {
            total_seen += 1;
            let key_str = String::from_utf8_lossy(&key).into_owned();
            if tombstoned.contains(&key_str) {
                continue;
            }
            // Skip keys already in target (idempotent resume + already-copied-up rows).
            if probe_target_key(state, tenant_id, db_id, &target_qualified, &key).await? {
                continue;
            }

            let surrogate =
                state
                    .surrogate_assigner
                    .assign(db_id, tenant_id, &target_qualified, &key)?;
            let plan = PhysicalPlan::Kv(KvOp::Put {
                collection: nodedb_types::QualifiedCollection::new(db_id, &coll.name),
                key: key.clone(),
                value,
                ttl_ms: 0,
                surrogate,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            });
            let resp =
                dispatch_local(state, tenant_id, db_id, &target_qualified, plan, None).await?;
            if resp.status != Status::Ok {
                return Err(crate::Error::Storage {
                    engine: "clone_materializer".into(),
                    detail: format!(
                        "kv put on target '{target_qualified}' for key {key_str} returned status {:?}",
                        resp.status
                    ),
                });
            }
            copied += 1;
        }

        // Persist a chunk-level progress checkpoint so a crash after this
        // batch resumes without re-walking already-copied keys (the per-key
        // probe makes that safe regardless, but cheaper to skip the round-trip).
        checkpoint_progress(
            state,
            catalog,
            db_id,
            coll,
            origin.as_of_lsn,
            copied,
            total_seen,
        )?;

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }

    tracing::info!(
        db_id = db_id.as_u64(),
        collection = %coll.name,
        copied,
        skipped_tombstoned = tombstoned.len(),
        source_total = total_seen,
        "kv materialize: source rows copied to target",
    );

    reap_materialized_collection(ReapParams {
        target_collection_qualified: &target_qualified,
        db_id,
        tenant_id: coll.tenant_id,
        name: &coll.name,
        state,
        catalog,
    })?;

    Ok(())
}

/// Persist a `Materializing { progress_lsn, .. }` checkpoint between scan pages.
fn checkpoint_progress(
    state: &SharedState,
    catalog: &SystemCatalog,
    db_id: DatabaseId,
    coll: &StoredCollection,
    as_of_lsn: Lsn,
    copied: u64,
    total_seen: u64,
) -> crate::Result<()> {
    let mut updated = coll.clone();
    updated.clone_status = CloneStatus::Materializing {
        progress_lsn: as_of_lsn,
        bytes_done: copied,
        bytes_total: total_seen,
    };
    let outcome = propose_catalog_entry(
        state,
        &CatalogEntry::PutCollection(Box::new(updated.clone())),
    )?;
    if outcome.needs_local_apply() {
        catalog.put_collection(db_id, &updated)?;
    }
    Ok(())
}

/// Run one source-side `MaterializeScan` round-trip. Returns the entries in
/// this page (raw `(key, value)` byte pairs) plus the next-cursor; the
/// cursor is empty when the scan is complete.
async fn scan_source_page(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    cursor: &[u8],
) -> crate::Result<ScanPage> {
    let plan = PhysicalPlan::Kv(KvOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
    });
    let resp = dispatch_local(state, tenant_id, source_db_id, source_qualified, plan, None).await?;
    if resp.status != Status::Ok {
        return Err(crate::Error::Storage {
            engine: "clone_materializer".into(),
            detail: format!(
                "kv materialize-scan on source '{source_qualified}' returned status {:?}",
                resp.status
            ),
        });
    }
    parse_materialize_scan_payload(resp.payload.as_ref())
}

/// `(key, value)` pairs returned by one materialize-scan page.
type ScanPage = (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>);

/// Parse the msgpack payload emitted by `execute_kv_materialize_scan`:
///   `[next_cursor: bin, entries: [[key: bin, value: bin], ...]]`.
fn parse_materialize_scan_payload(payload: &[u8]) -> crate::Result<ScanPage> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let bad = || crate::Error::Serialization {
        format: "msgpack".into(),
        detail: "materialize-scan response: malformed payload".into(),
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
        let key = msgpack_scan::read_bin_advance(payload, &mut pair_off)
            .ok_or_else(bad)?
            .to_vec();
        let value = msgpack_scan::read_bin_advance(payload, &mut pair_off)
            .ok_or_else(bad)?
            .to_vec();
        entries.push((key, value));
        entry_off = pair_off;
    }

    Ok((entries, next_cursor))
}

/// Returns `true` if `key` is already present in target storage.
async fn probe_target_key(
    state: &SharedState,
    tenant_id: TenantId,
    db_id: DatabaseId,
    target_qualified: &str,
    key: &[u8],
) -> crate::Result<bool> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: nodedb_types::QualifiedCollection::from_stored(target_qualified.to_string()),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        // Materializer is the writer that COPIES rows from source into
        // target — it must see every source binding regardless of clone
        // ceiling, so reads against the target stay unbounded here.
        surrogate_ceiling: None,
    });
    let resp = dispatch_local(state, tenant_id, db_id, target_qualified, plan, None).await?;
    Ok(resp.status == Status::Ok && !resp.payload.is_empty())
}
