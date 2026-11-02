// SPDX-License-Identifier: BUSL-1.1

//! KV engine source-to-target row copy.
//!
//! Drives the source `KvOp::MaterializeScan` cursor to completion on the
//! source shard's owner, and copies each non-tombstoned key not yet present
//! in target into the target with a bound surrogate. Each copy installs at
//! its source version's own ordinal and expiry, so a snapshot that read the
//! key through the source reads the same row from the clone. Calls the reaper
//! at the end to flip status to `Materialized` and clear `cloned_from`.
//!
//! ## Idempotency / restart-safety
//!
//! Resume after a crash works because every step is observable via the
//! catalog: the target's per-key `Get` probe filters out keys already
//! copied, tombstones survive in `clone_kv_tombstones`, and the status flip
//! is the atomic Raft proposal at the end of the per-collection pass. The
//! walker re-runs this function on the next sweep until the reaper succeeds.

use nodedb_types::{DatabaseId, SystemTimeScope, TenantId};

use crate::control::clone::copy::{CopyTarget, KvCopy, commit_kv_copies};
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::KvVersionStamp;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::dispatch::dispatch_to_owner;
use super::reaper::{ReapParams, reap_materialized_collection};
use super::status::{check_bound_surrogates, checkpoint_progress, mark_materializing};

/// Rows fetched per scan round-trip. Larger = fewer round-trips, more memory
/// per response. 4096 is a balance for typical clone sizes; very large
/// collections still complete because the cursor loop drains the source.
const SCAN_PAGE: usize = 4_096;

/// One source key a scan returned, with its value and version stamps.
pub(crate) struct SourceKey {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub stamp: KvVersionStamp,
}

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
    mark_materializing(state, coll).await?;

    let tombstoned = catalog.list_kv_clone_tombstones(&target_qualified)?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut copied: u64 = 0;
    let mut total_seen: u64 = 0;

    loop {
        let (entries, next_cursor) = scan_source_page(
            state,
            SourceScan {
                tenant_id,
                source_db_id: origin.source_database,
                source_qualified: &source_qualified,
                cursor: &cursor,
                only_key: None,
            },
        )
        .await?;

        let mut pending: Vec<SourceKey> = Vec::with_capacity(entries.len());
        for entry in entries {
            total_seen += 1;
            let key_str = String::from_utf8_lossy(&entry.key).into_owned();
            // The catalog is read per key, so a tombstone recorded while the
            // copy runs counts.
            if tombstoned.contains(&key_str)
                || catalog.is_kv_clone_tombstoned(&target_qualified, &key_str)?
            {
                continue;
            }
            // Skip keys already in target (idempotent resume + already-copied-up rows).
            if probe_target_key(state, tenant_id, db_id, &target_qualified, &entry.key).await? {
                continue;
            }
            pending.push(entry);
        }

        // The page's surrogates in one batch at the target collection's home.
        let keys: Vec<&[u8]> = pending.iter().map(|entry| entry.key.as_slice()).collect();
        let surrogates = crate::control::server::surrogate_exchange::assign_surrogates_routed(
            state,
            nodedb_types::CollectionKey::from_bare(db_id, &coll.name),
            tenant_id,
            &keys,
            crate::types::TraceId::ZERO,
        )
        .await?;
        check_bound_surrogates(&target_qualified, surrogates.len(), pending.len())?;
        let copies: Vec<KvCopy> = pending
            .into_iter()
            .zip(surrogates)
            .map(|(entry, target_surrogate)| KvCopy {
                key: entry.key,
                target_surrogate,
                value: entry.value,
                stamp: entry.stamp,
            })
            .collect();
        copied += copies.len() as u64;
        commit_kv_copies(
            state,
            CopyTarget {
                tenant_id,
                database_id: db_id,
                collection: &coll.name,
            },
            copies,
        )
        .await?;

        // Persist a chunk-level progress checkpoint so a crash after this
        // batch resumes without re-walking already-copied keys (the per-key
        // probe makes that safe regardless, but cheaper to skip the round-trip).
        checkpoint_progress(state, coll, origin.as_of_lsn, copied, total_seen).await?;

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
        db_id,
        tenant_id: coll.tenant_id,
        name: &coll.name,
        state,
        catalog,
    })
    .await?;

    Ok(())
}

/// Read the one source key `key`, as the live version, with its stamps.
/// `None` when the source holds no live row for it. The read runs on the
/// source shard's owner.
pub(crate) async fn read_source_key(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &str,
    key: &[u8],
) -> crate::Result<Option<SourceKey>> {
    let (entries, _) = scan_source_page(
        state,
        SourceScan {
            tenant_id,
            source_db_id,
            source_qualified,
            cursor: &[],
            only_key: Some(key),
        },
    )
    .await?;
    Ok(entries.into_iter().find(|entry| entry.key == key))
}

/// One source-side scan request.
struct SourceScan<'a> {
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_qualified: &'a str,
    cursor: &'a [u8],
    /// The one key to read in place of a page.
    only_key: Option<&'a [u8]>,
}

/// Run one source-side `MaterializeScan` round-trip. Returns the entries in
/// this page plus the next-cursor; the cursor is empty when the scan is
/// complete.
async fn scan_source_page(state: &SharedState, scan: SourceScan<'_>) -> crate::Result<ScanPage> {
    let SourceScan {
        tenant_id,
        source_db_id,
        source_qualified,
        cursor,
        only_key,
    } = scan;
    let plan = PhysicalPlan::Kv(KvOp::MaterializeScan {
        collection: nodedb_types::QualifiedCollection::from_stored(source_qualified.to_string()),
        cursor: cursor.to_vec(),
        count: SCAN_PAGE,
        // The clone copies the source's live rows.
        system_time: SystemTimeScope::Current,
        only_key: only_key.map(<[u8]>::to_vec),
    });
    let payload = dispatch_to_owner(state, tenant_id, source_db_id, source_qualified, plan).await?;
    parse_materialize_scan_payload(&payload)
}

/// The entries of one materialize-scan page and the next cursor.
type ScanPage = (Vec<SourceKey>, Vec<u8>);

/// Parse the msgpack payload emitted by `execute_kv_materialize_scan`:
///   `[next_cursor: bin, entries: [[key: bin, value: bin, sys_from, expire_at_ms], ...]]`.
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
        let (entry_len, mut field_off) =
            msgpack_scan::array_header(payload, entry_off).ok_or_else(bad)?;
        if entry_len != 4 {
            return Err(bad());
        }
        let key = msgpack_scan::read_bin_advance(payload, &mut field_off)
            .ok_or_else(bad)?
            .to_vec();
        let value = msgpack_scan::read_bin_advance(payload, &mut field_off)
            .ok_or_else(bad)?
            .to_vec();
        let stamp = KvVersionStamp::read(payload, &mut field_off).ok_or_else(bad)?;
        entries.push(SourceKey { key, value, stamp });
        entry_off = field_off;
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
        system_time: nodedb_types::SystemTimeScope::Current,
    });
    Ok(
        !dispatch_to_owner(state, tenant_id, db_id, target_qualified, plan)
            .await?
            .is_empty(),
    )
}
