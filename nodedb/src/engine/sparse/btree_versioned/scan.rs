// SPDX-License-Identifier: BUSL-1.1

//! Collection-wide scans over the versioned document table.
//!
//! Both scans push the caller's filter predicate down to each version so the
//! `limit` counts matching rows rather than raw scanned rows, and both consult
//! the caller's `stop` signal once per scanned version so a long scan can be
//! ended from outside.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};

use super::doc::{DOCUMENTS_VERSIONED, VersionedRow};
use super::key::{coll_prefix, coll_prefix_end, format_sys_from};
use super::value::{TAG_TOMBSTONE, VersionedScanParams, decode_value};
use crate::engine::sparse::btree::{KeyedTable, SparseEngine, invalid_storage_key_err, redb_err};

impl SparseEngine {
    /// Scan every doc_id in a collection at the requested cutoff.
    /// Returns `(doc_id, body)` pairs for live versions only. O(N)
    /// collection-wide; callers add filter/limit on top.
    /// `predicate` receives each surviving version's doc-id and document body,
    /// and is evaluated before the version counts toward `limit`, so a
    /// selective filter never causes the scan to early-stop with fewer
    /// matching rows than exist. The doc-id carries a schemaless row's
    /// identity when its body has no `id` field. Pass `&|_, _| true` for an
    /// unfiltered scan.
    /// `stop` is consulted once per scanned version and ends the scan where it
    /// stands. The caller owns the signal and decides whether the short result
    /// is an answer or an error. Pass
    /// [`never_stop`](crate::engine::sparse::scan_stop::never_stop) when
    /// nothing can cut the scan short.
    pub fn versioned_scan_as_of(
        &self,
        params: VersionedScanParams<'_>,
        predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
        stop: &dyn Fn() -> bool,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        self.versioned_scan_as_of_after(params, None, predicate, stop)
    }

    /// [`Self::versioned_scan_as_of`] over the doc ids after `after` only, in
    /// doc-id order. A paginated reader passes the last doc id of its
    /// previous page, so each page starts where the last one stopped.
    pub fn versioned_scan_as_of_after(
        &self,
        params: VersionedScanParams<'_>,
        after: Option<&StorageKey>,
        predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
        stop: &dyn Fn() -> bool,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let VersionedScanParams {
            database_id,
            tenant,
            coll,
            sys_cutoff,
            valid_at_ms,
            limit,
        } = params;
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        // Every version key of `after` is `{lo}{after}\x00{sys_from}`, so
        // `\x01` after the id is the first key past all of them.
        let start = match after {
            Some(after) => format!("{lo}{after}\x01"),
            None => lo.clone(),
        };
        let cutoff_key = sys_cutoff.map(format_sys_from);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(start.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;

        // Group entries by doc_id; keep the newest-in-window per group.
        // The table is sorted by (doc_id, sys_from) ascending so we can
        // stream and flush whenever doc_id changes.
        let mut out = Vec::new();
        let mut current_id: Option<StorageKey> = None;
        let mut best_for_current: Option<(i64, Vec<u8>)> = None;

        for r in range {
            if stop() {
                break;
            }
            let (k, v) = r.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let Some((id_part, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Some(seg) = id_part.strip_prefix(lo.as_str()) else {
                continue;
            };
            let doc_id = StorageKey::parse(seg).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::DocumentsVersioned, coll, seg)
            })?;
            if let Some(ref c) = cutoff_key
                && suffix > c.as_str()
            {
                continue;
            }
            let Ok(sf) = suffix.parse::<i64>() else {
                continue;
            };
            if current_id != Some(doc_id) {
                if let Some(prev_id) = current_id {
                    flush_scan(prev_id, &best_for_current, valid_at_ms, predicate, &mut out)?;
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
                current_id = Some(doc_id);
                best_for_current = None;
            }
            let val = v.value().to_vec();
            // Keep the newest entry for this doc_id (largest sys_from).
            best_for_current = Some(match best_for_current.take() {
                Some((prev_sf, prev_v)) if prev_sf >= sf => (prev_sf, prev_v),
                _ => (sf, val),
            });
        }
        if let Some(prev_id) = current_id {
            flush_scan(prev_id, &best_for_current, valid_at_ms, predicate, &mut out)?;
        }
        Ok(out)
    }

    /// Audit-log scan: every system-time version of every doc in the
    /// collection, delete versions included, ordered ascending by system-time
    /// ordinal (ties broken by `doc_id`). A delete version
    /// ([`VersionedRow::deleted`]) repeats the body of the live version it
    /// closes under an empty valid interval (`i64::MIN` for both bounds); one
    /// whose closed version is gone is skipped. GDPR-erased versions are
    /// skipped. Unlike
    /// [`Self::versioned_scan_as_of`] this does **not** collapse to the
    /// newest-per-id — it yields the full history (`AS OF SYSTEM TIME NULL`).
    ///
    /// Returns [`VersionedRow`] values carrying `doc_id`, `system_from`, the
    /// row's stored valid-time interval, and `body`. The handler projects these
    /// into the output as the synthetic temporal columns.
    ///
    /// `predicate` receives each version's doc-id and document body (the
    /// closed version's body for a delete version), and is evaluated
    /// **before** the `limit` truncation, so a selective filter
    /// never causes the scan to return fewer rows than exist (the caller must
    /// push its scan filters in here rather than filtering the truncated
    /// result). The doc-id carries a schemaless row's identity when its body
    /// has no `id` field. Pass `&|_, _| true` for an unfiltered scan.
    ///
    /// `stop` is consulted once per scanned version and ends the scan where it
    /// stands. The caller owns the signal and decides whether the short result
    /// is an answer or an error. Pass
    /// [`never_stop`](crate::engine::sparse::scan_stop::never_stop) when
    /// nothing can cut the scan short.
    ///
    /// `params.sys_cutoff` is ignored: an audit-log scan yields every
    /// system-time version, so there is no cutoff to apply.
    pub fn versioned_scan_all(
        &self,
        params: VersionedScanParams<'_>,
        predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
        stop: &dyn Fn() -> bool,
    ) -> crate::Result<Vec<VersionedRow>> {
        let VersionedScanParams {
            database_id,
            tenant,
            coll,
            sys_cutoff: _,
            valid_at_ms,
            limit,
        } = params;
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;

        let mut all: Vec<VersionedRow> = Vec::new();
        // The newest live version seen so far, with its document: a
        // document's versions are contiguous and ascend by system time, so
        // a delete version closes the live version before it.
        let mut last_live: Option<(StorageKey, Vec<u8>)> = None;
        for r in range {
            if stop() {
                break;
            }
            let (k, v) = r.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let Some((id_part, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Some(seg) = id_part.strip_prefix(lo.as_str()) else {
                continue;
            };
            let doc_id = StorageKey::parse(seg).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::DocumentsVersioned, coll, seg)
            })?;
            let Ok(sf) = suffix.parse::<i64>() else {
                continue;
            };
            let bytes = v.value().to_vec();
            let decoded = decode_value(&bytes)?;
            let deleted = decoded.tag == TAG_TOMBSTONE;
            let (valid_from_ms, valid_until_ms, body) = if decoded.is_live() {
                last_live = Some((doc_id, decoded.body.to_vec()));
                (
                    decoded.valid_from_ms,
                    decoded.valid_until_ms,
                    decoded.body.to_vec(),
                )
            } else if deleted {
                // A delete version repeats the body of the live version it
                // closes, under an empty valid interval. With no such version
                // left (a retention purge or an erasure removed it) there is
                // no row to show.
                match last_live.take() {
                    Some((live_id, live_body)) if live_id == doc_id => {
                        (i64::MIN, i64::MIN, live_body)
                    }
                    _ => continue,
                }
            } else {
                // A GDPR-erased version is not a row, and its content is
                // never repeated.
                last_live = None;
                continue;
            };
            if let Some(vt) = valid_at_ms
                && (vt < valid_from_ms || vt >= valid_until_ms)
            {
                continue;
            }
            // Push the caller's scan filters down here so the `limit` truncation
            // below counts only matching versions, never raw scanned rows.
            if !predicate(&doc_id, &body) {
                continue;
            }
            all.push(VersionedRow {
                doc_id,
                system_from: sf,
                valid_from_ms,
                valid_until_ms,
                body,
                deleted,
            });
        }
        // Global ascending order by system time; deterministic on ties.
        all.sort_by(|a, b| {
            a.system_from
                .cmp(&b.system_from)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        all.truncate(limit);
        Ok(all)
    }
}

/// Emit the newest-per-doc-id entry into `out` if it's live and passes
/// the valid-time predicate.
fn flush_scan(
    id: StorageKey,
    pick: &Option<(i64, Vec<u8>)>,
    valid_at_ms: Option<i64>,
    predicate: &dyn Fn(&StorageKey, &[u8]) -> bool,
    out: &mut Vec<(StorageKey, Vec<u8>)>,
) -> crate::Result<()> {
    let Some((_sf, v)) = pick else { return Ok(()) };
    let decoded = decode_value(v)?;
    if !decoded.is_live() {
        return Ok(());
    }
    if let Some(vt) = valid_at_ms
        && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
    {
        return Ok(());
    }
    // Caller's scan filters are pushed down here so they are applied before the
    // row counts toward the scan's `limit`.
    if !predicate(&id, decoded.body) {
        return Ok(());
    }
    out.push((id, decoded.body.to_vec()));
    Ok(())
}
