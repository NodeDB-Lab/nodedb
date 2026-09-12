// SPDX-License-Identifier: BUSL-1.1

//! Document-level operations on the versioned document table.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::key::{doc_prefix, doc_prefix_end, versioned_doc_key};
use super::value::{
    TAG_GDPR_ERASED, TAG_LIVE, TAG_TOMBSTONE, VersionedPut, decode_value, encode_value,
};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// One row of an audit-log (`AS OF SYSTEM TIME NULL`) scan: a single
/// system-time version of a document plus the temporal coordinates read from
/// the row's real stored envelope. `valid_from_ms` / `valid_until_ms` carry the
/// raw envelope sentinels (`i64::MIN` / `i64::MAX` when unbounded) so the
/// handler can surface them uniformly across engines.
pub struct VersionedRow {
    pub doc_id: StorageKey,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: Vec<u8>,
}

/// Versioned document table. Distinct from `super::super::btree::DOCUMENTS`
/// so bitemporal and current-only collections coexist. Keys carry the leading
/// `{database_id}:` component.
pub(crate) const DOCUMENTS_VERSIONED: TableDefinition<&str, &[u8]> =
    TableDefinition::new("documents_versioned");

impl SparseEngine {
    /// Bootstrap: ensure the versioned document table exists. Called by
    /// `open()` alongside [`super::index::ensure_indexes_versioned_table`].
    pub(in crate::engine::sparse) fn ensure_documents_versioned_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let _ = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open documents_versioned", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append one version. Always creates a new key; never overwrites an
    /// earlier version at the same `sys_from_ms`.
    pub fn versioned_put(&self, p: VersionedPut<'_>) -> crate::Result<()> {
        let key = versioned_doc_key(p.database_id, p.tenant, p.coll, p.doc_id, p.sys_from_ms);
        let val = encode_value(TAG_LIVE, p.valid_from_ms, p.valid_until_ms, p.body);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            t.insert(key.as_str(), val.as_slice())
                .map_err(|e| redb_err("insert", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_put` inside a caller-owned write transaction. Used by
    /// composite write paths (PointPut / UPSERT / UPDATE on bitemporal
    /// collections) so the document + its versioned index entries + the
    /// current-state side-effects (stats, text/vector/spatial indexes,
    /// document cache) commit atomically.
    pub fn versioned_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        p: VersionedPut<'_>,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(p.database_id, p.tenant, p.coll, p.doc_id, p.sys_from_ms);
        let val = encode_value(TAG_LIVE, p.valid_from_ms, p.valid_until_ms, p.body);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), val.as_slice())
            .map_err(|e| redb_err("insert", e))?;
        Ok(())
    }

    /// Append a tombstone version.
    pub fn versioned_tombstone(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms);
        let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            t.insert(key.as_str(), val.as_slice())
                .map_err(|e| redb_err("insert tombstone", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_tombstone` inside a caller-owned write transaction.
    pub fn versioned_tombstone_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms);
        let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), val.as_slice())
            .map_err(|e| redb_err("insert tombstone", e))?;
        Ok(())
    }

    /// Physically remove one version's redb entry inside a caller-owned
    /// write transaction. Unlike [`Self::versioned_tombstone_in_txn`] (which
    /// appends a tombstone marker version), this deletes the entry at
    /// `sys_from_ms` outright. Used by the transaction-rollback path to
    /// undo a `versioned_put_in_txn` that must not survive an aborted
    /// transaction. Removing a non-existent key is a no-op.
    pub fn versioned_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.remove(key.as_str()).map_err(|e| redb_err("remove", e))?;
        Ok(())
    }

    /// Check whether the current-state version of `doc_id` is live
    /// (not tombstoned, not GDPR-erased, and at least one version
    /// exists) inside a caller-owned write transaction. Used by
    /// PointInsert on bitemporal collections to enforce primary-key
    /// uniqueness linearizably with the subsequent `versioned_put`.
    pub fn versioned_exists_current_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<bool> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;
        let mut last: Option<Vec<u8>> = None;
        for r in range {
            let (_k, v) = r.map_err(|e| redb_err("entry", e))?;
            last = Some(v.value().to_vec());
        }
        match last {
            Some(bytes) => Ok(decode_value(&bytes)?.is_live()),
            None => Ok(false),
        }
    }

    /// GDPR erasure: replace the body of every existing version for a
    /// doc_id with an empty body tagged `0xFE`. Preserves history
    /// structure (so AS-OF queries still see the doc existed) but removes
    /// personal data.
    pub fn versioned_gdpr_erase(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<usize> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        let mut replaced = 0;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            let keys: Vec<String> = t
                .range(lo.as_str()..hi.as_str())
                .map_err(|e| redb_err("range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for k in keys {
                let erased = encode_value(TAG_GDPR_ERASED, 0, 0, &[]);
                t.insert(k.as_str(), erased.as_slice())
                    .map_err(|e| redb_err("erase insert", e))?;
                replaced += 1;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(replaced)
    }

    /// Ceiling read: most recent version at or before `sys_cutoff_ms`.
    /// Returns `Ok(None)` when nothing is present, or the newest entry is
    /// a tombstone / GDPR-erased (both hide the row from normal reads).
    pub fn versioned_get_as_of(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_cutoff_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = match sys_cutoff_ms {
            Some(ms) => versioned_doc_key(database_id, tenant, coll, doc_id, ms),
            None => doc_prefix_end(database_id, tenant, coll, doc_id),
        };
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = match sys_cutoff_ms {
            Some(_) => t
                .range(lo.as_str()..=hi.as_str())
                .map_err(|e| redb_err("range", e))?,
            None => t
                .range(lo.as_str()..hi.as_str())
                .map_err(|e| redb_err("range", e))?,
        };
        let mut entries: Vec<Vec<u8>> = Vec::new();
        for r in range {
            let (_k, v) = r.map_err(|e| redb_err("entry", e))?;
            entries.push(v.value().to_vec());
        }
        // Iterate in reverse to pick the newest version ≤ cutoff whose
        // valid-time predicate holds.
        for v in entries.into_iter().rev() {
            let decoded = decode_value(&v)?;
            if !decoded.is_live() {
                // Ceiling hit a tombstone / erasure as the newest entry —
                // the row is absent at this cutoff, even if live versions
                // exist further back.
                return Ok(None);
            }
            if let Some(vt) = valid_at_ms
                && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
            {
                continue;
            }
            return Ok(Some(decoded.body.to_vec()));
        }
        Ok(None)
    }

    /// Current-state read = `versioned_get_as_of(None, None)`.
    pub fn versioned_get_current(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.versioned_get_as_of(database_id, tenant, coll, doc_id, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::super::value::VersionedScanParams;
    use super::*;

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = SparseEngine::open(&dir.path().join("v.redb")).unwrap();
        (engine, dir)
    }

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(nodedb_types::Surrogate::new(surrogate))
    }

    fn put(e: &SparseEngine, coll: &str, id: u32, sys_from: i64, body: &[u8]) {
        e.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll,
            doc_id: &key(id),
            sys_from_ms: sys_from,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
            body,
        })
        .unwrap();
    }

    fn put_valid(
        e: &SparseEngine,
        coll: &str,
        id: u32,
        sys_from: i64,
        valid_from: i64,
        valid_until: i64,
        body: &[u8],
    ) {
        e.versioned_put(VersionedPut {
            database_id: 1,
            tenant: 1,
            coll,
            doc_id: &key(id),
            sys_from_ms: sys_from,
            valid_from_ms: valid_from,
            valid_until_ms: valid_until,
            body,
        })
        .unwrap();
    }

    #[test]
    fn put_and_read_current() {
        let (e, _d) = open_temp();
        put(&e, "users", 1, 100, b"v1");
        let got = e.versioned_get_current(1, 1, "users", &key(1)).unwrap();
        assert_eq!(got.as_deref(), Some(b"v1" as &[u8]));
    }

    #[test]
    fn ceiling_picks_newest_le_cutoff() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"a");
        put(&e, "c", 1, 200, b"b");
        put(&e, "c", 1, 300, b"c");
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(150), None)
                .unwrap()
                .as_deref(),
            Some(b"a" as &[u8])
        );
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(250), None)
                .unwrap()
                .as_deref(),
            Some(b"b" as &[u8])
        );
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(400), None)
                .unwrap()
                .as_deref(),
            Some(b"c" as &[u8])
        );
    }

    #[test]
    fn ceiling_before_first_version_is_none() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 200, b"x");
        assert!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(100), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn tombstone_hides_row_at_and_after_cutoff() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"x");
        e.versioned_tombstone(1, 1, "c", &key(1), 200).unwrap();
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(150), None)
                .unwrap()
                .as_deref(),
            Some(b"x" as &[u8])
        );
        assert!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(250), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn valid_time_predicate_skips_out_of_window_versions() {
        let (e, _d) = open_temp();
        put_valid(&e, "c", 1, 10, 0, 100, b"v1");
        put_valid(&e, "c", 1, 20, 200, 300, b"v2");
        // valid-time hole at 150: neither version applies.
        assert!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(10_000), Some(150))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(10_000), Some(50))
                .unwrap()
                .as_deref(),
            Some(b"v1" as &[u8])
        );
        assert_eq!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(10_000), Some(250))
                .unwrap()
                .as_deref(),
            Some(b"v2" as &[u8])
        );
    }

    #[test]
    fn gdpr_erase_preserves_history_structure_but_hides_body() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"pii");
        put(&e, "c", 1, 200, b"more-pii");
        let n = e.versioned_gdpr_erase(1, 1, "c", &key(1)).unwrap();
        assert_eq!(n, 2);
        assert!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(150), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn scan_returns_latest_per_doc_id() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"a1");
        put(&e, "c", 1, 200, b"a2");
        put(&e, "c", 2, 150, b"b1");
        let all = e
            .versioned_scan_as_of(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        let find = |id: StorageKey| {
            all.iter()
                .find(|(k, _)| *k == id)
                .map(|(_, v)| v.as_slice())
        };
        assert_eq!(find(key(1)), Some(b"a2" as &[u8]));
        assert_eq!(find(key(2)), Some(b"b1" as &[u8]));
    }

    #[test]
    fn scan_all_returns_every_version_in_system_time_order() {
        let (e, _d) = open_temp();
        // One document updated three times under different system times.
        put(&e, "c", 1, 100, b"a1");
        put(&e, "c", 1, 200, b"a2");
        put(&e, "c", 1, 300, b"a3");
        // A second document interleaved by system time.
        put(&e, "c", 2, 150, b"b1");

        let all = e
            .versioned_scan_all(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        // Every version is present (no newest-per-id collapse).
        assert_eq!(all.len(), 4);
        // Ascending by system time globally.
        let times: Vec<i64> = all.iter().map(|r| r.system_from_ms).collect();
        assert_eq!(times, vec![100, 150, 200, 300]);
        // System-time and body line up per version.
        let row_of = |r: &VersionedRow| (r.doc_id, r.system_from_ms, r.body.clone());
        assert_eq!(row_of(&all[0]), (key(1), 100, b"a1".to_vec()));
        assert_eq!(row_of(&all[1]), (key(2), 150, b"b1".to_vec()));
        assert_eq!(row_of(&all[2]), (key(1), 200, b"a2".to_vec()));
        assert_eq!(row_of(&all[3]), (key(1), 300, b"a3".to_vec()));
    }

    #[test]
    fn scan_all_skips_tombstoned_versions() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"a1");
        e.versioned_tombstone(1, 1, "c", &key(1), 200).unwrap();
        put(&e, "c", 1, 300, b"a3");
        let all = e
            .versioned_scan_all(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        // The tombstone version is excluded; the two live versions remain.
        let times: Vec<i64> = all.iter().map(|r| r.system_from_ms).collect();
        assert_eq!(times, vec![100, 300]);
    }

    #[test]
    fn scan_all_pushes_predicate_down_so_limit_counts_matches() {
        // The audit-log handler must not fetch a capped window then filter —
        // that silently under-returns for a selective predicate. The predicate
        // must apply inside the scan, before `limit` truncation, so `limit`
        // counts MATCHING versions — not raw scanned rows.
        let (e, _d) = open_temp();
        for i in 0..10i64 {
            put(&e, "c", 1, 100 + i, format!("v{i}").as_bytes());
        }
        // Match only odd-suffixed bodies: v1, v3, v5, v7, v9.
        let odd =
            |_: &StorageKey, body: &[u8]| body.last().map(|b| (b - b'0') % 2 == 1).unwrap_or(false);

        let rows = e
            .versioned_scan_all(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: 3,
                },
                &odd,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            3,
            "limit must count matching versions, not scanned rows"
        );
        let bodies: Vec<Vec<u8>> = rows.into_iter().map(|r| r.body).collect();
        // Oldest three matches in ascending system-time order.
        assert_eq!(bodies, vec![b"v1".to_vec(), b"v3".to_vec(), b"v5".to_vec()]);
    }

    #[test]
    fn scan_as_of_pushes_predicate_down_so_limit_counts_matches() {
        // Same regression for the point-in-time (newest-per-doc) scan: the `limit`
        // early-stop must count matching documents, so a selective filter cannot
        // make the scan return fewer rows than exist.
        let (e, _d) = open_temp();
        for (i, id) in [1u32, 2, 3, 4, 5, 6].iter().enumerate() {
            put(&e, "c", *id, 100 + i as i64, format!("x{i}").as_bytes());
        }
        // Match only even-suffixed bodies: x0 (id 1), x2 (id 3), x4 (id 5).
        let even = |_: &StorageKey, body: &[u8]| {
            body.last()
                .map(|b| (b - b'0').is_multiple_of(2))
                .unwrap_or(false)
        };

        let rows = e
            .versioned_scan_as_of(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: 2,
                },
                &even,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "limit must count matching docs, not scanned rows"
        );
        for (_, body) in &rows {
            assert_eq!(
                (body.last().unwrap() - b'0') % 2,
                0,
                "only even-suffixed docs match"
            );
        }
    }

    #[test]
    fn scan_as_of_hides_tombstoned_rows() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"a1");
        e.versioned_tombstone(1, 1, "c", &key(1), 200).unwrap();
        let at_150 = e
            .versioned_scan_as_of(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: Some(150),
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        assert_eq!(at_150.len(), 1);
        let at_250 = e
            .versioned_scan_as_of(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff_ms: Some(250),
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        assert!(at_250.is_empty());
    }

    #[test]
    fn versioned_remove_in_txn_deletes_the_version() {
        let (e, _d) = open_temp();
        put(&e, "c", 1, 100, b"v1");
        assert_eq!(
            e.versioned_get_current(1, 1, "c", &key(1))
                .unwrap()
                .as_deref(),
            Some(b"v1" as &[u8])
        );

        let txn = e.db.begin_write().unwrap();
        e.versioned_remove_in_txn(&txn, 1, 1, "c", &key(1), 100)
            .unwrap();
        txn.commit().unwrap();

        assert!(
            e.versioned_get_current(1, 1, "c", &key(1))
                .unwrap()
                .is_none()
        );
        assert!(
            e.versioned_get_as_of(1, 1, "c", &key(1), Some(100), None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn versioned_remove_in_txn_on_missing_key_is_ok() {
        let (e, _d) = open_temp();
        let txn = e.db.begin_write().unwrap();
        let r = e.versioned_remove_in_txn(&txn, 1, 1, "c", &key(999), 999);
        assert!(r.is_ok());
        txn.commit().unwrap();
    }
}
