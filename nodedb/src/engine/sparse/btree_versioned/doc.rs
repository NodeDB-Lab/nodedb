// SPDX-License-Identifier: BUSL-1.1

//! Document-level operations on the versioned document table.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::history_bytes::{insert_counted, remove_counted};
use super::key::{doc_prefix, doc_prefix_end, versioned_doc_key};
use super::value::{
    TAG_GDPR_ERASED, TAG_LIVE, TAG_TOMBSTONE, VersionedPut, decode_value, encode_value,
};
use super::visible::{RowRef, visible_body, visible_version};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// One row of an audit-log (`AS OF SYSTEM TIME NULL`) scan: a single
/// system-time version of a document plus the temporal coordinates read from
/// the row's real stored envelope. `valid_from_ms` / `valid_until_ms` carry the
/// raw envelope sentinels (`i64::MIN` / `i64::MAX` when unbounded) so the
/// handler can surface them uniformly across engines.
///
/// A delete version repeats the body of the live version it closes under an
/// empty valid interval (`valid_from_ms = valid_until_ms = i64::MIN`), the
/// convention a bitemporal columnar tombstone follows.
pub struct VersionedRow {
    pub doc_id: StorageKey,
    /// The version's system-time ordinal.
    pub system_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: Vec<u8>,
    /// The version deletes the document.
    pub deleted: bool,
}

/// The document table. Every document collection keeps every version of
/// every row here, bitemporal or not. Keys carry the leading
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
        self.ensure_history_bytes_table()
    }

    /// Append one version in its own write transaction.
    pub fn versioned_put(&self, p: VersionedPut<'_>) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_put_in_txn(&txn, p)?;
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append one version inside a caller-owned write transaction. Composite
    /// write paths (PointPut / UPSERT / UPDATE) use it so the document, its
    /// index entries and the current-state side effects (stats, text, vector,
    /// spatial indexes, document cache) commit atomically. A version at an
    /// existing `sys_from` is replaced.
    pub fn versioned_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        p: VersionedPut<'_>,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(p.database_id, p.tenant, p.coll, p.doc_id, p.sys_from);
        let val = encode_value(TAG_LIVE, p.valid_from_ms, p.valid_until_ms, p.body);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        insert_counted(txn, &mut t, &key, &val)
    }

    /// Append a tombstone version in its own write transaction.
    pub fn versioned_tombstone(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        self.versioned_tombstone_in_txn(&txn, database_id, tenant, coll, doc_id, sys_from)?;
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
        sys_from: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from);
        let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        insert_counted(txn, &mut t, &key, &val)
    }

    /// Physically remove one version's redb entry inside a caller-owned
    /// write transaction. Unlike [`Self::versioned_tombstone_in_txn`] (which
    /// appends a tombstone marker version), this deletes the entry at
    /// `sys_from` outright. Used by the transaction-rollback path to
    /// undo a `versioned_put_in_txn` that must not survive an aborted
    /// transaction. Removing a non-existent key is a no-op.
    pub fn versioned_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        remove_counted(txn, &mut t, &key)
    }

    /// Check whether the current-state version of `doc_id` is live (not
    /// tombstoned, not GDPR-erased, and at least one version exists) inside a
    /// caller-owned write transaction, so it sees the versions `txn` staged.
    /// PointInsert enforces primary-key uniqueness through it, linearizably
    /// with the version it appends.
    pub fn versioned_exists_current_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<bool> {
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let row = RowRef {
            database_id,
            tenant,
            coll,
            doc_id,
        };
        match visible_version(&t, row, None)? {
            Some((_, guard)) => Ok(decode_value(guard.value())?.is_live()),
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
                .map(|r| r.map(|(k, _)| k.value().to_string()))
                .collect::<Result<_, _>>()
                .map_err(|e| redb_err("entry", e))?;
            let erased = encode_value(TAG_GDPR_ERASED, 0, 0, &[]);
            for k in keys {
                insert_counted(&txn, &mut t, &k, &erased)?;
                replaced += 1;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(replaced)
    }

    /// The body of `doc_id` visible at `sys_cutoff` (`None` = current).
    ///
    /// Without `valid_at_ms` this is one reverse seek: the newest version at
    /// or below the cut decides, and a tombstone or an erasure hides the row.
    /// With `valid_at_ms` the read walks back from that version to the newest
    /// live one whose valid interval holds `valid_at_ms`. A tombstone or an
    /// erasure on the way hides the row.
    pub fn versioned_get_as_of(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_cutoff: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let row = RowRef {
            database_id,
            tenant,
            coll,
            doc_id,
        };
        let Some(vt) = valid_at_ms else {
            return Ok(visible_body(&t, row, sys_cutoff)?.map(|(_, body)| body));
        };
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = match sys_cutoff {
            Some(cutoff) => versioned_doc_key(database_id, tenant, coll, doc_id, cutoff),
            None => doc_prefix_end(database_id, tenant, coll, doc_id),
        };
        let range = match sys_cutoff {
            Some(_) => t.range(lo.as_str()..=hi.as_str()),
            None => t.range(lo.as_str()..hi.as_str()),
        }
        .map_err(|e| redb_err("range", e))?;
        for entry in range.rev() {
            let (_key, value) = entry.map_err(|e| redb_err("entry", e))?;
            let decoded = decode_value(value.value())?;
            if !decoded.is_live() {
                return Ok(None);
            }
            if vt >= decoded.valid_from_ms && vt < decoded.valid_until_ms {
                return Ok(Some(decoded.body.to_vec()));
            }
        }
        Ok(None)
    }

    /// The current body of `doc_id`: one reverse seek.
    pub fn versioned_get_current(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        Ok(self
            .versioned_get_current_with_sys_from(database_id, tenant, coll, doc_id)?
            .map(|(_, body)| body))
    }

    /// The current body of `doc_id` with the system-time ordinal of the
    /// version that holds it. One reverse seek.
    pub fn versioned_get_current_with_sys_from(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<(i64, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        visible_body(
            &t,
            RowRef {
                database_id,
                tenant,
                coll,
                doc_id,
            },
            None,
        )
    }

    /// Visit every version of every row of `coll`, in row order and, within
    /// a row, in ascending system time. A live version hands over its body.
    /// A tombstone or an erasure hands over `None`. A visit error ends the
    /// scan and is returned.
    pub fn versioned_for_each_version(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        mut visit: impl FnMut(&StorageKey, i64, Option<&[u8]>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let lo = super::key::coll_prefix(database_id, tenant, coll);
        let hi = super::key::coll_prefix_end(database_id, tenant, coll);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let invalid = || {
                crate::engine::sparse::btree::invalid_storage_key_err(
                    crate::engine::sparse::btree::KeyedTable::DocumentsVersioned,
                    coll,
                    key_str,
                )
            };
            let (id_part, suffix) = key_str.rsplit_once('\x00').ok_or_else(invalid)?;
            let seg = id_part.strip_prefix(lo.as_str()).ok_or_else(invalid)?;
            let doc_id = StorageKey::parse(seg).ok_or_else(invalid)?;
            let sys_from = suffix.parse::<i64>().map_err(|_| invalid())?;
            let decoded = decode_value(v.value())?;
            visit(&doc_id, sys_from, decoded.is_live().then_some(decoded.body))?;
        }
        Ok(())
    }

    /// [`Self::versioned_get_current`] read inside `txn`, so it sees the
    /// versions `txn` staged. The newest version decides: a tombstone or an
    /// erasure hides the row.
    pub fn versioned_get_current_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<Vec<u8>>> {
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        Ok(visible_body(
            &t,
            RowRef {
                database_id,
                tenant,
                coll,
                doc_id,
            },
            None,
        )?
        .map(|(_, body)| body))
    }

    /// The system-time ordinal of the newest stored version of `doc_id`,
    /// live or not. `None` when the row has no version.
    pub fn versioned_latest_sys_from(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
    ) -> crate::Result<Option<i64>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let row = RowRef {
            database_id,
            tenant,
            coll,
            doc_id,
        };
        Ok(visible_version(&t, row, None)?.map(|(sys_from, _)| sys_from))
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
            sys_from,
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
            sys_from,
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
                    sys_cutoff: None,
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
                    sys_cutoff: None,
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
        let times: Vec<i64> = all.iter().map(|r| r.system_from).collect();
        assert_eq!(times, vec![100, 150, 200, 300]);
        // System-time and body line up per version.
        let row_of = |r: &VersionedRow| (r.doc_id, r.system_from, r.body.clone());
        assert_eq!(row_of(&all[0]), (key(1), 100, b"a1".to_vec()));
        assert_eq!(row_of(&all[1]), (key(2), 150, b"b1".to_vec()));
        assert_eq!(row_of(&all[2]), (key(1), 200, b"a2".to_vec()));
        assert_eq!(row_of(&all[3]), (key(1), 300, b"a3".to_vec()));
    }

    #[test]
    fn scan_all_emits_tombstoned_versions_as_deletes() {
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
                    sys_cutoff: None,
                    valid_at_ms: None,
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        // The tombstone version is a delete row between the live versions.
        let times: Vec<i64> = all.iter().map(|r| r.system_from).collect();
        assert_eq!(times, vec![100, 200, 300]);
        let delete = &all[1];
        assert!(delete.deleted && !all[0].deleted && !all[2].deleted);
        assert_eq!(
            delete.body,
            b"a1".to_vec(),
            "the body of the version it closes"
        );
        assert_eq!(
            (delete.valid_from_ms, delete.valid_until_ms),
            (i64::MIN, i64::MIN),
            "a delete version has an empty valid interval"
        );

        // A valid-time point never falls in an empty interval.
        let at_valid = e
            .versioned_scan_all(
                VersionedScanParams {
                    database_id: 1,
                    tenant: 1,
                    coll: "c",
                    sys_cutoff: None,
                    valid_at_ms: Some(0),
                    limit: 100,
                },
                &|_: &StorageKey, _: &[u8]| true,
                &crate::engine::sparse::scan_stop::never_stop,
            )
            .unwrap();
        assert!(at_valid.iter().all(|r| !r.deleted));
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
                    sys_cutoff: None,
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
                    sys_cutoff: None,
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
                    sys_cutoff: Some(150),
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
                    sys_cutoff: Some(250),
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
    fn latest_sys_from_reads_the_newest_version_live_or_not() {
        let (e, _d) = open_temp();
        assert_eq!(
            e.versioned_latest_sys_from(1, 1, "c", &key(1)).unwrap(),
            None
        );
        put(&e, "c", 1, 100, b"v1");
        put(&e, "c", 1, 300, b"v2");
        assert_eq!(
            e.versioned_latest_sys_from(1, 1, "c", &key(1)).unwrap(),
            Some(300)
        );
        e.versioned_tombstone(1, 1, "c", &key(1), 400).unwrap();
        assert_eq!(
            e.versioned_latest_sys_from(1, 1, "c", &key(1)).unwrap(),
            Some(400)
        );
        assert_eq!(
            e.versioned_latest_sys_from(1, 1, "c", &key(2)).unwrap(),
            None
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
