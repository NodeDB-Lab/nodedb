// SPDX-License-Identifier: BUSL-1.1

//! Reads that hand back each visible version with its stamps.
//!
//! A clone copies a source row as the same logical version: the copy keeps
//! the source version's system-time ordinal and valid interval. These reads
//! give the copy both, beside the body.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::ReadableDatabase;

use super::doc::DOCUMENTS_VERSIONED;
use super::key::{coll_prefix, coll_prefix_end};
use super::value::decode_value;
use super::visible::{RowRef, ScanRange, for_each_visible, visible_version};
use crate::engine::sparse::btree::{SparseEngine, redb_err};
use crate::engine::sparse::scan_stop::never_stop;

/// One live version with the stamps it was written at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StampedVersion {
    pub doc_id: StorageKey,
    /// The version's system-time ordinal.
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: Vec<u8>,
}

/// Where one stamped page reads.
pub struct StampedScan<'a> {
    pub database_id: u64,
    pub tenant: u64,
    pub coll: &'a str,
    /// System-time ordinal cutoff (`None` = current).
    pub sys_cutoff: Option<i64>,
    /// Rows the page holds at most.
    pub limit: usize,
}

impl SparseEngine {
    /// The rows after `after` in doc-id order, each as its version visible
    /// at `scan.sys_cutoff`. A row whose visible version is a tombstone or
    /// an erasure is absent.
    pub fn versioned_scan_stamped_after(
        &self,
        scan: StampedScan<'_>,
        after: Option<&StorageKey>,
    ) -> crate::Result<Vec<StampedVersion>> {
        let StampedScan {
            database_id,
            tenant,
            coll,
            sys_cutoff,
            limit,
        } = scan;
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        // Every version key of `after` is `{lo}{after}\x00{sys_from}`, so
        // `\x01` after the id is the first key past all of them.
        let start = match after {
            Some(after) => format!("{lo}{after}\x01"),
            None => lo.clone(),
        };
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        for_each_visible(
            &t,
            ScanRange {
                coll,
                prefix: &lo,
                start: &start,
                end: &hi,
            },
            sys_cutoff,
            &never_stop,
            |row| {
                out.push(StampedVersion {
                    doc_id: row.doc_id,
                    sys_from: row.sys_from,
                    valid_from_ms: row.value.valid_from_ms,
                    valid_until_ms: row.value.valid_until_ms,
                    body: row.value.body.to_vec(),
                });
                Ok(if out.len() >= limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            },
        )?;
        Ok(out)
    }

    /// The version of `doc_id` visible at `sys_cutoff` (`None` = current),
    /// with its stamps. `None` when no version is visible or the visible one
    /// is a tombstone or an erasure.
    pub fn versioned_get_stamped(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_cutoff: Option<i64>,
    ) -> crate::Result<Option<StampedVersion>> {
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
        let Some((sys_from, guard)) = visible_version(&t, row, sys_cutoff)? else {
            return Ok(None);
        };
        let decoded = decode_value(guard.value())?;
        if !decoded.is_live() {
            return Ok(None);
        }
        Ok(Some(StampedVersion {
            doc_id: *doc_id,
            sys_from,
            valid_from_ms: decoded.valid_from_ms,
            valid_until_ms: decoded.valid_until_ms,
            body: decoded.body.to_vec(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::engine::sparse::btree_versioned::VersionedPut;

    const COLL: &str = "c";

    fn put(engine: &SparseEngine, surrogate: u32, sys_from: i64, valid: (i64, i64), body: &[u8]) {
        engine
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant: 1,
                coll: COLL,
                doc_id: &StorageKey::for_surrogate(Surrogate::new(surrogate)),
                sys_from,
                valid_from_ms: valid.0,
                valid_until_ms: valid.1,
                body,
            })
            .expect("versioned put");
    }

    fn engine() -> (tempfile::TempDir, SparseEngine) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = SparseEngine::open(&dir.path().join("sparse.redb")).expect("open");
        (dir, engine)
    }

    /// Each scanned row carries the stamps of the version the cut sees.
    #[test]
    fn a_stamped_scan_carries_each_visible_version_stamps() {
        let (_dir, engine) = engine();
        put(&engine, 1, 100, (5, 50), b"a1");
        put(&engine, 1, 300, (i64::MIN, i64::MAX), b"a2");
        put(&engine, 2, 200, (i64::MIN, i64::MAX), b"b1");
        let scan = |sys_cutoff| StampedScan {
            database_id: 0,
            tenant: 1,
            coll: COLL,
            sys_cutoff,
            limit: 16,
        };

        let at_250 = engine
            .versioned_scan_stamped_after(scan(Some(250)), None)
            .expect("scan");
        let stamps: Vec<(i64, i64, i64, Vec<u8>)> = at_250
            .iter()
            .map(|v| {
                (
                    v.sys_from,
                    v.valid_from_ms,
                    v.valid_until_ms,
                    v.body.clone(),
                )
            })
            .collect();
        assert_eq!(
            stamps,
            vec![
                (100, 5, 50, b"a1".to_vec()),
                (200, i64::MIN, i64::MAX, b"b1".to_vec())
            ]
        );
        let current = engine
            .versioned_scan_stamped_after(scan(None), None)
            .expect("scan");
        assert_eq!(current[0].sys_from, 300);
    }

    /// A point read answers the visible version's stamps, and nothing for a
    /// row the cut does not see.
    #[test]
    fn a_stamped_get_answers_the_visible_version() {
        let (_dir, engine) = engine();
        put(&engine, 7, 100, (i64::MIN, i64::MAX), b"old");
        put(&engine, 7, 300, (i64::MIN, i64::MAX), b"new");
        let doc_id = StorageKey::for_surrogate(Surrogate::new(7));
        let get = |cut| {
            engine
                .versioned_get_stamped(0, 1, COLL, &doc_id, cut)
                .expect("get")
        };
        assert_eq!(
            get(Some(200)).map(|v| (v.sys_from, v.body)),
            Some((100, b"old".to_vec()))
        );
        assert_eq!(get(None).map(|v| v.sys_from), Some(300));
        assert_eq!(get(Some(50)), None);
    }
}
