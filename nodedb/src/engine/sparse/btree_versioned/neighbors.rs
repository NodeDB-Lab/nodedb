// SPDX-License-Identifier: BUSL-1.1

//! The stored versions of one row next to a system-time ordinal.
//!
//! A version installed below the row's newest version lands between two
//! stored versions. Its predecessor is the newest version below its ordinal.
//! Its successor is the oldest version above it. The install diffs its index
//! entries against the predecessor and closes them at the successor.

use nodedb_types::StorageKey;
use redb::ReadableTable;

use super::doc::DOCUMENTS_VERSIONED;
use super::key::{doc_prefix, doc_prefix_end, versioned_doc_key};
use super::value::decode_value;
use super::visible::version_sys_from;
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// One stored version of a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacentVersion {
    /// The version's system-time ordinal.
    pub sys_from: i64,
    /// The version's body. `None` for a tombstone or an erasure.
    pub body: Option<Vec<u8>>,
}

/// The stored versions of one row on either side of an ordinal. A version
/// stored at exactly the ordinal is neither.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionNeighbors {
    /// The newest version below the ordinal.
    pub predecessor: Option<AdjacentVersion>,
    /// The oldest version above the ordinal.
    pub successor: Option<AdjacentVersion>,
}

impl SparseEngine {
    /// The versions of `doc_id` adjacent to `sys_from`, read inside `txn`
    /// so they include the versions `txn` staged. Two seeks.
    pub fn versioned_neighbors_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &StorageKey,
        sys_from: i64,
    ) -> crate::Result<VersionNeighbors> {
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
        let at = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from);

        let predecessor = match t
            .range(lo.as_str()..at.as_str())
            .map_err(|e| redb_err("range", e))?
            .next_back()
        {
            Some(entry) => {
                let (key, value) = entry.map_err(|e| redb_err("entry", e))?;
                Some(adjacent(key.value(), value.value(), coll)?)
            }
            None => None,
        };

        let above = t
            .range(at.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;
        let mut successor = None;
        for entry in above {
            let (key, value) = entry.map_err(|e| redb_err("entry", e))?;
            // The version at exactly `sys_from` is the one being installed.
            if key.value() == at.as_str() {
                continue;
            }
            successor = Some(adjacent(key.value(), value.value(), coll)?);
            break;
        }

        Ok(VersionNeighbors {
            predecessor,
            successor,
        })
    }
}

/// Decode one stored version into an [`AdjacentVersion`].
fn adjacent(key: &str, value: &[u8], coll: &str) -> crate::Result<AdjacentVersion> {
    let sys_from = version_sys_from(key, coll)?;
    let decoded = decode_value(value)?;
    Ok(AdjacentVersion {
        sys_from,
        body: decoded.is_live().then(|| decoded.body.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sparse::btree_versioned::VersionedPut;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "c";

    fn open_temp() -> (SparseEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let e = SparseEngine::open(&dir.path().join("t.redb")).unwrap();
        (e, dir)
    }

    fn key() -> StorageKey {
        StorageKey::for_surrogate(nodedb_types::Surrogate::new(9))
    }

    fn put(e: &SparseEngine, sys_from: i64, body: &[u8]) {
        e.versioned_put(VersionedPut {
            database_id: DB,
            tenant: TID,
            coll: COLL,
            doc_id: &key(),
            sys_from,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
            body,
        })
        .unwrap();
    }

    fn neighbors(e: &SparseEngine, sys_from: i64) -> VersionNeighbors {
        let txn = e.begin_write().unwrap();
        e.versioned_neighbors_in_txn(&txn, DB, TID, COLL, &key(), sys_from)
            .unwrap()
    }

    #[test]
    fn neighbors_skip_the_version_at_the_ordinal() {
        let (e, _d) = open_temp();
        put(&e, 10, b"a");
        put(&e, 20, b"b");
        e.versioned_tombstone(DB, TID, COLL, &key(), 30).unwrap();

        let mid = neighbors(&e, 20);
        assert_eq!(
            mid.predecessor,
            Some(AdjacentVersion {
                sys_from: 10,
                body: Some(b"a".to_vec())
            })
        );
        assert_eq!(
            mid.successor,
            Some(AdjacentVersion {
                sys_from: 30,
                body: None
            })
        );

        let between = neighbors(&e, 15);
        assert_eq!(between.predecessor.map(|v| v.sys_from), Some(10));
        assert_eq!(between.successor.map(|v| v.sys_from), Some(20));
    }

    #[test]
    fn neighbors_at_the_ends_are_absent() {
        let (e, _d) = open_temp();
        put(&e, 10, b"a");
        assert_eq!(neighbors(&e, 5).predecessor, None);
        assert_eq!(neighbors(&e, 10).successor, None);
        assert_eq!(neighbors(&e, 11).successor, None);
    }
}
