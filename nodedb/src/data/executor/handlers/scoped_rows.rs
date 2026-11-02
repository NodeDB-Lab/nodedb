// SPDX-License-Identifier: BUSL-1.1

//! Every row of a document collection at one system-time scope, for the
//! handlers that read a whole collection by name: the recursive CTE scan.
//!
//! `Current` reads each row's current version. `AsOf`, `Pinned`, and
//! `TxnSnapshot` read the version visible at the scope's ordinal. `Current`
//! and `TxnSnapshot` fold the reading transaction's staged writes on top.
//! `AllVersions` is refused: a whole-collection read answers one version
//! per row.

use nodedb_types::{StorageKey, SystemTimeScope};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// One whole-collection read at a scope.
pub(in crate::data::executor) struct ScopedRowsRead<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub system_time: &'a SystemTimeScope,
    /// The most rows read.
    pub limit: usize,
}

impl CoreLoop {
    /// The first `read.limit` rows of `read.collection` in key order, each
    /// at the version `read.system_time` selects, with the task's staged
    /// writes on top when the scope merges them.
    pub(in crate::data::executor) fn scoped_collection_rows(
        &self,
        task: &ExecutionTask,
        read: ScopedRowsRead<'_>,
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        let ScopedRowsRead {
            tid,
            collection,
            system_time,
            limit,
        } = read;
        let database_id = task.request.database_id.as_u64();
        let system_as_of = match system_time {
            SystemTimeScope::Current => None,
            SystemTimeScope::AsOf(_)
            | SystemTimeScope::Pinned { .. }
            | SystemTimeScope::TxnSnapshot { .. } => system_time.as_of_ordinal(),
            SystemTimeScope::AllVersions => {
                return Err(crate::Error::DataPlane(ErrorCode::Unsupported {
                    detail: format!(
                        "AS OF SYSTEM TIME NULL (all-versions) is not supported on a \
                         whole-collection read of '{collection}'"
                    ),
                }));
            }
        };
        let overlay_txn = task
            .request
            .txn_id
            .filter(|_| system_time.merges_txn_overlay());
        let Some(txn_id) = overlay_txn else {
            if system_as_of.is_none() {
                return self.current_rows(database_id, tid, collection, limit);
            }
            let mut rows = self.rows_where_at(
                database_id,
                tid,
                collection,
                system_as_of,
                &|_: &StorageKey, _: &[u8]| true,
            )?;
            rows.truncate(limit);
            return Ok(rows);
        };
        let coll_key = (
            task.request.database_id,
            TenantId::new(tid),
            collection.to_string(),
        );
        let mut rows = self.rows_where_at(
            database_id,
            tid,
            collection,
            system_as_of,
            &|_: &StorageKey, _: &[u8]| true,
        )?;
        self.merge_overlay_into_scan(txn_id, &coll_key, &mut rows, &|_: &StorageKey, _: &[u8]| {
            true
        });
        // A staged row the base lacks joins at the end: restore key order.
        rows.sort_unstable_by_key(|(key, _)| *key);
        rows.truncate(limit);
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::engine::sparse::btree_versioned::VersionedPut;
    use crate::types::{DatabaseId, TxnId};
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const COLL: &str = "tree";

    fn key(surrogate: u32) -> StorageKey {
        StorageKey::for_surrogate(Surrogate::new(surrogate))
    }

    fn put(core: &CoreLoop, surrogate: u32, name: &str, sys_from: i64) {
        let body = doc_format::encode_to_msgpack(&serde_json::json!({ "name": name }));
        core.sparse
            .versioned_put(VersionedPut {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant: TID,
                coll: COLL,
                doc_id: &key(surrogate),
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &body,
            })
            .expect("seed version");
    }

    fn names(rows: &[(StorageKey, Vec<u8>)]) -> Vec<String> {
        rows.iter()
            .map(|(_, body)| {
                doc_format::decode_document(body)
                    .expect("row decodes")
                    .get("name")
                    .and_then(|v| v.as_str())
                    .expect("row carries a name")
                    .to_string()
            })
            .collect()
    }

    /// A block's read sees the rows at its snapshot plus its staged rows,
    /// never a commit after the snapshot.
    #[test]
    fn a_snapshot_read_sees_its_cut_and_its_staged_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        put(&core, 1, "root", 100);
        // A concurrent commit after the snapshot at 150.
        put(&core, 1, "renamed", 200);
        put(&core, 2, "late", 200);

        let txn_id = TxnId::new(9);
        let staged = doc_format::encode_to_msgpack(&serde_json::json!({ "name": "staged" }));
        core.txn_overlay_mut(txn_id).insert_put(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            3,
            &key(3).to_identity(),
            staged,
        );
        let mut task = make_default_task();
        task.request.txn_id = Some(txn_id);

        let snapshot = SystemTimeScope::TxnSnapshot {
            ordinal: 150,
            cut_marker: 0,
        };
        let rows = core
            .scoped_collection_rows(
                &task,
                ScopedRowsRead {
                    tid: TID,
                    collection: COLL,
                    system_time: &snapshot,
                    limit: usize::MAX,
                },
            )
            .expect("snapshot read");
        assert_eq!(names(&rows), vec!["root", "staged"]);

        let pinned = SystemTimeScope::Pinned {
            ordinal: 150,
            cut_marker: 0,
        };
        let rows = core
            .scoped_collection_rows(
                &task,
                ScopedRowsRead {
                    tid: TID,
                    collection: COLL,
                    system_time: &pinned,
                    limit: usize::MAX,
                },
            )
            .expect("pinned read");
        assert_eq!(names(&rows), vec!["root"], "a pinned read merges nothing");
    }
}
