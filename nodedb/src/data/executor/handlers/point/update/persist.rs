// SPDX-License-Identifier: BUSL-1.1

//! Atomic publication of an admitted, fully materialized point UPDATE.

use super::prepared::{PreparedUpdateRow, PreparedUpdateUndo};
use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::write_hook::HookCtx;
use crate::data::executor::handlers::point::apply_put::InstalledDocumentRow;
use crate::engine::document::store::{RowIdentity, StorageKey};
use crate::types::{Lsn, SharedInstalledDocument};
use nodedb_physical::physical_plan::ResolvedSumTarget;

pub(in crate::data::executor) struct PointUpdatePersist<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: StorageKey,
    pub identity: RowIdentity,
    pub image: SharedInstalledDocument,
    pub wal_lsn: Option<Lsn>,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    pub(in crate::data::executor) fn persist_point_update(
        &mut self,
        params: PointUpdatePersist<'_>,
    ) -> crate::Result<(
        Vec<WriteSetEntry>,
        crate::types::document_event::DocumentPutEvent,
    )> {
        let PointUpdatePersist {
            database_id,
            tid,
            collection,
            storage_key,
            identity,
            image,
            wal_lsn,
            resolved_sum_targets,
        } = params;
        let txn = self.sparse.begin_write()?;
        let mut undo = PreparedUpdateUndo::default();
        let applied = self.apply_prepared_update_row(
            &txn,
            PreparedUpdateRow {
                row: InstalledDocumentRow {
                    database_id,
                    tid,
                    collection,
                    storage_key,
                    surrogate: storage_key.surrogate(),
                    wal_lsn,
                },
                identity,
                image,
                hook: &HookCtx {
                    target_unique: crate::data::executor::enforcement::unique::UniqueJudge::Unit,
                    database_id,
                    tid,
                    collection,
                    resolved_targets: resolved_sum_targets,
                    deferred_sum_targets: &[],
                    wal_lsn,
                },
            },
            &mut undo,
        );
        let applied = match applied {
            Ok(applied) => applied,
            Err(error) => {
                drop(txn);
                return Err(undo.abort(self, database_id, tid, error));
            }
        };
        if let Err(error) = undo.check_unique(self, &txn, database_id, tid) {
            drop(txn);
            return Err(undo.abort(self, database_id, tid, error));
        }
        if let Err(error) =
            self.settle_balanced_entries(database_id, tid, collection, applied.balanced_entries)
        {
            drop(txn);
            return Err(undo.abort(self, database_id, tid, error));
        }
        if let Err(error) = txn.commit() {
            let error = crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("point update commit: {error}"),
            };
            self.fail_journalled(&error);
            return Err(undo.abort(self, database_id, tid, error));
        }
        Ok((applied.write_set, applied.event))
    }
}
