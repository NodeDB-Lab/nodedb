// SPDX-License-Identifier: BUSL-1.1

//! Replay of a collection's stored sync frames into the live text index,
//! as a snapshot restore runs it.

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::inverted::IndexDocScope;
use crate::types::TenantId;

/// Frame versions one replay write transaction holds before it commits.
pub(in crate::data::executor) const REPLAY_BATCH_VERSIONS: usize = 1024;

impl CoreLoop {
    /// Replay every stored sync-frame version of `collection` whose
    /// document passes `replays`, each at its own ordinal, in ordinal order
    /// per document. A tombstone version ends the document's text. Returns
    /// the versions replayed.
    ///
    /// A snapshot restore replays the frames of every restored document the
    /// snapshot carries no version of.
    pub(in crate::data::executor) fn replay_synced_text(
        &self,
        database_id: u64,
        tenant_id: TenantId,
        collection: &str,
        replays: &dyn Fn(nodedb_types::Surrogate) -> bool,
    ) -> crate::Result<u64> {
        let mut open: Option<WriteTransaction> = None;
        let mut in_batch = 0usize;
        let mut replayed = 0u64;
        self.inverted
            .for_each_synced_text(database_id, tenant_id, Some(collection), |synced| {
                if !replays(synced.surrogate) {
                    return Ok(());
                }
                let txn = match open.take() {
                    Some(txn) => txn,
                    None => self.sparse.begin_write()?,
                };
                self.replay_frame_text(
                    &txn,
                    IndexDocScope {
                        database_id,
                        tid: tenant_id,
                        collection,
                        surrogate: synced.surrogate,
                        ordinal: synced.ordinal,
                    },
                    &synced.fields,
                )?;
                replayed += 1;
                in_batch += 1;
                if in_batch == REPLAY_BATCH_VERSIONS {
                    commit_batch(txn, collection)?;
                    in_batch = 0;
                } else {
                    open = Some(txn);
                }
                Ok(())
            })?;
        if let Some(txn) = open {
            commit_batch(txn, collection)?;
        }
        Ok(replayed)
    }
}

/// Commit one replay batch.
pub(in crate::data::executor) fn commit_batch(
    txn: WriteTransaction,
    collection: &str,
) -> crate::Result<()> {
    txn.commit().map_err(|e| crate::Error::Storage {
        engine: "sparse".into(),
        detail: format!("text index replay commit ({collection}): {e}"),
    })
}
