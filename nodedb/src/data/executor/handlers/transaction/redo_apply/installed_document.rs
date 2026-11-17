// SPDX-License-Identifier: BUSL-1.1

//! Exact installed rows bypass write-time derivation during recovery.

use super::state::AppliedDocWrite;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::{AbandonedWrite, abandon_write};
use crate::data::executor::handlers::point::apply_put::InstalledDocumentRow;
use crate::data::executor::handlers::transaction::undo::document_outcome::{
    DocumentRow, push_put_undo,
};
use crate::engine::document::store::StorageKey;
use crate::event::WriteOp;
use crate::types::{
    Lsn,
    installed_document::{InstalledDocument, SharedInstalledDocument},
};
use crate::wal::redo::installed_document::InstalledDocumentRedo;
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::{RowIdentity, Surrogate};

impl CoreLoop {
    pub(in crate::data::executor) fn replay_installed_document(
        &mut self,
        database_id: u64,
        tid: u64,
        lsn: u64,
        row: InstalledDocumentRedo<StorageMode>,
    ) -> crate::Result<()> {
        let image: SharedInstalledDocument = InstalledDocument {
            canonical: row.image.canonical,
            stored: row.image.stored,
            version: row.image.version,
            schema: std::sync::Arc::new(row.image.schema),
            chain_head: row.image.chain_head,
        };
        self.observe_bitemporal_ordinal(image.version.sys_from);
        let surrogate = Surrogate::new(row.surrogate);
        let storage_key = StorageKey::for_surrogate(surrogate);
        let txn = self.sparse.begin_write()?;
        let (mut outcome, head) = self.apply_installed_document(
            &txn,
            InstalledDocumentRow {
                database_id,
                tid,
                collection: &row.collection,
                storage_key,
                surrogate,
                wal_lsn: (lsn != 0).then(|| Lsn::new(lsn)),
            },
            &image,
        )?;
        if let Err(error) = txn.commit() {
            return Err(abandon_write(
                self,
                AbandonedWrite::row(database_id, tid, &row.collection, &storage_key)
                    .undo(std::mem::take(&mut outcome.memory_undo)),
                crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("installed document redo commit: {error}"),
                },
            ));
        }
        let chain_prior = head
            .as_ref()
            .filter(|receipt| receipt.changed)
            .map(|receipt| receipt.prior.clone());
        if let Some(receipt) = &head {
            self.publish_installed_head(database_id, tid, receipt);
        }
        self.checkpoint_coordinator.mark_dirty("sparse", 1);
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.doc_writes.push(AppliedDocWrite {
                collection: row.collection.clone(),
                identity: RowIdentity::from_user_key(row.document_id),
                op: if row.event.old_value.is_some() {
                    WriteOp::Update
                } else {
                    WriteOp::Insert
                },
                old_value: None,
                event: Some(row.event),
                emits_event: true,
            });
        }
        if self.recording_redo_undo() {
            let mut undo = Vec::new();
            push_put_undo(
                &mut undo,
                DocumentRow {
                    collection: &row.collection,
                    storage_key,
                },
                outcome,
                chain_prior,
            );
            self.record_redo_undo(undo);
        }
        Ok(())
    }
}
