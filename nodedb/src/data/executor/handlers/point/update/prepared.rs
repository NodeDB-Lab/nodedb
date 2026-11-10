// SPDX-License-Identifier: BUSL-1.1

//! Prepared UPDATE rows and statement-owned memory rollback.

use crate::bridge::envelope::WriteSetEntry;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::enforcement::write_hook::{self, HookCtx, ImageBody, WriteImages};
use crate::data::executor::handlers::point::apply_put::InstalledDocumentRow;
use crate::data::executor::handlers::transaction::undo::{UndoEntry, memory::abort_error};
use crate::engine::document::store::{RowIdentity, StorageKey};
use crate::types::SharedInstalledDocument;
use redb::WriteTransaction;

#[derive(Default)]
pub(in crate::data::executor) struct PreparedUpdateUndo {
    touched: Vec<(String, StorageKey)>,
    memory: Vec<UndoEntry>,
}

impl PreparedUpdateUndo {
    pub(in crate::data::executor) fn check_unique(
        &self,
        core: &CoreLoop,
        txn: &WriteTransaction,
        database_id: u64,
        tid: u64,
    ) -> crate::Result<()> {
        core.check_written_unit_unique(txn, database_id, tid, &self.touched)
    }

    pub(in crate::data::executor) fn abort(
        self,
        core: &mut CoreLoop,
        database_id: u64,
        tid: u64,
        error: crate::Error,
    ) -> crate::Error {
        for (collection, key) in self.touched {
            core.doc_cache
                .invalidate(database_id, tid, &collection, &key);
        }
        abort_error(
            error,
            core.undo_memory_effects(database_id, tid, self.memory),
        )
    }
}

pub(in crate::data::executor) struct PreparedUpdateRow<'a> {
    pub row: InstalledDocumentRow<'a>,
    pub identity: RowIdentity,
    pub image: SharedInstalledDocument,
    pub hook: &'a HookCtx<'a>,
}

pub(in crate::data::executor) struct AppliedUpdateRow {
    pub write_set: Vec<WriteSetEntry>,
    pub balanced_entries: Vec<BalancedEntry>,
    pub event: crate::types::document_event::DocumentPutEvent,
}

impl CoreLoop {
    /// Apply an already-admitted row without recomputing its stored body.
    pub(in crate::data::executor) fn apply_prepared_update_row(
        &mut self,
        txn: &WriteTransaction,
        prepared: PreparedUpdateRow<'_>,
        undo: &mut PreparedUpdateUndo,
    ) -> crate::Result<AppliedUpdateRow> {
        let PreparedUpdateRow {
            row,
            identity,
            image,
            hook,
        } = prepared;
        undo.touched
            .push((row.collection.to_owned(), row.storage_key));
        let surrogate = row.surrogate;
        let mut outcome = self.apply_prepared_update(txn, row, image)?;
        undo.memory.append(&mut outcome.memory_undo);
        let Some(prior) = outcome.prior_value.as_deref() else {
            return Err(crate::Error::Internal {
                detail: "prepared UPDATE row has no transaction-local pre-image".into(),
            });
        };
        let event = self.prepare_installed_put_event(
            &crate::data::executor::core_loop::redo_image::WriteSetRow {
                database_id: hook.database_id,
                tid: hook.tid,
                collection: hook.collection,
                identity: identity.clone(),
            },
            &outcome.installed,
            Some(prior),
        )?;
        let mut enforcement = write_hook::run(
            self,
            txn,
            hook,
            WriteImages::Update {
                old: ImageBody::Stored(prior),
                new: ImageBody::Stored(&outcome.installed.stored),
            },
        )?;
        let mut write_set = vec![WriteSetEntry::installed_put(
            surrogate.as_u32(),
            identity,
            outcome.installed,
            event.clone(),
        )];
        write_set.extend(write_hook::target_write_set(&enforcement.target_writes));
        for target in &mut enforcement.target_writes {
            undo.touched.push((
                target.collection.clone(),
                StorageKey::for_surrogate(target.surrogate),
            ));
            undo.memory.append(&mut target.outcome.memory_undo);
        }
        Ok(AppliedUpdateRow {
            write_set,
            balanced_entries: enforcement.balanced_entries,
            event,
        })
    }
}
