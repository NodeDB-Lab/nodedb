// SPDX-License-Identifier: BUSL-1.1

//! Undo of one CRDT write a committed redo record installs.
//!
//! A Loro import merges, so a write cannot be withdrawn by importing
//! anything. The pre-image is the collection's Loro snapshot before the
//! write, and the undo replaces the collection's document with it. A tenant
//! engine the write created is removed again. The sparse projection of a
//! document row records its own undo where it is written.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

use super::UndoEntry;

/// The Loro state of one CRDT collection before a write.
pub(in crate::data::executor) struct CrdtCollectionUndo {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    /// Whether the tenant's CRDT engine existed.
    pub engine_existed: bool,
    /// The collection's snapshot, `None` when it held no document.
    pub snapshot: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Capture the Loro state of `collection` before a write.
    pub(in crate::data::executor) fn capture_crdt_collection_undo(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> crate::Result<UndoEntry> {
        let engine = self.crdt_engines.get(&(database_id, tenant_id));
        let snapshot = match engine {
            Some(engine) => engine.export_snapshot_bytes(collection)?,
            None => None,
        };
        Ok(UndoEntry::CrdtCollection(Box::new(CrdtCollectionUndo {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            engine_existed: engine.is_some(),
            snapshot,
        })))
    }

    /// Put the collection's Loro document back.
    pub(super) fn apply_undo_crdt_collection(
        &mut self,
        entry_index: usize,
        undo: CrdtCollectionUndo,
    ) -> Result<(), (usize, String)> {
        let key = (undo.database_id, undo.tenant_id);
        if !undo.engine_existed {
            self.crdt_engines.remove(&key);
            return Ok(());
        }
        let Some(engine) = self.crdt_engines.get_mut(&key) else {
            return Err((
                entry_index,
                format!(
                    "the CRDT engine of '{}' vanished before its write was rolled back",
                    undo.collection
                ),
            ));
        };
        engine
            .restore_collection_snapshot(&undo.collection, undo.snapshot.as_deref())
            .map_err(|e| {
                (
                    entry_index,
                    format!("restoring the CRDT collection '{}': {e}", undo.collection),
                )
            })
    }
}
