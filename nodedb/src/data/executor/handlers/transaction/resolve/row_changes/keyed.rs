// SPDX-License-Identifier: BUSL-1.1

//! Net changes of engines whose change events name each row: documents
//! (plain and bitemporal), CRDT documents, KV rows and vector-primary rows.
//!
//! Each row is named once, by the identity its autocommit event carries, with
//! the net kind of every write the transaction made to it.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};
use nodedb_types::{RowIdentity, StorageKey, Surrogate};

use super::super::sink::ChangeSink;
use super::super::vector_primary::VectorPrimaryCollections;
use super::entry::ResolveScope;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedSlot};
use crate::data::executor::handlers::transaction::stage_write::unhex_key;
use crate::types::{DatabaseId, TenantId};
use crate::wal::{EVERY_ROW, RedoRowChange, RedoRowKind};

type CollKey = (DatabaseId, TenantId, String);

impl CoreLoop {
    /// Document rows. A bitemporal collection's row existed when it has a
    /// current version.
    pub(in crate::data::executor) fn document_row_changes(
        &self,
        scope: ResolveScope,
        collections: &BTreeSet<String>,
        changes: &mut dyn ChangeSink,
    ) -> crate::Result<()> {
        for collection in collections {
            self.overlay_document_changes(scope, collection, changes)?;
        }
        Ok(())
    }

    /// CRDT document rows. A row upsert or delete stages the row in the
    /// overlay, and the collection's materialized rows are its committed
    /// state. A block-list mutation of a row the transaction did not
    /// otherwise write updates it. A snapshot import updates every row.
    pub(in crate::data::executor) fn crdt_row_changes(
        &self,
        scope: ResolveScope,
        plans: &[PhysicalPlan],
        changes: &mut dyn ChangeSink,
    ) -> crate::Result<()> {
        let mut staged_collections = BTreeSet::new();
        let mut list_rows: BTreeSet<(String, String)> = BTreeSet::new();
        let mut imported = BTreeSet::new();
        for plan in plans {
            let PhysicalPlan::Crdt(op) = plan else {
                continue;
            };
            match op {
                CrdtOp::DocUpsert { collection, .. } | CrdtOp::DocDelete { collection, .. } => {
                    staged_collections.insert(collection.to_string());
                }
                CrdtOp::ListInsert {
                    collection,
                    document_id,
                    ..
                }
                | CrdtOp::ListDelete {
                    collection,
                    document_id,
                    ..
                }
                | CrdtOp::ListMove {
                    collection,
                    document_id,
                    ..
                } => {
                    list_rows.insert((collection.to_string(), document_id.clone()));
                }
                CrdtOp::ImportSnapshot { collection, .. } => {
                    imported.insert(collection.to_string());
                }
                // Reads, policy and constraint changes and history maintenance
                // change no row. A raw delta is refused inside a transaction.
                _ => {}
            }
        }
        for collection in &staged_collections {
            self.overlay_document_changes(scope, collection, changes)?;
        }
        // A list row a staged upsert or delete of its collection already
        // named takes no second entry.
        let overlay = self.txn_overlays.get(&scope.txn_id);
        for (collection, document_id) in list_rows {
            let named = match overlay {
                Some(overlay) if staged_collections.contains(&collection) => overlay
                    .get_by_doc_id(
                        &coll_key(scope, &collection),
                        &RowIdentity::from_user_key(document_id.as_str()),
                    )?
                    .is_some(),
                Some(_) | None => false,
            };
            if !named {
                changes.push(RedoRowChange {
                    collection,
                    row: document_id,
                    kind: RedoRowKind::Update,
                });
            }
        }
        for collection in imported {
            changes.push(RedoRowChange {
                collection,
                row: EVERY_ROW.to_owned(),
                kind: RedoRowKind::Update,
            });
        }
        Ok(())
    }

    /// KV rows, named by their key as text. A TTL change updates a committed
    /// row and changes nothing on a missing one.
    pub(in crate::data::executor) fn kv_row_changes(
        &self,
        scope: ResolveScope,
        collections: &BTreeSet<String>,
        changes: &mut dyn ChangeSink,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        let now_ms = self.kv_read_now_ms();
        for collection in collections {
            let coll = coll_key(scope, collection);
            let truncated = overlay.is_truncated(&coll);
            push_truncate(collection, truncated, changes);
            for slot in overlay.iter_slots_for_collection(&coll, !truncated) {
                let (identity, slot) = slot?;
                let key = unhex_key(identity.as_str()).ok_or_else(|| crate::Error::Internal {
                    detail: format!("kv resolve: overlay row '{identity}' is not valid hex"),
                })?;
                let existed = !truncated
                    && self
                        .kv_engine
                        .get(
                            scope.database_id.as_u64(),
                            scope.tid,
                            collection,
                            &key,
                            now_ms,
                        )
                        .is_some();
                let kind = match slot {
                    StagedSlot::Written { staged, .. } => {
                        RedoRowKind::net(existed, matches!(&*staged, Staged::Put(_)))
                    }
                    StagedSlot::TtlOnly(_) if existed => RedoRowKind::Update,
                    StagedSlot::TtlOnly(_) => RedoRowKind::NoChange,
                };
                changes.push(RedoRowChange {
                    collection: collection.clone(),
                    row: String::from_utf8_lossy(&key).into_owned(),
                    kind,
                });
            }
        }
        Ok(())
    }

    /// Vector-primary rows, named by their surrogate's identity. A row
    /// existed when the collection's index binds its surrogate.
    pub(in crate::data::executor::handlers::transaction::resolve) fn vector_primary_row_changes(
        &self,
        scope: ResolveScope,
        collections: &VectorPrimaryCollections,
        changes: &mut dyn ChangeSink,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        for (collection, writes) in collections {
            let key = coll_key(scope, collection);
            let truncated = overlay.is_truncated(&key);
            push_truncate(collection, truncated, changes);
            let index_key = CoreLoop::vector_index_key(
                scope.database_id.as_u64(),
                scope.tid,
                collection,
                &writes.field,
            );
            // The scan yields ascending surrogates.
            for item in overlay.iter_for_collection(&key) {
                let (surrogate, staged) = item?;
                let holds_value = matches!(&*staged, Staged::Put(_));
                let surrogate = Surrogate::new(surrogate);
                let existed =
                    !truncated && self.vector_direct_node(&index_key, surrogate).is_some();
                changes.push(RedoRowChange {
                    collection: collection.clone(),
                    row: StorageKey::for_surrogate(surrogate)
                        .to_identity()
                        .as_str()
                        .to_owned(),
                    kind: RedoRowKind::net(existed, holds_value),
                });
            }
        }
        Ok(())
    }

    /// The staged rows of one document or CRDT collection, against its
    /// committed rows.
    fn overlay_document_changes(
        &self,
        scope: ResolveScope,
        collection: &str,
        changes: &mut dyn ChangeSink,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        let key = coll_key(scope, collection);
        let truncated = overlay.is_truncated(&key);
        push_truncate(collection, truncated, changes);
        for slot in overlay.iter_slots_for_collection(&key, false) {
            let (identity, slot) = slot?;
            let StagedSlot::Written { surrogate, staged } = slot else {
                continue;
            };
            let holds_value = matches!(&*staged, Staged::Put(_));
            let existed = !truncated
                && self.document_exists(
                    scope.database_id.as_u64(),
                    scope.tid,
                    collection,
                    surrogate,
                )?;
            changes.push(RedoRowChange {
                collection: collection.to_owned(),
                row: identity.as_str().to_owned(),
                kind: RedoRowKind::net(existed, holds_value),
            });
        }
        Ok(())
    }

    /// Whether `collection` holds a committed row under `surrogate`: the
    /// current version on a bitemporal collection.
    fn document_exists(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<bool> {
        let key = StorageKey::for_surrogate(Surrogate::new(surrogate));
        Ok(self
            .current_row_body(database_id, tid, collection, &key)?
            .is_some())
    }
}

fn coll_key(scope: ResolveScope, collection: &str) -> CollKey {
    (
        scope.database_id,
        TenantId::new(scope.tid),
        collection.to_owned(),
    )
}

/// Append the whole-collection delete a staged TRUNCATE makes.
pub(super) fn push_truncate(collection: &str, truncated: bool, changes: &mut dyn ChangeSink) {
    if truncated {
        changes.push(RedoRowChange {
            collection: collection.to_owned(),
            row: EVERY_ROW.to_owned(),
            kind: RedoRowKind::Delete,
        });
    }
}
