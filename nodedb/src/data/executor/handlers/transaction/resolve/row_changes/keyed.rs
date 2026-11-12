// SPDX-License-Identifier: BUSL-1.1

//! Net changes of engines whose change events name each row: documents
//! (plain and bitemporal), CRDT documents, KV rows and vector-primary rows.
//!
//! Each row is named once, by the identity its autocommit event carries, with
//! the net kind of every write the transaction made to it.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};
use nodedb_types::{RowIdentity, StorageKey, Surrogate};

use super::super::vector_primary::VectorPrimaryCollections;
use super::entry::ResolveScope;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, TxnOverlay};
use crate::data::executor::handlers::transaction::stage_write::unhex_key;
use crate::types::{DatabaseId, TenantId};
use crate::wal::{EVERY_ROW, RedoRowChange, RedoRowKind};

type CollKey = (DatabaseId, TenantId, String);

/// One collection's staged rows and whether a TRUNCATE hid its base.
struct StagedCollection {
    truncated: bool,
    rows: BTreeMap<RowIdentity, RowState>,
}

/// A staged row's final state.
#[derive(Clone, Copy)]
enum RowState {
    /// A value or a tombstone, staged under this surrogate.
    Written { surrogate: u32, holds_value: bool },
    /// Only a TTL delta on a committed KV row.
    TtlOnly,
}

impl CoreLoop {
    /// Document rows. A bitemporal collection's row existed when it has a
    /// current version.
    pub(in crate::data::executor) fn document_row_changes(
        &self,
        scope: ResolveScope,
        collections: &BTreeSet<String>,
        changes: &mut Vec<RedoRowChange>,
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
        changes: &mut Vec<RedoRowChange>,
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
        let mut named: BTreeSet<(String, String)> = BTreeSet::new();
        for collection in &staged_collections {
            let first = changes.len();
            self.overlay_document_changes(scope, collection, changes)?;
            for change in &changes[first..] {
                named.insert((change.collection.clone(), change.row.clone()));
            }
        }
        for row in list_rows {
            if !named.contains(&row) {
                changes.push(RedoRowChange {
                    collection: row.0,
                    row: row.1,
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
        changes: &mut Vec<RedoRowChange>,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        let now_ms = self.kv_read_now_ms();
        for collection in collections {
            let staged = staged_collection(overlay, &coll_key(scope, collection))?;
            push_truncate(collection, staged.truncated, changes);
            for (identity, state) in &staged.rows {
                let key = unhex_key(identity.as_str()).ok_or_else(|| crate::Error::Internal {
                    detail: format!("kv resolve: overlay row '{identity}' is not valid hex"),
                })?;
                let existed = !staged.truncated
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
                let kind = match *state {
                    RowState::Written { holds_value, .. } => RedoRowKind::net(existed, holds_value),
                    RowState::TtlOnly if existed => RedoRowKind::Update,
                    RowState::TtlOnly => RedoRowKind::NoChange,
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
        changes: &mut Vec<RedoRowChange>,
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
            let rows: BTreeMap<u32, bool> = overlay
                .iter_for_collection(&key)
                .map(|item| {
                    item.map(|(surrogate, staged)| (surrogate, matches!(&*staged, Staged::Put(_))))
                })
                .collect::<crate::Result<_>>()?;
            for (surrogate, holds_value) in rows {
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
        changes: &mut Vec<RedoRowChange>,
    ) -> crate::Result<()> {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        let staged = staged_collection(overlay, &coll_key(scope, collection))?;
        push_truncate(collection, staged.truncated, changes);
        for (identity, state) in &staged.rows {
            let RowState::Written {
                surrogate,
                holds_value,
            } = *state
            else {
                continue;
            };
            let existed = !staged.truncated
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
pub(super) fn push_truncate(collection: &str, truncated: bool, changes: &mut Vec<RedoRowChange>) {
    if truncated {
        changes.push(RedoRowChange {
            collection: collection.to_owned(),
            row: EVERY_ROW.to_owned(),
            kind: RedoRowKind::Delete,
        });
    }
}

/// The rows `overlay` staged under `key`, by identity.
fn staged_collection(overlay: &TxnOverlay, key: &CollKey) -> crate::Result<StagedCollection> {
    let truncated = overlay.is_truncated(key);
    let mut rows = BTreeMap::new();
    for item in overlay.iter_staged_bindings_for_collection(key) {
        let (identity, surrogate, staged) = item?;
        let holds_value = matches!(&*staged, Staged::Put(_));
        rows.insert(
            identity,
            RowState::Written {
                surrogate,
                holds_value,
            },
        );
    }
    if !truncated {
        for item in overlay.iter_ttl_only_for_collection(key) {
            let (identity, _ttl) = item?;
            rows.insert(identity, RowState::TtlOnly);
        }
    }
    Ok(StagedCollection { truncated, rows })
}
