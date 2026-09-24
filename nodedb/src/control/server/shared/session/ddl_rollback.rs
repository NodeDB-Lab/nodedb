// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane restoration for a rolled-back transaction's DDL.
//!
//! Buffered collection DDL is visible to its own transaction, so the statement
//! that issued it also registers the new shape with this node's Data Plane —
//! an in-transaction write has to be encoded and enforced against the shape the
//! transaction sees. ROLLBACK discards the catalog side by dropping the buffer;
//! this puts the Data Plane back to the committed shape, so nothing of the
//! rolled-back transaction survives in `doc_configs` either.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored;
use crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::ddl_buffer::DdlBuffer;

/// One collection a rolled-back transaction touched.
type Target = (DatabaseId, u64, String);

/// Re-derive every touched collection's Data-Plane registration from the
/// committed catalog. Best-effort, like the rest of ROLLBACK: a failure is
/// logged and the remaining collections are still restored.
pub async fn restore_data_plane(state: &SharedState, buffered: &DdlBuffer) {
    let catalog = state.credentials.catalog();
    for (database_id, tenant_id, name) in targets(buffered) {
        let committed = match catalog.get_committed_collection(database_id, tenant_id, &name) {
            Ok(committed) => committed,
            Err(error) => {
                tracing::error!(
                    collection = %name,
                    tenant = tenant_id,
                    %error,
                    "rollback: catalog read failed, Data Plane keeps the rolled-back shape"
                );
                continue;
            }
        };
        let restored = match committed {
            Some(stored) => dispatch_register_from_stored(state, &stored).await,
            // Nothing committed under this name, so the registration exists
            // only because this transaction created it. Reclaim is safe: every
            // write the transaction made lived in its staging overlay.
            None => {
                dispatch_unregister_collection(
                    state,
                    database_id.as_u64(),
                    tenant_id,
                    &name,
                    state.wal.next_lsn().as_u64(),
                )
                .await
            }
        };
        if let Err(error) = restored {
            tracing::error!(
                collection = %name,
                tenant = tenant_id,
                %error,
                "rollback: Data Plane restoration failed; this node may serve the \
                 rolled-back schema until the next DDL on this collection"
            );
        }
    }
}

/// Every collection the buffer mutated, first mention first, without repeats.
fn targets(buffered: &DdlBuffer) -> Vec<Target> {
    let mut targets: Vec<Target> = Vec::new();
    for item in buffered {
        let Some(target) = target_of(&item.entry) else {
            continue;
        };
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    targets
}

/// The collection a buffered entry mutates, when it mutates one.
fn target_of(entry: &CatalogEntry) -> Option<Target> {
    match entry {
        CatalogEntry::PutCollection(stored) | CatalogEntry::PutCollectionIfAbsent(stored) => {
            Some((stored.database_id, stored.tenant_id, stored.name.clone()))
        }
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            ..
        }
        | CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
        } => Some((DatabaseId::new(*database_id), *tenant_id, name.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::server::shared::session::ddl_buffer::BufferedDdl;

    const TENANT: u64 = 3;

    fn buffered(entry: CatalogEntry) -> BufferedDdl {
        BufferedDdl {
            entry,
            audit: None,
            effects: Vec::new(),
        }
    }

    fn put(name: &str) -> BufferedDdl {
        buffered(CatalogEntry::PutCollection(Box::new(
            StoredCollection::new(TENANT, name, "alice"),
        )))
    }

    fn purge(name: &str) -> BufferedDdl {
        buffered(CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TENANT,
            name: name.to_owned(),
        })
    }

    #[test]
    fn each_collection_is_restored_once_in_first_mention_order() {
        let batch = vec![
            put("orders"),
            put("invoices"),
            put("orders"),
            purge("orders"),
        ];
        let names: Vec<String> = targets(&batch)
            .into_iter()
            .map(|(_, _, name)| name)
            .collect();
        assert_eq!(names, vec!["orders".to_owned(), "invoices".to_owned()]);
    }

    #[test]
    fn non_collection_entries_are_not_targets() {
        let batch = vec![buffered(CatalogEntry::DeleteSequence {
            database_id: 0,
            tenant_id: TENANT,
            name: "orders_seq".to_owned(),
        })];
        assert!(targets(&batch).is_empty());
    }
}
