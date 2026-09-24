// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay for index registry records.
//!
//! See [`super::collection`] for the mechanism this mirrors: a `CREATE
//! INDEX` buffered inside an open transaction must be visible to a later
//! `DROP INDEX` or read in that same transaction, before COMMIT ever writes
//! the row to redb.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::index_record::StoredIndexRecord;

/// True when `entry` mutates the index `(database_id, tenant_id, name)`.
fn targets(entry: &CatalogEntry, database_id: u64, tenant_id: u64, name: &str) -> bool {
    match entry {
        CatalogEntry::PutIndexRecord(stored) => {
            stored.database_id == database_id
                && stored.tenant_id == tenant_id
                && stored.name == name
        }
        CatalogEntry::DeleteIndexRecord {
            database_id: entry_db,
            tenant_id: entry_tenant,
            name: entry_name,
            ..
        } => *entry_db == database_id && *entry_tenant == tenant_id && entry_name == name,
        _ => false,
    }
}

/// Replay one buffered entry over the state resolved so far.
fn step(current: Option<StoredIndexRecord>, entry: &CatalogEntry) -> Option<StoredIndexRecord> {
    match entry {
        CatalogEntry::PutIndexRecord(stored) => Some((**stored).clone()),
        CatalogEntry::DeleteIndexRecord { .. } => None,
        _ => current,
    }
}

/// Resolve one index record through this connection's uncommitted DDL.
pub fn resolve_index_record(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    committed: Option<StoredIndexRecord>,
) -> Option<StoredIndexRecord> {
    super::core::resolve(
        committed,
        |entry| targets(entry, database_id, tenant_id, name),
        step,
    )
}

/// Every index record of one `(database, tenant)`, with this connection's
/// uncommitted DDL replayed over the committed list in statement order. A
/// buffered create is listed, a buffered drop is not, and the result stays
/// in name order.
pub fn resolve_index_records(
    database_id: u64,
    tenant_id: u64,
    committed: Vec<StoredIndexRecord>,
) -> Vec<StoredIndexRecord> {
    let replayed = crate::control::server::shared::session::ddl_buffer::with_buffered(|buffered| {
        let mut by_name: std::collections::BTreeMap<String, StoredIndexRecord> = committed
            .iter()
            .map(|record| (record.name.clone(), record.clone()))
            .collect();
        let mut touched = false;
        for item in buffered {
            match &item.entry {
                CatalogEntry::PutIndexRecord(stored)
                    if stored.database_id == database_id && stored.tenant_id == tenant_id =>
                {
                    by_name.insert(stored.name.clone(), (**stored).clone());
                    touched = true;
                }
                CatalogEntry::DeleteIndexRecord {
                    database_id: entry_db,
                    tenant_id: entry_tenant,
                    name,
                    ..
                } if *entry_db == database_id && *entry_tenant == tenant_id => {
                    by_name.remove(name);
                    touched = true;
                }
                _ => {}
            }
        }
        touched.then(|| by_name.into_values().collect::<Vec<_>>())
    });
    match replayed {
        Some(Some(records)) => records,
        Some(None) | None => committed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::index_record::IndexKind;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};

    fn stored(name: &str) -> StoredIndexRecord {
        StoredIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_owned(),
            kind: IndexKind::Vector,
            collection: "docs".to_owned(),
            fields: vec!["embedding".to_owned()],
            is_active: true,
        }
    }

    fn put(name: &str) -> CatalogEntry {
        CatalogEntry::PutIndexRecord(Box::new(stored(name)))
    }

    fn delete(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteIndexRecord {
            database_id: 0,
            tenant_id: 1,
            name: name.to_owned(),
            collection: "docs".to_owned(),
        }
    }

    fn resolve(name: &str, committed: Option<StoredIndexRecord>) -> Option<StoredIndexRecord> {
        resolve_index_record(0, 1, name, committed)
    }

    #[tokio::test]
    async fn a_buffered_create_is_visible_to_the_same_transaction() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            assert!(ddl_buffer::try_buffer(put("idx_a")));
            let resolved = resolve("idx_a", None).expect("buffered create resolves");
            assert_eq!(resolved.name, "idx_a");
        })
        .await;
    }

    #[tokio::test]
    async fn create_then_drop_in_one_transaction_resolves_to_nothing() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("idx_a"));
            ddl_buffer::try_buffer(delete("idx_a"));
            assert!(resolve("idx_a", None).is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn the_listing_shows_a_buffered_create_and_hides_a_buffered_drop() {
        conn_scope::scoped(async {
            ddl_buffer::activate();
            ddl_buffer::try_buffer(put("idx_new"));
            ddl_buffer::try_buffer(delete("idx_old"));
            let names: Vec<String> = resolve_index_records(0, 1, vec![stored("idx_old")])
                .into_iter()
                .map(|record| record.name)
                .collect();
            assert_eq!(names, vec!["idx_new".to_string()]);
        })
        .await;
    }

    #[tokio::test]
    async fn outside_a_transaction_the_committed_row_wins() {
        conn_scope::scoped(async {
            assert!(resolve("idx_a", None).is_none());
            assert!(resolve("idx_a", Some(stored("idx_a"))).is_some());
        })
        .await;
    }
}
