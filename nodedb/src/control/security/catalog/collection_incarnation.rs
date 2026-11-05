// SPDX-License-Identifier: BUSL-1.1

//! Every committed collection row names an incarnation.
//!
//! Every writer stamps a collection row before it reaches the catalog: the
//! proposer stamps each entry, with or without a metadata group, and every
//! apply that rewrites a row carries the row's own incarnation. The catalog
//! refuses a row with a zero incarnation, so none is ever committed.

use nodedb_types::{DatabaseId, Hlc};

use super::types::{StoredCollection, SystemCatalog};

/// The catalog key `collection` names in `database_id`: a plan's qualified
/// name or a bare name. Any other form is an error.
fn key_of(
    database_id: DatabaseId,
    collection: &str,
) -> Result<nodedb_types::CollectionKey<'_>, nodedb_types::CollectionKeyError> {
    nodedb_types::CollectionKey::from_relation_name(database_id, collection)
}

impl SystemCatalog {
    /// The incarnation this node's catalog holds for `collection`, as a plan
    /// names it in `database_id`, with a transaction's buffered DDL merged in.
    /// `Hlc::ZERO` when no row exists.
    pub fn incarnation_of(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Hlc> {
        let key = key_of(database_id, collection)?;
        Ok(self
            .get_collection(key.database_id(), tenant_id, key.name())?
            .map_or(Hlc::ZERO, |row| row.incarnation))
    }

    /// Whether the committed row of `collection`, as a plan names it in
    /// `database_id`, still holds `incarnation`.
    pub fn holds_incarnation(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
        incarnation: Hlc,
    ) -> crate::Result<bool> {
        let key = key_of(database_id, collection)?;
        Ok(self
            .get_committed_collection(key.database_id(), tenant_id, key.name())?
            .is_some_and(|row| row.incarnation == incarnation))
    }
}

/// Refuse `coll` when it carries no incarnation.
pub(super) fn require_incarnation(
    database_id: DatabaseId,
    coll: &StoredCollection,
) -> crate::Result<()> {
    if coll.incarnation == Hlc::ZERO {
        return Err(crate::Error::CollectionUnstamped {
            database_id: database_id.as_u64(),
            tenant_id: coll.tenant_id,
            name: coll.name.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalog refuses an unstamped row by name and writes nothing. A
    /// stamped row commits with its own incarnation.
    #[test]
    fn an_unstamped_row_is_refused() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let catalog = SystemCatalog::open(&tmp.path().join("system.redb")).expect("open");

        let unstamped = StoredCollection::new(1, "orders", "admin");
        for refused in [
            catalog.put_collection(DatabaseId::DEFAULT, &unstamped),
            catalog
                .put_collection_if_absent(DatabaseId::DEFAULT, &unstamped)
                .map(|_| ()),
        ] {
            assert!(
                matches!(
                    refused,
                    Err(crate::Error::CollectionUnstamped { ref name, .. }) if name == "orders"
                ),
                "{refused:?}"
            );
        }
        assert!(
            catalog
                .get_committed_collection(DatabaseId::DEFAULT, 1, "orders")
                .expect("read")
                .is_none()
        );

        let stamped = StoredCollection::stamped_for_test(1, "orders", "admin");
        catalog
            .put_collection(DatabaseId::DEFAULT, &stamped)
            .expect("a stamped row commits");
        let row = catalog
            .get_committed_collection(DatabaseId::DEFAULT, 1, "orders")
            .expect("read")
            .expect("row");
        assert_eq!(row.incarnation, stamped.incarnation);
        assert_eq!(row.descriptor_version, 1);
    }
}
