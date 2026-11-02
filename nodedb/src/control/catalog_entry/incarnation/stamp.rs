// SPDX-License-Identifier: BUSL-1.1

//! Propose-time stamps for fenced deletes and for puts whose stored type has
//! no descriptor version.

use nodedb_types::{Hlc, HlcClock};

use super::target::{Incarnation, RowKey, delete_key, with_target, written_row};
use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

/// A clock strictly above `prior`, so a mutation always orders after the row
/// it replaces even when that row came from a peer whose clock ran ahead.
pub fn after(prior: Option<Hlc>, clock: &HlcClock, now: Hlc) -> Hlc {
    match prior {
        Some(prior) if prior >= now => clock.update(prior),
        _ => now,
    }
}

/// The incarnation a collection put leaves on its row. A put carrying one
/// (ALTER, UNDROP, MOVE TENANT) keeps it. Otherwise an active prior row keeps
/// its own, and a create names a new one with the put's clock.
pub fn collection_incarnation(
    carried: Hlc,
    prior: Option<&crate::control::security::catalog::StoredCollection>,
    stamped: Hlc,
) -> Hlc {
    if carried != Hlc::ZERO {
        return carried;
    }
    match prior {
        Some(prior) if prior.is_active && prior.incarnation != Hlc::ZERO => prior.incarnation,
        _ => stamped,
    }
}

/// Freeze a fenced delete's target from the committed row it names. An absent
/// row leaves the target `UNSTAMPED`. A failed read fails the stamp.
pub fn stamp_delete(entry: CatalogEntry, catalog: &SystemCatalog) -> crate::Result<CatalogEntry> {
    let target = match delete_key(&entry) {
        Some(key) => key.read(catalog)?,
        None => return Ok(entry),
    };
    Ok(with_target(entry, target.unwrap_or(Incarnation::UNSTAMPED)))
}

/// Stamp `modification_hlc` on a put whose stored type carries no descriptor
/// version. A create-only put that finds its row keeps the existing clock,
/// since apply keeps the existing definition.
pub fn stamp_put(
    mut entry: CatalogEntry,
    clock: &HlcClock,
    catalog: &SystemCatalog,
    now: Hlc,
) -> crate::Result<CatalogEntry> {
    let prior = match written_row(&entry) {
        Some((key, _)) => key.read(catalog)?.map(|row| row.hlc),
        None => return Ok(entry),
    };
    match &mut entry {
        CatalogEntry::CreateTopicIfAbsent(def) => def.modification_hlc = prior.unwrap_or(now),
        CatalogEntry::PutSynonymGroup(row) => row.modification_hlc = after(prior, clock, now),
        CatalogEntry::PutVectorIndexParams(row) => {
            row.modification_hlc = after(prior, clock, now);
        }
        CatalogEntry::PutChangeStream(row) => row.modification_hlc = after(prior, clock, now),
        CatalogEntry::PutConsumerGroupIfAbsent(def) => def.modification_hlc = prior.unwrap_or(now),
        CatalogEntry::PutArray(row) => {
            row.modification_hlc = after(prior, clock, now);
            // A create names its incarnation. ALTER and MOVE TENANT carry the
            // incarnation of the row they replace.
            if row.incarnation == Hlc::ZERO {
                row.incarnation = row.modification_hlc;
            }
        }
        _ => {}
    }
    Ok(entry)
}

/// Name the incarnation a clone's shadow collections take: the propose clock,
/// fresh for every clone. A re-stamp keeps the named one.
pub fn stamp_clone(mut entry: CatalogEntry, now: Hlc) -> CatalogEntry {
    if let CatalogEntry::CloneDatabase { incarnation, .. } = &mut entry
        && *incarnation == Hlc::ZERO
    {
        *incarnation = now;
    }
    entry
}

/// Retarget a fenced delete at the row an earlier entry of the same batch
/// leaves behind. Committed state does not yet hold that row.
pub fn retarget_in_batch(prior: &[CatalogEntry], entry: CatalogEntry) -> CatalogEntry {
    let Some(key) = delete_key(&entry) else {
        return entry;
    };
    let target = prior
        .iter()
        .rev()
        .find_map(|earlier| batch_row(earlier, &key));
    match target {
        Some(target) => with_target(entry, target),
        None => entry,
    }
}

/// The incarnation `earlier` leaves on `key`'s row. A delete of the same row
/// leaves it absent, so a later delete in the batch applies unfenced.
fn batch_row(earlier: &CatalogEntry, key: &RowKey<'_>) -> Option<Incarnation> {
    if let Some((written, incarnation)) = written_row(earlier)
        && written == *key
    {
        return Some(incarnation);
    }
    if delete_key(earlier).as_ref() == Some(key) {
        return Some(Incarnation::UNSTAMPED);
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::DatabaseId;

    use super::*;
    use crate::control::catalog_entry::descriptor_stamp::{stamp, stamp_batch};
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn purge(name: &str) -> CatalogEntry {
        CatalogEntry::PurgeCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: name.to_string(),
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        }
    }

    fn target(entry: &CatalogEntry) -> Incarnation {
        super::super::target::carried_target(entry).expect("fenced delete")
    }

    #[test]
    fn purge_targets_the_committed_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let mut row = StoredCollection::stamped_for_test(1, "orders", "tester");
        row.descriptor_version = 3;
        row.modification_hlc = Hlc::new(30, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &row)
            .expect("seed collection");

        let stamped = stamp(purge("orders"), &HlcClock::new(), catalog).expect("stamp");
        assert_eq!(
            target(&stamped),
            Incarnation {
                descriptor_version: 3,
                hlc: Hlc::new(30, 0),
            }
        );
    }

    /// An absent row leaves the target unstamped. Apply then acknowledges the
    /// purge against any stamped row, which can only be a later incarnation.
    #[test]
    fn purge_of_an_absent_row_stays_unstamped() {
        let (store, _tmp) = make_catalog();
        let stamped = stamp(purge("orders"), &HlcClock::new(), store.catalog()).expect("stamp");
        assert_eq!(target(&stamped), Incarnation::UNSTAMPED);
    }

    /// `CREATE t; DROP t PURGE` in one transaction: committed state has no
    /// row yet, so the purge targets the create stamped before it.
    #[test]
    fn batched_purge_targets_the_preceding_create() {
        let (store, _tmp) = make_catalog();
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "orders", "tester"))),
                purge("orders"),
            ],
            &HlcClock::new(),
            store.catalog(),
        )
        .expect("stamp batch");
        let CatalogEntry::PutCollection(created) = &stamped[0] else {
            panic!("expected PutCollection");
        };
        assert_eq!(
            target(&stamped[1]),
            Incarnation {
                descriptor_version: created.descriptor_version,
                hlc: created.modification_hlc,
            }
        );
    }

    #[test]
    fn unversioned_put_orders_after_the_row_it_replaces() {
        let clock = HlcClock::new();
        let ahead = Hlc::new(u64::MAX / 2, 0);
        assert!(after(Some(ahead), &clock, clock.now()) > ahead);
        let now = clock.now();
        assert_eq!(after(None, &clock, now), now);
    }
}
