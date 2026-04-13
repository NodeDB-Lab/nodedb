//! Collection-family tests: roundtrip + apply semantics.

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::codec::{decode, encode};
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::tests::open_catalog;
use crate::control::security::catalog::StoredCollection;

#[test]
fn roundtrip_put_collection() {
    let stored = StoredCollection::new(7, "orders", "alice");
    let entry = CatalogEntry::PutCollection(Box::new(stored));
    let bytes = encode(&entry).expect("encode");
    let decoded = decode(&bytes).expect("decode");
    match decoded {
        CatalogEntry::PutCollection(s) => {
            assert_eq!(s.tenant_id, 7);
            assert_eq!(s.name, "orders");
            assert_eq!(s.owner, "alice");
        }
        other => panic!("expected PutCollection, got {other:?}"),
    }
}

#[test]
fn roundtrip_deactivate_collection() {
    let entry = CatalogEntry::DeactivateCollection {
        tenant_id: 3,
        name: "legacy".into(),
    };
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            assert_eq!(tenant_id, 3);
            assert_eq!(name, "legacy");
        }
        other => panic!("expected DeactivateCollection, got {other:?}"),
    }
}

#[test]
fn apply_put_collection_writes_redb() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    let stored = StoredCollection::new(1, "widgets", "carol");
    apply_to(&CatalogEntry::PutCollection(Box::new(stored)), catalog);

    let loaded = catalog
        .get_collection(1, "widgets")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "widgets");
    assert_eq!(loaded.owner, "carol");
    assert!(loaded.is_active);
}

#[test]
fn apply_deactivate_collection_preserves_record() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    let stored = StoredCollection::new(1, "archived", "carol");
    catalog.put_collection(&stored).unwrap();

    apply_to(
        &CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "archived".into(),
        },
        catalog,
    );

    let loaded = catalog
        .get_collection(1, "archived")
        .unwrap()
        .expect("record preserved");
    assert!(!loaded.is_active);
}

#[test]
fn apply_deactivate_missing_is_noop() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");
    apply_to(
        &CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "ghost".into(),
        },
        catalog,
    );
    assert!(catalog.get_collection(1, "ghost").unwrap().is_none());
}
