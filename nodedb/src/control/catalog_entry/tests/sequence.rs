//! Sequence-family tests: roundtrip + apply semantics.

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::codec::{decode, encode};
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::tests::open_catalog;
use crate::control::security::catalog::sequence_types::StoredSequence;

#[test]
fn roundtrip_put_sequence() {
    let seq = StoredSequence::new(1, "counter".into(), "bob".into());
    let entry = CatalogEntry::PutSequence(Box::new(seq));
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::PutSequence(s) => {
            assert_eq!(s.tenant_id, 1);
            assert_eq!(s.name, "counter");
            assert_eq!(s.owner, "bob");
        }
        other => panic!("expected PutSequence, got {other:?}"),
    }
}

#[test]
fn roundtrip_delete_sequence() {
    let entry = CatalogEntry::DeleteSequence {
        tenant_id: 42,
        name: "gone".into(),
    };
    let bytes = encode(&entry).unwrap();
    match decode(&bytes).unwrap() {
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            assert_eq!(tenant_id, 42);
            assert_eq!(name, "gone");
        }
        other => panic!("expected DeleteSequence, got {other:?}"),
    }
}

#[test]
fn apply_put_then_delete_sequence() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog().as_ref().expect("catalog present");

    let seq = StoredSequence::new(1, "orders_id_seq".into(), "alice".into());
    apply_to(&CatalogEntry::PutSequence(Box::new(seq)), catalog);

    let loaded = catalog
        .get_sequence(1, "orders_id_seq")
        .unwrap()
        .expect("present");
    assert_eq!(loaded.name, "orders_id_seq");

    apply_to(
        &CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "orders_id_seq".into(),
        },
        catalog,
    );

    assert!(catalog.get_sequence(1, "orders_id_seq").unwrap().is_none());
}
