//! Stable `kind()` label coverage across variants.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::catalog::sequence_types::StoredSequence;

#[test]
fn kind_label_is_stable() {
    assert_eq!(
        CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "a", "b"))).kind(),
        "put_collection"
    );
    assert_eq!(
        CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "a".into()
        }
        .kind(),
        "deactivate_collection"
    );
    assert_eq!(
        CatalogEntry::PutSequence(Box::new(StoredSequence::new(1, "c".into(), "b".into()))).kind(),
        "put_sequence"
    );
    assert_eq!(
        CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "c".into()
        }
        .kind(),
        "delete_sequence"
    );
}
