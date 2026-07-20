// SPDX-License-Identifier: BUSL-1.1

use crate::control::catalog_entry::apply::apply_to;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::tests::open_catalog;
use crate::control::security::catalog::StoredContinuousAggregate;

fn stored(database_id: u64, owner: &str) -> StoredContinuousAggregate {
    StoredContinuousAggregate {
        database_id,
        tenant_id: 1,
        name: "shared".into(),
        source: "events".into(),
        def_bytes: Vec::new(),
        owner: owner.into(),
        created_at: 0,
        descriptor_version: 0,
        modification_hlc: Default::default(),
    }
}

#[test]
fn delete_is_scoped_to_database() {
    let (credentials, _tmp) = open_catalog();
    let catalog = credentials.catalog();
    apply_to(
        &CatalogEntry::PutContinuousAggregate(Box::new(stored(0, "default_owner"))),
        catalog,
    );
    apply_to(
        &CatalogEntry::PutContinuousAggregate(Box::new(stored(9, "other_owner"))),
        catalog,
    );

    apply_to(
        &CatalogEntry::DeleteContinuousAggregate {
            database_id: 9,
            tenant_id: 1,
            name: "shared".into(),
        },
        catalog,
    );

    assert!(
        catalog
            .get_continuous_aggregate(9, 1, "shared")
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .get_continuous_aggregate(0, 1, "shared")
            .unwrap()
            .is_some()
    );
    let owners = catalog.load_all_owners().unwrap();
    assert!(!owners.iter().any(|owner| owner.database_id == 9));
    assert!(owners.iter().any(|owner| owner.database_id == 0));
}
