// SPDX-License-Identifier: BUSL-1.1

//! Policy fixtures shared by the redaction hook tests.

use nodedb_types::TenantId;

use crate::control::security::redaction::{
    RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
};

use super::super::query::QueryRedaction;

pub(super) fn store_with_mask(
    collection: &str,
    role: &str,
    field: &str,
    mask: &str,
) -> RedactionStore {
    let store = RedactionStore::new();
    store.create_policy(RedactionPolicy {
        name: format!("{collection}_{role}_{field}"),
        tenant_id: 1,
        collection: collection.into(),
        display_collection: collection.into(),
        for_role: role.into(),
        rules: vec![RedactionRule {
            field: field.into(),
            mode: RedactionMode::Mask(mask.into()),
        }],
    });
    store
}

pub(super) fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
    QueryRedaction::new(
        TenantId::new(1),
        vec![role.to_string()],
        vec![(String::new(), collection.to_string())],
    )
}
