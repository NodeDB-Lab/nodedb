// SPDX-License-Identifier: BUSL-1.1

//! Column redaction for the CRDT row pushes a session delivers.
//!
//! A `RowPush` carries a row post-image — the stored MessagePack map — straight
//! to a device, which persists it in its local replica. Like the shape
//! snapshot beside it, that payload never reaches the SELECT-path shaping core
//! where a query's rows are redacted, so the subscriber's rules are applied to
//! the stored bytes here through the one shared hook.
//!
//! The roles a policy is keyed on belong to the session, and the rules are
//! keyed per collection. The push gate resolves one [`RowPushRedaction`] per
//! collection per drain and reuses it for every delta of that collection.

use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::redaction::{
    QueryRedaction, redact_document_row_bytes,
};
use crate::types::{DatabaseId, TenantId};

/// The subscriber's redaction inputs for the row pushes of one collection.
pub(super) struct RowPushRedaction {
    query: QueryRedaction,
}

impl RowPushRedaction {
    /// The redaction a principal holding `roles` gets on `collection` in
    /// `database_id`.
    ///
    /// Policies are stored under the database-qualified name, the same key
    /// the snapshot path's `QueryRedaction::for_plan` resolves, so the bare
    /// name is qualified here. Outside the default database a bare key
    /// matches no policy.
    pub(super) fn new(
        tenant_id: TenantId,
        roles: Vec<String>,
        database_id: DatabaseId,
        collection: &str,
    ) -> Self {
        let qualified = nodedb_types::QualifiedCollection::new(database_id, collection);
        Self {
            query: QueryRedaction::new(
                tenant_id,
                roles,
                vec![(String::new(), qualified.as_str().to_string())],
            ),
        }
    }

    /// Redact one row image in place, reporting whether it can be sent.
    ///
    /// `false` means a rule covers the collection but the payload does not
    /// rewrite; the caller must drop the push rather than deliver it.
    pub(super) fn redact(&self, store: &RedactionStore, payload: &mut Vec<u8>) -> bool {
        redact_document_row_bytes(Some(&self.query), store, payload)
    }

    /// The first field of a shape predicate a redaction rule covers, if any.
    ///
    /// A shape predicate tests stored values, so a predicate on a redacted
    /// column discloses the column through which rows arrive. The push gate
    /// lets such a shape match nothing, as the snapshot path refuses it.
    pub(super) fn predicate_redacted_field(
        &self,
        store: &RedactionStore,
        predicate_bytes: &[u8],
    ) -> Option<String> {
        crate::control::server::sync::async_dispatch::predicate_redacted_field(
            predicate_bytes,
            &self.query,
            store,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    use super::*;

    fn store_with_mask(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn redaction_for(role: &str, collection: &str) -> RowPushRedaction {
        RowPushRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            DatabaseId::DEFAULT,
            collection,
        )
    }

    /// A policy created in a non-default database is stored under the
    /// qualified name. The push resolves the same key, so the mask applies
    /// there too, and a predicate on the masked column is reported.
    #[test]
    fn policy_in_a_non_default_database_masks_the_push() {
        let store = store_with_mask("7/users", "support", "email");
        let redaction = RowPushRedaction::new(
            TenantId::new(1),
            vec!["support".to_string()],
            DatabaseId::new(7),
            "users",
        );
        let mut payload = row("a@b.c");

        assert!(redaction.redact(&store, &mut payload));
        assert_eq!(email_of(&payload), "***");
        assert_eq!(
            redaction.predicate_redacted_field(&store, &eq_predicate("email")),
            Some("email".to_string())
        );

        // The same bare name in the default database names another collection.
        let other = redaction_for("support", "users");
        let mut clear = row("a@b.c");
        assert!(other.redact(&store, &mut clear));
        assert_eq!(email_of(&clear), "a@b.c");
    }

    fn row(email: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"email": email, "name": "Alice"}))
            .expect("encode row post-image")
    }

    fn email_of(payload: &[u8]) -> serde_json::Value {
        nodedb_types::json_from_msgpack(payload).expect("decode row post-image")["email"].clone()
    }

    fn eq_predicate(field: &str) -> Vec<u8> {
        zerompk::to_msgpack_vec(&nodedb_types::filter::MetadataFilter::Eq {
            field: field.into(),
            value: nodedb_types::Value::String("x".into()),
        })
        .expect("encode predicate")
    }

    /// The leak this type closes: a row post-image pushed to a device carried
    /// every column in the clear, and the device then persisted it.
    #[test]
    fn pushed_row_is_masked_for_a_ruled_role() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("support", "users");
        let mut payload = row("a@b.c");

        assert!(redaction.redact(&store, &mut payload));

        assert_eq!(email_of(&payload), "***");
    }

    /// The rules are keyed per collection: a collection no rule names reads
    /// in the clear for the same role.
    #[test]
    fn rules_apply_only_to_their_collection() {
        let store = store_with_mask("users", "support", "email");
        let unruled = redaction_for("support", "audit");
        let mut payload = row("d@e.f");

        assert!(unruled.redact(&store, &mut payload));
        assert_eq!(email_of(&payload), "d@e.f");
    }

    /// A role no policy names reads the pushed row in the clear, byte for
    /// byte — the fix must not perturb an installation with no policy.
    #[test]
    fn pushed_row_is_byte_identical_for_an_unruled_role() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("analyst", "users");
        let original = row("a@b.c");
        let mut payload = original.clone();

        assert!(redaction.redact(&store, &mut payload));

        assert_eq!(payload, original);
    }

    /// A delete tombstone carries no post-image, so it is delivered as-is.
    #[test]
    fn delete_tombstone_is_delivered_unchanged() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("support", "users");
        let mut payload = Vec::new();

        assert!(redaction.redact(&store, &mut payload));
        assert!(payload.is_empty());
    }

    /// A shape predicate on a redacted column is reported, so the push gate
    /// lets that shape match nothing.
    #[test]
    fn predicate_on_a_redacted_column_is_reported() {
        let store = store_with_mask("users", "support", "email");
        let redaction = redaction_for("support", "users");

        assert_eq!(
            redaction.predicate_redacted_field(&store, &eq_predicate("email")),
            Some("email".to_string())
        );
        assert_eq!(
            redaction.predicate_redacted_field(&store, &eq_predicate("name")),
            None
        );
        assert_eq!(redaction.predicate_redacted_field(&store, &[]), None);
    }
}
