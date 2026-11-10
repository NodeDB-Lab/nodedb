// SPDX-License-Identifier: BUSL-1.1

//! Immutable redaction rules for one authenticated response scope.

use std::collections::BTreeMap;

use nodedb_types::{TenantId, Value};

use super::apply::redacted_typed_value;
use super::store::RedactionStore;
use super::types::{RedactionRule, policy_key};

/// Owned policy state captured before response evaluation.
#[derive(Debug, Clone)]
pub struct RedactionSnapshot {
    tenant_id: TenantId,
    collection: String,
    roles: Vec<String>,
    version: u64,
    rules: Vec<RedactionRule>,
}

impl RedactionStore {
    /// Capture rules using the authenticated tenant, collection, and ordered roles.
    /// The read lock covers both the rules and their policy version.
    pub fn snapshot_rules(
        &self,
        tenant_id: TenantId,
        collection: &str,
        roles: &[String],
    ) -> RedactionSnapshot {
        let policies = self.lock_read();
        let rules = roles
            .iter()
            .flat_map(|role| {
                policies
                    .get(&policy_key(tenant_id.as_u64(), collection, role))
                    .into_iter()
                    .flat_map(|policy| policy.rules.iter().cloned())
            })
            .collect();
        RedactionSnapshot {
            tenant_id,
            collection: collection.to_owned(),
            roles: roles.to_vec(),
            version: self.version(),
            rules,
        }
    }
}

impl RedactionSnapshot {
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
    pub fn collection(&self) -> &str {
        &self.collection
    }
    pub fn roles(&self) -> &[String] {
        &self.roles
    }
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Mask source fields before aliases or computed expressions exist.
    pub fn apply(&self, row: &mut BTreeMap<String, Value>) {
        for rule in &self.rules {
            if let Some(current) = row.get(&rule.field) {
                let value = redacted_typed_value(&rule.mode, Some(current));
                row.insert(rule.field.clone(), value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{RedactionMode, RedactionPolicy};
    use super::*;

    fn policy(role: &str, modes: Vec<RedactionMode>) -> RedactionPolicy {
        RedactionPolicy {
            name: role.into(),
            tenant_id: 7,
            collection: "users".into(),
            display_collection: "users".into(),
            for_role: role.into(),
            rules: modes
                .into_iter()
                .map(|mode| RedactionRule {
                    field: "secret".into(),
                    mode,
                })
                .collect(),
        }
    }

    #[test]
    fn snapshot_preserves_repeated_roles_rule_order_and_policy_version() {
        let store = RedactionStore::new();
        store.create_policy(policy(
            "a",
            vec![RedactionMode::Mask("masked".into()), RedactionMode::Hash],
        ));
        store.create_policy(policy("b", vec![RedactionMode::Null]));
        let roles = vec!["a".into(), "b".into(), "a".into()];
        let snapshot = store.snapshot_rules(TenantId::new(7), "users", &roles);
        let mut json = serde_json::json!({"secret": "raw"});
        store.apply(7, "users", &roles, &mut json);
        store.drop_policy(7, "users", "a");
        let mut row = BTreeMap::from([("secret".into(), Value::String("raw".into()))]);
        snapshot.apply(&mut row);
        assert_eq!(row["secret"], Value::from(json["secret"].clone()));
        assert_eq!(snapshot.version(), 2);
        assert_eq!(snapshot.roles(), roles);
        assert_eq!(snapshot.tenant_id(), TenantId::new(7));
        assert_eq!(snapshot.collection(), "users");
    }

    #[test]
    fn snapshot_is_tenant_scoped_and_does_not_create_absent_fields() {
        let store = RedactionStore::new();
        store.create_policy(policy("a", vec![RedactionMode::Null]));
        let snapshot = store.snapshot_rules(TenantId::new(8), "users", &["a".into()]);
        let mut row = BTreeMap::from([("secret".into(), Value::Integer(9))]);
        snapshot.apply(&mut row);
        assert_eq!(row["secret"], Value::Integer(9));
        row.clear();
        store
            .snapshot_rules(TenantId::new(7), "users", &["a".into()])
            .apply(&mut row);
        assert!(row.is_empty());
    }
}
