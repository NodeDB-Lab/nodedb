// SPDX-License-Identifier: BUSL-1.1

//! Applying redaction rules to already-decoded values.
//!
//! [`redacted_value`] is the single definition of what each [`RedactionMode`]
//! turns a field into; both the whole-document `RedactionStore::apply` and the
//! per-row [`RedactionStore::apply_flat_row`] go through it, so the mask / hash
//! / null semantics exist exactly once.
//!
//! `apply_flat_row` (JSON cells) and `apply_flat_row_typed` (typed
//! `nodedb_types::Value` cells) are the SELECT-path entry points: each
//! rewrites one already flattened result row, whose columns may come from
//! several source collections at once (a join), rather than one document
//! belonging to a single collection. Both share one rule-resolution body
//! through [`FlatRow`], and a typed cell hashes through the same JSON text
//! its wire rendering produces, so the two cannot drift.

use std::collections::BTreeMap;

use nodedb_types::Value;
use serde_json::{Map, Value as JsonValue};

use crate::util::wire_json::value_to_wire_json;

use super::store::RedactionStore;
use super::types::{RedactionMode, RedactionRule, policy_key};

/// The value `mode` produces for a field whose present value is `current`.
pub(super) fn redacted_value(mode: &RedactionMode, current: Option<&JsonValue>) -> JsonValue {
    match mode {
        RedactionMode::Mask(mask) => JsonValue::String(mask.clone()),
        RedactionMode::Hash => JsonValue::String(hash_value(current.unwrap_or(&JsonValue::Null))),
        // Writes an explicit null instead of removing the key: a redacted
        // column must still appear in a `SELECT *` result — valued null —
        // rather than disappearing from the derived column union.
        RedactionMode::Null => JsonValue::Null,
    }
}

/// SHA-256 hash for pseudonymization.
///
/// Hashes the raw scalar value, not its JSON-serialized form — a string
/// field hashes its bytes directly (not `"quoted"`), matching what an
/// operator expects `hash(email)` to mean.
fn hash_value(value: &JsonValue) -> String {
    use sha2::{Digest, Sha256};

    let digest = match value {
        JsonValue::String(s) => Sha256::digest(s.as_bytes()),
        JsonValue::Null => Sha256::digest(b""),
        other => Sha256::digest(other.to_string().as_bytes()),
    };
    format!("hash:{digest:x}")
}

/// The value `mode` produces for a typed field whose present value is
/// `current`. A mask or hash renders through the JSON path: the hash input
/// is the cell's wire JSON, exactly what the JSON row would have held.
fn redacted_typed_value(mode: &RedactionMode, current: Option<&Value>) -> Value {
    match mode {
        RedactionMode::Mask(mask) => Value::String(mask.clone()),
        RedactionMode::Hash => {
            let wire = current.map(value_to_wire_json);
            Value::String(hash_value(wire.as_ref().unwrap_or(&JsonValue::Null)))
        }
        RedactionMode::Null => Value::Null,
    }
}

/// One flattened result row, whatever its cell type.
///
/// The rule-resolution body in `RedactionStore::apply_flat_row_impl` is
/// written once against this trait; the JSON and typed row maps each
/// implement it.
trait FlatRow {
    fn is_empty(&self) -> bool;
    fn keys(&self) -> impl Iterator<Item = &str>;
    /// Rewrite `self[key]` per `mode`, if the row actually carries that key.
    fn redact_key(&mut self, key: &str, mode: &RedactionMode);
}

impl FlatRow for Map<String, JsonValue> {
    fn is_empty(&self) -> bool {
        Map::is_empty(self)
    }

    fn keys(&self) -> impl Iterator<Item = &str> {
        Map::keys(self).map(String::as_str)
    }

    fn redact_key(&mut self, key: &str, mode: &RedactionMode) {
        if !self.contains_key(key) {
            return;
        }
        let value = redacted_value(mode, self.get(key));
        self.insert(key.to_string(), value);
    }
}

impl FlatRow for BTreeMap<String, Value> {
    fn is_empty(&self) -> bool {
        BTreeMap::is_empty(self)
    }

    fn keys(&self) -> impl Iterator<Item = &str> {
        BTreeMap::keys(self).map(String::as_str)
    }

    fn redact_key(&mut self, key: &str, mode: &RedactionMode) {
        if !self.contains_key(key) {
            return;
        }
        let value = redacted_typed_value(mode, self.get(key));
        self.insert(key.to_string(), value);
    }
}

/// How many of the plan's sources a row-map key can be attributed to.
///
/// A key belongs to a source when it is that source's qualifier followed by a
/// dot. Anything else — a bare name, or a prefix matching two sources — is
/// unattributable and handled by the fail-closed pass.
fn attribution_count(key: &str, sources: &[(&str, Vec<&RedactionRule>)]) -> usize {
    sources
        .iter()
        .filter(|(qualifier, _)| {
            !qualifier.is_empty()
                && key.len() > qualifier.len()
                && key.is_char_boundary(qualifier.len())
                && key.as_bytes()[qualifier.len()] == b'.'
                && key.starts_with(*qualifier)
        })
        .count()
}

impl RedactionStore {
    /// Redact one already-flattened SELECT result row of JSON cells in place.
    ///
    /// `collections` lists the plan's source collections as
    /// `(qualifier, collection)`, where `qualifier` is the prefix that appears
    /// on that collection's keys in the row map: empty for a single-collection
    /// plan, and the join alias (or the collection name when there is no
    /// alias) for each side of a join.
    ///
    /// Matching differs by shape, and the difference is load-bearing:
    ///
    /// - **One source.** Row keys are bare field names, so a rule's `field`
    ///   matches the bare key.
    /// - **More than one source.** A rule matches ONLY the qualified key
    ///   `"{qualifier}.{field}"`. Matching the bare name here would redact the
    ///   wrong side of a join whenever both sides carry an identically named
    ///   column (`SELECT w.id, b.id`).
    /// - **Keys that cannot be attributed to exactly one source.** These are
    ///   redacted if ANY of the plan's collections has a rule for that bare
    ///   field name. This deliberately over-redacts: when the row map cannot
    ///   say which collection a column came from, delivering it in the clear
    ///   would be a policy bypass, so the ambiguous case fails closed.
    pub fn apply_flat_row(
        &self,
        tenant_id: u64,
        roles: &[String],
        collections: &[(String, String)],
        row: &mut Map<String, JsonValue>,
    ) {
        self.apply_flat_row_impl(tenant_id, roles, collections, row);
    }

    /// Redact one already-flattened SELECT result row of typed cells in
    /// place. Same matching rules as [`RedactionStore::apply_flat_row`]; a
    /// `Null` mode writes `Value::Null`, a mask or hash writes the same text
    /// the JSON path writes.
    pub fn apply_flat_row_typed(
        &self,
        tenant_id: u64,
        roles: &[String],
        collections: &[(String, String)],
        row: &mut BTreeMap<String, Value>,
    ) {
        self.apply_flat_row_impl(tenant_id, roles, collections, row);
    }

    fn apply_flat_row_impl<R: FlatRow>(
        &self,
        tenant_id: u64,
        roles: &[String],
        collections: &[(String, String)],
        row: &mut R,
    ) {
        if roles.is_empty() || collections.is_empty() || row.is_empty() {
            return;
        }

        let policies = self.lock_read();
        let mut sources: Vec<(&str, Vec<&RedactionRule>)> = Vec::with_capacity(collections.len());
        for (qualifier, collection) in collections {
            let mut rules: Vec<&RedactionRule> = Vec::new();
            for role in roles {
                if let Some(policy) = policies.get(&policy_key(tenant_id, collection, role)) {
                    rules.extend(policy.rules.iter());
                }
            }
            sources.push((qualifier.as_str(), rules));
        }
        if sources.iter().all(|(_, rules)| rules.is_empty()) {
            return;
        }

        if let [(_, rules)] = sources.as_slice() {
            for rule in rules {
                row.redact_key(&rule.field, &rule.mode);
            }
            return;
        }

        for (qualifier, rules) in &sources {
            for rule in rules {
                row.redact_key(&format!("{qualifier}.{}", rule.field), &rule.mode);
            }
        }

        // Fail-closed pass over the keys no single source owns.
        let unattributed: Vec<String> = row
            .keys()
            .filter(|key| attribution_count(key, &sources) != 1)
            .map(str::to_owned)
            .collect();
        for key in unattributed {
            let bare = key.rfind('.').map_or(key.as_str(), |dot| &key[dot + 1..]);
            let mode = sources
                .iter()
                .flat_map(|(_, rules)| rules.iter())
                .find(|rule| rule.field == bare || rule.field == key)
                .map(|rule| rule.mode.clone());
            if let Some(mode) = mode {
                row.redact_key(&key, &mode);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::types::RedactionPolicy;
    use super::*;

    fn store_with(collection: &str, role: &str, rules: Vec<RedactionRule>) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules,
        });
        store
    }

    fn mask(field: &str, with: &str) -> RedactionRule {
        RedactionRule {
            field: field.into(),
            mode: RedactionMode::Mask(with.into()),
        }
    }

    fn row(value: serde_json::Value) -> Map<String, JsonValue> {
        match value {
            JsonValue::Object(map) => map,
            other => panic!("test row must be an object, got {other}"),
        }
    }

    #[test]
    fn single_source_matches_bare_keys() {
        let store = store_with("users", "support", vec![mask("email", "***")]);
        let mut r = row(json!({"email": "a@b.c", "name": "Alice"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r["email"], "***");
        assert_eq!(r["name"], "Alice");
    }

    #[test]
    fn role_without_the_policy_sees_the_clear_value() {
        let store = store_with("users", "support", vec![mask("email", "***")]);
        let mut r = row(json!({"email": "a@b.c"}));
        store.apply_flat_row(
            1,
            &["analyst".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r["email"], "a@b.c");
    }

    /// The rule belongs to the left side only; the right side's identically
    /// named column must survive in the clear.
    #[test]
    fn join_matches_only_the_ruled_sides_qualified_key() {
        let store = store_with("workspaces", "support", vec![mask("id", "***")]);
        let mut r = row(json!({"w.id": "w1", "b.id": "b1"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[
                ("w".into(), "workspaces".into()),
                ("b".into(), "boards".into()),
            ],
            &mut r,
        );
        assert_eq!(r["w.id"], "***");
        assert_eq!(r["b.id"], "b1");
    }

    /// A join whose row map carries a bare key cannot say which side it came
    /// from, so the rule applies rather than being skipped.
    #[test]
    fn join_fails_closed_on_unattributable_keys() {
        let store = store_with("workspaces", "support", vec![mask("id", "***")]);
        let mut r = row(json!({"id": "w1", "b.title": "t"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[
                ("w".into(), "workspaces".into()),
                ("b".into(), "boards".into()),
            ],
            &mut r,
        );
        assert_eq!(r["id"], "***");
        assert_eq!(r["b.title"], "t");
    }

    #[test]
    fn null_mode_keeps_the_key_present() {
        let store = store_with(
            "users",
            "support",
            vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Null,
            }],
        );
        let mut r = row(json!({"email": "a@b.c"}));
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert!(r.contains_key("email"), "the column must not disappear");
        assert_eq!(r["email"], JsonValue::Null);
    }

    fn typed_row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// The typed entry point resolves rules exactly as the JSON one: same
    /// mask on the ruled side of a join, clear value on the other, and a
    /// `Null` mode keeps the key present.
    #[test]
    fn typed_row_matches_the_json_rules() {
        let store = store_with(
            "workspaces",
            "support",
            vec![
                mask("id", "***"),
                RedactionRule {
                    field: "note".into(),
                    mode: RedactionMode::Null,
                },
            ],
        );
        let mut r = typed_row(&[
            ("w.id", Value::String("w1".into())),
            ("b.id", Value::String("b1".into())),
            ("w.note", Value::Integer(7)),
        ]);
        store.apply_flat_row_typed(
            1,
            &["support".into()],
            &[
                ("w".into(), "workspaces".into()),
                ("b".into(), "boards".into()),
            ],
            &mut r,
        );
        assert_eq!(r["w.id"], Value::String("***".into()));
        assert_eq!(r["b.id"], Value::String("b1".into()));
        assert_eq!(r["w.note"], Value::Null);
    }

    /// A hashed typed cell yields the digest the JSON path yields for the
    /// same cell: a string hashes its bytes, a number hashes its JSON text,
    /// and bytes hash their base64 wire text.
    #[test]
    fn typed_hash_matches_the_json_hash() {
        let store = store_with(
            "users",
            "support",
            vec![
                RedactionRule {
                    field: "email".into(),
                    mode: RedactionMode::Hash,
                },
                RedactionRule {
                    field: "n".into(),
                    mode: RedactionMode::Hash,
                },
                RedactionRule {
                    field: "blob".into(),
                    mode: RedactionMode::Hash,
                },
            ],
        );
        let roles = ["support".to_string()];
        let sources = [(String::new(), "users".to_string())];

        let mut typed = typed_row(&[
            ("email", Value::String("a@b.c".into())),
            ("n", Value::Integer(42)),
            ("blob", Value::Bytes(vec![0, 255, 7])),
        ]);
        store.apply_flat_row_typed(1, &roles, &sources, &mut typed);

        let mut json = row(json!({"email": "a@b.c", "n": 42, "blob": "AP8H"}));
        store.apply_flat_row(1, &roles, &sources, &mut json);

        for key in ["email", "n", "blob"] {
            assert_eq!(
                typed[key],
                Value::String(json[key].as_str().expect("hashed text").to_string()),
                "{key}"
            );
        }
    }

    #[test]
    fn no_policy_leaves_the_row_untouched() {
        let store = RedactionStore::new();
        let original = row(json!({"email": "a@b.c", "name": "Alice"}));
        let mut r = original.clone();
        store.apply_flat_row(
            1,
            &["support".into()],
            &[(String::new(), "users".into())],
            &mut r,
        );
        assert_eq!(r, original);
    }
}
