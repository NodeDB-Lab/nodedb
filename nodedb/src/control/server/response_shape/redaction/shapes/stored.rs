// SPDX-License-Identifier: BUSL-1.1

//! Redaction of stored row bytes handed to a client verbatim.

use crate::control::security::redaction::RedactionStore;

use super::super::query::QueryRedaction;

/// Redact one KV-stored value in place, preserving its storage encoding.
///
/// The Redis-wire surface hands the client the stored bytes themselves rather
/// than a decoded row map, so neither [`redact_envelope_row`](super::redact_envelope_row) nor
/// [`redact_rows_payload`] can reach the fields inside them. The KV engine
/// stores exactly the two shapes its scan handler decodes, and both are
/// matched here the way that handler presents them to the SELECT path:
///
/// - A msgpack map of typed columns — its keys ARE the stored field names, so
///   the rules match them directly.
/// - Opaque bytes from a single-value put — the scan handler wraps these as
///   `{value: <bytes>}` before any SQL-side read sees them, so the rule that
///   covers them is the one naming the column `value`.
///
/// The bytes are rewritten only when a rule actually changed something: a read
/// under no policy hands back the identical bytes it was given, never a
/// re-encoded round trip. A value that a rule DOES cover but that cannot be
/// re-encoded is cleared rather than delivered — an unreadable result is a
/// recoverable client error, an unredacted one is a policy bypass.
pub fn redact_stored_value_bytes(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    value: &mut Vec<u8>,
) {
    let Some(resolved) = redaction else {
        return;
    };
    if value.is_empty() || !resolved.has_any_rule(store) {
        return;
    }
    let ctx = resolved.ctx(store);

    // Same discriminator the Data-Plane KV scan handler uses to tell the two
    // storage shapes apart, so a value is matched under the same column names
    // on both paths.
    if nodedb_query::msgpack_scan::map_header(value, 0).is_some() {
        let Ok(serde_json::Value::Object(fields)) = nodedb_types::json_from_msgpack(value) else {
            value.clear();
            return;
        };
        let mut redacted = fields.clone();
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut redacted);
        if redacted == fields {
            return;
        }
        match nodedb_types::json_to_msgpack(&serde_json::Value::Object(redacted)) {
            Ok(bytes) => *value = bytes,
            Err(_) => value.clear(),
        }
        return;
    }

    let original = String::from_utf8_lossy(value).into_owned();
    let mut row = serde_json::Map::with_capacity(1);
    row.insert(
        "value".to_string(),
        serde_json::Value::String(original.clone()),
    );
    ctx.store
        .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut row);
    match row.get("value") {
        // A `Null` rule leaves no value to deliver, so the read presents as a
        // miss rather than as the literal text `null`.
        Some(serde_json::Value::Null) => value.clear(),
        Some(serde_json::Value::String(redacted)) => {
            if redacted.as_str() != original {
                *value = redacted.as_bytes().to_vec();
            }
        }
        Some(other) => *value = other.to_string().into_bytes(),
        None => {}
    }
}

/// Redact one stored document row's MessagePack bytes in place, reporting
/// whether the result is safe to deliver.
///
/// The device-sync surfaces hand the client the stored row bytes themselves:
/// a shape snapshot is a msgpack array of `{id, data}` envelopes whose `data`
/// value is the storage map verbatim, and a CRDT row push carries the same map
/// as its payload. Neither reaches a decoded row map, so
/// [`redact_envelope_row`](super::redact_envelope_row) cannot see the fields, and
/// [`redact_stored_value_bytes`] does not fit either — its non-map branch is
/// the KV single-value form, and its failure mode is to clear the value, which
/// in a snapshot envelope would leave a `data` key with no value at all and
/// corrupt the frame.
///
/// Returns `false` when a rule covers this row's collection but the bytes
/// could not be read or rewritten. The caller must then deliver nothing —
/// these are the bytes that would have gone out unredacted, and on a sync
/// surface they would be persisted on the device rather than merely displayed.
/// Returns `true` in every other case, including when no rule applies at all,
/// and the bytes are then left byte-identical rather than round-tripped.
pub fn redact_document_row_bytes(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    row: &mut Vec<u8>,
) -> bool {
    let Some(resolved) = redaction else {
        return true;
    };
    // An empty row is a delete tombstone on the row-push path; there is
    // nothing stored in it to redact.
    if row.is_empty() || !resolved.has_any_rule(store) {
        return true;
    }
    let ctx = resolved.ctx(store);

    let Ok(serde_json::Value::Object(fields)) = nodedb_types::json_from_msgpack(row) else {
        return false;
    };
    let mut redacted = fields.clone();
    ctx.store
        .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut redacted);
    if redacted == fields {
        return true;
    }
    match nodedb_types::json_to_msgpack(&serde_json::Value::Object(redacted)) {
        Ok(bytes) => {
            *row = bytes;
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{redaction_for, store_with_mask};
    use super::*;
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    /// The RESP `GET` shape for a row of typed columns: the stored bytes are a
    /// msgpack map keyed by field name, handed to the client verbatim. Before
    /// the fix the Redis wire shipped them unredacted.
    #[test]
    fn stored_msgpack_map_value_is_masked_field_by_field() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = nodedb_types::json_to_msgpack(&serde_json::json!({
            "email": "a@b.c",
            "name": "Alice",
        }))
        .expect("encode stored value");

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        let decoded = nodedb_types::json_from_msgpack(&value).expect("decode redacted value");
        assert_eq!(decoded["email"], "***");
        assert_eq!(decoded["name"], "Alice");
    }

    /// A role the policy does not name reads the stored bytes unchanged — and
    /// byte-identical, not a re-encoded round trip.
    #[test]
    fn stored_value_is_byte_identical_for_an_unruled_role() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "analyst");
        let original = nodedb_types::json_to_msgpack(&serde_json::json!({"email": "a@b.c"}))
            .expect("encode stored value");
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, original);
    }

    /// No policy at all must leave the bytes exactly as delivered.
    #[test]
    fn stored_value_is_byte_identical_without_any_policy() {
        let store = RedactionStore::new();
        let redaction = redaction_for("users", "support");
        let original = b"plain-resp-set-value".to_vec();
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);
        redact_stored_value_bytes(None, &store, &mut value);

        assert_eq!(value, original);
    }

    /// A `SET key value` writes opaque bytes; every SQL-side scan of that row
    /// presents them under the column `value`, so that is the rule that
    /// covers them on the Redis wire too.
    #[test]
    fn single_value_stored_form_is_masked_under_the_value_column() {
        let store = store_with_mask("cache", "support", "value", "***");
        let redaction = redaction_for("cache", "support");
        let mut value = b"secret-token".to_vec();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, b"***".to_vec());
    }

    /// A `Null` rule on the single-value form leaves nothing to deliver, so
    /// the value is cleared and the read presents as a miss.
    #[test]
    fn single_value_null_rule_clears_the_stored_value() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "cache_support_value".into(),
            tenant_id: 1,
            collection: "cache".into(),
            display_collection: "cache".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "value".into(),
                mode: RedactionMode::Null,
            }],
        });
        let redaction = redaction_for("cache", "support");
        let mut value = b"secret-token".to_vec();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert!(value.is_empty());
    }

    /// The device-sync shape: the stored row map is shipped verbatim inside a
    /// snapshot envelope, so the rule has to reach it at the byte level.
    #[test]
    fn document_row_bytes_are_masked_field_by_field() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": "u1",
            "email": "a@b.c",
            "name": "Alice",
        }))
        .expect("encode stored row");

        assert!(redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));

        let decoded = nodedb_types::json_from_msgpack(&row).expect("decode redacted row");
        assert_eq!(decoded["email"], "***");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["id"], "u1");
    }

    /// A role the policy does not name, and a store with no policy at all,
    /// both leave the stored bytes byte-identical — never a re-encoded round
    /// trip that could perturb the wire shape.
    #[test]
    fn document_row_bytes_are_byte_identical_without_a_matching_rule() {
        let store = store_with_mask("users", "support", "email", "***");
        let original = nodedb_types::json_to_msgpack(&serde_json::json!({"email": "a@b.c"}))
            .expect("encode stored row");

        let mut unruled_role = original.clone();
        assert!(redact_document_row_bytes(
            Some(&redaction_for("users", "analyst")),
            &store,
            &mut unruled_role
        ));
        assert_eq!(unruled_role, original);

        let mut no_policy = original.clone();
        assert!(redact_document_row_bytes(
            Some(&redaction_for("users", "support")),
            &RedactionStore::new(),
            &mut no_policy
        ));
        assert_eq!(no_policy, original);

        let mut no_redaction = original.clone();
        assert!(redact_document_row_bytes(None, &store, &mut no_redaction));
        assert_eq!(no_redaction, original);
    }

    /// A row a rule covers but whose bytes cannot be read is refused, not
    /// delivered: these are exactly the bytes that would otherwise reach the
    /// device unredacted.
    #[test]
    fn unreadable_document_row_is_refused_when_a_rule_applies() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = vec![0xc1_u8];

        assert!(!redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));
    }

    /// A delete tombstone carries no stored row, so there is nothing to refuse.
    #[test]
    fn empty_document_row_is_deliverable() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = Vec::new();

        assert!(redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));
        assert!(row.is_empty());
    }

    /// A rule naming a different column must not disturb an opaque value.
    #[test]
    fn single_value_form_is_untouched_by_a_rule_on_another_column() {
        let store = store_with_mask("cache", "support", "email", "***");
        let redaction = redaction_for("cache", "support");
        let original = b"secret-token".to_vec();
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, original);
    }
}
