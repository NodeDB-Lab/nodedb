// SPDX-License-Identifier: BUSL-1.1

//! `RedactionPolicy` / `RedactionRule` / `RedactionMode` data shapes and the
//! internal `policy_key` helper used by the store.

use serde::{Deserialize, Serialize};

/// A redaction policy: specifies which fields to redact for which roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionPolicy {
    /// Policy name.
    pub name: String,
    /// Tenant scope.
    pub tenant_id: u64,
    /// Collection this policy applies to, `db_qualified` — the same key
    /// physical plan ops carry, so lookups against a plan's collection field
    /// match regardless of which database it was created in.
    pub collection: String,
    /// The bare, user-typed collection name, for `SHOW REDACTION POLICIES`
    /// output — a user must never see the internal `{db_id}/{name}` form.
    pub display_collection: String,
    /// Role this policy applies to (e.g., "support").
    pub for_role: String,
    /// Field → redaction rule.
    pub rules: Vec<RedactionRule>,
}

/// A single field redaction rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionRule {
    /// Field name to redact.
    pub field: String,
    /// Redaction mode.
    pub mode: RedactionMode,
}

/// How a field value is redacted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedactionMode {
    /// Replace with a static mask string (e.g., "***@***.com").
    Mask(String),
    /// Replace with SHA-256 hash of the original value (pseudonymization).
    /// Joinable across queries but not human-readable.
    Hash,
    /// Replace with null.
    Null,
}

/// The longest mask a `MASK` rule replaces a field with, in bytes. A
/// `RETURNING` cell can hold a mask, so the cap bounds the response.
pub const MAX_REDACTION_MASK_BYTES: usize = 1024;

/// The length of the text a `HASH` rule replaces a field with, in bytes:
/// `hash:` and the 64 hex digits of a SHA-256 digest.
pub const HASH_REDACTION_TEXT_BYTES: usize = 69;

/// Build the lookup key for the policy map: `"{tenant_id}:{collection}:{for_role}"`.
pub(super) fn policy_key(tenant_id: u64, collection: &str, for_role: &str) -> String {
    format!("{tenant_id}:{collection}:{for_role}")
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::super::apply::redacted_typed_value;
    use super::*;

    #[test]
    fn a_hash_is_fixed_length_text() {
        for current in [
            None,
            Some(Value::Null),
            Some(Value::String("s".repeat(10_000))),
            Some(Value::Integer(-1)),
        ] {
            let Value::String(text) = redacted_typed_value(&RedactionMode::Hash, current.as_ref())
            else {
                panic!("a hash is text");
            };
            assert_eq!(text.len(), HASH_REDACTION_TEXT_BYTES);
        }
    }
}
