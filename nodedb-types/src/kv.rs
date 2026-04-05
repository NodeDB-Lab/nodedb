//! KV engine configuration types.
//!
//! Key-Value collections use a hash-indexed primary key for O(1) point lookups.
//! Value fields are encoded as Binary Tuples (same codec as strict mode).

use serde::{Deserialize, Serialize};

use crate::columnar::ColumnType;

/// Default inline value threshold in bytes. Values at or below this size are
/// stored directly in the hash entry (no pointer chase).
pub const KV_DEFAULT_INLINE_THRESHOLD: u16 = 64;

/// Configuration for a Key-Value collection.
///
/// KV collections use a hash-indexed primary key for O(1) point lookups.
/// Value fields are encoded as Binary Tuples (same codec as strict mode)
/// providing O(1) field extraction by byte offset.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvConfig {
    /// Typed schema for this KV collection (key + value columns).
    ///
    /// The PRIMARY KEY column is the hash key. Remaining columns are value
    /// fields encoded as Binary Tuples with O(1) field extraction.
    /// The schema reuses `StrictSchema` from the strict document engine.
    pub schema: crate::columnar::StrictSchema,

    /// TTL policy for automatic key expiration. `None` = keys never expire.
    pub ttl: Option<KvTtlPolicy>,

    /// Initial capacity hint for the hash table. Avoids early rehash churn
    /// for known-size workloads. `0` = use engine default (from `KvTuning`).
    #[serde(default)]
    pub capacity_hint: u32,

    /// Inline value threshold in bytes. Values at or below this size are stored
    /// directly in the hash entry (no pointer chase). Larger values overflow to
    /// slab-allocated Binary Tuples referenced by slab ID.
    #[serde(default = "default_inline_threshold")]
    pub inline_threshold: u16,
}

fn default_inline_threshold() -> u16 {
    KV_DEFAULT_INLINE_THRESHOLD
}

impl KvConfig {
    /// Get the primary key column from the schema.
    ///
    /// KV collections always have exactly one PRIMARY KEY column.
    pub fn primary_key_column(&self) -> Option<&crate::columnar::ColumnDef> {
        self.schema.columns.iter().find(|c| c.primary_key)
    }

    /// Whether this KV collection has TTL enabled.
    pub fn has_ttl(&self) -> bool {
        self.ttl.is_some()
    }
}

/// TTL policy for KV collection key expiration.
///
/// Two modes:
/// - `FixedDuration`: All keys share the same lifetime from insertion time.
/// - `FieldBased`: Each key expires when a referenced timestamp field plus an
///   offset exceeds the current time, allowing per-key variable expiration.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(tag = "kind")]
pub enum KvTtlPolicy {
    /// Fixed duration from insertion time. All keys share the same lifetime.
    ///
    /// Example DDL: `WITH storage = 'kv', ttl = INTERVAL '15 minutes'`
    FixedDuration {
        /// Duration in milliseconds.
        duration_ms: u64,
    },
    /// Field-based expiration. Each key expires when the referenced timestamp
    /// field plus the offset exceeds the current time.
    ///
    /// Example DDL: `WITH storage = 'kv', ttl = last_active + INTERVAL '1 hour'`
    FieldBased {
        /// Name of the timestamp field in the value schema.
        field: String,
        /// Offset in milliseconds added to the field value.
        offset_ms: u64,
    },
}

/// Column types valid as KV primary key (hash key).
///
/// Only types with well-defined equality and hash behavior are allowed.
/// Floating-point types are excluded (equality is unreliable for hashing).
const KV_VALID_KEY_TYPES: &[ColumnType] = &[
    ColumnType::String,    // TEXT, VARCHAR
    ColumnType::Uuid,      // UUID
    ColumnType::Int64,     // INT, BIGINT, INTEGER
    ColumnType::Bytes,     // BYTES, BYTEA, BLOB
    ColumnType::Timestamp, // TIMESTAMP (epoch-based, deterministic equality)
];

/// Check whether a column type is valid as a KV primary key.
pub fn is_valid_kv_key_type(ct: &ColumnType) -> bool {
    KV_VALID_KEY_TYPES.contains(ct)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnDef, ColumnType, StrictSchema};

    #[test]
    fn kv_config_primary_key() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("session_id", ColumnType::String).with_primary_key(),
            ColumnDef::required("user_id", ColumnType::Uuid),
            ColumnDef::nullable("payload", ColumnType::Bytes),
        ])
        .unwrap();
        let config = KvConfig {
            schema,
            ttl: None,
            capacity_hint: 0,
            inline_threshold: KV_DEFAULT_INLINE_THRESHOLD,
        };
        let pk = config.primary_key_column().unwrap();
        assert_eq!(pk.name, "session_id");
        assert_eq!(pk.column_type, ColumnType::String);
        assert!(pk.primary_key);
    }

    #[test]
    fn kv_valid_key_types() {
        assert!(is_valid_kv_key_type(&ColumnType::String));
        assert!(is_valid_kv_key_type(&ColumnType::Uuid));
        assert!(is_valid_kv_key_type(&ColumnType::Int64));
        assert!(is_valid_kv_key_type(&ColumnType::Bytes));
        assert!(is_valid_kv_key_type(&ColumnType::Timestamp));
        // Invalid key types:
        assert!(!is_valid_kv_key_type(&ColumnType::Float64));
        assert!(!is_valid_kv_key_type(&ColumnType::Bool));
        assert!(!is_valid_kv_key_type(&ColumnType::Geometry));
        assert!(!is_valid_kv_key_type(&ColumnType::Vector(128)));
        assert!(!is_valid_kv_key_type(&ColumnType::Decimal));
    }

    #[test]
    fn kv_ttl_policy_serde() {
        let policies = vec![
            KvTtlPolicy::FixedDuration {
                duration_ms: 60_000,
            },
            KvTtlPolicy::FieldBased {
                field: "last_active".into(),
                offset_ms: 3_600_000,
            },
        ];
        for p in policies {
            let json = sonic_rs::to_string(&p).unwrap();
            let back: KvTtlPolicy = sonic_rs::from_str(&json).unwrap();
            assert_eq!(back, p);
        }
    }
}
