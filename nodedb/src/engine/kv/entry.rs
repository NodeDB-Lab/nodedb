// SPDX-License-Identifier: BUSL-1.1

//! KV hash table entry: key + inline/overflow value + TTL metadata.
//!
//! Each slot in the Robin Hood hash table stores a `KvEntry`. Small values
//! (≤ inline threshold) are embedded directly to avoid a pointer chase.
//! Larger values are stored in an overflow buffer and referenced by index.

use nodedb_types::Surrogate;

/// Sentinel value indicating no TTL (key never expires).
pub const NO_EXPIRY: u64 = 0;

/// A single entry in the KV hash table.
///
/// Layout is optimized for cache-line-friendly access:
/// - `hash` is stored to avoid rehashing during probes and incremental rehash.
/// - `key` is the primary key bytes (serialized per the collection's key column type).
/// - Value is either inline (small, ≤ threshold) or overflow (index into separate buffer).
/// - `expire_at_ms` is the absolute wall-clock millisecond timestamp for expiry,
///   or [`NO_EXPIRY`] if the key has no TTL.
#[derive(Debug, Clone)]
pub struct KvEntry {
    /// Cached hash of the key (avoids rehashing during Robin Hood probing).
    pub hash: u64,
    /// Primary key bytes.
    pub key: Vec<u8>,
    /// Value storage: inline for small values, overflow index for large.
    pub value: KvValue,
    /// Absolute expiry timestamp in milliseconds since Unix epoch.
    /// `0` = no expiry.
    pub expire_at_ms: u64,
    /// Stable global row identity assigned by the Control-Plane allocator.
    /// Never `Surrogate::ZERO`: `KvEngine` refuses an unbound write, so every
    /// stored entry is bound.
    pub surrogate: Surrogate,
    /// System-time ordinal of the write that set this value and expiry. A
    /// read at ordinal `S` sees this entry only when `sys_from <= S`.
    pub sys_from: i64,
}

impl KvEntry {
    /// Whether this key has a TTL set (regardless of whether it has already expired).
    ///
    /// To check if a key is still accessible, use [`is_expired`](Self::is_expired).
    pub fn has_ttl(&self) -> bool {
        self.expire_at_ms != NO_EXPIRY
    }

    /// Whether this key is expired relative to the given wall-clock time.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms != NO_EXPIRY && now_ms >= self.expire_at_ms
    }

    /// Get the inline value bytes, if stored inline.
    pub fn inline_value(&self) -> Option<&[u8]> {
        match &self.value {
            KvValue::Inline(data) => Some(data),
            KvValue::Overflow { .. } => None,
        }
    }

    /// Approximate memory usage in bytes (for budget accounting).
    pub fn mem_size(&self) -> usize {
        // Fixed overhead: hash(8) + expire(8) + enum discriminant(8) + vec overhead(24)
        // + surrogate(4) + pad(4) + sys_from(8).
        let fixed = 64;
        let key_size = self.key.len();
        let value_size = match &self.value {
            KvValue::Inline(data) => data.len(),
            // Overflow stores index+len (8 bytes), actual data is in the overflow pool.
            KvValue::Overflow { .. } => 8,
        };
        fixed + key_size + value_size
    }
}

/// Value storage mode for a KV entry.
///
/// Inline: value bytes embedded directly in the hash entry. No pointer chase.
/// Overflow: value stored in a separate pool, referenced by index.
#[derive(Debug, Clone)]
pub enum KvValue {
    /// Small value stored directly in the hash entry.
    Inline(Vec<u8>),
    /// Large value stored in the overflow pool.
    Overflow {
        /// Index into the per-collection overflow buffer.
        index: u32,
        /// Length of the value in bytes.
        len: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bound entry holding `value` under `key`.
    fn entry(hash: u64, key: &[u8], value: KvValue, expire_at_ms: u64) -> KvEntry {
        KvEntry {
            hash,
            key: key.to_vec(),
            value,
            expire_at_ms,
            surrogate: Surrogate::new(1),
            sys_from: 1,
        }
    }

    #[test]
    fn entry_inline_creation() {
        let entry = entry(
            12345,
            b"mykey",
            KvValue::Inline(b"myvalue".to_vec()),
            NO_EXPIRY,
        );
        assert_eq!(entry.hash, 12345);
        assert_eq!(entry.key, b"mykey");
        assert_eq!(entry.inline_value(), Some(b"myvalue".as_slice()));
        assert!(!entry.has_ttl());
        assert!(!entry.is_expired(1_000_000));
    }

    #[test]
    fn entry_with_ttl() {
        let entry = entry(1, b"k", KvValue::Inline(b"v".to_vec()), 5000);
        assert!(entry.has_ttl());
        assert!(!entry.is_expired(4999));
        assert!(entry.is_expired(5000));
        assert!(entry.is_expired(5001));
    }

    #[test]
    fn entry_overflow() {
        let entry = entry(
            1,
            b"k",
            KvValue::Overflow {
                index: 0,
                len: 1024,
            },
            NO_EXPIRY,
        );
        assert!(entry.inline_value().is_none());
        assert!(!entry.has_ttl());
        match &entry.value {
            KvValue::Overflow { index, len } => {
                assert_eq!(*index, 0);
                assert_eq!(*len, 1024);
            }
            _ => panic!("expected overflow"),
        }
    }

    #[test]
    fn mem_size_is_reasonable() {
        let entry = entry(1, b"key", KvValue::Inline(b"val".to_vec()), NO_EXPIRY);
        let size = entry.mem_size();
        // 64 fixed + 3 key + 3 value = 70
        assert_eq!(size, 70);
    }
}
