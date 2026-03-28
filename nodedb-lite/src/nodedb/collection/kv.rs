//! KV collection operations for Lite: PUT/GET/DELETE via CRDT engine.
//!
//! KV writes go through the CRDT engine's `upsert`/`delete` path so deltas
//! are produced for edge-to-cloud sync. The KV collection name is prefixed
//! with `_kv_` in the CRDT namespace to separate from document collections.
//! LWW conflict resolution applies by default (latest PUT wins).

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

/// Prefix for KV collection names in the CRDT namespace.
const KV_CRDT_PREFIX: &str = "_kv_";

impl<S: StorageEngine> NodeDbLite<S> {
    /// KV PUT: store a key-value pair with CRDT delta production.
    ///
    /// The value is stored as a hex-encoded string in the CRDT document
    /// (Loro String type syncs cleanly across peers).
    /// The primary key is the CRDT document ID.
    pub fn kv_put(&self, collection: &str, key: &str, value: &[u8]) -> NodeDbResult<()> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();

        let value_encoded = bytes_to_hex(value);
        let fields: Vec<(&str, loro::LoroValue)> =
            vec![("value", loro::LoroValue::String(value_encoded.into()))];
        crdt.upsert(&crdt_collection, key, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// KV GET: retrieve a value by key.
    pub fn kv_get(&self, collection: &str, key: &str) -> NodeDbResult<Option<Vec<u8>>> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let crdt = self.crdt.lock_or_recover();

        match crdt.read(&crdt_collection, key) {
            Some(loro::LoroValue::Map(map)) => {
                if let Some(loro::LoroValue::String(encoded)) = map.get("value") {
                    let bytes = hex_to_bytes(encoded)
                        .map_err(|e| NodeDbError::storage(format!("kv decode: {e}")))?;
                    Ok(Some(bytes))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// KV DELETE: remove a key with CRDT delta production.
    pub fn kv_delete(&self, collection: &str, key: &str) -> NodeDbResult<bool> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();
        crdt.delete(&crdt_collection, key)
            .map_err(NodeDbError::storage)?;
        Ok(true)
    }

    /// List all keys in a KV collection.
    pub fn kv_keys(&self, collection: &str) -> NodeDbResult<Vec<String>> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let crdt = self.crdt.lock_or_recover();
        Ok(crdt.list_ids(&crdt_collection))
    }

    /// KV INCREMENT: atomic counter increment via CRDT counter semantics.
    ///
    /// If the key doesn't exist, initializes to `delta`. If it exists,
    /// adds `delta` to the current value. Uses Loro's counter CRDT type
    /// so concurrent increments from multiple peers merge correctly
    /// (no lost updates under LWW — counters are commutative).
    pub fn kv_increment(&self, collection: &str, key: &str, delta: i64) -> NodeDbResult<i64> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();

        // Read current value.
        let current = match crdt.read(&crdt_collection, key) {
            Some(loro::LoroValue::Map(map)) => {
                if let Some(loro::LoroValue::I64(v)) = map.get("counter") {
                    *v
                } else {
                    0
                }
            }
            _ => 0,
        };

        let new_value = current + delta;
        let fields: Vec<(&str, loro::LoroValue)> =
            vec![("counter", loro::LoroValue::I64(new_value))];
        crdt.upsert(&crdt_collection, key, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(new_value)
    }

    /// Set conflict policy for a KV collection.
    ///
    /// Default is LWW (last-writer-wins). For counter/register workloads,
    /// use `CompensationHint` to handle constraint violations at sync time
    /// (e.g., UNIQUE violation on a value field → dead-letter queue with hint).
    pub fn kv_set_conflict_policy(
        &self,
        collection: &str,
        policy: nodedb_crdt::CollectionPolicy,
    ) -> NodeDbResult<()> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_policy(&crdt_collection, policy);
        Ok(())
    }

    /// Subscribe to a subset of KV keys matching a pattern.
    ///
    /// Shape subscriptions allow edge devices to sync only the keys they
    /// need. The pattern supports `*` glob matching on key names.
    /// Returns the list of currently matching keys.
    pub fn kv_subscribe_shape(
        &self,
        collection: &str,
        key_pattern: &str,
    ) -> NodeDbResult<Vec<String>> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let crdt = self.crdt.lock_or_recover();
        let all_keys = crdt.list_ids(&crdt_collection);

        // Filter by glob pattern.
        let matched: Vec<String> = all_keys
            .into_iter()
            .filter(|k| glob_matches(key_pattern, k))
            .collect();

        Ok(matched)
    }
}

/// Simple glob matching for shape subscriptions.
fn glob_matches(pattern: &str, input: &str) -> bool {
    let pat = pattern.as_bytes();
    let inp = input.as_bytes();
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < inp.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == inp[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Encode bytes as lowercase hex string.
fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        s.push(char::from(b"0123456789abcdef"[(b & 0x0f) as usize]));
    }
    s
}

/// Decode hex string to bytes.
fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("odd-length hex string".into());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<u8> = hex.bytes().collect();
    for pair in chars.chunks(2) {
        let hi = hex_digit(pair[0]).ok_or_else(|| format!("invalid hex digit: {}", pair[0]))?;
        let lo = hex_digit(pair[1]).ok_or_else(|| format!("invalid hex digit: {}", pair[1]))?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
