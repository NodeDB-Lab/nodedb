// SPDX-License-Identifier: BUSL-1.1

//! Canonical row hashing for backup verification.
//!
//! A row hashes as its engine part, its canonical key and its canonical value.
//! Every item is written with a type tag and a length prefix, so two different
//! rows never feed the hasher the same bytes. Map entries hash in key order, so
//! a map's iteration order never reaches the digest.

use loro::LoroValue;
use nodedb_types::Value;
use nodedb_types::backup_envelope::VerifiedPart;
use sha2::{Digest, Sha256};

use crate::Error;

/// The hash of a row's canonical key. Every version of one row shares it, so
/// the restore can match a destination row to a backed-up one.
pub(crate) type KeyHash = u128;

/// One canonical row of one collection part.
pub(crate) struct Row {
    /// Bare catalog name of the collection.
    pub collection: String,
    pub part: VerifiedPart,
    /// `None` for a part whose rows have no key: timeseries.
    pub key: Option<KeyHash>,
    pub hash: [u8; 32],
    /// The row's TTL deadline in Unix milliseconds, `0` for none.
    pub expire_at_ms: u64,
    /// The system time the version starts at, on a part that keeps versions
    /// of one key: documents, edges, KV, array cells, columnar rows and
    /// key-bound vectors. The destination counts a version only at a
    /// `(key, stamp)` the backup holds. `None` on every other part.
    pub stamp: Option<i64>,
    /// The key's current state, without system times. Set on the key's
    /// newest version only, `None` on every other row.
    pub state: Option<[u8; 32]>,
}

/// The hash of `key`, the canonical key parts of a row of `part`.
pub(crate) fn key_hash(part: VerifiedPart, key: &[&[u8]]) -> KeyHash {
    let mut hasher = RowHasher::start(b"nodedb-verify-key", part);
    for item in key {
        hasher.bytes(b'k', item);
    }
    let digest = hasher.finish();
    let mut head = [0u8; 16];
    head.copy_from_slice(&digest[..16]);
    u128::from_le_bytes(head)
}

/// Streams one row's canonical key and value into SHA-256.
pub(crate) struct RowHasher(Sha256);

impl RowHasher {
    /// A row of `part` keyed by `key`.
    pub fn new(part: VerifiedPart, key: &[&[u8]]) -> Self {
        let mut hasher = Self::start(b"nodedb-verify-row", part);
        hasher.count(b'K', key.len());
        for item in key {
            hasher.bytes(b'k', item);
        }
        hasher
    }

    /// The current state of a key of `part`. It hashes apart from every
    /// row: no state hash equals a row hash.
    pub fn state(part: VerifiedPart, key: &[&[u8]]) -> Self {
        let mut hasher = Self::start(b"nodedb-verify-state", part);
        hasher.count(b'K', key.len());
        for item in key {
            hasher.bytes(b'k', item);
        }
        hasher
    }

    fn start(domain: &[u8], part: VerifiedPart) -> Self {
        let mut sha = Sha256::new();
        sha.update(domain);
        sha.update([part.tag()]);
        Self(sha)
    }

    pub fn bytes(&mut self, tag: u8, bytes: &[u8]) {
        self.0.update([tag]);
        self.0.update((bytes.len() as u64).to_le_bytes());
        self.0.update(bytes);
    }

    pub fn int(&mut self, tag: u8, value: i64) {
        self.bytes(tag, &value.to_le_bytes());
    }

    fn count(&mut self, tag: u8, len: usize) {
        self.0.update([tag]);
        self.0.update((len as u64).to_le_bytes());
    }

    /// A native value. Objects hash their fields in name order.
    pub fn value(&mut self, value: &Value) -> Result<(), Error> {
        match value {
            Value::Object(map) => {
                let mut fields: Vec<(&String, &Value)> = map.iter().collect();
                fields.sort_by(|a, b| a.0.cmp(b.0));
                self.count(b'O', fields.len());
                for (name, field) in fields {
                    self.bytes(b's', name.as_bytes());
                    self.value(field)?;
                }
            }
            Value::Array(items) => {
                self.count(b'A', items.len());
                for item in items {
                    self.value(item)?;
                }
            }
            Value::Set(items) => {
                self.count(b'S', items.len());
                for item in items {
                    self.value(item)?;
                }
            }
            leaf => {
                let bytes =
                    nodedb_types::value_to_msgpack(leaf).map_err(|e| Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("backup verification: encode a row value: {e}"),
                    })?;
                self.bytes(b'L', &bytes);
            }
        }
        Ok(())
    }

    /// A CRDT value. Maps hash their entries in key order.
    pub fn loro(&mut self, value: &LoroValue) {
        match value {
            LoroValue::Null => self.bytes(b'n', &[]),
            LoroValue::Bool(b) => self.bytes(b'b', &[u8::from(*b)]),
            LoroValue::Double(d) => self.bytes(b'd', &d.to_bits().to_le_bytes()),
            LoroValue::I64(i) => self.int(b'i', *i),
            LoroValue::Binary(bytes) => self.bytes(b'y', bytes.as_slice()),
            LoroValue::String(s) => self.bytes(b's', s.as_bytes()),
            LoroValue::List(items) => {
                self.count(b'A', items.len());
                for item in items.iter() {
                    self.loro(item);
                }
            }
            LoroValue::Map(map) => {
                let mut entries: Vec<(&String, &LoroValue)> = map.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                self.count(b'O', entries.len());
                for (name, entry) in entries {
                    self.bytes(b's', name.as_bytes());
                    self.loro(entry);
                }
            }
            LoroValue::Container(id) => self.bytes(b'c', id.to_string().as_bytes()),
        }
    }

    pub fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn object(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn hash(value: &Value) -> [u8; 32] {
        let mut hasher = RowHasher::new(VerifiedPart::Documents, &[b"k1"]);
        hasher.value(value).expect("hash");
        hasher.finish()
    }

    #[test]
    fn field_order_does_not_reach_the_hash() {
        let a = object(&[("x", Value::Integer(1)), ("y", Value::String("s".into()))]);
        let b = object(&[("y", Value::String("s".into())), ("x", Value::Integer(1))]);
        assert_eq!(hash(&a), hash(&b));
        let c = object(&[("x", Value::Integer(2)), ("y", Value::String("s".into()))]);
        assert_ne!(hash(&a), hash(&c));
    }

    #[test]
    fn the_key_and_the_part_reach_the_hash() {
        let value = Value::Integer(1);
        let mut other_key = RowHasher::new(VerifiedPart::Documents, &[b"k2"]);
        other_key.value(&value).expect("hash");
        assert_ne!(hash(&value), other_key.finish());
        let mut other_part = RowHasher::new(VerifiedPart::KeyValue, &[b"k1"]);
        other_part.value(&value).expect("hash");
        assert_ne!(hash(&value), other_part.finish());
        assert_ne!(
            key_hash(VerifiedPart::Documents, &[b"ab", b"c"]),
            key_hash(VerifiedPart::Documents, &[b"a", b"bc"])
        );
    }
}
