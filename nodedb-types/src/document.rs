//! Document type: the universal data container for CRDT-backed records.
//!
//! Documents are the primary unit of storage, sync, and conflict resolution.
//! Internally stored as MessagePack bytes for compact wire representation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// A CRDT-backed document. The primary data unit across all NodeDB engines.
///
/// Documents are schemaless: each field maps a string key to a `Value`.
/// The `id` field is mandatory and immutable after creation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct Document {
    /// Document identifier (unique within a collection).
    pub id: String,
    /// Key-value fields. Schema enforcement (if any) happens at the
    /// collection level, not the document level.
    pub fields: HashMap<String, Value>,
}

impl Document {
    /// Create a new document with the given ID and no fields.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            fields: HashMap::new(),
        }
    }

    /// Set a field value. Returns `&mut Self` for chaining.
    pub fn set(&mut self, key: impl Into<String>, value: Value) -> &mut Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Get a field value by key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    /// Get a field as a string, if it exists and is a string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        match self.fields.get(key) {
            Some(Value::String(s)) => Some(s),
            _ => None,
        }
    }

    /// Get a field as f64, if it exists and is numeric.
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        match self.fields.get(key) {
            Some(Value::Float(f)) => Some(*f),
            Some(Value::Integer(i)) => Some(*i as f64),
            _ => None,
        }
    }

    /// Serialize the document to MessagePack bytes.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, zerompk::Error> {
        zerompk::to_msgpack_vec(self)
    }

    /// Deserialize a document from MessagePack bytes.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self, zerompk::Error> {
        zerompk::from_msgpack(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_builder() {
        let mut doc = Document::new("user-1");
        doc.set("name", Value::String("Alice".into()))
            .set("age", Value::Integer(30))
            .set("score", Value::Float(9.5));

        assert_eq!(doc.id, "user-1");
        assert_eq!(doc.get_str("name"), Some("Alice"));
        assert_eq!(doc.get_f64("age"), Some(30.0));
        assert_eq!(doc.get_f64("score"), Some(9.5));
        assert!(doc.get("missing").is_none());
    }

    #[test]
    fn msgpack_roundtrip() {
        let mut doc = Document::new("d1");
        doc.set("key", Value::String("val".into()));

        let bytes = doc.to_msgpack().unwrap();
        let decoded = Document::from_msgpack(&bytes).unwrap();
        assert_eq!(doc, decoded);
    }
}
