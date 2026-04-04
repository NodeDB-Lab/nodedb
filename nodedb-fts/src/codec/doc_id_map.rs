//! Bidirectional `u32 ↔ String` document ID mapping.
//!
//! Each collection maintains its own DocIdMap. Integer IDs eliminate
//! string allocation/comparison in the BM25 scoring hot path and enable
//! delta encoding for posting list compression.

use std::collections::HashMap;

/// Bidirectional map: `u32 ↔ String` document ID.
///
/// - `get_or_assign` assigns a new u32 for an unseen string.
/// - `to_u32` / `to_string` for lookup without assignment.
/// - Serializable to/from bytes for persistence.
#[derive(Debug, Clone, Default)]
pub struct DocIdMap {
    /// String → u32.
    forward: HashMap<String, u32>,
    /// u32 → String (indexed by ID value).
    reverse: Vec<String>,
}

impl DocIdMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or assign a u32 ID for the given string doc_id.
    ///
    /// If the string is already mapped, returns the existing u32.
    /// Otherwise, assigns the next sequential u32 and returns it.
    pub fn get_or_assign(&mut self, doc_id: &str) -> u32 {
        if let Some(&id) = self.forward.get(doc_id) {
            return id;
        }
        let id = self.reverse.len() as u32;
        self.forward.insert(doc_id.to_string(), id);
        self.reverse.push(doc_id.to_string());
        id
    }

    /// Look up the u32 ID for a string doc_id. Returns `None` if not mapped.
    pub fn to_u32(&self, doc_id: &str) -> Option<u32> {
        self.forward.get(doc_id).copied()
    }

    /// Look up the string doc_id for a u32 ID. Returns `None` if out of range.
    pub fn to_string(&self, id: u32) -> Option<&str> {
        self.reverse.get(id as usize).map(String::as_str)
    }

    /// Number of mapped IDs.
    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }

    /// Remove a doc_id from the map.
    ///
    /// Note: this does NOT compact IDs — the u32 slot becomes a tombstone.
    /// The forward map entry is removed so `to_u32` returns None, but the
    /// reverse slot keeps the string for in-flight references. New assignments
    /// always use the next sequential ID (no reuse).
    pub fn remove(&mut self, doc_id: &str) {
        self.forward.remove(doc_id);
        // Reverse entry stays as tombstone — do not shift IDs.
    }

    /// Serialize the map to bytes for persistence.
    ///
    /// Format: `[count: u32 LE][for each entry: len: u16 LE, utf8 bytes]`
    /// Tombstoned entries (removed from forward map) are stored as empty strings.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.reverse.len() as u32).to_le_bytes());
        for (i, s) in self.reverse.iter().enumerate() {
            // If removed from forward map, store as empty (tombstone).
            let stored = if self.forward.get(s).copied() == Some(i as u32) {
                s.as_str()
            } else {
                ""
            };
            buf.extend_from_slice(&(stored.len() as u16).to_le_bytes());
            buf.extend_from_slice(stored.as_bytes());
        }
        buf
    }

    /// Deserialize from bytes. Returns `None` if the buffer is malformed.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let mut pos = 4;
        let mut forward = HashMap::with_capacity(count);
        let mut reverse = Vec::with_capacity(count);

        for i in 0..count {
            if pos + 2 > buf.len() {
                return None;
            }
            let len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
            pos += 2;
            if pos + len > buf.len() {
                return None;
            }
            let s = std::str::from_utf8(&buf[pos..pos + len]).ok()?;
            pos += len;

            if !s.is_empty() {
                forward.insert(s.to_string(), i as u32);
            }
            reverse.push(s.to_string());
        }

        Some(Self { forward, reverse })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign_and_lookup() {
        let mut map = DocIdMap::new();
        assert_eq!(map.get_or_assign("doc1"), 0);
        assert_eq!(map.get_or_assign("doc2"), 1);
        assert_eq!(map.get_or_assign("doc1"), 0); // Idempotent.

        assert_eq!(map.to_u32("doc1"), Some(0));
        assert_eq!(map.to_u32("doc2"), Some(1));
        assert_eq!(map.to_u32("doc3"), None);

        assert_eq!(map.to_string(0), Some("doc1"));
        assert_eq!(map.to_string(1), Some("doc2"));
        assert_eq!(map.to_string(99), None);
    }

    #[test]
    fn remove_tombstones() {
        let mut map = DocIdMap::new();
        map.get_or_assign("doc1");
        map.get_or_assign("doc2");
        map.get_or_assign("doc3");

        map.remove("doc2");
        assert_eq!(map.to_u32("doc2"), None);
        // doc1 and doc3 unaffected.
        assert_eq!(map.to_u32("doc1"), Some(0));
        assert_eq!(map.to_u32("doc3"), Some(2));
        // New assignment doesn't reuse slot 1.
        assert_eq!(map.get_or_assign("doc4"), 3);
    }

    #[test]
    fn serialization_roundtrip() {
        let mut map = DocIdMap::new();
        map.get_or_assign("hello");
        map.get_or_assign("world");
        map.get_or_assign("foo");
        map.remove("world");

        let bytes = map.to_bytes();
        let restored = DocIdMap::from_bytes(&bytes).unwrap();

        assert_eq!(restored.to_u32("hello"), Some(0));
        assert_eq!(restored.to_u32("world"), None); // Tombstoned.
        assert_eq!(restored.to_u32("foo"), Some(2));
        assert_eq!(restored.len(), 3); // 3 slots (including tombstone).
    }

    #[test]
    fn from_bytes_malformed() {
        assert!(DocIdMap::from_bytes(&[]).is_none());
        assert!(DocIdMap::from_bytes(&[1, 0, 0, 0]).is_none()); // Claims 1 entry, no data.
    }

    #[test]
    fn empty_map() {
        let map = DocIdMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);

        let bytes = map.to_bytes();
        let restored = DocIdMap::from_bytes(&bytes).unwrap();
        assert!(restored.is_empty());
    }
}
