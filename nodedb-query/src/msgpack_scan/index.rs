//! Per-document structural index for O(1) field access.
//!
//! When a query accesses multiple fields from the same document (e.g.,
//! GROUP BY + aggregate + filter), building a `FieldIndex` once and
//! reusing it for all field lookups avoids repeated O(N) key scanning.
//!
//! Uses a flat array with linear search for small docs (≤ 16 fields) to
//! avoid HashMap allocation overhead. Falls back to HashMap for large docs.

use crate::msgpack_scan::reader::{map_header, skip_value, str_bounds};

/// Threshold: docs with more fields than this use HashMap, otherwise flat array.
const HASH_THRESHOLD: usize = 16;

/// Pre-computed field offset table for a single MessagePack document.
pub struct FieldIndex {
    inner: IndexInner,
}

enum IndexInner {
    /// Flat array for small documents — linear scan, zero HashMap overhead.
    Flat(Vec<(Box<str>, usize, usize)>),
    /// HashMap for large documents — O(1) lookup.
    Map(std::collections::HashMap<Box<str>, (usize, usize)>),
}

impl FieldIndex {
    /// Build an index for the msgpack map at `offset` in `buf`.
    ///
    /// Scans all map keys once and records value byte ranges.
    /// Returns `None` if `buf` is not a valid map at `offset`.
    pub fn build(buf: &[u8], offset: usize) -> Option<Self> {
        let (count, mut pos) = map_header(buf, offset)?;

        if count <= HASH_THRESHOLD {
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key_str = if let Some((start, len)) = str_bounds(buf, pos) {
                    std::str::from_utf8(buf.get(start..start + len)?).ok()
                } else {
                    None
                };
                pos = skip_value(buf, pos)?;
                let value_start = pos;
                let value_end = skip_value(buf, pos)?;
                if let Some(key) = key_str {
                    entries.push((key.into(), value_start, value_end));
                }
                pos = value_end;
            }
            Some(Self {
                inner: IndexInner::Flat(entries),
            })
        } else {
            // Cap pre-allocation: adversarial buffers may claim enormous counts.
            // Actual insertions are bounded by the buffer size, so over-allocating
            // wastes memory and under-allocating just triggers rehashing.
            let cap = count.min(buf.len() / 2 + 1);
            let mut offsets = std::collections::HashMap::with_capacity(cap);
            for _ in 0..count {
                let key_str = if let Some((start, len)) = str_bounds(buf, pos) {
                    std::str::from_utf8(buf.get(start..start + len)?).ok()
                } else {
                    None
                };
                pos = skip_value(buf, pos)?;
                let value_start = pos;
                let value_end = skip_value(buf, pos)?;
                if let Some(key) = key_str {
                    offsets.insert(key.into(), (value_start, value_end));
                }
                pos = value_end;
            }
            Some(Self {
                inner: IndexInner::Map(offsets),
            })
        }
    }

    /// Create an empty index (no fields).
    pub fn empty() -> Self {
        Self {
            inner: IndexInner::Flat(Vec::new()),
        }
    }

    /// Look up a field's byte range.
    #[inline]
    pub fn get(&self, field: &str) -> Option<(usize, usize)> {
        match &self.inner {
            IndexInner::Flat(entries) => entries
                .iter()
                .find(|(k, _, _)| k.as_ref() == field)
                .map(|(_, s, e)| (*s, *e)),
            IndexInner::Map(map) => map.get(field).copied(),
        }
    }

    /// Number of indexed fields.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            IndexInner::Flat(entries) => entries.len(),
            IndexInner::Map(map) => map.len(),
        }
    }

    /// Whether the index is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::{read_f64, read_i64, read_str};
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn build_and_lookup() {
        let buf = encode(&json!({"name": "alice", "age": 30, "score": 99.5}));
        let idx = FieldIndex::build(&buf, 0).unwrap();

        assert_eq!(idx.len(), 3);

        let (s, _) = idx.get("name").unwrap();
        assert_eq!(read_str(&buf, s), Some("alice"));

        let (s, _) = idx.get("age").unwrap();
        assert_eq!(read_i64(&buf, s), Some(30));

        let (s, _) = idx.get("score").unwrap();
        assert_eq!(read_f64(&buf, s), Some(99.5));
    }

    #[test]
    fn missing_field() {
        let buf = encode(&json!({"x": 1}));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert!(idx.get("y").is_none());
    }

    #[test]
    fn empty_map() {
        let buf = encode(&json!({}));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert!(idx.is_empty());
    }

    #[test]
    fn small_doc_uses_flat() {
        let mut map = serde_json::Map::new();
        for i in 0..10 {
            map.insert(format!("f{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert_eq!(idx.len(), 10);
        assert!(matches!(idx.inner, IndexInner::Flat(_)));
    }

    #[test]
    fn large_doc_uses_hashmap() {
        let mut map = serde_json::Map::new();
        for i in 0..20 {
            map.insert(format!("field_{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let idx = FieldIndex::build(&buf, 0).unwrap();
        assert_eq!(idx.len(), 20);
        assert!(matches!(idx.inner, IndexInner::Map(_)));

        for i in 0..20 {
            let (s, _) = idx.get(&format!("field_{i}")).unwrap();
            assert_eq!(read_i64(&buf, s), Some(i));
        }
    }

    #[test]
    fn not_a_map() {
        let buf = encode(&json!([1, 2, 3]));
        assert!(FieldIndex::build(&buf, 0).is_none());
    }

    #[test]
    fn indexed_vs_sequential_same_result() {
        let buf = encode(&json!({"a": 1, "b": "two", "c": 3.0}));
        let idx = FieldIndex::build(&buf, 0).unwrap();

        for field in &["a", "b", "c"] {
            let indexed = idx.get(field);
            let sequential = crate::msgpack_scan::field::extract_field(&buf, 0, field);
            assert_eq!(indexed, sequential, "mismatch for field {field}");
        }
    }

    // ── Fuzz-style tests ───────────────────────────────────────────────────

    /// Truncate valid msgpack at every byte position — FieldIndex::build must
    /// never panic; it should return None on truncated input.
    #[test]
    fn fuzz_truncated_buffers() {
        let docs = [
            json!({"name": "alice", "age": 30, "score": 9.5}),
            json!({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}),
            json!({"nested": {"inner": 42}}),
        ];

        for doc in &docs {
            let full = encode(doc);
            for truncate_at in 0..full.len() {
                let slice = &full[..truncate_at];
                // Must not panic — None is the valid outcome for truncated data.
                let _ = FieldIndex::build(slice, 0);
            }
        }
    }

    /// Deterministic random byte sequences — FieldIndex::build must never panic.
    #[test]
    fn fuzz_random_payloads() {
        let mut state: u64 = 0xabad1dea_deadc0de;
        let next = |s: &mut u64| -> u8 {
            *s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (*s >> 33) as u8
        };

        let mut buf = vec![0u8; 128];
        for _ in 0..1000 {
            let len = (next(&mut state) as usize % 128) + 1;
            for b in buf[..len].iter_mut() {
                *b = next(&mut state);
            }
            let slice = &buf[..len];
            let idx = FieldIndex::build(slice, 0);
            // If build succeeded, get() on the result must not panic either.
            if let Some(ref idx) = idx {
                let _ = idx.get("any_key");
                let _ = idx.len();
                let _ = idx.is_empty();
            }
        }
    }

    /// Adversarial map headers — FieldIndex::build must return None.
    #[test]
    fn fuzz_adversarial_map_count() {
        // MAP32 claiming 0xffffffff pairs with empty body
        let buf = [0xdfu8, 0xff, 0xff, 0xff, 0xff];
        assert_eq!(FieldIndex::build(&buf, 0).map(|_| ()), None);

        // MAP16 claiming 0xffff pairs with empty body
        let buf = [0xdeu8, 0xff, 0xff];
        assert_eq!(FieldIndex::build(&buf, 0).map(|_| ()), None);

        // Fixmap claiming 15 pairs but only 1 byte
        let buf = [0x8fu8];
        assert_eq!(FieldIndex::build(&buf, 0).map(|_| ()), None);
    }

    /// Non-map inputs must return None from build.
    #[test]
    fn fuzz_non_map_inputs() {
        let array_buf = encode(&json!([1, 2, 3]));
        assert!(FieldIndex::build(&array_buf, 0).is_none());

        let int_buf = encode(&json!(99));
        assert!(FieldIndex::build(&int_buf, 0).is_none());

        assert!(FieldIndex::build(&[], 0).is_none());

        let nil_buf = [0xc0u8];
        assert!(FieldIndex::build(&nil_buf, 0).is_none());
    }

    /// Out-of-bounds offset must return None.
    #[test]
    fn fuzz_out_of_bounds_offset() {
        let buf = encode(&json!({"x": 1}));
        assert!(FieldIndex::build(&buf, buf.len() + 100).is_none());
    }

    /// Threshold boundary: a 16-field doc should use Flat, 17-field should
    /// use HashMap. Fuzz both paths with truncation.
    #[test]
    fn fuzz_flat_vs_hashmap_threshold_truncation() {
        // 16 fields — uses Flat path
        let mut map16 = serde_json::Map::new();
        for i in 0..16 {
            map16.insert(format!("f{i}"), json!(i));
        }
        let buf16 = encode(&serde_json::Value::Object(map16));
        let idx16 = FieldIndex::build(&buf16, 0).unwrap();
        assert!(matches!(idx16.inner, IndexInner::Flat(_)));
        assert_eq!(idx16.len(), 16);

        // Truncate the 16-field buffer
        for t in 0..buf16.len() {
            let _ = FieldIndex::build(&buf16[..t], 0);
        }

        // 17 fields — uses HashMap path
        let mut map17 = serde_json::Map::new();
        for i in 0..17 {
            map17.insert(format!("g{i}"), json!(i));
        }
        let buf17 = encode(&serde_json::Value::Object(map17));
        let idx17 = FieldIndex::build(&buf17, 0).unwrap();
        assert!(matches!(idx17.inner, IndexInner::Map(_)));
        assert_eq!(idx17.len(), 17);

        // Truncate the 17-field buffer
        for t in 0..buf17.len() {
            let _ = FieldIndex::build(&buf17[..t], 0);
        }
    }

    /// Build a valid index then look up every present and absent key.
    #[test]
    fn fuzz_lookup_all_present_and_absent_keys() {
        let mut map = serde_json::Map::new();
        for i in 0..20u64 {
            map.insert(format!("key{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let idx = FieldIndex::build(&buf, 0).unwrap();

        for i in 0..20u64 {
            let k = format!("key{i}");
            let (start, _end) = idx.get(&k).unwrap();
            assert_eq!(read_i64(&buf, start), Some(i as i64));
        }

        // These keys are absent
        for absent in &["KEY0", "key20", "key-1", "", "key 0"] {
            assert!(idx.get(absent).is_none(), "key '{absent}' should be absent");
        }
    }
}
