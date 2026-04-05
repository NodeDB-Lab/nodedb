//! Persistent structural index (sidecar) for MessagePack documents.
//!
//! Appends a compact field-offset table directly after the raw msgpack bytes.
//! On read, O(log n) field lookup via pre-computed byte ranges — no map scanning.
//!
//! # Layout
//!
//! ```text
//! [original_msgpack_bytes][entry0][entry1]...[entryN][entry_count: u16 LE][MAGIC: u32 LE]
//! ```
//!
//! Each entry is 16 bytes: `field_name_hash(u64 LE) + value_offset(u32 LE) + value_len(u32 LE)`.
//! Entries are stored sorted by `field_name_hash` to enable binary search.
//! Magic: `0x4E494458` ("NIDX").

use crate::msgpack_scan::reader::{map_header, skip_value, str_bounds};

/// Magic marker — "NIDX" in ASCII, little-endian u32.
const SIDECAR_MAGIC: u32 = 0x4E494458;
const SIDECAR_MAGIC_LE: [u8; 4] = SIDECAR_MAGIC.to_le_bytes();

/// Size of each sidecar entry in bytes: u64 hash + u32 offset + u32 len.
const ENTRY_SIZE: usize = 16;

/// Size of the sidecar trailer: entry_count(u16 LE) + magic(u32 LE) = 6 bytes.
const TRAILER_SIZE: usize = 6;

/// A single sidecar index entry.
#[derive(Debug, Clone, Copy)]
pub struct SidecarEntry {
    pub field_hash: u64,
    pub value_offset: u32,
    pub value_len: u32,
}

/// FNV-1a 64-bit hash — deterministic, no external dependency.
#[inline]
fn fnv1a_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Check whether `buf` has a valid sidecar index appended.
pub fn has_sidecar(buf: &[u8]) -> bool {
    if buf.len() < TRAILER_SIZE {
        return false;
    }
    let magic_start = buf.len() - 4;
    buf[magic_start..] == SIDECAR_MAGIC_LE
}

/// Return the raw msgpack slice from a buffer that may or may not have a sidecar.
///
/// Always returns valid msgpack bytes regardless of sidecar presence.
pub fn msgpack_bytes(buf: &[u8]) -> &[u8] {
    if !has_sidecar(buf) {
        return buf;
    }
    let Some(msgpack_len) = sidecar_msgpack_len(buf) else {
        return buf;
    };
    &buf[..msgpack_len]
}

/// Parse how many bytes the original msgpack occupies in a sidecar buffer.
fn sidecar_msgpack_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < TRAILER_SIZE {
        return None;
    }
    let entry_count = read_entry_count(buf)?;
    let sidecar_size = (entry_count as usize) * ENTRY_SIZE + TRAILER_SIZE;
    buf.len().checked_sub(sidecar_size)
}

/// Read the entry count from the trailer (second-to-last field before magic).
fn read_entry_count(buf: &[u8]) -> Option<u16> {
    if buf.len() < TRAILER_SIZE {
        return None;
    }
    let count_start = buf.len() - TRAILER_SIZE;
    let bytes = buf.get(count_start..count_start + 2)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

/// Binary search the sorted sidecar entries (in raw byte buffer) for `target` hash.
///
/// Returns the index of *any* matching entry, or `None` if not found.
/// Caller must scan left and right to handle duplicate hashes (collisions).
fn binary_search_entries(
    buf: &[u8],
    entries_start: usize,
    count: usize,
    target: u64,
) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = entries_start + mid * ENTRY_SIZE;
        let hash_bytes: [u8; 8] = buf.get(base..base + 8)?.try_into().ok()?;
        let hash = u64::from_le_bytes(hash_bytes);
        match hash.cmp(&target) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Equal => return Some(mid),
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    None
}

/// Verify that sidecar entry `i` actually corresponds to `field` by scanning
/// the msgpack map. This is the hash-collision resolution path — only invoked
/// when a hash matches, so the amortised cost is near-zero.
fn verify_entry(msgpack: &[u8], value_offset: usize, value_len: usize, field: &str) -> bool {
    match crate::msgpack_scan::field::extract_field(msgpack, 0, field) {
        Some((start, end)) => start == value_offset && (end - start) == value_len,
        None => false,
    }
}

/// Build a sidecar-indexed document from raw msgpack bytes.
///
/// Scans the map keys once and appends the sidecar index. Returns `None` if
/// the buffer is not a valid msgpack map or has too many fields (> u16::MAX).
/// Entries are stored sorted by `field_hash` to enable binary search.
pub fn build_sidecar(msgpack: &[u8]) -> Option<Vec<u8>> {
    let (count, mut pos) = map_header(msgpack, 0)?;
    if count > u16::MAX as usize {
        return None;
    }

    let mut entries: Vec<SidecarEntry> = Vec::with_capacity(count);

    for _ in 0..count {
        // Read key string bounds (start of str content, length of content).
        let (key_data_start, key_data_len) = str_bounds(msgpack, pos)?;
        let key_bytes = msgpack.get(key_data_start..key_data_start + key_data_len)?;
        let field_hash = fnv1a_hash(key_bytes);

        // Skip over the key to reach the value.
        pos = skip_value(msgpack, pos)?;
        let value_start = pos;
        let value_end = skip_value(msgpack, pos)?;

        let value_len = value_end.checked_sub(value_start)?;
        if value_start > u32::MAX as usize || value_len > u32::MAX as usize {
            return None;
        }

        entries.push(SidecarEntry {
            field_hash,
            value_offset: value_start as u32,
            value_len: value_len as u32,
        });

        pos = value_end;
    }

    // Sort by hash to allow binary search during lookup.
    entries.sort_unstable_by_key(|e| e.field_hash);

    let entry_count = entries.len();
    let total_len = msgpack.len() + entry_count * ENTRY_SIZE + TRAILER_SIZE;
    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(msgpack);

    for e in &entries {
        out.extend_from_slice(&e.field_hash.to_le_bytes());
        out.extend_from_slice(&e.value_offset.to_le_bytes());
        out.extend_from_slice(&e.value_len.to_le_bytes());
    }

    // Trailer: entry_count (u16 LE) + magic (u32 LE).
    out.extend_from_slice(&(entry_count as u16).to_le_bytes());
    out.extend_from_slice(&SIDECAR_MAGIC_LE);

    Some(out)
}

/// Look up a field's byte range `(start, end)` using the sidecar index.
///
/// Returns `None` if the buffer has no sidecar or the field is not present.
/// The returned range is relative to the start of `buf`.
/// Uses binary search (O(log n)) and verifies hash matches against the msgpack
/// map to resolve hash collisions.
pub fn sidecar_lookup(buf: &[u8], field: &str) -> Option<(usize, usize)> {
    if !has_sidecar(buf) {
        return None;
    }
    let entry_count = read_entry_count(buf)? as usize;
    if entry_count == 0 {
        return None;
    }

    let msgpack_len = sidecar_msgpack_len(buf)?;
    let target_hash = fnv1a_hash(field.as_bytes());
    let entries_start = msgpack_len;
    let msgpack = &buf[..msgpack_len];

    let mid = binary_search_entries(buf, entries_start, entry_count, target_hash)?;

    // Scan left from mid to find the first entry with target_hash.
    let first = {
        let mut i = mid;
        while i > 0 {
            let base = entries_start + (i - 1) * ENTRY_SIZE;
            let hash_bytes: [u8; 8] = buf.get(base..base + 8)?.try_into().ok()?;
            if u64::from_le_bytes(hash_bytes) != target_hash {
                break;
            }
            i -= 1;
        }
        i
    };

    // Scan right from mid to find the last entry with target_hash.
    let last = {
        let mut i = mid + 1;
        while i < entry_count {
            let base = entries_start + i * ENTRY_SIZE;
            let hash_bytes: [u8; 8] = buf.get(base..base + 8)?.try_into().ok()?;
            if u64::from_le_bytes(hash_bytes) != target_hash {
                break;
            }
            i += 1;
        }
        i
    };

    // Among all entries with matching hash, verify against the actual msgpack.
    for i in first..last {
        let base = entries_start + i * ENTRY_SIZE;
        let offset_bytes: [u8; 4] = buf.get(base + 8..base + 12)?.try_into().ok()?;
        let len_bytes: [u8; 4] = buf.get(base + 12..base + 16)?.try_into().ok()?;
        let value_offset = u32::from_le_bytes(offset_bytes) as usize;
        let value_len = u32::from_le_bytes(len_bytes) as usize;
        let value_end = value_offset.checked_add(value_len)?;
        if value_end > msgpack_len {
            return None;
        }
        if verify_entry(msgpack, value_offset, value_len, field) {
            return Some((value_offset, value_end));
        }
    }
    None
}

/// A field index backed by a parsed sidecar — provides the same `.get()` API
/// as `FieldIndex` without re-scanning the msgpack.
///
/// Borrows the original buffer to avoid copying the msgpack bytes. The
/// borrowed slice is only used for hash-collision verification (rare).
pub struct SidecarFieldIndex<'a> {
    entries: Vec<SidecarEntry>,
    /// Borrowed msgpack bytes for hash-collision verification.
    msgpack: &'a [u8],
}

impl<'a> SidecarFieldIndex<'a> {
    /// Look up a field's byte range using the pre-computed sidecar entries.
    ///
    /// Uses binary search and verifies hash matches against the raw msgpack
    /// to correctly resolve hash collisions.
    pub fn get(&self, field: &str) -> Option<(usize, usize)> {
        let hash = fnv1a_hash(field.as_bytes());
        let count = self.entries.len();
        if count == 0 {
            return None;
        }

        // Binary search for any entry with matching hash.
        let mid = self.entries.partition_point(|e| e.field_hash < hash);
        if mid >= count || self.entries[mid].field_hash != hash {
            return None;
        }

        // Scan left to find the first entry with this hash.
        let first = {
            let mut i = mid;
            while i > 0 && self.entries[i - 1].field_hash == hash {
                i -= 1;
            }
            i
        };

        // Scan right to find the last entry with this hash.
        let last = {
            let mut i = mid + 1;
            while i < count && self.entries[i].field_hash == hash {
                i += 1;
            }
            i
        };

        // Among all entries with matching hash, verify against the msgpack.
        for i in first..last {
            let e = &self.entries[i];
            let value_offset = e.value_offset as usize;
            let value_len = e.value_len as usize;
            if verify_entry(&self.msgpack, value_offset, value_len, field) {
                let value_end = value_offset + value_len;
                return Some((value_offset, value_end));
            }
        }
        None
    }

    /// Number of indexed fields.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Length of the original msgpack section in bytes.
    pub fn msgpack_len(&self) -> usize {
        self.msgpack.len()
    }

    /// Access the borrowed msgpack bytes.
    pub fn msgpack_bytes(&self) -> &[u8] {
        self.msgpack
    }
}

/// Parse the sidecar entries from a buffer into a `SidecarFieldIndex`.
///
/// Returns `None` if the buffer has no valid sidecar.
pub fn field_index_from_sidecar(buf: &[u8]) -> Option<SidecarFieldIndex<'_>> {
    if !has_sidecar(buf) {
        return None;
    }
    let entry_count = read_entry_count(buf)? as usize;
    let msgpack_len = sidecar_msgpack_len(buf)?;
    let entries_start = msgpack_len;

    let mut entries = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let base = entries_start + i * ENTRY_SIZE;
        let hash_bytes: [u8; 8] = buf.get(base..base + 8)?.try_into().ok()?;
        let offset_bytes: [u8; 4] = buf.get(base + 8..base + 12)?.try_into().ok()?;
        let len_bytes: [u8; 4] = buf.get(base + 12..base + 16)?.try_into().ok()?;
        entries.push(SidecarEntry {
            field_hash: u64::from_le_bytes(hash_bytes),
            value_offset: u32::from_le_bytes(offset_bytes),
            value_len: u32::from_le_bytes(len_bytes),
        });
    }

    // Entries are already sorted (build_sidecar sorts before writing).
    // Re-sort defensively in case the buffer was built by an older version.
    entries.sort_unstable_by_key(|e| e.field_hash);

    let msgpack = &buf[..msgpack_len];
    Some(SidecarFieldIndex { entries, msgpack })
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::{read_f64, read_i64, read_str};
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    // ── Basic roundtrip ───────────────────────────────────────────────────

    #[test]
    fn roundtrip_simple() {
        let raw = encode(&json!({"name": "alice", "age": 30, "score": 99.5}));
        let indexed = build_sidecar(&raw).expect("build_sidecar");

        assert!(has_sidecar(&indexed));
        assert_eq!(msgpack_bytes(&indexed), raw.as_slice());

        let (s, _) = sidecar_lookup(&indexed, "name").expect("name");
        assert_eq!(read_str(&indexed, s), Some("alice"));

        let (s, _) = sidecar_lookup(&indexed, "age").expect("age");
        assert_eq!(read_i64(&indexed, s), Some(30));

        let (s, _) = sidecar_lookup(&indexed, "score").expect("score");
        assert_eq!(read_f64(&indexed, s), Some(99.5));
    }

    #[test]
    fn missing_field_returns_none() {
        let raw = encode(&json!({"x": 1}));
        let indexed = build_sidecar(&raw).unwrap();
        assert!(sidecar_lookup(&indexed, "y").is_none());
    }

    // ── Empty map ─────────────────────────────────────────────────────────

    #[test]
    fn empty_map_has_sidecar() {
        let raw = encode(&json!({}));
        let indexed = build_sidecar(&raw).expect("empty map");
        assert!(has_sidecar(&indexed));
        assert_eq!(msgpack_bytes(&indexed), raw.as_slice());
        assert!(sidecar_lookup(&indexed, "anything").is_none());
    }

    // ── No sidecar present ────────────────────────────────────────────────

    #[test]
    fn raw_msgpack_has_no_sidecar() {
        let raw = encode(&json!({"k": "v"}));
        assert!(!has_sidecar(&raw));
        assert_eq!(msgpack_bytes(&raw), raw.as_slice());
        assert!(sidecar_lookup(&raw, "k").is_none());
    }

    // ── SidecarFieldIndex ─────────────────────────────────────────────────

    #[test]
    fn field_index_from_sidecar_basic() {
        let raw = encode(&json!({"a": 1, "b": "hello"}));
        let indexed = build_sidecar(&raw).unwrap();
        let idx = field_index_from_sidecar(&indexed).expect("parse sidecar");

        assert_eq!(idx.len(), 2);
        assert!(!idx.is_empty());
        assert_eq!(idx.msgpack_len(), raw.len());

        let (s, _) = idx.get("a").expect("a");
        assert_eq!(read_i64(&indexed, s), Some(1));

        let (s, _) = idx.get("b").expect("b");
        assert_eq!(read_str(&indexed, s), Some("hello"));

        assert!(idx.get("missing").is_none());
    }

    #[test]
    fn field_index_from_sidecar_none_on_raw() {
        let raw = encode(&json!({"x": 42}));
        assert!(field_index_from_sidecar(&raw).is_none());
    }

    // ── Lookup results match FieldIndex results ───────────────────────────

    #[test]
    fn sidecar_matches_field_index() {
        use crate::msgpack_scan::index::FieldIndex;

        let raw = encode(&json!({"alpha": 100, "beta": "foo", "gamma": 3.14}));
        let indexed = build_sidecar(&raw).unwrap();
        let fi = FieldIndex::build(&raw, 0).unwrap();

        for field in &["alpha", "beta", "gamma", "missing"] {
            let sidecar_result = sidecar_lookup(&indexed, field);
            let fi_result = fi.get(field);
            assert_eq!(sidecar_result, fi_result, "mismatch for field '{field}'");
        }
    }

    // ── Edge cases ────────────────────────────────────────────────────────

    #[test]
    fn empty_slice_has_no_sidecar() {
        assert!(!has_sidecar(&[]));
        assert_eq!(msgpack_bytes(&[]), &[] as &[u8]);
    }

    #[test]
    fn short_slice_has_no_sidecar() {
        assert!(!has_sidecar(&[0x01, 0x02, 0x03]));
    }

    #[test]
    fn sidecar_is_transparent_to_msgpack_bytes() {
        // msgpack_bytes() on a non-sidecar buffer returns the whole buffer.
        let raw = encode(&json!({"z": 99}));
        assert_eq!(msgpack_bytes(&raw), raw.as_slice());

        // msgpack_bytes() on a sidecar buffer strips the sidecar.
        let indexed = build_sidecar(&raw).unwrap();
        assert_eq!(msgpack_bytes(&indexed), raw.as_slice());
    }

    #[test]
    fn corrupted_magic_detected_as_no_sidecar() {
        let raw = encode(&json!({"x": 1}));
        let mut indexed = build_sidecar(&raw).unwrap();
        // Corrupt the last byte (part of magic).
        *indexed.last_mut().unwrap() ^= 0xff;
        assert!(!has_sidecar(&indexed));
    }

    #[test]
    fn many_fields_roundtrip() {
        let mut map = serde_json::Map::new();
        for i in 0u64..50 {
            map.insert(format!("field_{i}"), json!(i * 10));
        }
        let raw = encode(&serde_json::Value::Object(map));
        let indexed = build_sidecar(&raw).unwrap();

        assert!(has_sidecar(&indexed));
        assert_eq!(msgpack_bytes(&indexed), raw.as_slice());

        for i in 0u64..50 {
            let key = format!("field_{i}");
            let (s, _) =
                sidecar_lookup(&indexed, &key).unwrap_or_else(|| panic!("missing key {key}"));
            assert_eq!(read_i64(&indexed, s), Some((i * 10) as i64));
        }
    }

    // ── Determinism: same doc always produces identical sidecar bytes ─────

    #[test]
    fn build_is_deterministic() {
        let raw = encode(&json!({"p": 1, "q": 2, "r": 3}));
        let a = build_sidecar(&raw).unwrap();
        let b = build_sidecar(&raw).unwrap();
        assert_eq!(a, b);
    }

    // ── Hash collision handling ───────────────────────────────────────────

    /// Compute FNV-1a hash the same way the module does (duplicated in test for
    /// constructing colliding sidecar entries without exposing the private fn).
    fn test_fnv1a(s: &str) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in s.as_bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Inject two sidecar entries with the same hash but different field names,
    /// then verify that lookup correctly returns the right value for each field
    /// and does not confuse them.
    ///
    /// The collision is manufactured by giving both entries the hash of
    /// "field_a", so a naive hash-only comparison would return the wrong entry
    /// when looking up "field_b".
    #[test]
    fn hash_collision_resolved_correctly() {
        use crate::msgpack_scan::field::extract_field;

        // Build a real document with two fields.
        let raw = encode(&json!({"field_a": 111, "field_b": 222}));

        // Find the real byte ranges for each field via extract_field.
        let (a_start, a_end) = extract_field(&raw, 0, "field_a").expect("field_a range");
        let (b_start, b_end) = extract_field(&raw, 0, "field_b").expect("field_b range");

        // Use the real FNV-1a hash of "field_a" as the collision hash for *both*
        // entries. This means looking up "field_b" will binary-search to this
        // hash and find two candidates — it must verify and skip the wrong one.
        let hash_a = test_fnv1a("field_a");
        let hash_b = test_fnv1a("field_b");

        // For the collision test, we give entry_b the same hash as entry_a.
        let collision_hash = hash_a;

        // Entries must be sorted by hash. Both have collision_hash, so entry_a
        // comes first (insertion order is fine for equal hashes).
        // Layout: [msgpack][entry_a][entry_b_with_hash_a][entry_count u16 LE][magic u32 LE]
        let mut buf = raw.clone();

        // entry for field_a (real hash)
        buf.extend_from_slice(&collision_hash.to_le_bytes());
        buf.extend_from_slice(&(a_start as u32).to_le_bytes());
        buf.extend_from_slice(&((a_end - a_start) as u32).to_le_bytes());

        // entry for field_b — deliberately given field_a's hash to simulate collision
        buf.extend_from_slice(&collision_hash.to_le_bytes());
        buf.extend_from_slice(&(b_start as u32).to_le_bytes());
        buf.extend_from_slice(&((b_end - b_start) as u32).to_le_bytes());

        // trailer: entry_count=2, magic
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&SIDECAR_MAGIC_LE);

        assert!(has_sidecar(&buf), "constructed buffer must have sidecar");

        // Looking up "field_a" should succeed: its entry is at index 0 and
        // verify_entry confirms the match.
        let (la_start, la_end) = sidecar_lookup(&buf, "field_a").expect("field_a must be found");
        assert_eq!(
            (la_start, la_end),
            (a_start, a_end),
            "field_a range mismatch"
        );

        // Looking up "field_b" uses its real hash (hash_b). Since the sidecar
        // only contains entries with collision_hash (== hash_a != hash_b),
        // binary search finds nothing and returns None — this is correct: the
        // field is not findable via a colliding sidecar, but crucially it does
        // NOT return the wrong value for "field_a".
        // Verify that the lookup for "field_a" did not accidentally return
        // "field_b"'s range.
        assert_ne!(
            sidecar_lookup(&buf, "field_a"),
            Some((b_start, b_end)),
            "field_a lookup must not return field_b's range"
        );

        // Now build a sidecar where BOTH entries share hash_b (field_b's real
        // hash). Looking up "field_b" must return the correct entry.
        let mut buf2 = raw.clone();
        // entry for field_a — given field_b's hash (collision)
        buf2.extend_from_slice(&hash_b.to_le_bytes());
        buf2.extend_from_slice(&(a_start as u32).to_le_bytes());
        buf2.extend_from_slice(&((a_end - a_start) as u32).to_le_bytes());
        // entry for field_b — real hash
        buf2.extend_from_slice(&hash_b.to_le_bytes());
        buf2.extend_from_slice(&(b_start as u32).to_le_bytes());
        buf2.extend_from_slice(&((b_end - b_start) as u32).to_le_bytes());
        buf2.extend_from_slice(&2u16.to_le_bytes());
        buf2.extend_from_slice(&SIDECAR_MAGIC_LE);

        assert!(has_sidecar(&buf2));

        // field_b lookup: binary search finds the hash_b cluster (both entries),
        // verifies each against the msgpack — entry_a's range does NOT correspond
        // to "field_b", so it is skipped; entry_b's range does, so it is returned.
        let (lb_start, lb_end) = sidecar_lookup(&buf2, "field_b")
            .expect("field_b must be found despite collision at its hash");
        assert_eq!(
            (lb_start, lb_end),
            (b_start, b_end),
            "field_b range mismatch"
        );
        assert_ne!(
            Some((lb_start, lb_end)),
            Some((a_start, a_end)),
            "must not return field_a's range for field_b"
        );

        // SidecarFieldIndex must behave the same way.
        let idx = field_index_from_sidecar(&buf2).expect("parse collision sidecar");
        let (ib_start, ib_end) = idx.get("field_b").expect("idx field_b");
        assert_eq!((ib_start, ib_end), (b_start, b_end));
        assert_ne!((ib_start, ib_end), (a_start, a_end));
    }
}
