//! Bitemporal EdgeStore primitives.
//!
//! Key layout appends a 20-digit zero-padded `system_from` ordinal suffix to
//! the legacy composite key:
//!
//! ```text
//! {collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}
//! ```
//!
//! `system_from` is an HLC-aligned `i64` ordinal (nanosecond-precision,
//! strictly monotonic per write) produced by
//! [`nodedb_types::OrdinalClock::next_ordinal`]. User-facing "AS OF SYSTEM
//! TIME `<ms>`" queries translate via
//! [`nodedb_types::ms_to_ordinal_upper`].
//!
//! Reverse-range scanning within the `edge_version_prefix` range yields the
//! latest-written version first — the basis for the Ceiling algorithm.
//!
//! Values are either a [`EdgeValuePayload`] (zerompk-encoded as a fixarray-3,
//! first byte `0x93`) or one of two single-byte sentinels:
//! [`TOMBSTONE_SENTINEL`] and [`GDPR_ERASURE_SENTINEL`]. The sentinel bytes
//! (`0xFF`, `0xFE`) never collide with a fixarray prefix (`0x90..=0x9f`).

use nodedb_types::TenantId;
use serde::{Deserialize, Serialize};

use super::store::{EDGES, Edge, EdgeStore, REVERSE_EDGES, redb_err};

/// Soft-delete marker.
pub const TOMBSTONE_SENTINEL: &[u8] = &[0xFF];

/// GDPR erasure marker — preserves coordinate existence, removes content.
/// Distinct from tombstone so audits can tell "user-deleted" from "legally erased".
pub const GDPR_ERASURE_SENTINEL: &[u8] = &[0xFE];

/// Width of the zero-padded `system_from` ordinal suffix.
pub const SYSTEM_TIME_WIDTH: usize = 20;

/// Bitemporal edge value payload. Wraps the caller's MessagePack property
/// blob with valid-time bounds.
///
/// `valid_until_ms` uses [`nodedb_types::OPEN_UPPER`] (`i64::MAX`) as the
/// open-upper sentinel.
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
pub struct EdgeValuePayload {
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub properties: Vec<u8>,
}

impl EdgeValuePayload {
    pub fn new(valid_from_ms: i64, valid_until_ms: i64, properties: Vec<u8>) -> Self {
        Self {
            valid_from_ms,
            valid_until_ms,
            properties,
        }
    }

    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode EdgeValuePayload: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode EdgeValuePayload: {e}"),
        })
    }
}

/// Is this raw redb value a soft-delete tombstone?
pub fn is_tombstone(bytes: &[u8]) -> bool {
    bytes == TOMBSTONE_SENTINEL
}

/// Is this raw redb value a GDPR erasure marker?
pub fn is_gdpr_erasure(bytes: &[u8]) -> bool {
    bytes == GDPR_ERASURE_SENTINEL
}

/// Is this raw redb value any non-payload sentinel?
pub fn is_sentinel(bytes: &[u8]) -> bool {
    is_tombstone(bytes) || is_gdpr_erasure(bytes)
}

/// Build a versioned edge key.
///
/// Returns an error if `system_from` is negative — key ordering semantics
/// require a non-negative suffix.
pub fn versioned_edge_key(
    collection: &str,
    src: &str,
    label: &str,
    dst: &str,
    system_from: i64,
) -> crate::Result<String> {
    if system_from < 0 {
        return Err(crate::Error::BadRequest {
            detail: format!("versioned_edge_key: negative system_from={system_from}"),
        });
    }
    Ok(format!(
        "{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:0width$}",
        width = SYSTEM_TIME_WIDTH
    ))
}

/// Build the version-range prefix for a base edge (collection, src, label, dst).
/// Reverse-scanning from `prefix_upper` back into this range yields versions
/// newest-first.
pub fn edge_version_prefix(collection: &str, src: &str, label: &str, dst: &str) -> String {
    format!("{collection}\x00{src}\x00{label}\x00{dst}\x00")
}

/// Decompose a versioned edge key into its components.
pub fn parse_versioned_edge_key(key: &str) -> Option<(&str, &str, &str, &str, i64)> {
    let mut parts = key.splitn(5, '\x00');
    let collection = parts.next()?;
    let src = parts.next()?;
    let label = parts.next()?;
    let dst = parts.next()?;
    let version = parts.next()?;
    if version.len() != SYSTEM_TIME_WIDTH {
        return None;
    }
    let system_from: i64 = version.parse().ok()?;
    Some((collection, src, label, dst, system_from))
}

impl EdgeStore {
    /// Write a new version of an edge at `system_from`. Maintains
    /// the reverse index with the same suffix so inbound traversal can
    /// version-scan symmetrically.
    ///
    /// Does NOT close prior versions' `system_until_ms` — that's Ceiling's job
    /// at read time (the closed-open interval is inferred from the next-newer
    /// version's `system_from`).
    pub fn put_edge_versioned(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> crate::Result<()> {
        let fwd = versioned_edge_key(collection, src, label, dst, system_from)?;
        let rev = versioned_edge_key(collection, dst, label, src, system_from)?;
        let payload =
            EdgeValuePayload::new(valid_from_ms, valid_until_ms, properties.to_vec()).encode()?;
        let t = tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, fwd.as_str()), payload.as_slice())
                .map_err(|e| redb_err("insert versioned edge", e))?;

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append a tombstone version at `system_from` — subsequent Ceiling
    /// reads within that system-time window return `None`.
    pub fn soft_delete_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        system_from: i64,
    ) -> crate::Result<()> {
        self.write_sentinel(
            tid,
            collection,
            src,
            label,
            dst,
            system_from,
            TOMBSTONE_SENTINEL,
        )
    }

    /// Append a GDPR-erasure version — distinct from a soft-delete so audits
    /// can distinguish user-visible removal from regulatory erasure.
    pub fn gdpr_erase_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        system_from: i64,
    ) -> crate::Result<()> {
        self.write_sentinel(
            tid,
            collection,
            src,
            label,
            dst,
            system_from,
            GDPR_ERASURE_SENTINEL,
        )
    }

    fn write_sentinel(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        system_from: i64,
        sentinel: &[u8],
    ) -> crate::Result<()> {
        let fwd = versioned_edge_key(collection, src, label, dst, system_from)?;
        let rev = versioned_edge_key(collection, dst, label, src, system_from)?;
        let t = tid.as_u32();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((t, fwd.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel edge", e))?;

            // Reverse-index tombstones are also single-byte sentinels so
            // reverse traversal can short-circuit on sighting one.
            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((t, rev.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel reverse", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit sentinel", e))?;
        Ok(())
    }

    /// Resolve the Ceiling: the latest version of
    /// `(collection, src, label, dst)` whose `system_from ≤ system_as_of`.
    ///
    /// Returns `Ok(None)` if no version exists at or before the cutoff, or if
    /// the latest qualifying version is a tombstone/GDPR erasure.
    ///
    /// When `valid_at_ms` is supplied, the resolved version must also satisfy
    /// `valid_from_ms ≤ valid_at_ms < valid_until_ms`; otherwise the method
    /// continues scanning to earlier system-time versions.
    pub fn ceiling_resolve_edge(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label: &str,
        dst: &str,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        if system_as_of < 0 {
            return Err(crate::Error::BadRequest {
                detail: format!("ceiling_resolve_edge: negative system_as_of={system_as_of}"),
            });
        }
        let prefix = edge_version_prefix(collection, src, label, dst);
        let upper = versioned_edge_key(collection, src, label, dst, system_as_of)?;
        let t = tid.as_u32();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Inclusive upper — the exact key at system_as_of is a valid ceiling.
        let range = table
            .range((t, prefix.as_str())..=(t, upper.as_str()))
            .map_err(|e| redb_err("ceiling range", e))?;

        // Walk newest-first by reversing the iterator.
        for entry in range.rev() {
            let (k, v) = entry.map_err(|e| redb_err("ceiling iter", e))?;
            let (kt, composite) = k.value();
            if kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let bytes = v.value();
            if is_tombstone(bytes) || is_gdpr_erasure(bytes) {
                return Ok(None);
            }
            let payload = EdgeValuePayload::decode(bytes)?;
            match valid_at_ms {
                Some(vt) if !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms) => {
                    // This system-time version didn't assert the fact at `vt` —
                    // keep scanning backwards for an earlier version whose
                    // valid-time covers `vt`.
                    continue;
                }
                _ => return Ok(Some(payload.properties)),
            }
        }
        Ok(None)
    }
}

/// Decode a raw edge value to an [`Edge`] projection, treating sentinels as
/// absent. Used by future current-state scanners.
#[allow(dead_code)] // consumed by Batch 2b when current-state filters wire up
pub(super) fn edge_from_versioned_entry(
    composite: &str,
    value: &[u8],
) -> Option<(Edge, EdgeValuePayload)> {
    if is_sentinel(value) {
        return None;
    }
    let (collection, src, label, dst, _sys) = parse_versioned_edge_key(composite)?;
    let payload = EdgeValuePayload::decode(value).ok()?;
    Some((
        Edge {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            properties: payload.properties.clone(),
        },
        payload,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: TenantId = TenantId::new(1);
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    #[test]
    fn version_key_zero_padded_20_digits() {
        let k = versioned_edge_key("c", "a", "L", "b", 42).unwrap();
        assert!(k.ends_with("\x0000000000000000000042"));
        assert_eq!(k.len(), "c\x00a\x00L\x00b\x00".len() + SYSTEM_TIME_WIDTH);
    }

    #[test]
    fn version_key_sorts_chronologically() {
        let a = versioned_edge_key("c", "a", "L", "b", 100).unwrap();
        let b = versioned_edge_key("c", "a", "L", "b", 2_000).unwrap();
        let c = versioned_edge_key("c", "a", "L", "b", 30_000_000).unwrap();
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn negative_system_time_rejected() {
        assert!(versioned_edge_key("c", "a", "L", "b", -1).is_err());
    }

    #[test]
    fn parse_versioned_roundtrip() {
        let k = versioned_edge_key("coll", "alice", "KNOWS", "bob", 1_700_000_000_000).unwrap();
        let (c, s, l, d, t) = parse_versioned_edge_key(&k).unwrap();
        assert_eq!(
            (c, s, l, d, t),
            ("coll", "alice", "KNOWS", "bob", 1_700_000_000_000)
        );
    }

    #[test]
    fn parse_rejects_wrong_width() {
        let bad = "c\x00a\x00L\x00b\x0042";
        assert!(parse_versioned_edge_key(bad).is_none());
    }

    #[test]
    fn sentinel_distinctness() {
        assert!(is_tombstone(TOMBSTONE_SENTINEL));
        assert!(!is_tombstone(GDPR_ERASURE_SENTINEL));
        assert!(is_gdpr_erasure(GDPR_ERASURE_SENTINEL));
        assert!(!is_gdpr_erasure(TOMBSTONE_SENTINEL));
        assert!(is_sentinel(TOMBSTONE_SENTINEL));
        assert!(is_sentinel(GDPR_ERASURE_SENTINEL));
        // MessagePack fixarray/fixmap prefix is NOT a sentinel.
        assert!(!is_sentinel(&[0x93]));
        assert!(!is_sentinel(&[0x83]));
    }

    #[test]
    fn payload_msgpack_roundtrip() {
        let p = EdgeValuePayload::new(1_000, 2_000, b"props".to_vec());
        let bytes = p.encode().unwrap();
        // zerompk encodes derived structs as fixarray — 3 fields → 0x93.
        assert_eq!(bytes[0], 0x93);
        // Sentinels (0xFF, 0xFE) stay disjoint from fixarray range (0x90..=0x9f).
        assert!(!is_sentinel(&bytes[..1]));
        let decoded = EdgeValuePayload::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn put_and_ceiling_resolves_latest_at_cutoff() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v2", 200, 200, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v3", 300, 300, i64::MAX)
            .unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 99, None)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 100, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 250, None)
                .unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 1_000, None)
                .unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[test]
    fn soft_delete_shadows_prior_version() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 100, 100, i64::MAX)
            .unwrap();
        store.soft_delete_edge(T, COLL, "a", "L", "b", 200).unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 150, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 200, None)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 1_000, None)
                .unwrap(),
            None
        );
    }

    #[test]
    fn gdpr_erase_distinct_from_tombstone_but_both_hide() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 100, 100, i64::MAX)
            .unwrap();
        store.gdpr_erase_edge(T, COLL, "a", "L", "b", 200).unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 150, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 300, None)
                .unwrap(),
            None
        );

        // Raw read confirms the sentinel byte is 0xFE, distinct from 0xFF.
        let key = versioned_edge_key(COLL, "a", "L", "b", 200).unwrap();
        let txn = store.db.begin_read().unwrap();
        let table = txn.open_table(EDGES).unwrap();
        let val = table.get((T.as_u32(), key.as_str())).unwrap().unwrap();
        assert_eq!(val.value(), GDPR_ERASURE_SENTINEL);
    }

    #[test]
    fn valid_time_filter_skips_nonmatching_versions() {
        let (store, _dir) = make_store();
        // v1: valid_time [0, 100)
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 10, 0, 100)
            .unwrap();
        // v2: valid_time [200, 300)  — disjoint hole between 100 and 200
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v2", 20, 200, 300)
            .unwrap();

        // Asking for valid_at=150 at any system-time cutoff: v2 fails valid
        // filter, v1 also fails — result is None.
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 1_000, Some(150))
                .unwrap(),
            None
        );
        // valid_at=50 → v2 fails, v1 passes.
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 1_000, Some(50))
                .unwrap(),
            Some(b"v1".to_vec())
        );
        // valid_at=250 → v2 passes immediately.
        assert_eq!(
            store
                .ceiling_resolve_edge(T, COLL, "a", "L", "b", 1_000, Some(250))
                .unwrap(),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn reverse_index_is_versioned_symmetrically() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(T, COLL, "a", "L", "b", b"v1", 100, 100, i64::MAX)
            .unwrap();

        let rev_key = versioned_edge_key(COLL, "b", "L", "a", 100).unwrap();
        let txn = store.db.begin_read().unwrap();
        let table = txn.open_table(REVERSE_EDGES).unwrap();
        let val = table.get((T.as_u32(), rev_key.as_str())).unwrap().unwrap();
        assert!(val.value().is_empty());
    }
}
