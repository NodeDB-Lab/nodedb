// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal write paths on `EdgeStore`:
//! `put_edge_versioned`, `soft_delete_edge`, `gdpr_erase_edge`.

use redb::{ReadableDatabase, ReadableTable, WriteTransaction};

use super::keys::{
    EdgeRef, GDPR_ERASURE_SENTINEL, TOMBSTONE_SENTINEL, edge_version_prefix, is_sentinel,
    versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use super::revert::{EdgeCountChange, EdgeVersionWrite};
use crate::engine::graph::edge_store::stats::table::{GRAPH_STATS, SummaryRow, summary_key};
use crate::engine::graph::edge_store::stats::update::{
    EdgeStatsKey, decrement_for_delete, increment_for_insert,
};
use crate::engine::graph::edge_store::store::{
    EDGES, EdgeStore, NODE_SURROGATES, REVERSE_EDGES, redb_err,
};

impl EdgeStore {
    /// Write a new version of an edge at `system_from`. Maintains
    /// the reverse index with the same suffix so inbound traversal can
    /// version-scan symmetrically.
    ///
    /// Does NOT close prior versions' `system_until` — Ceiling infers the
    /// closed-open interval at read time from the next-newer version's
    /// `system_from`.
    pub fn put_edge_versioned(
        &self,
        edge: EdgeRef<'_>,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> crate::Result<()> {
        self.put_edge_versioned_with_stats(
            edge,
            properties,
            system_from,
            valid_from_ms,
            valid_until_ms,
            true,
        )
    }

    /// Write a version while allowing a dual-home replica to skip logical
    /// cardinality accounting. Cross-vShard graph writes persist the same edge
    /// on both endpoint homes for OUT and IN traversal, but only the source home
    /// owns the logical edge for global statistics.
    pub fn put_edge_versioned_with_stats(
        &self,
        edge: EdgeRef<'_>,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
        account_stats: bool,
    ) -> crate::Result<()> {
        self.put_edge_version_recorded(
            edge,
            properties,
            system_from,
            valid_from_ms,
            valid_until_ms,
            account_stats,
        )
        .map(drop)
    }

    /// Write a version like [`Self::put_edge_versioned_with_stats`] and
    /// return what it changed, so [`Self::remove_edge_version`] can remove it.
    pub fn put_edge_version_recorded(
        &self,
        edge: EdgeRef<'_>,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
        account_stats: bool,
    ) -> crate::Result<EdgeVersionWrite> {
        let mut written = EdgeVersionWrite::at(system_from);
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
        let payload =
            EdgeValuePayload::new(valid_from_ms, valid_until_ms, properties.to_vec()).encode()?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            written.prior_forward = edges
                .insert((d, t, fwd.as_str()), payload.as_slice())
                .map_err(|e| redb_err("insert versioned edge", e))?
                .map(|prior| prior.value().to_vec());
            drop(edges);

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            written.prior_reverse = rev_t
                .insert((d, t, rev.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?
                .map(|prior| prior.value().to_vec());
            drop(rev_t);

            if account_stats {
                written.counted = increment_for_insert(
                    &write_txn,
                    EdgeStatsKey {
                        db: d,
                        tid: t,
                        collection: edge.collection,
                        label: edge.label,
                        src: edge.src,
                        dst: edge.dst,
                    },
                    system_from,
                )?
                .then_some(EdgeCountChange::Added);
            } else {
                // Suppress the lazy edge-scan rebuild on a destination-only
                // replica: absence of a summary means "legacy stats missing",
                // while an explicit zero summary means "this shard owns no
                // logical edges for this collection". Preserve an existing
                // nonzero summary because this core may canonically own other
                // source-homed edges in the same collection.
                let key = summary_key(edge.collection);
                let mut stats = write_txn
                    .open_table(GRAPH_STATS)
                    .map_err(|e| redb_err("open graph_stats", e))?;
                if stats
                    .get((d, t, key.as_str()))
                    .map_err(|e| redb_err("read graph summary", e))?
                    .is_none()
                {
                    let zero = SummaryRow::zero().encode()?;
                    stats
                        .insert((d, t, key.as_str()), zero.as_slice())
                        .map_err(|e| redb_err("insert zero graph summary", e))?;
                    written.summary_created = true;
                }
            }

            // Endpoint identities commit with the edge, not after it: a crash
            // between two transactions would leave an edge whose nodes have no
            // global identity, which is exactly the post-restart state this
            // table exists to prevent.
            let mut surrogates = write_txn
                .open_table(NODE_SURROGATES)
                .map_err(|e| redb_err("open node_surrogates", e))?;
            for (node, surrogate) in [
                (edge.src, edge.src_surrogate),
                (edge.dst, edge.dst_surrogate),
            ] {
                let raw = surrogate.as_u32();
                if raw == 0 {
                    continue;
                }
                let prior = surrogates
                    .insert((d, t, node), raw)
                    .map_err(|e| redb_err("insert node surrogate", e))?
                    .map(|prior| prior.value());
                // A self-loop binds one node twice. Its first prior is the
                // one the node held before this write.
                if !written.prior_bindings.iter().any(|(seen, _)| seen == node) {
                    written.prior_bindings.push((node.to_string(), prior));
                }
            }
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(written)
    }

    /// BiTemporalFK enforcement: close a referrer edge by appending a new
    /// version that copies the latest live version's properties and
    /// `valid_from_ms`, but bounds `valid_until_ms` to `now`. Preserves
    /// historical truth — the edge existed in valid time `[valid_from, now)`.
    ///
    /// Returns `Ok(false)` when no live version exists (already closed,
    /// tombstoned, GDPR-erased, or never written) — the caller may treat
    /// this as a no-op.
    pub fn close_referrer_edge(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        now_valid_ms: i64,
    ) -> crate::Result<bool> {
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper =
            versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let prior = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| redb_err("begin_read", e))?;
            let table = read_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let range = table
                .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
                .map_err(|e| redb_err("close referrer range", e))?;
            let mut found: Option<EdgeValuePayload> = None;
            for entry in range.rev() {
                let (k, v) = entry.map_err(|e| redb_err("close referrer iter", e))?;
                let (kd, kt, composite) = k.value();
                if kd != d || kt != t || !composite.starts_with(&prefix) {
                    break;
                }
                let bytes = v.value();
                if is_sentinel(bytes) {
                    return Ok(false);
                }
                let payload = EdgeValuePayload::decode(bytes)?;
                if payload.valid_from_ms <= now_valid_ms && now_valid_ms < payload.valid_until_ms {
                    found = Some(payload);
                    break;
                }
            }
            match found {
                Some(p) => p,
                None => return Ok(false),
            }
        };

        self.put_edge_versioned(
            edge,
            &prior.properties,
            system_from,
            prior.valid_from_ms,
            now_valid_ms,
        )?;
        Ok(true)
    }

    /// Append a tombstone version at `system_from`.
    pub fn soft_delete_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, TOMBSTONE_SENTINEL, true)
            .map(drop)
    }

    /// Append a tombstone without double-decrementing global statistics on the
    /// destination-home replica of a dual-homed edge.
    pub fn soft_delete_edge_with_stats(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        account_stats: bool,
    ) -> crate::Result<()> {
        self.soft_delete_edge_recorded(edge, system_from, account_stats)
            .map(drop)
    }

    /// Append a tombstone like [`Self::soft_delete_edge_with_stats`] and
    /// return what it changed, so [`Self::remove_edge_version`] can remove it.
    pub fn soft_delete_edge_recorded(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        account_stats: bool,
    ) -> crate::Result<EdgeVersionWrite> {
        self.write_sentinel(edge, system_from, TOMBSTONE_SENTINEL, account_stats)
    }

    /// Append a GDPR-erasure version — distinct from a soft-delete so audits
    /// can distinguish user-visible removal from regulatory erasure.
    pub fn gdpr_erase_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, GDPR_ERASURE_SENTINEL, true)
            .map(drop)
    }

    fn write_sentinel(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        sentinel: &[u8],
        account_stats: bool,
    ) -> crate::Result<EdgeVersionWrite> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let written = write_sentinel_in(&write_txn, edge, system_from, sentinel, account_stats)?;
        write_txn
            .commit()
            .map_err(|e| redb_err("commit sentinel", e))?;
        Ok(written)
    }
}

/// Append a sentinel version at `system_from` inside `write_txn`, and return
/// what it changed. The caller commits.
pub(in crate::engine::graph::edge_store) fn write_sentinel_in(
    write_txn: &WriteTransaction,
    edge: EdgeRef<'_>,
    system_from: i64,
    sentinel: &[u8],
    account_stats: bool,
) -> crate::Result<EdgeVersionWrite> {
    let mut written = EdgeVersionWrite::at(system_from);
    debug_assert!(
        is_sentinel(sentinel),
        "write_sentinel called with non-sentinel bytes"
    );
    let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
    let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
    let d = edge.db.as_u64();
    let t = edge.tid.as_u64();

    let mut edges = write_txn
        .open_table(EDGES)
        .map_err(|e| redb_err("open edges", e))?;
    written.prior_forward = edges
        .insert((d, t, fwd.as_str()), sentinel)
        .map_err(|e| redb_err("insert sentinel edge", e))?
        .map(|prior| prior.value().to_vec());
    drop(edges);

    let mut rev_t = write_txn
        .open_table(REVERSE_EDGES)
        .map_err(|e| redb_err("open reverse", e))?;
    written.prior_reverse = rev_t
        .insert((d, t, rev.as_str()), sentinel)
        .map_err(|e| redb_err("insert sentinel reverse", e))?
        .map(|prior| prior.value().to_vec());
    drop(rev_t);

    if account_stats {
        written.counted = decrement_for_delete(
            write_txn,
            EdgeStatsKey {
                db: d,
                tid: t,
                collection: edge.collection,
                label: edge.label,
                src: edge.src,
                dst: edge.dst,
            },
            system_from,
        )?
        .then_some(EdgeCountChange::Removed);
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, label, dst)
    }

    #[test]
    fn soft_delete_shadows_prior_version() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store.soft_delete_edge(e("a", "L", "b"), 200).unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 150, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 200, None)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
                .unwrap(),
            None
        );
    }

    #[test]
    fn gdpr_erase_distinct_from_tombstone_but_both_hide() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store.gdpr_erase_edge(e("a", "L", "b"), 200).unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 150, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 300, None)
                .unwrap(),
            None
        );

        // Raw read confirms the sentinel byte is 0xFE, distinct from 0xFF.
        let key = versioned_edge_key(COLL, "a", "L", "b", 200).unwrap();
        let txn = store.db.begin_read().unwrap();
        let table = txn.open_table(EDGES).unwrap();
        let val = table
            .get((DB.as_u64(), T.as_u64(), key.as_str()))
            .unwrap()
            .unwrap();
        assert_eq!(val.value(), GDPR_ERASURE_SENTINEL);
        assert_ne!(val.value(), TOMBSTONE_SENTINEL);
    }

    #[test]
    fn reverse_index_is_versioned_symmetrically() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();

        let rev_key = versioned_edge_key(COLL, "b", "L", "a", 100).unwrap();
        let txn = store.db.begin_read().unwrap();
        let table = txn.open_table(REVERSE_EDGES).unwrap();
        let val = table
            .get((DB.as_u64(), T.as_u64(), rev_key.as_str()))
            .unwrap()
            .unwrap();
        assert!(val.value().is_empty());

        // Payload encoding check.
        let p = EdgeValuePayload::new(0, i64::MAX, b"x".to_vec());
        assert_eq!(p.encode().unwrap()[0], 0x93);
    }

    #[test]
    fn close_referrer_edge_appends_closed_version() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"props", 100, 100, i64::MAX)
            .unwrap();

        // Close at system=200, valid_until=500.
        let closed = store
            .close_referrer_edge(e("a", "L", "b"), 200, 500)
            .unwrap();
        assert!(closed);

        // Read inside the closed valid interval still returns the props.
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(300))
                .unwrap(),
            Some(b"props".to_vec())
        );
    }

    #[test]
    fn close_referrer_edge_noop_when_already_closed_or_absent() {
        let (store, _dir) = make_store();
        // No prior version → false.
        let r = store
            .close_referrer_edge(e("missing", "L", "x"), 100, 200)
            .unwrap();
        assert!(!r);

        // Tombstoned referent → false.
        store
            .put_edge_versioned(e("a", "L", "b"), b"p", 50, 50, i64::MAX)
            .unwrap();
        store.soft_delete_edge(e("a", "L", "b"), 60).unwrap();
        let r = store
            .close_referrer_edge(e("a", "L", "b"), 200, 100)
            .unwrap();
        assert!(!r);
    }
}
