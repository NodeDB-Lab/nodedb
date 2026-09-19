// SPDX-License-Identifier: BUSL-1.1

//! Edge staging and read-your-own-writes accessors for [`GraphTxnOverlay`].

use crate::types::{DatabaseId, TenantId};

use super::txn_overlay::GraphTxnOverlay;
use super::types::GraphCollKey;

impl GraphTxnOverlay {
    /// Stage an edge put: adds to the pending add-set and clears any
    /// pending tombstone for the same identity (last-writer-wins within the
    /// transaction).
    pub fn stage_edge_put(
        &mut self,
        coll_key: GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
        properties: Vec<u8>,
    ) {
        let key = (src.to_string(), label.to_string(), dst.to_string());
        self.record_edge_undo(&coll_key, &key);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.pending_edge_tombstones.remove(&key);
        overlay.pending_edges.insert(key, properties);
    }

    /// Stage an edge delete: adds a tombstone and clears any pending put for
    /// the same identity.
    pub fn stage_edge_delete(&mut self, coll_key: GraphCollKey, src: &str, label: &str, dst: &str) {
        let key = (src.to_string(), label.to_string(), dst.to_string());
        self.record_edge_undo(&coll_key, &key);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.pending_edges.remove(&key);
        overlay.pending_edge_tombstones.insert(key);
    }

    /// Whether `(src, label, dst)` is currently staged in this transaction's
    /// own overlay: `Some(true)` when staged as a put, `Some(false)` when
    /// staged as a tombstone, `None` when this transaction has not touched
    /// the edge — the caller must then resolve it against BASE state.
    pub fn staged_edge_presence(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> Option<bool> {
        let overlay = self.collections.get(coll_key)?;
        let key = (src.to_string(), label.to_string(), dst.to_string());
        if overlay.pending_edges.contains_key(&key) {
            Some(true)
        } else if overlay.pending_edge_tombstones.contains(&key) {
            Some(false)
        } else {
            None
        }
    }

    /// True if `(src, label, dst)` has been staged-deleted in this
    /// transaction.
    pub fn is_edge_tombstoned(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> bool {
        self.collections.get(coll_key).is_some_and(|overlay| {
            overlay.pending_edge_tombstones.contains(&(
                src.to_string(),
                label.to_string(),
                dst.to_string(),
            ))
        })
    }

    /// Staged out-edges from `src_id`: `(label, dst, properties)`, excluding
    /// anything tombstoned (staging never leaves a key in both sets, so no
    /// extra filter is needed here).
    pub fn edges_for_src<'a>(
        &'a self,
        coll_key: &GraphCollKey,
        src_id: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a [u8])> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(move |overlay| {
                overlay
                    .pending_edges
                    .iter()
                    .filter_map(move |((s, l, d), props)| {
                        (s == src_id).then_some((l.as_str(), d.as_str(), props.as_slice()))
                    })
            })
    }

    /// Staged in-edges into `dst_id`: `(label, src, properties)`.
    pub fn edges_for_dst<'a>(
        &'a self,
        coll_key: &GraphCollKey,
        dst_id: &str,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a [u8])> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(move |overlay| {
                overlay
                    .pending_edges
                    .iter()
                    .filter_map(move |((s, l, d), props)| {
                        (d == dst_id).then_some((l.as_str(), s.as_str(), props.as_slice()))
                    })
            })
    }

    /// Staged out-edges from `src_id` across every collection this
    /// transaction has touched for `(database_id, tenant)`: `(label, dst,
    /// properties)`. Neighbors/Hop read the CSR partition tenant-wide (it
    /// carries no `collection` field on the plan), so the read-merge cannot
    /// scope to one collection the way the write side does.
    pub fn edges_for_src_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src_id: &str,
    ) -> Vec<(String, String, Vec<u8>)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .iter()
                    .filter(move |((s, _, _), _)| s == src_id)
                    .map(|((_, l, d), props)| (l.clone(), d.clone(), props.clone()))
            })
            .collect()
    }

    /// Staged in-edges into `dst_id` across every collection for
    /// `(database_id, tenant)`: `(label, src, properties)`.
    pub fn edges_for_dst_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        dst_id: &str,
    ) -> Vec<(String, String, Vec<u8>)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .iter()
                    .filter(move |((_, _, d), _)| d == dst_id)
                    .map(|((s, l, _), props)| (l.clone(), s.clone(), props.clone()))
            })
            .collect()
    }

    /// True if `(src, label, dst)` was tombstoned in ANY collection this
    /// transaction has touched for `(database_id, tenant)` -- the tenant-wide
    /// counterpart of `is_edge_tombstoned`.
    pub fn is_edge_tombstoned_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src: &str,
        label: &str,
        dst: &str,
    ) -> bool {
        let key = (src.to_string(), label.to_string(), dst.to_string());
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .any(|(_, overlay)| overlay.pending_edge_tombstones.contains(&key))
    }

    /// Every staged edge put `(src, label, dst)` across every collection this
    /// transaction has touched for `(database_id, tenant)`. Feeds the
    /// multi-hop / subgraph read-your-own-writes overlay translation.
    pub fn all_staged_edges(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> Vec<(String, String, String)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edges
                    .keys()
                    .map(|(s, l, d)| (s.clone(), l.clone(), d.clone()))
            })
            .collect()
    }

    /// Every staged edge tombstone `(src, label, dst)` across every collection
    /// this transaction has touched for `(database_id, tenant)`.
    pub fn all_tombstones(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> Vec<(String, String, String)> {
        self.collections
            .iter()
            .filter(|((db, t, _), _)| *db == database_id && *t == tenant)
            .flat_map(|(_, overlay)| {
                overlay
                    .pending_edge_tombstones
                    .iter()
                    .map(|(s, l, d)| (s.clone(), l.clone(), d.clone()))
            })
            .collect()
    }

    /// Every staged edge put in `coll_key`: `(src, label, dst, properties)`.
    /// Feeds transaction-resolve serialization, which needs the full put
    /// set (identity + properties) for exactly one collection rather than
    /// the tenant-wide identity-only views `all_staged_edges` returns.
    pub fn staged_edges_for_collection<'a>(
        &'a self,
        coll_key: &GraphCollKey,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a str, &'a [u8])> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay.pending_edges.iter().map(|((s, l, d), props)| {
                    (s.as_str(), l.as_str(), d.as_str(), props.as_slice())
                })
            })
    }

    /// Every staged edge tombstone in `coll_key`: `(src, label, dst)`. The
    /// per-collection counterpart of `staged_edges_for_collection`.
    pub fn staged_tombstones_for_collection<'a>(
        &'a self,
        coll_key: &GraphCollKey,
    ) -> impl Iterator<Item = (&'a str, &'a str, &'a str)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay
                    .pending_edge_tombstones
                    .iter()
                    .map(|(s, l, d)| (s.as_str(), l.as_str(), d.as_str()))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn stage_edge_put_then_visible_for_src() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1, 2]);
        let out: Vec<_> = overlay.edges_for_src(&key("g"), "a").collect();
        assert_eq!(out, vec![("knows", "b", &[1u8, 2u8][..])]);
        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
    }

    #[test]
    fn stage_edge_delete_tombstones_and_clears_put() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![1]);
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");
        assert!(overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 0);
    }

    #[test]
    fn stage_put_after_delete_clears_tombstone() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_delete(key("g"), "a", "knows", "b");
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![9]);
        assert!(!overlay.is_edge_tombstoned(&key("g"), "a", "knows", "b"));
        assert_eq!(overlay.edges_for_src(&key("g"), "a").count(), 1);
    }

    #[test]
    fn edges_for_dst_returns_in_edges() {
        let mut overlay = GraphTxnOverlay::new();
        overlay.stage_edge_put(key("g"), "a", "knows", "b", vec![]);
        let out: Vec<_> = overlay.edges_for_dst(&key("g"), "b").collect();
        assert_eq!(out, vec![("knows", "a", &[][..])]);
    }
}
