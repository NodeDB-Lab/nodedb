// SPDX-License-Identifier: BUSL-1.1

//! Edge staging and read-your-own-writes accessors for [`GraphTxnOverlay`].
//!
//! Every read merges the hot tier with the spilled runs. Edges order by
//! their encoded `(src, label, dst)`, so the out-edges of one source are one
//! prefix range. The in-edges of one destination need a full scan.

use std::borrow::Cow;

use super::txn_overlay::GraphTxnOverlay;
use super::types::{
    CollectionEdgePut, EdgeKey, EdgeRef, EdgeSlot, EdgeValidTime, GraphCollKey, StagedEdge,
    StagedEdgePut,
};
use crate::data::executor::handlers::spill::tiered::ScanBounds;
use crate::data::executor::handlers::spill::tiered::map::encode_key;
use crate::types::{DatabaseId, TenantId};

/// The MessagePack header of a three-element array: the first byte of every
/// encoded edge key.
const EDGE_KEY_HEADER: u8 = 0x93;

fn edge_key(src: &str, label: &str, dst: &str) -> EdgeKey {
    (src.to_string(), label.to_string(), dst.to_string())
}

/// The encoded-key prefix every out-edge of `src` shares.
fn src_prefix(src: &str) -> crate::Result<Vec<u8>> {
    let mut prefix = vec![EDGE_KEY_HEADER];
    prefix.extend(encode_key(&src)?);
    Ok(prefix)
}

impl GraphTxnOverlay {
    /// Stage an edge put (`(src, label, dst)` -> `properties`) with the
    /// apply's default valid time. Clears any pending tombstone for the same
    /// edge.
    pub fn stage_edge_put(
        &mut self,
        coll_key: GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
        properties: Vec<u8>,
    ) -> crate::Result<()> {
        self.stage_edge_put_valid(
            coll_key,
            (src, label, dst),
            properties,
            EdgeValidTime::default(),
        )
    }

    /// Stage an edge put with the valid time its statement named. Clears
    /// any pending tombstone for the same edge.
    pub fn stage_edge_put_valid(
        &mut self,
        coll_key: GraphCollKey,
        (src, label, dst): (&str, &str, &str),
        properties: Vec<u8>,
        valid: EdgeValidTime,
    ) -> crate::Result<()> {
        let key = edge_key(src, label, dst);
        let seq = self.record_edge_undo(&coll_key, &key)?;
        self.collections.entry(coll_key).or_default().edges.insert(
            key,
            EdgeSlot::Put { properties, valid },
            seq,
        );
        self.spill_past_threshold()
    }

    /// The valid time the staged put of `(src, label, dst)` named. The
    /// default when no staged put names one.
    pub fn staged_edge_valid_time(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<EdgeValidTime> {
        Ok(match self.slot(coll_key, src, label, dst)?.as_deref() {
            Some(EdgeSlot::Put { valid, .. }) => *valid,
            Some(EdgeSlot::Tombstone) | None => EdgeValidTime::default(),
        })
    }

    /// Stage an edge delete (tombstone). Clears any pending put for the same
    /// edge.
    pub fn stage_edge_delete(
        &mut self,
        coll_key: GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<()> {
        let key = edge_key(src, label, dst);
        let seq = self.record_edge_undo(&coll_key, &key)?;
        self.collections
            .entry(coll_key)
            .or_default()
            .edges
            .insert(key, EdgeSlot::Tombstone, seq);
        self.spill_past_threshold()
    }

    /// The staged state of one edge, in any tier.
    fn slot(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<Cow<'_, EdgeSlot>>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        overlay
            .edges
            .get(&EdgeRef(src, label, dst), self.space.reader())
    }

    /// Whether `(src, label, dst)` is currently staged in this transaction's
    /// own overlay: `Some(true)` when staged as a put, `Some(false)` when
    /// staged as a tombstone, `None` when this transaction has not touched
    /// the edge. The caller then resolves it against BASE state.
    pub fn staged_edge_presence(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<bool>> {
        Ok(self
            .slot(coll_key, src, label, dst)?
            .map(|slot| matches!(*slot, EdgeSlot::Put { .. })))
    }

    /// The staged put properties of `(src, label, dst)`, when one is staged.
    pub fn staged_edge_properties(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        Ok(match self.slot(coll_key, src, label, dst)?.as_deref() {
            Some(EdgeSlot::Put { properties, .. }) => Some(properties.clone()),
            Some(EdgeSlot::Tombstone) | None => None,
        })
    }

    /// True if `(src, label, dst)` has been staged-deleted in this
    /// transaction.
    pub fn is_edge_tombstoned(
        &self,
        coll_key: &GraphCollKey,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<bool> {
        Ok(matches!(
            self.slot(coll_key, src, label, dst)?.as_deref(),
            Some(EdgeSlot::Tombstone)
        ))
    }

    /// Every staged edge of `coll_key` that passes `keep`, in key order.
    fn scan_edges<T>(
        &self,
        coll_key: &GraphCollKey,
        bounds: ScanBounds,
        mut keep: impl FnMut(EdgeKey, Cow<'_, EdgeSlot>) -> Option<T>,
    ) -> crate::Result<Vec<T>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for item in overlay.edges.scan(bounds, self.space.reader()) {
            let (key, slot) = item?;
            if let Some(kept) = keep(key, slot) {
                out.push(kept);
            }
        }
        Ok(out)
    }

    /// Staged OUT-edges of `src_id` in `coll_key`: `(label, dst, props)`.
    pub fn edges_for_src(
        &self,
        coll_key: &GraphCollKey,
        src_id: &str,
    ) -> crate::Result<Vec<(String, String, Vec<u8>)>> {
        let bounds = ScanBounds {
            after: None,
            prefix: Some(src_prefix(src_id)?),
        };
        self.scan_edges(coll_key, bounds, |(s, l, d), slot| {
            if s != src_id {
                return None;
            }
            match slot.into_owned() {
                EdgeSlot::Put { properties, .. } => Some((l, d, properties)),
                EdgeSlot::Tombstone => None,
            }
        })
    }

    /// Staged IN-edges of `dst_id` in `coll_key`: `(label, src, props)`.
    pub fn edges_for_dst(
        &self,
        coll_key: &GraphCollKey,
        dst_id: &str,
    ) -> crate::Result<Vec<(String, String, Vec<u8>)>> {
        self.scan_edges(coll_key, ScanBounds::default(), |(s, l, d), slot| {
            if d != dst_id {
                return None;
            }
            match slot.into_owned() {
                EdgeSlot::Put { properties, .. } => Some((l, s, properties)),
                EdgeSlot::Tombstone => None,
            }
        })
    }

    /// The collections of `(database_id, tenant)` this transaction staged.
    fn tenant_collections(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> impl Iterator<Item = &GraphCollKey> {
        self.collections
            .keys()
            .filter(move |(db, t, _)| *db == database_id && *t == tenant)
    }

    /// Staged OUT-edges of `src_id` across every collection of the tenant.
    pub fn edges_for_src_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src_id: &str,
    ) -> crate::Result<Vec<(String, String, Vec<u8>)>> {
        let mut out = Vec::new();
        for coll_key in self.tenant_collections(database_id, tenant) {
            out.extend(self.edges_for_src(coll_key, src_id)?);
        }
        Ok(out)
    }

    /// Staged IN-edges of `dst_id` across every collection of the tenant.
    pub fn edges_for_dst_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        dst_id: &str,
    ) -> crate::Result<Vec<(String, String, Vec<u8>)>> {
        let mut out = Vec::new();
        for coll_key in self.tenant_collections(database_id, tenant) {
            out.extend(self.edges_for_dst(coll_key, dst_id)?);
        }
        Ok(out)
    }

    /// Whether `(src, label, dst)` is staged-deleted in any collection of
    /// the tenant.
    pub fn is_edge_tombstoned_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        src: &str,
        label: &str,
        dst: &str,
    ) -> crate::Result<bool> {
        for coll_key in self.tenant_collections(database_id, tenant) {
            if self.is_edge_tombstoned(coll_key, src, label, dst)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Every staged edge put identity of the tenant.
    pub fn all_staged_edges(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> crate::Result<Vec<(String, String, String)>> {
        let mut out = Vec::new();
        for coll_key in self.tenant_collections(database_id, tenant) {
            out.extend(
                self.scan_edges(coll_key, ScanBounds::default(), |key, slot| {
                    matches!(*slot, EdgeSlot::Put { .. }).then_some(key)
                })?,
            );
        }
        Ok(out)
    }

    /// Every staged edge put of the tenant with its collection and
    /// properties.
    pub fn staged_edge_puts(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> crate::Result<Vec<StagedEdgePut>> {
        let mut out = Vec::new();
        for coll_key in self.tenant_collections(database_id, tenant) {
            let collection = &coll_key.2;
            out.extend(
                self.scan_edges(coll_key, ScanBounds::default(), |key, slot| {
                    match slot.into_owned() {
                        EdgeSlot::Put { properties, .. } => {
                            Some((collection.clone(), key, properties))
                        }
                        EdgeSlot::Tombstone => None,
                    }
                })?,
            );
        }
        Ok(out)
    }

    /// Every staged edge tombstone identity of the tenant.
    pub fn all_tombstones(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
    ) -> crate::Result<Vec<(String, String, String)>> {
        let mut out = Vec::new();
        for coll_key in self.tenant_collections(database_id, tenant) {
            out.extend(self.staged_tombstones_for_collection(coll_key)?);
        }
        Ok(out)
    }

    /// Every staged edge of one collection, merged across the hot tier and
    /// the runs, in encoded-key order. The walk holds no edge it has passed.
    pub fn iter_staged_edges<'a>(
        &'a self,
        coll_key: &GraphCollKey,
    ) -> impl Iterator<Item = crate::Result<StagedEdge>> + 'a {
        let poisoned = self.guard().err();
        let scan = match poisoned {
            Some(_) => None,
            None => self
                .collections
                .get(coll_key)
                .map(|overlay| overlay.edges.iter()),
        };
        poisoned
            .map(Err)
            .into_iter()
            .chain(scan.into_iter().flatten().map(|item| {
                item.map(|((src, label, dst), slot)| match slot.into_owned() {
                    EdgeSlot::Put { properties, valid } => StagedEdge::Put {
                        src,
                        label,
                        dst,
                        properties,
                        valid,
                    },
                    EdgeSlot::Tombstone => StagedEdge::Tombstone { src, label, dst },
                })
            }))
    }

    /// Every staged edge put of one collection: `(src, label, dst, props,
    /// valid time)`.
    pub fn staged_edges_for_collection(
        &self,
        coll_key: &GraphCollKey,
    ) -> crate::Result<Vec<CollectionEdgePut>> {
        self.scan_edges(
            coll_key,
            ScanBounds::default(),
            |(s, l, d), slot| match slot.into_owned() {
                EdgeSlot::Put { properties, valid } => Some((s, l, d, properties, valid)),
                EdgeSlot::Tombstone => None,
            },
        )
    }

    /// Every staged edge tombstone of one collection: `(src, label, dst)`.
    pub fn staged_tombstones_for_collection(
        &self,
        coll_key: &GraphCollKey,
    ) -> crate::Result<Vec<(String, String, String)>> {
        self.scan_edges(coll_key, ScanBounds::default(), |key, slot| {
            matches!(*slot, EdgeSlot::Tombstone).then_some(key)
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
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![1, 2])
            .unwrap();
        let out = overlay.edges_for_src(&key("g"), "a").unwrap();
        assert_eq!(
            out,
            vec![("knows".to_string(), "b".to_string(), vec![1u8, 2u8])]
        );
        assert!(
            !overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap()
        );
    }

    #[test]
    fn stage_edge_delete_tombstones_and_clears_put() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![1])
            .unwrap();
        overlay
            .stage_edge_delete(key("g"), "a", "knows", "b")
            .unwrap();
        assert!(
            overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap()
        );
        assert!(overlay.edges_for_src(&key("g"), "a").unwrap().is_empty());
    }

    #[test]
    fn stage_put_after_delete_clears_tombstone() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_delete(key("g"), "a", "knows", "b")
            .unwrap();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![9])
            .unwrap();
        assert!(
            !overlay
                .is_edge_tombstoned(&key("g"), "a", "knows", "b")
                .unwrap()
        );
        assert_eq!(overlay.edges_for_src(&key("g"), "a").unwrap().len(), 1);
    }

    #[test]
    fn edges_for_dst_returns_in_edges() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_edge_put(key("g"), "a", "knows", "b", vec![])
            .unwrap();
        let out = overlay.edges_for_dst(&key("g"), "b").unwrap();
        assert_eq!(
            out,
            vec![("knows".to_string(), "a".to_string(), Vec::new())]
        );
    }
}
