// SPDX-License-Identifier: BUSL-1.1

//! Pure collection-name extractors for `TenantDataSnapshot` section keys.
//!
//! `TenantDataSnapshot` sections key their entries with three distinct scoped
//! formats (see [`crate::types::TenantDataSnapshot`] field docs):
//!
//! - **db-tenant-scoped** — `"{db}:{tid}:{collection}[:suffix...]"` (documents,
//!   indexes, timeseries memtable, vectors). Documents and indexes carry a
//!   trailing per-row suffix after the collection; vectors and timeseries have
//!   no suffix. The collection never contains `':'` or `'\0'`. Use
//!   [`extract_db_tenant_scoped_collection`].
//! - **db-scoped (collection-last)** — `"{db}:{tid}:{collection}"` where the
//!   collection is the remainder and may itself contain `':'` (flushed-ts
//!   segments, columnar engines). Use [`extract_db_scoped_collection`].
//! - **collection-name-only** — the key IS the bare collection name (kv tables).
//!   Routed directly; no extractor needed.
//!
//! Both the RESTORE topology splitter and the Raft snapshot SEND builder filter
//! sections by which vshard each entry's collection routes to, so the parsing
//! lives here once and is shared by both — never duplicated ad-hoc.
//!
//! The backup orchestrator additionally needs to filter a fully-gathered,
//! single-tenant [`TenantDataSnapshot`] *in place* to a set of source vshards
//! (the vshards a given node is the assigned source for), preserving the
//! per-tenant section shapes the RESTORE merge path consumes. That per-section
//! classification is the SAME vshard-of-collection logic the Raft snapshot SEND
//! builder applies, so it lives here once as [`retain_tenant_data_for_vshards`]
//! and is shared rather than duplicated.

use std::collections::HashSet;

use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::TenantDataSnapshot;

/// Extract the collection from a `"{db}:{tid}:{collection}[:suffix...]"` key.
///
/// Used by documents, indexes, vectors, and timeseries-memtable sections,
/// whose keys carry the leading `{db}:{tid}:` component and (for documents /
/// indexes) a trailing per-row suffix after the collection. Verifies the
/// embedded tenant matches `tenant_id`; the collection is the first
/// ':'-or-'\0'-delimited token after the prefix. Returns `None` on prefix
/// mismatch, too-few parts, or empty collection.
pub fn extract_db_tenant_scoped_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let mut it = key.splitn(3, ':');
    let _db = it.next()?;
    let tid = it.next()?;
    if tid.parse::<u64>().ok()? != tenant_id {
        return None;
    }
    let rest = it.next()?;
    let coll = rest.split([':', '\u{0}']).next()?;
    if coll.is_empty() { None } else { Some(coll) }
}

/// Extract the collection from a db-scoped `"{db}:{tid}:{collection}"` key,
/// verifying the embedded tenant matches `tenant_id`.
///
/// The first two ':' are structural (db, tid); the collection may itself
/// contain ':'. Returns `None` when the key has fewer than three parts or the
/// tenant does not match.
pub fn extract_db_scoped_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let mut it = key.splitn(3, ':');
    let _db = it.next()?;
    let tid = it.next()?;
    let coll = it.next()?;
    if tid.parse::<u64>().ok()? != tenant_id || coll.is_empty() {
        return None;
    }
    Some(coll)
}

/// Filter a single-tenant [`TenantDataSnapshot`] in place to only those
/// sections whose collection routes to a vshard in `source_vshards`.
///
/// The backup orchestrator gathers a full per-node snapshot (under RF>1 every
/// replica holds the full vshard data), then calls this so each node
/// contributes EXACTLY the vshards it is the assigned source for — the union
/// over nodes covers each vshard once (no duplication, no loss). The retained
/// section shapes are unchanged, so the RESTORE merge path (`merge_sections`)
/// consumes the output exactly as before.
///
/// `vshard_of` maps a collection name to its vshard (the caller passes the
/// canonical `vshard_for_collection(DEFAULT, _)`), matching the Raft snapshot
/// SEND builder. Every section kind the snapshot carries is classified here so
/// adding a section without updating this filter is impossible to miss:
///
/// - db-tenant-scoped keys (`documents`, `indexes`, `vectors`, `timeseries`)
///   via [`extract_db_tenant_scoped_collection`].
/// - db-scoped keys (`flushed_ts_segments`, `columnar_engines`) via
///   [`extract_db_scoped_collection`].
/// - collection-name-only keys (`kv_tables`) routed directly.
/// - graph `edges` via [`parse_versioned_edge_key`] (key embeds the collection).
/// - `surrogate_pk` by its explicit `collection` field.
/// - CRDT (`crdt_state`): per-collection, tenant-explicit. Each entry carries
///   its single collection, so it is kept iff that collection's vshard is in
///   `source_vshards` — the node owning the collection keeps it, every other
///   node drops it (captured exactly once, never duplicated).
pub fn retain_tenant_data_for_vshards(
    snap: &mut TenantDataSnapshot,
    tenant_id: u64,
    source_vshards: &HashSet<u32>,
    vshard_of: impl Fn(&str) -> u32,
) {
    let in_group_db_tenant_scoped = |key: &str| {
        extract_db_tenant_scoped_collection(key, tenant_id)
            .map(|c| source_vshards.contains(&vshard_of(c)))
            .unwrap_or(false)
    };
    let in_group_db_scoped = |key: &str| {
        extract_db_scoped_collection(key, tenant_id)
            .map(|c| source_vshards.contains(&vshard_of(c)))
            .unwrap_or(false)
    };

    snap.documents.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.indexes.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.vectors.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.timeseries
        .retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.flushed_ts_segments
        .retain(|b| in_group_db_scoped(&b.collection_key));
    snap.columnar_engines.retain(|(k, _)| in_group_db_scoped(k));
    // kv_tables / surrogate_pk: the key / field IS the collection name.
    snap.kv_tables
        .retain(|(k, _)| source_vshards.contains(&vshard_of(k)));
    snap.surrogate_pk
        .retain(|e| source_vshards.contains(&vshard_of(&e.collection)));
    // Graph edges: collection is the first '\0'-delimited key component. An
    // unparseable key has no determinable vshard; drop it from EVERY node's
    // retained set rather than duplicate it across all sources.
    snap.edges.retain(|(k, _)| {
        parse_versioned_edge_key(k)
            .map(|(collection, ..)| source_vshards.contains(&vshard_of(collection)))
            .unwrap_or(false)
    });

    // CRDT (`crdt_state`): each entry carries its single collection. Keep it iff
    // that collection's vshard is in this source set — exactly one source node
    // (the collection's owner) retains each entry.
    snap.crdt_state
        .retain(|(_, _, collection, _)| source_vshards.contains(&vshard_of(collection)));
}

#[cfg(test)]
mod tests {
    use super::{extract_db_scoped_collection, extract_db_tenant_scoped_collection};

    #[test]
    fn extract_db_tenant_scoped_collection_parses_key() {
        // Documents / indexes: collection is the 3rd token, suffix follows.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:snap_rt_docs:abcd1234", 1),
            Some("snap_rt_docs")
        );
        // '\0'-delimited per-row suffix.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:users\u{0}doc1", 1),
            Some("users")
        );
        // Vectors / timeseries: no suffix — collection is the whole remainder.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:metrics", 1),
            Some("metrics")
        );
        // Tenant mismatch → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:2:x:y", 1), None);
        // Empty collection → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:1:", 1), None);
        // Too few parts → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:1", 1), None);
    }

    #[test]
    fn extract_db_scoped_collection_parses_db_prefixed_key() {
        // "{db}:{tid}:{collection}" — first two ':' are structural.
        assert_eq!(
            extract_db_scoped_collection("0:7:metrics", 7),
            Some("metrics")
        );
        // Collection may itself contain ':'.
        assert_eq!(
            extract_db_scoped_collection("0:7:a:b", 7),
            Some("a:b"),
            "collection retains embedded ':'"
        );
        // Tenant mismatch → None.
        assert_eq!(extract_db_scoped_collection("0:8:metrics", 7), None);
        // Missing collection part → None.
        assert_eq!(extract_db_scoped_collection("0:7", 7), None);
        // Empty collection → None.
        assert_eq!(extract_db_scoped_collection("0:7:", 7), None);
    }

    /// The vshard-ownership filter must keep a section iff its collection routes
    /// into the node's assigned source vshards — and, across the three replicas
    /// of an RF=3 group, the UNION of retained columnar/timeseries sections must
    /// cover the data exactly once (no replica multiplication, no loss).
    #[test]
    fn retain_filters_append_sections_to_owning_vshard_only() {
        use super::retain_tenant_data_for_vshards;
        use crate::types::{TenantDataSnapshot, TsFlushedCollectionBlob};
        use std::collections::HashSet;

        const TID: u64 = 1;
        // Two collections deterministically mapped to two distinct vshards via
        // the test `vshard_of` closure (first char's code).
        let vshard_of = |c: &str| c.bytes().next().map(u32::from).unwrap_or(0);
        let va = vshard_of("alpha"); // 97
        let vb = vshard_of("beta"); //  98

        let template = || TenantDataSnapshot {
            timeseries: vec![
                (format!("0:{TID}:alpha"), b"a".to_vec()),
                (format!("0:{TID}:beta"), b"b".to_vec()),
            ],
            columnar_engines: vec![
                (format!("0:{TID}:alpha"), b"a".to_vec()),
                (format!("0:{TID}:beta"), b"b".to_vec()),
            ],
            flushed_ts_segments: vec![
                TsFlushedCollectionBlob {
                    collection_key: format!("0:{TID}:alpha"),
                    partitions: vec![],
                },
                TsFlushedCollectionBlob {
                    collection_key: format!("0:{TID}:beta"),
                    partitions: vec![],
                },
            ],
            kv_tables: vec![("alpha".into(), b"a".to_vec())],
            ..Default::default()
        };

        // Node owning only vshard(alpha) keeps alpha sections, drops beta.
        let mut node_a = template();
        let only_a: HashSet<u32> = [va].into_iter().collect();
        retain_tenant_data_for_vshards(&mut node_a, TID, &only_a, vshard_of);
        assert_eq!(node_a.timeseries.len(), 1);
        assert_eq!(node_a.timeseries[0].0, format!("0:{TID}:alpha"));
        assert_eq!(node_a.columnar_engines.len(), 1);
        assert_eq!(node_a.flushed_ts_segments.len(), 1);
        assert_eq!(node_a.kv_tables.len(), 1);

        // Node owning only vshard(beta) keeps beta sections, drops alpha.
        let mut node_b = template();
        let only_b: HashSet<u32> = [vb].into_iter().collect();
        retain_tenant_data_for_vshards(&mut node_b, TID, &only_b, vshard_of);
        assert_eq!(node_b.timeseries.len(), 1);
        assert_eq!(node_b.timeseries[0].0, format!("0:{TID}:beta"));
        assert_eq!(node_b.kv_tables.len(), 0, "alpha kv not owned by beta node");

        // Third replica owns neither → contributes nothing for these vshards.
        let mut node_c = template();
        let none: HashSet<u32> = HashSet::new();
        retain_tenant_data_for_vshards(&mut node_c, TID, &none, vshard_of);
        assert!(node_c.timeseries.is_empty());
        assert!(node_c.columnar_engines.is_empty());

        // Union over the three replicas = each collection's append section
        // exactly once (the fix: no ~3× multiplication).
        let union_ts = node_a.timeseries.len() + node_b.timeseries.len() + node_c.timeseries.len();
        assert_eq!(
            union_ts, 2,
            "each timeseries collection captured exactly once"
        );
    }

    /// A single source node owning ALL vshards (single-node / single-replica)
    /// must retain every section — the filter is a no-op there.
    #[test]
    fn retain_is_noop_when_node_owns_all_vshards() {
        use super::retain_tenant_data_for_vshards;
        use crate::types::TenantDataSnapshot;
        use std::collections::HashSet;

        let vshard_of = |c: &str| c.bytes().next().map(u32::from).unwrap_or(0);
        let mut snap = TenantDataSnapshot {
            timeseries: vec![("0:1:alpha".into(), b"a".to_vec())],
            columnar_engines: vec![("0:1:beta".into(), b"b".to_vec())],
            kv_tables: vec![("gamma".into(), b"g".to_vec())],
            ..Default::default()
        };
        let all: HashSet<u32> = ["alpha", "beta", "gamma"]
            .iter()
            .map(|c| vshard_of(c))
            .collect();
        retain_tenant_data_for_vshards(&mut snap, 1, &all, vshard_of);
        assert_eq!(snap.timeseries.len(), 1);
        assert_eq!(snap.columnar_engines.len(), 1);
        assert_eq!(snap.kv_tables.len(), 1);
    }
}
