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
//!   collection is the remainder and can itself contain `':'` (flushed-ts
//!   segments, columnar engines, kv tables). Use [`extract_db_scoped_collection`].
//!
//! Every extracted collection is the name the Data Plane stores it under:
//! database-qualified (`"{db}/{name}"`) outside the default database.
//! [`homes_of_stored`] maps a stored record to its homes by the shared
//! [`RecordHomes`] rule: a row by its collection, an edge by its endpoints.
//!
//! The Raft snapshot SEND builder filters sections by the homes of each
//! entry, so the parsing lives here once.
//!
//! The backup orchestrator and MOVE TENANT capture filter a fully-gathered,
//! single-tenant [`TenantDataSnapshot`] *in place* to the vShards one node is
//! the assigned source for, preserving the section shapes the RESTORE merge
//! path consumes. [`retain_tenant_data_for_vshards`] keeps each record on the
//! source of its owner home, so every record is captured exactly once.

use std::collections::HashSet;

use nodedb_types::{CollectionKey, DatabaseId};

use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::{HomedRecord, RecordHomes, TenantDataSnapshot};

/// One snapshot entry, named as the Data Plane stores it.
#[derive(Debug, Clone, Copy)]
pub enum StoredRecord<'a> {
    /// A row, or a per-collection section, of `collection`.
    Row { collection: &'a str },
    /// A graph edge of `collection` from `src` to `dst`.
    Edge {
        collection: &'a str,
        src: &'a str,
        dst: &'a str,
    },
    /// Cells of the array `array` that route to `vshard`.
    ArrayCells { array: &'a str, vshard: u32 },
}

impl<'a> StoredRecord<'a> {
    /// The collection the entry belongs to, as the Data Plane stores it.
    pub fn collection(self) -> &'a str {
        match self {
            Self::Row { collection } | Self::Edge { collection, .. } => collection,
            Self::ArrayCells { array, .. } => array,
        }
    }

    /// The edge a versioned edge key names, or `None` for a malformed key.
    pub fn from_edge_key(key: &'a str) -> Option<Self> {
        parse_versioned_edge_key(key).map(|(collection, src, _, dst, _)| Self::Edge {
            collection,
            src,
            dst,
        })
    }
}

/// The catalog key of a collection named as the Data Plane stores it in
/// `database_id`. A name without the qualifier resolves as a bare name.
pub fn stored_collection_key(database_id: DatabaseId, stored: &str) -> CollectionKey<'_> {
    CollectionKey::from_qualified_str(database_id, stored)
        .unwrap_or_else(|_| CollectionKey::from_bare(database_id, stored))
}

/// The homes of a record stored in `database_id`.
///
/// Deterministic, so every node that filters the same entry assigns it the
/// same homes.
pub fn homes_of_stored(database_id: DatabaseId, record: StoredRecord<'_>) -> RecordHomes {
    match record {
        StoredRecord::Row { collection } => RecordHomes::of(HomedRecord::Row(
            stored_collection_key(database_id, collection),
        )),
        StoredRecord::Edge { src, dst, .. } => RecordHomes::of(HomedRecord::Edge { src, dst }),
        StoredRecord::ArrayCells { vshard, .. } => RecordHomes::of(HomedRecord::ArrayCells {
            vshard: crate::types::VShardId::new(vshard),
        }),
    }
}

/// The vShard of a collection named as the Data Plane stores it in
/// `database_id`: the home of its rows.
pub fn vshard_of_stored(database_id: DatabaseId, stored: &str) -> u32 {
    homes_of_stored(database_id, StoredRecord::Row { collection: stored })
        .owner()
        .as_u32()
}

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
/// The first two ':' are structural (db, tid); the collection can itself
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

/// Filter a single-tenant [`TenantDataSnapshot`] in place to the records whose
/// owner home is in `source_vshards`.
///
/// The backup orchestrator gathers a full per-node snapshot (under RF>1 every
/// replica holds the full vshard data), then calls this so each node
/// contributes EXACTLY the vshards it is the assigned source for — the union
/// over nodes covers each record once (no duplication, no loss). A graph edge
/// is kept by the source of its `from_key(src)` home, never by the source of
/// its collection: that node holds the edge whatever the node count and RF.
/// The retained section shapes are unchanged, so the RESTORE merge path
/// (`merge_sections`) consumes the output exactly as before.
///
/// `homes_of` maps a record to its homes, or to `None` to drop it (the caller
/// passes [`homes_of_stored`] for the snapshot's database). Every section kind
/// the snapshot carries is classified here so adding a section without
/// updating this filter is impossible to miss:
///
/// - db-tenant-scoped keys (`documents`, `indexes`, `documents_versioned`,
///   `indexes_versioned`, `vectors`, `timeseries`) via
///   [`extract_db_tenant_scoped_collection`].
/// - db-scoped keys (`flushed_ts_segments`, `columnar_engines`, `kv_tables`)
///   via [`extract_db_scoped_collection`].
/// - graph `edges` via [`StoredRecord::from_edge_key`], homed on endpoints.
///   The hidden edge versions, cuts and applied ordinals are dropped.
/// - `surrogate_pk` by its explicit `collection` field (the bare name).
/// - CRDT (`crdt_state`): per-collection, tenant-explicit. Each entry carries
///   its single collection, so it is kept iff that collection's vshard is in
///   `source_vshards` — the node owning the collection keeps it, every other
///   node drops it (captured exactly once, never duplicated).
/// - `arrays` by the vShard each blob's cells route to.
pub fn retain_tenant_data_for_vshards(
    snap: &mut TenantDataSnapshot,
    tenant_id: u64,
    source_vshards: &HashSet<u32>,
    homes_of: impl Fn(StoredRecord<'_>) -> Option<RecordHomes>,
) {
    let owned = |record: StoredRecord<'_>| {
        homes_of(record).is_some_and(|homes| homes.owned_by(source_vshards))
    };
    let owned_row = |collection: &str| owned(StoredRecord::Row { collection });
    let in_group_db_tenant_scoped =
        |key: &str| extract_db_tenant_scoped_collection(key, tenant_id).is_some_and(owned_row);
    let in_group_db_scoped =
        |key: &str| extract_db_scoped_collection(key, tenant_id).is_some_and(owned_row);

    snap.documents.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.indexes.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.documents_versioned
        .retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.indexes_versioned
        .retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.vectors.retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.timeseries
        .retain(|(k, _)| in_group_db_tenant_scoped(k));
    snap.flushed_ts_segments
        .retain(|b| in_group_db_scoped(&b.collection_key));
    snap.columnar_engines.retain(|(k, _)| in_group_db_scoped(k));
    snap.kv_tables.retain(|(k, _)| in_group_db_scoped(k));
    // surrogate_pk: the field IS the collection name.
    snap.surrogate_pk.retain(|e| owned_row(&e.collection));
    // An unparseable edge key has no determinable home; drop it from EVERY
    // node's retained set rather than duplicate it across all sources.
    snap.edges
        .retain(|(k, _)| StoredRecord::from_edge_key(k).is_some_and(owned));
    // A backup carries the edge versions a current read reaches. Its RESTORE
    // re-issues them, each applied at the restore's own ordinal, so the
    // versions a TRUNCATE hides, the cuts and the applied ordinals of the
    // source stay behind.
    snap.edge_hidden.clear();
    snap.edge_cuts.clear();
    snap.edge_applied.clear();
    // A node's labels home by the node, whatever collection its edges live
    // in. The source of that home keeps them.
    let owned_node =
        |node: &str| RecordHomes::of(HomedRecord::NodeLabels { node }).owned_by(source_vshards);
    snap.node_labels
        .retain(|(_, _, node, _)| owned_node(node.as_str()));
    snap.node_label_versions
        .retain(|(_, _, node, _, _)| owned_node(node.as_str()));

    // CRDT (`crdt_state`): each entry carries its single collection. Keep it iff
    // that collection's vshard is in this source set — exactly one source node
    // (the collection's owner) retains each entry.
    snap.crdt_state
        .retain(|(_, _, collection, _)| owned_row(collection));
    snap.arrays.retain(|blob| {
        owned(StoredRecord::ArrayCells {
            array: &blob.array,
            vshard: blob.vshard,
        })
    });
}

#[cfg(test)]
mod tests {
    use super::{
        StoredRecord, extract_db_scoped_collection, extract_db_tenant_scoped_collection,
        homes_of_stored, vshard_of_stored,
    };
    use nodedb_types::DatabaseId;

    /// A qualified Data-Plane name in a named database routes to the vShard
    /// of its bare catalog key, never to the vShard of the qualified string.
    #[test]
    fn a_stored_name_routes_by_its_bare_key() {
        use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

        let db = DatabaseId::new(1025);
        let stored = QualifiedCollection::new(db, "orders");
        let expected =
            nodedb_cluster::routing::vshard_for_collection(CollectionKey::from_bare(db, "orders"));
        assert_eq!(super::vshard_of_stored(db, stored.as_str()), expected);
        assert_eq!(
            super::vshard_of_stored(DatabaseId::DEFAULT, "orders"),
            nodedb_cluster::routing::vshard_for_collection(CollectionKey::from_bare(
                DatabaseId::DEFAULT,
                "orders"
            ))
        );
    }

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
        // Collection can itself contain ':'.
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
        let homes_of = |r: StoredRecord<'_>| Some(homes_of_stored(DatabaseId::DEFAULT, r));
        let va = vshard_of_stored(DatabaseId::DEFAULT, "alpha");
        let vb = vshard_of_stored(DatabaseId::DEFAULT, "beta");
        assert_ne!(va, vb);

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
                    cagg_image: Vec::new(),
                },
                TsFlushedCollectionBlob {
                    collection_key: format!("0:{TID}:beta"),
                    partitions: vec![],
                    cagg_image: Vec::new(),
                },
            ],
            kv_tables: vec![(format!("0:{TID}:alpha"), b"a".to_vec())],
            ..Default::default()
        };

        // Node owning only vshard(alpha) keeps alpha sections, drops beta.
        let mut node_a = template();
        let only_a: HashSet<u32> = [va].into_iter().collect();
        retain_tenant_data_for_vshards(&mut node_a, TID, &only_a, homes_of);
        assert_eq!(node_a.timeseries.len(), 1);
        assert_eq!(node_a.timeseries[0].0, format!("0:{TID}:alpha"));
        assert_eq!(node_a.columnar_engines.len(), 1);
        assert_eq!(node_a.flushed_ts_segments.len(), 1);
        assert_eq!(node_a.kv_tables.len(), 1);

        // Node owning only vshard(beta) keeps beta sections, drops alpha.
        let mut node_b = template();
        let only_b: HashSet<u32> = [vb].into_iter().collect();
        retain_tenant_data_for_vshards(&mut node_b, TID, &only_b, homes_of);
        assert_eq!(node_b.timeseries.len(), 1);
        assert_eq!(node_b.timeseries[0].0, format!("0:{TID}:beta"));
        assert_eq!(node_b.kv_tables.len(), 0, "alpha kv not owned by beta node");

        // Third replica owns neither → contributes nothing for these vshards.
        let mut node_c = template();
        let none: HashSet<u32> = HashSet::new();
        retain_tenant_data_for_vshards(&mut node_c, TID, &none, homes_of);
        assert!(node_c.timeseries.is_empty());
        assert!(node_c.columnar_engines.is_empty());

        // Union over the three replicas = each collection's append section
        // exactly once, with no ~3× multiplication.
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

        let homes_of = |r: StoredRecord<'_>| Some(homes_of_stored(DatabaseId::DEFAULT, r));
        let mut snap = TenantDataSnapshot {
            timeseries: vec![("0:1:alpha".into(), b"a".to_vec())],
            columnar_engines: vec![("0:1:beta".into(), b"b".to_vec())],
            kv_tables: vec![("0:1:gamma".into(), b"g".to_vec())],
            ..Default::default()
        };
        let all: HashSet<u32> = ["alpha", "beta", "gamma"]
            .iter()
            .map(|c| vshard_of_stored(DatabaseId::DEFAULT, c))
            .collect();
        retain_tenant_data_for_vshards(&mut snap, 1, &all, homes_of);
        assert_eq!(snap.timeseries.len(), 1);
        assert_eq!(snap.columnar_engines.len(), 1);
        assert_eq!(snap.kv_tables.len(), 1);
    }

    /// An edge is kept by the source of its `from_key(src)` home, never by the
    /// source of its collection's home.
    #[test]
    fn an_edge_is_kept_by_the_source_of_its_src_home() {
        use super::retain_tenant_data_for_vshards;
        use crate::types::{TenantDataSnapshot, VShardId};
        use std::collections::HashSet;

        let collection_home = vshard_of_stored(DatabaseId::DEFAULT, "follows");
        let src = (0..4096)
            .map(|i| format!("n{i}"))
            .find(|k| VShardId::from_key(k.as_bytes()).as_u32() != collection_home)
            .expect("a node key off the collection home");
        let src_home = VShardId::from_key(src.as_bytes()).as_u32();
        let key = format!("follows\x00{src}\x00L\x00{src}\x00{:020}", 1);
        let homes_of = |r: StoredRecord<'_>| Some(homes_of_stored(DatabaseId::DEFAULT, r));
        let template = || TenantDataSnapshot {
            edges: vec![(key.clone(), vec![])],
            ..Default::default()
        };

        let mut by_collection = template();
        let collection_only: HashSet<u32> = [collection_home].into_iter().collect();
        retain_tenant_data_for_vshards(&mut by_collection, 1, &collection_only, homes_of);
        assert!(by_collection.edges.is_empty());

        let mut by_src = template();
        let src_only: HashSet<u32> = [src_home].into_iter().collect();
        retain_tenant_data_for_vshards(&mut by_src, 1, &src_only, homes_of);
        assert_eq!(by_src.edges.len(), 1);
    }

    /// Array cells are kept by the source of the vShard they route to.
    #[test]
    fn array_cells_are_kept_by_the_source_of_their_vshard() {
        use super::retain_tenant_data_for_vshards;
        use crate::types::{ArrayCellsBlob, TenantDataSnapshot};
        use std::collections::HashSet;

        let blob = |vshard: u32| ArrayCellsBlob {
            database_id: 0,
            tenant_id: 1,
            array: "grid".into(),
            vshard,
            cells: vec![vshard as u8],
        };
        let mut snap = TenantDataSnapshot {
            arrays: vec![blob(3), blob(9)],
            ..Default::default()
        };
        let homes_of = |r: StoredRecord<'_>| Some(homes_of_stored(DatabaseId::DEFAULT, r));
        let source: HashSet<u32> = [9].into_iter().collect();
        retain_tenant_data_for_vshards(&mut snap, 1, &source, homes_of);
        assert_eq!(snap.arrays, vec![blob(9)]);
    }

    #[test]
    fn a_stored_edge_key_names_its_endpoints() {
        let key = format!("1025/follows\x00a\x00L\x00b\x00{:020}", 7);
        let record = StoredRecord::from_edge_key(&key).expect("versioned edge key");
        assert_eq!(record.collection(), "1025/follows");
        assert_eq!(
            homes_of_stored(DatabaseId::new(1025), record),
            crate::types::RecordHomes::edge("a", "b")
        );
        assert!(StoredRecord::from_edge_key("not-an-edge-key").is_none());
    }
}
