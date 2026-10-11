// SPDX-License-Identifier: BUSL-1.1

//! Split a data-group snapshot into one share per owning Data-Plane core.
//!
//! Each core stores only the state its vShards home to, and reads route to
//! that owning core. A row installed on any other core is invisible to reads
//! and duplicated by later writes. Every section therefore routes with the
//! function its live writes route with:
//!
//! - collection-homed sections (documents, indexes, vectors, KV, columnar,
//!   timeseries, CRDT): the vShard of the collection's `(database, bare name)`
//!   key, the same key [`vshard_of_stored`] resolves;
//! - graph edges: dual-homed on `VShardId::from_key(src)` and
//!   `VShardId::from_key(dst)`, the homes an `EdgePut` is written to.
//!
//! The vShard → core step is the dispatcher's own [`VShardRouter`].

use nodedb_types::DatabaseId;

use crate::control::backup::snapshot_keys::vshard_of_stored;
use crate::control::router::vshard::VShardRouter;
use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, VShardId};

use super::error::SnapshotInstallError;

/// Longest key prefix an error carries. Keys hold user data.
const KEY_PREFIX_CHARS: usize = 32;

/// The local vShard → core map, captured once per install.
pub struct CoreMap {
    core_of: Vec<Option<usize>>,
    num_cores: usize,
}

impl CoreMap {
    /// Capture the dispatcher router's current map.
    pub fn from_router(router: &VShardRouter) -> Self {
        let core_of = (0..VShardId::COUNT)
            .map(|v| router.resolve(VShardId::new(v)))
            .collect();
        Self {
            core_of,
            num_cores: router.num_cores(),
        }
    }

    pub fn num_cores(&self) -> usize {
        self.num_cores
    }

    /// The core that owns `vshard`.
    pub fn core_of(&self, group_id: u64, vshard: u32) -> Result<usize, SnapshotInstallError> {
        self.core_of
            .get(vshard as usize)
            .copied()
            .flatten()
            .ok_or(SnapshotInstallError::NoCoreForVShard { group_id, vshard })
    }

    /// The core that owns the collection `stored` in `database_id`.
    pub fn collection_core(
        &self,
        group_id: u64,
        database_id: u64,
        stored: &str,
    ) -> Result<usize, SnapshotInstallError> {
        self.core_of(
            group_id,
            vshard_of_stored(DatabaseId::new(database_id), stored),
        )
    }
}

/// A snapshot split for install: one Data-Plane share per core, plus the
/// Control-Plane state the applier binds itself.
pub struct CoreShares {
    /// Index `i` is the share of core `i`.
    pub per_core: Vec<TenantDataSnapshot>,
    pub surrogate_pk: Vec<SurrogateBindEntry>,
    pub group_write_marks: Vec<(u64, u64, u8, String, u64)>,
    /// `(log_index, proposal_key)` of the committed entries the snapshot
    /// covers, for the propose-waiter window.
    pub proposal_keys: Vec<(u64, u64)>,
    /// Lowest index `proposal_keys` is complete from; `0` when complete.
    pub proposal_keys_complete_from: u64,
    /// The group's applied index the snapshot was captured at.
    pub cut_index: u64,
    /// The Event Plane lane state of the group's vShards.
    pub event_lane: crate::types::snapshot::GroupEventLane,
    /// The Calvin cut of the group's vShards.
    pub calvin: Option<crate::types::GroupCalvinCut>,
    /// The group's open chunked redo streams.
    pub redo_streams: Vec<crate::wal::CarriedRedoStream>,
}

/// `(database, collection)` of a `"{db}:{tid}:{collection}[:suffix]"` key.
/// The collection is the first `':'`- or `'\0'`-delimited token.
fn db_tenant_scoped(key: &str) -> Option<(u64, &str)> {
    let mut it = key.splitn(3, ':');
    let db = it.next()?.parse().ok()?;
    it.next()?.parse::<u64>().ok()?;
    let collection = it.next()?.split([':', '\u{0}']).next()?;
    (!collection.is_empty()).then_some((db, collection))
}

/// `(database, collection)` of a `"{db}:{tid}:{collection}"` key. The
/// collection is the whole remainder and can contain `':'`.
fn db_scoped(key: &str) -> Option<(u64, &str)> {
    let mut it = key.splitn(3, ':');
    let db = it.next()?.parse().ok()?;
    it.next()?.parse::<u64>().ok()?;
    let collection = it.next()?;
    (!collection.is_empty()).then_some((db, collection))
}

fn unroutable(group_id: u64, section: &'static str, key: &str) -> SnapshotInstallError {
    SnapshotInstallError::UnroutableKey {
        group_id,
        section,
        key_prefix: key.chars().take(KEY_PREFIX_CHARS).collect(),
    }
}

/// A string-keyed snapshot section: its `(key, value)` entries.
type KeyedSection = Vec<(String, Vec<u8>)>;

/// Selects one string-keyed section of a core's share.
type SectionOf = fn(&mut TenantDataSnapshot) -> &mut KeyedSection;

struct Splitter<'a> {
    group_id: u64,
    cores: &'a CoreMap,
    per_core: Vec<TenantDataSnapshot>,
}

impl Splitter<'_> {
    /// Route every entry of a string-keyed section to its collection's core.
    fn keyed(
        &mut self,
        section: &'static str,
        entries: KeyedSection,
        locate: fn(&str) -> Option<(u64, &str)>,
        field: SectionOf,
    ) -> Result<(), SnapshotInstallError> {
        for (key, value) in entries {
            let (db, collection) =
                locate(&key).ok_or_else(|| unroutable(self.group_id, section, &key))?;
            let core = self.cores.collection_core(self.group_id, db, collection)?;
            field(&mut self.per_core[core]).push((key, value));
        }
        Ok(())
    }

    /// The cores an edge is stored on: the homes of its two endpoints.
    fn edge_cores(
        &self,
        section: &'static str,
        key: &str,
    ) -> Result<(usize, Option<usize>), SnapshotInstallError> {
        let (_, src, _, dst, _) =
            parse_versioned_edge_key(key).ok_or_else(|| unroutable(self.group_id, section, key))?;
        let src_core = self
            .cores
            .core_of(self.group_id, VShardId::from_key(src.as_bytes()).as_u32())?;
        let dst_core = self
            .cores
            .core_of(self.group_id, VShardId::from_key(dst.as_bytes()).as_u32())?;
        Ok((src_core, (dst_core != src_core).then_some(dst_core)))
    }
}

/// Split `snap` into one share per local core.
///
/// A key that names no routable collection or endpoint fails the split: a
/// row with no owner has no core it can be read back from.
pub fn split_by_core(
    group_id: u64,
    snap: TenantDataSnapshot,
    cores: &CoreMap,
) -> Result<CoreShares, SnapshotInstallError> {
    // Exhaustive destructure: a new section fails to compile here instead of
    // being dropped from the install.
    let TenantDataSnapshot {
        documents,
        indexes,
        edges,
        vectors,
        kv_tables,
        crdt_state,
        crdt_constraints,
        timeseries,
        flushed_ts_segments,
        columnar_engines,
        vector_params,
        index_configs,
        surrogate_pk,
        tenant_edges,
        edge_hidden,
        edge_cuts,
        edge_applied,
        tenant_edge_cuts,
        tenant_edge_applied,
        group_write_marks,
        group_proposal_keys,
        group_proposal_keys_complete_from,
        documents_versioned,
        indexes_versioned,
        vector_multi_documents,
        arrays,
        // The install waits on the floor before it splits; no core reads it.
        metadata_floor: _,
        group_cut_index,
        group_event_lane,
        group_calvin,
        group_redo_streams,
    } = snap;

    let mut s = Splitter {
        group_id,
        cores,
        per_core: (0..cores.num_cores())
            .map(|_| TenantDataSnapshot::default())
            .collect(),
    };

    s.keyed("documents", documents, db_tenant_scoped, |t| {
        &mut t.documents
    })?;
    s.keyed("indexes", indexes, db_tenant_scoped, |t| &mut t.indexes)?;
    s.keyed(
        "documents_versioned",
        documents_versioned,
        db_tenant_scoped,
        |t| &mut t.documents_versioned,
    )?;
    s.keyed(
        "indexes_versioned",
        indexes_versioned,
        db_tenant_scoped,
        |t| &mut t.indexes_versioned,
    )?;
    s.keyed("vectors", vectors, db_tenant_scoped, |t| &mut t.vectors)?;
    // Membership goes to the core its index's rows go to, keyed alike.
    for (key, documents) in vector_multi_documents {
        let (db, collection) = db_tenant_scoped(&key)
            .ok_or_else(|| unroutable(group_id, "vector_multi_documents", &key))?;
        let core = cores.collection_core(group_id, db, collection)?;
        s.per_core[core]
            .vector_multi_documents
            .push((key, documents));
    }
    s.keyed("vector_params", vector_params, db_tenant_scoped, |t| {
        &mut t.vector_params
    })?;
    s.keyed("index_configs", index_configs, db_tenant_scoped, |t| {
        &mut t.index_configs
    })?;
    s.keyed("timeseries", timeseries, db_tenant_scoped, |t| {
        &mut t.timeseries
    })?;
    s.keyed("kv_tables", kv_tables, db_scoped, |t| &mut t.kv_tables)?;
    s.keyed("columnar_engines", columnar_engines, db_scoped, |t| {
        &mut t.columnar_engines
    })?;

    for blob in flushed_ts_segments {
        let (db, collection) = db_scoped(&blob.collection_key)
            .ok_or_else(|| unroutable(group_id, "flushed_ts_segments", &blob.collection_key))?;
        let core = cores.collection_core(group_id, db, collection)?;
        s.per_core[core].flushed_ts_segments.push(blob);
    }

    for entry in crdt_state {
        let core = cores.collection_core(group_id, entry.0, &entry.2)?;
        s.per_core[core].crdt_state.push(entry);
    }

    for entry in crdt_constraints {
        let core = cores.collection_core(group_id, entry.database_id, &entry.collection)?;
        s.per_core[core].crdt_constraints.push(entry);
    }

    for (key, value) in edges {
        let (first, second) = s.edge_cores("edges", &key)?;
        if let Some(second) = second {
            s.per_core[second].edges.push((key.clone(), value.clone()));
        }
        s.per_core[first].edges.push((key, value));
    }

    for blob in arrays {
        let core = cores.core_of(group_id, blob.vshard)?;
        s.per_core[core].arrays.push(blob);
    }

    for (db, tid, key, value) in tenant_edges {
        let (first, second) = s.edge_cores("tenant_edges", &key)?;
        if let Some(second) = second {
            s.per_core[second]
                .tenant_edges
                .push((db, tid, key.clone(), value.clone()));
        }
        s.per_core[first].tenant_edges.push((db, tid, key, value));
    }

    // A hidden version and an applied ordinal go where their edge goes.
    for (key, value) in edge_hidden {
        let (first, second) = s.edge_cores("edge_hidden", &key)?;
        if let Some(second) = second {
            s.per_core[second]
                .edge_hidden
                .push((key.clone(), value.clone()));
        }
        s.per_core[first].edge_hidden.push((key, value));
    }
    for (key, applied) in edge_applied {
        let (first, second) = s.edge_cores("edge_applied", &key)?;
        if let Some(second) = second {
            s.per_core[second].edge_applied.push((key.clone(), applied));
        }
        s.per_core[first].edge_applied.push((key, applied));
    }
    for (db, tid, key, applied) in tenant_edge_applied {
        let (first, second) = s.edge_cores("tenant_edge_applied", &key)?;
        if let Some(second) = second {
            s.per_core[second]
                .tenant_edge_applied
                .push((db, tid, key.clone(), applied));
        }
        s.per_core[first]
            .tenant_edge_applied
            .push((db, tid, key, applied));
    }
    // A cut covers every edge of its collection, on whichever core it lives.
    for share in &mut s.per_core {
        share.edge_cuts.extend(edge_cuts.iter().cloned());
        share
            .tenant_edge_cuts
            .extend(tenant_edge_cuts.iter().cloned());
    }

    Ok(CoreShares {
        per_core: s.per_core,
        surrogate_pk,
        group_write_marks,
        proposal_keys: group_proposal_keys,
        proposal_keys_complete_from: group_proposal_keys_complete_from,
        cut_index: group_cut_index,
        event_lane: group_event_lane,
        calvin: group_calvin,
        redo_streams: group_redo_streams,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::versioned_edge_key;
    use crate::types::snapshot::CrdtConstraintEntry;
    use nodedb_types::QualifiedCollection;

    const NUM_CORES: usize = 4;
    const GROUP: u64 = 3;

    fn cores() -> CoreMap {
        CoreMap::from_router(&VShardRouter::round_robin(NUM_CORES))
    }

    /// The core a live write to `collection` in `db` routes to.
    fn home(db: u64, collection: &str) -> usize {
        let key = nodedb_types::CollectionKey::from_bare(DatabaseId::new(db), collection);
        VShardRouter::round_robin(NUM_CORES)
            .resolve(key.vshard())
            .unwrap()
    }

    fn endpoint_core(node: &str) -> usize {
        VShardRouter::round_robin(NUM_CORES)
            .resolve(VShardId::from_key(node.as_bytes()))
            .unwrap()
    }

    /// Collections whose homes cover more than one core, so a one-core
    /// install is caught.
    fn spread_collections() -> Vec<String> {
        let names: Vec<String> = (0..32).map(|i| format!("coll_{i}")).collect();
        let homes: std::collections::HashSet<usize> = names.iter().map(|n| home(0, n)).collect();
        assert!(homes.len() > 1, "test collections must span several cores");
        names
    }

    #[test]
    fn rows_of_many_vshards_land_on_their_owning_cores() {
        let names = spread_collections();
        let mut snap = TenantDataSnapshot::default();
        for name in &names {
            for doc in 0..3 {
                snap.documents
                    .push((format!("0:1:{name}:doc{doc}"), b"v".to_vec()));
            }
            snap.indexes
                .push((format!("0:1:{name}:f:x:doc0"), Vec::new()));
            snap.kv_tables.push((format!("0:1:{name}"), b"kv".to_vec()));
            snap.vectors
                .push((format!("0:1:{name}:emb"), b"vec".to_vec()));
            snap.crdt_state.push((0, 1, name.clone(), b"loro".to_vec()));
            snap.crdt_constraints.push(CrdtConstraintEntry {
                database_id: 0,
                tenant_id: 1,
                collection: name.clone(),
                version: 1,
                constraints: Vec::new(),
            });
        }

        let shares = split_by_core(GROUP, snap, &cores()).unwrap();
        assert_eq!(shares.per_core.len(), NUM_CORES);

        let mut documents = 0;
        for (core, share) in shares.per_core.iter().enumerate() {
            for (key, _) in &share.documents {
                let (_, coll) = db_tenant_scoped(key).unwrap();
                assert_eq!(home(0, coll), core, "document {key} on core {core}");
                documents += 1;
            }
            for (key, _) in &share.indexes {
                assert_eq!(home(0, db_tenant_scoped(key).unwrap().1), core);
            }
            for (key, _) in &share.kv_tables {
                assert_eq!(home(0, db_scoped(key).unwrap().1), core);
            }
            for (key, _) in &share.vectors {
                assert_eq!(home(0, db_tenant_scoped(key).unwrap().1), core);
            }
            for (_, _, coll, _) in &share.crdt_state {
                assert_eq!(home(0, coll), core);
            }
            for entry in &share.crdt_constraints {
                assert_eq!(home(0, &entry.collection), core);
            }
        }
        assert_eq!(documents, names.len() * 3, "no document lost or duplicated");
    }

    #[test]
    fn a_named_database_routes_by_its_bare_collection_key() {
        let db = 1025;
        let stored = QualifiedCollection::new(DatabaseId::new(db), "orders");
        let snap = TenantDataSnapshot {
            documents: vec![(format!("{db}:1:{}:doc0", stored.as_str()), b"v".to_vec())],
            kv_tables: vec![(format!("{db}:1:{}", stored.as_str()), b"kv".to_vec())],
            ..Default::default()
        };
        let shares = split_by_core(GROUP, snap, &cores()).unwrap();
        let core = home(db, "orders");
        assert_eq!(shares.per_core[core].documents.len(), 1);
        assert_eq!(shares.per_core[core].kv_tables.len(), 1);
    }

    #[test]
    fn an_edge_lands_on_both_endpoint_homes() {
        let (src, dst) = (0..64)
            .map(|i| (format!("a{i}"), format!("b{i}")))
            .find(|(s, d)| endpoint_core(s) != endpoint_core(d))
            .unwrap();
        let key = versioned_edge_key("g", &src, "L", &dst, 7).unwrap();
        let snap = TenantDataSnapshot {
            tenant_edges: vec![(0, 1, key.clone(), b"p".to_vec())],
            ..Default::default()
        };
        let shares = split_by_core(GROUP, snap, &cores()).unwrap();
        for (core, share) in shares.per_core.iter().enumerate() {
            let expected = usize::from(core == endpoint_core(&src) || core == endpoint_core(&dst));
            assert_eq!(share.tenant_edges.len(), expected, "core {core}");
        }
    }

    #[test]
    fn control_plane_sections_stay_off_the_cores() {
        let snap = TenantDataSnapshot {
            surrogate_pk: vec![SurrogateBindEntry {
                database_id: 0,
                tenant_id: 1,
                collection: "c".into(),
                pk: b"k".to_vec(),
                surrogate: 9,
            }],
            group_write_marks: vec![(1, 2, 0, "c".into(), 0)],
            group_proposal_keys: vec![(5, 0xab)],
            group_proposal_keys_complete_from: 4,
            ..Default::default()
        };
        let shares = split_by_core(GROUP, snap, &cores()).unwrap();
        assert_eq!(shares.surrogate_pk.len(), 1);
        assert_eq!(shares.group_write_marks.len(), 1);
        assert_eq!(shares.proposal_keys, vec![(5, 0xab)]);
        assert_eq!(shares.proposal_keys_complete_from, 4);
        assert!(shares.per_core.iter().all(|s| s.surrogate_pk.is_empty()
            && s.group_write_marks.is_empty()
            && s.group_proposal_keys.is_empty()));
    }

    #[test]
    fn an_unroutable_key_fails_the_split() {
        let snap = TenantDataSnapshot {
            documents: vec![("not-a-scoped-key".into(), b"v".to_vec())],
            ..Default::default()
        };
        let err = split_by_core(GROUP, snap, &cores())
            .err()
            .expect("an unroutable key must fail");
        assert!(matches!(
            err,
            SnapshotInstallError::UnroutableKey {
                section: "documents",
                ..
            }
        ));
        assert!(!err.is_retryable());
    }
}
