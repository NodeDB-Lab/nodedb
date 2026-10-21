// SPDX-License-Identifier: BUSL-1.1

//! The record filters a data-group snapshot build applies: which edges,
//! which node labels and which PK→surrogate binds have a home in the target
//! group.

use std::collections::HashSet;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::control::backup::snapshot_keys::{StoredRecord, homes_of_stored};
use crate::types::{
    HomedRecord, NodeLabelVersionEntry, NodeLabelsEntry, RecordHomes, TenantDataSnapshot,
};

/// The number of edge-key characters an unparseable-key error carries.
pub(crate) const EDGE_KEY_PREFIX_CHARS: usize = 32;

/// Whether the bind of `pk` in `collection` has a home in `group_vshards`.
pub(crate) fn bind_in_group(
    collection: nodedb_types::CollectionKey<'_>,
    pk: &[u8],
    holds_edges: bool,
    group_vshards: &HashSet<u32>,
) -> bool {
    RecordHomes::of(HomedRecord::Bind {
        collection,
        key: pk,
        holds_edges,
    })
    .intersects(group_vshards)
}

/// Push the current labels and the label versions of every node whose
/// labels home on a vShard of `group_vshards` onto `merged`.
pub(crate) fn push_group_node_labels(
    labels: Vec<NodeLabelsEntry>,
    versions: Vec<NodeLabelVersionEntry>,
    group_vshards: &HashSet<u32>,
    merged: &mut TenantDataSnapshot,
) {
    let in_group =
        |node: &str| RecordHomes::of(HomedRecord::NodeLabels { node }).owned_by(group_vshards);
    merged.node_labels.extend(
        labels
            .into_iter()
            .filter(|(_, _, node, _)| in_group(node.as_str())),
    );
    merged.node_label_versions.extend(
        versions
            .into_iter()
            .filter(|(_, _, node, _, _)| in_group(node.as_str())),
    );
}

/// Push every entry of `edges`, keyed by an edge version's key, with an
/// endpoint home in `group_vshards` onto `out`, tagged with its database and
/// tenant.
///
/// The edge key carries neither the database nor the tenant, and the merged
/// snapshot applies once with no per-database dispatch, so edges travel in
/// `tenant_edges`, never in the plain `edges` field.
///
/// An unparseable edge key has no home. Leaving it out makes the replica
/// diverge, so the build fails with a storage error naming the key prefix.
pub(crate) fn push_group_edges<V>(
    edges: Vec<(String, V)>,
    database_id: DatabaseId,
    tenant_id: u64,
    group_vshards: &HashSet<u32>,
    out: &mut Vec<(u64, u64, String, V)>,
) -> Result<(), Error> {
    for (key, value) in edges {
        let Some(record) = StoredRecord::from_edge_key(&key) else {
            // The error carries a short prefix, never the full key.
            let key_prefix: String = key.chars().take(EDGE_KEY_PREFIX_CHARS).collect();
            return Err(Error::Storage {
                engine: "graph".into(),
                detail: format!(
                    "snapshot build: unparseable edge key in database {} of tenant {tenant_id}, \
                     key prefix {key_prefix:?}",
                    database_id.as_u64()
                ),
            });
        };
        if homes_of_stored(database_id, record).intersects(group_vshards) {
            out.push((database_id.as_u64(), tenant_id, key, value));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::engine::graph::edge_store::versioned_edge_key;
    use crate::types::VShardId;

    const DB: DatabaseId = DatabaseId::new(1025);
    const TID: u64 = 7;

    fn edge_key(src: &str, dst: &str) -> String {
        let stored = nodedb_types::QualifiedCollection::new(DB, "follows");
        versioned_edge_key(stored.as_str(), src, "L", dst, 1).expect("edge key")
    }

    /// A node key whose key vShard satisfies `pred`.
    fn node_where(pred: impl Fn(u32) -> bool) -> String {
        (0..100_000)
            .map(|i| format!("n{i}"))
            .find(|k| pred(VShardId::from_key(k.as_bytes()).as_u32()))
            .expect("a matching node key")
    }

    /// A group gets exactly the edges with an endpoint home among its
    /// vShards: a dst-only edge is in, and an edge homed off the group is out
    /// even when its collection homes on the group.
    #[test]
    fn a_group_gets_exactly_its_edges() {
        let collection_home = nodedb_types::CollectionKey::from_bare(DB, "follows")
            .vshard()
            .as_u32();
        let group: HashSet<u32> = (0..VShardId::COUNT)
            .filter(|v| v % 4 == collection_home % 4)
            .collect();
        let inside = |v: u32| group.contains(&v);

        let a_in = node_where(inside);
        let b_in = node_where(|v| inside(v) && v != VShardId::from_key(a_in.as_bytes()).as_u32());
        let x_out = node_where(|v| !inside(v));
        let y_out =
            node_where(|v| !inside(v) && v != VShardId::from_key(x_out.as_bytes()).as_u32());

        let both = edge_key(&a_in, &b_in);
        let src_only = edge_key(&a_in, &x_out);
        let dst_only = edge_key(&x_out, &a_in);
        let neither = edge_key(&x_out, &y_out);
        let edges: Vec<(String, Vec<u8>)> = [&both, &src_only, &dst_only, &neither]
            .into_iter()
            .map(|k| (k.clone(), vec![1]))
            .collect();

        let mut out = Vec::new();
        push_group_edges(edges, DB, TID, &group, &mut out).expect("parseable edges");
        let got: BTreeSet<String> = out.iter().map(|(_, _, k, _)| k.clone()).collect();
        let want: BTreeSet<String> = [both, src_only, dst_only].into_iter().collect();
        assert_eq!(got, want);
        assert!(
            out.iter()
                .all(|(db, tid, ..)| *db == DB.as_u64() && *tid == TID)
        );
        assert!(
            RecordHomes::edge(&x_out, &y_out)
                .iter()
                .all(|h| !inside(h.as_u32()))
        );
    }

    /// The per-group slices of every group cover each edge on every group
    /// that homes it, and on no other.
    #[test]
    fn the_group_slices_cover_each_edge_on_its_homes() {
        const GROUPS: u32 = 4;
        let edges: Vec<String> = (0..128)
            .map(|i| edge_key(&format!("u{i}"), &format!("v{}", i * 5 + 1)))
            .collect();
        let mut holders: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
        for g in 0..GROUPS {
            let group: HashSet<u32> = (0..VShardId::COUNT).filter(|v| v % GROUPS == g).collect();
            let mut out = Vec::new();
            let input: Vec<(String, Vec<u8>)> =
                edges.iter().map(|k| (k.clone(), Vec::new())).collect();
            push_group_edges(input, DB, TID, &group, &mut out).expect("parseable edges");
            for (_, _, k, _) in out {
                holders.entry(k).or_default().insert(g);
            }
        }
        for k in &edges {
            let record = StoredRecord::from_edge_key(k).expect("edge key");
            let want: BTreeSet<u32> = homes_of_stored(DB, record)
                .iter()
                .map(|h| h.as_u32() % GROUPS)
                .collect();
            assert_eq!(holders.get(k), Some(&want), "edge {k:?}");
        }
    }

    /// An unparseable edge key fails the build with a storage error that
    /// names the key prefix. The edge never leaves the snapshot silently.
    #[test]
    fn an_unparseable_edge_key_fails_the_build() {
        let group: HashSet<u32> = (0..VShardId::COUNT).collect();
        let long_bad_key = format!("malformed-{}", "x".repeat(100));
        let edges = vec![
            (edge_key("a", "b"), vec![1]),
            (long_bad_key.clone(), vec![2]),
        ];
        let mut out = Vec::new();
        let err = push_group_edges(edges, DB, TID, &group, &mut out)
            .expect_err("an unparseable key must fail the build");
        let Error::Storage { engine, detail } = err else {
            panic!("expected a storage error, got {err:?}");
        };
        assert_eq!(engine, "graph");
        let prefix: String = long_bad_key.chars().take(EDGE_KEY_PREFIX_CHARS).collect();
        assert!(detail.contains(&format!("{prefix:?}")), "detail: {detail}");
        assert!(
            !detail.contains(&long_bad_key),
            "the full key must not leak"
        );
    }

    /// The group that homes an edge endpoint ships its bind even when the
    /// collection homes on another group. A collection without edges ships
    /// its binds to its home group only.
    #[test]
    fn a_group_ships_the_binds_of_its_endpoints() {
        let collection = nodedb_types::CollectionKey::from_bare(DB, "follows");
        let collection_home = collection.vshard().as_u32();
        let endpoint = node_where(|v| v % 4 != collection_home % 4);
        let endpoint_home = VShardId::from_key(endpoint.as_bytes()).as_u32();
        let group_of = |home: u32| -> HashSet<u32> {
            (0..VShardId::COUNT).filter(|v| v % 4 == home % 4).collect()
        };
        let endpoint_group = group_of(endpoint_home);
        let collection_group = group_of(collection_home);

        let pk = endpoint.as_bytes();
        assert!(bind_in_group(collection, pk, true, &endpoint_group));
        assert!(bind_in_group(collection, pk, true, &collection_group));
        assert!(!bind_in_group(collection, pk, false, &endpoint_group));
        assert!(bind_in_group(collection, pk, false, &collection_group));
        assert_eq!(
            RecordHomes::edge(&endpoint, "x").owner().as_u32(),
            endpoint_home,
            "the endpoint's bind ships with the group its edges home on"
        );
    }
}
