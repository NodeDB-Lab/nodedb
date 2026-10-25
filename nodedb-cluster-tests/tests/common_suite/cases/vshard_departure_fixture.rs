// SPDX-License-Identifier: BUSL-1.1

//! Shared setup of the vShard departure purge tests.
//!
//! A 3-node cluster at RF 3 gains a fourth node. Placement then picks three
//! of the four nodes for each data group by rendezvous hashing, so each
//! group drops one node. The fixture computes that placement up front, picks
//! a group one of the founding nodes leaves, and writes rows of every engine
//! homed on that group's vShards: schemaless documents with a search index,
//! strict documents, KV, columnar, timeseries, vector, spatial, graph edges,
//! a node label, array cells, and a CRDT row written over the native
//! protocol.
//!
//! Each probe reads one node's own cores (`stored_tenant_snapshots`), never
//! a pgwire query: the gateway forwards reads to the vShard owners.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb::types::{DatabaseId, TenantDataSnapshot, TenantId, VShardId};

use super::vshard_departure_crdt::{assert_crdt_served, holds_crdt_state, write_crdt_row};
use crate::common::cluster_harness::{
    TestCluster, TestClusterNode, read_once_a_leader_exists, wait_for,
};

pub const TENANT: u64 = 1;
const ARRAY_CELLS: u64 = 256;
const MAX_TRIES: u32 = 4096;

/// The names every engine's rows go under. Each ends in `x`, so no name is a
/// substring of another.
pub struct Names {
    pub doc: String,
    pub strict: String,
    pub kv: String,
    pub columnar: String,
    pub timeseries: String,
    pub vector: String,
    pub spatial: String,
    pub graph: String,
    pub array: String,
    /// The collection the native CRDT row goes under.
    pub crdt: String,
    /// Edge endpoints and the labeled node, all homed on departed vShards.
    pub src: String,
    pub dst: String,
    /// An endpoint homed on a vShard the leaving node keeps.
    pub kept_dst: String,
}

impl Names {
    /// The collections whose rows a stored snapshot keys by name.
    pub fn collections(&self) -> [&str; 7] {
        [
            &self.doc,
            &self.strict,
            &self.kv,
            &self.columnar,
            &self.timeseries,
            &self.vector,
            &self.spatial,
        ]
    }
}

pub struct DepartureFixture {
    pub cluster: TestCluster,
    /// The founding node placement drops from `group`.
    pub leaving: u64,
    pub group: u64,
    pub departed: BTreeSet<u32>,
    pub names: Names,
}

fn data_groups(routing: &nodedb_cluster::RoutingTable) -> Vec<u64> {
    let mut groups: Vec<u64> = routing
        .group_ids()
        .into_iter()
        .filter(|g| {
            *g != nodedb_cluster::METADATA_GROUP_ID
                && *g != nodedb_cluster::calvin::SEQUENCER_GROUP_ID
        })
        .collect();
    groups.sort_unstable();
    groups
}

fn collection_on(prefix: &str, vshards: &BTreeSet<u32>, wanted: bool) -> String {
    (0..MAX_TRIES)
        .map(|i| format!("{prefix}{i}x"))
        .find(|name| {
            let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
                .vshard()
                .as_u32();
            vshards.contains(&vshard) == wanted
        })
        .unwrap_or_else(|| panic!("no collection name under {prefix} in {MAX_TRIES} tries"))
}

fn key_on(prefix: &str, vshards: &BTreeSet<u32>, wanted: bool) -> String {
    (0..MAX_TRIES)
        .map(|i| format!("{prefix}{i}x"))
        .find(|key| vshards.contains(&VShardId::from_key(key.as_bytes()).as_u32()) == wanted)
        .unwrap_or_else(|| panic!("no node key under {prefix} in {MAX_TRIES} tries"))
}

impl DepartureFixture {
    /// Spawn the cluster, pick the leaving node and group, and write every
    /// engine's rows on the group's vShards.
    pub async fn seed() -> Self {
        let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
        let (group, leaving, departed, kept_out) = {
            let routing = cluster.nodes[0]
                .shared
                .cluster_routing
                .as_ref()
                .expect("cluster routing")
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let groups = data_groups(&routing);
            let placement =
                nodedb_cluster::rebalancer::placement::compute_placement(&[1, 2, 3, 4], &groups, 3);
            let (group, leaving) = placement
                .iter()
                .find_map(|(group, placed)| {
                    (1..=3u64)
                        .find(|node| !placed.contains(node))
                        .map(|node| (*group, node))
                })
                .expect("placement drops a founding node from some data group");
            let departed: BTreeSet<u32> = routing.vshards_for_group(group).into_iter().collect();
            // Every vShard of every group the leaving node leaves: a kept
            // endpoint homes outside all of them.
            let kept_out: BTreeSet<u32> = placement
                .iter()
                .filter(|(_, placed)| !placed.contains(&leaving))
                .flat_map(|(group, _)| routing.vshards_for_group(*group))
                .collect();
            (group, leaving, departed, kept_out)
        };
        let names = Names {
            doc: collection_on("vdpdoc", &departed, true),
            strict: collection_on("vdpstrict", &departed, true),
            kv: collection_on("vdpkv", &departed, true),
            columnar: collection_on("vdpcol", &departed, true),
            timeseries: collection_on("vdpts", &departed, true),
            vector: collection_on("vdpvec", &departed, true),
            spatial: collection_on("vdpgeo", &departed, true),
            graph: collection_on("vdpgraph", &departed, true),
            array: "vdparrx".to_string(),
            crdt: collection_on("vdpcrdt", &departed, true),
            src: key_on("vdpsrc", &departed, true),
            dst: key_on("vdpdst", &departed, true),
            kept_dst: key_on("vdpkept", &kept_out, false),
        };
        let fixture = Self {
            cluster,
            leaving,
            group,
            departed,
            names,
        };
        fixture.write_rows().await;
        fixture
            .cluster
            .wait_for_full_apply_convergence(Duration::from_secs(30))
            .await;
        fixture
    }

    async fn write_rows(&self) {
        let n = &self.names;
        let ddl = [
            format!(
                "CREATE COLLECTION {} (id STRING PRIMARY KEY, body STRING) \
                 WITH (engine='document_schemaless')",
                n.doc
            ),
            format!(
                "CREATE SEARCH INDEX idx_{0}_fts ON {0} FIELDS body ANALYZER 'standard'",
                n.doc
            ),
            format!(
                "CREATE COLLECTION {} (id STRING PRIMARY KEY, n INT) WITH (engine='document_strict')",
                n.strict
            ),
            format!(
                "CREATE COLLECTION {} (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
                n.kv
            ),
            format!(
                "CREATE COLLECTION {} COLUMNS (id TEXT, region TEXT, ts BIGINT) \
                 WITH (engine='columnar')",
                n.columnar
            ),
            format!(
                "CREATE COLLECTION {} COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, \
                 value FLOAT) WITH (engine='timeseries')",
                n.timeseries
            ),
            format!("CREATE COLLECTION {} TYPE document", n.vector),
            format!(
                "CREATE VECTOR INDEX idx_{0} ON {0} (embedding) METRIC cosine DIM 3",
                n.vector
            ),
            format!(
                "CREATE COLLECTION {} COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
                 WITH (engine='spatial')",
                n.spatial
            ),
            format!("CREATE COLLECTION {}", n.graph),
            format!("CREATE COLLECTION {}", n.crdt),
            format!(
                "CREATE ARRAY {} DIMS (x INT64 [0..63], y INT64 [0..63]) ATTRS (v INT64) \
                 TILE_EXTENTS (4, 4)",
                n.array
            ),
        ];
        for sql in &ddl {
            self.cluster
                .exec_ddl_on_any_leader(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let mut writes = vec![
            format!(
                "INSERT INTO {} (id, body) VALUES ('d1', 'alpha beta')",
                n.doc
            ),
            format!("INSERT INTO {} (id, n) VALUES ('s1', 7)", n.strict),
            format!("INSERT INTO {} (key, value) VALUES ('k1', 'v1')", n.kv),
            format!(
                "INSERT INTO {} (id, region, ts) VALUES ('c1', 'eu', 1)",
                n.columnar
            ),
            format!(
                "INSERT INTO {} (id, ts, metric, value) VALUES ('t1', 1000, 'cpu', 1.0)",
                n.timeseries
            ),
            format!(
                "INSERT INTO {} (id, body, embedding) VALUES ('v1', 'x', ARRAY[0.1,0.2,0.3])",
                n.vector
            ),
            format!(
                "INSERT INTO {} (id, location, name) VALUES \
                 ('p1', '{{\"type\":\"Point\",\"coordinates\":[-122.4,37.8]}}', 'SF')",
                n.spatial
            ),
            format!(
                "GRAPH INSERT EDGE IN '{}' FROM '{}' TO '{}' TYPE 'L'",
                n.graph, n.src, n.dst
            ),
            format!(
                "GRAPH INSERT EDGE IN '{}' FROM '{}' TO '{}' TYPE 'L'",
                n.graph, n.src, n.kept_dst
            ),
            format!("GRAPH LABEL '{}' AS 'Departed'", n.src),
        ];
        // One cell per 4x4 tile, so the cells spread over many vShards.
        for row in 0..16u64 {
            let cells: Vec<String> = (0..16u64)
                .map(|col| {
                    format!(
                        "COORDS ({}, {}) VALUES ({})",
                        row * 4,
                        col * 4,
                        row * 16 + col
                    )
                })
                .collect();
            writes.push(format!(
                "INSERT INTO ARRAY {} {}",
                n.array,
                cells.join(", ")
            ));
        }
        for sql in &writes {
            self.cluster.nodes[0]
                .exec(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        write_crdt_row(&self.cluster.nodes[0], &n.crdt).await;
    }

    /// The node placement drops from the group.
    pub fn leaving_node(&self) -> &TestClusterNode {
        self.cluster
            .nodes
            .iter()
            .find(|node| node.node_id == self.leaving)
            .expect("leaving node present")
    }

    /// Add the fourth node and wait until every node's routing view drops
    /// the leaving node from the group's voters.
    pub async fn depart(&mut self) {
        self.cluster
            .add_learner_node()
            .await
            .expect("add the fourth node");
        let (group, leaving) = (self.group, self.leaving);
        wait_for(
            "every node sees the leaving node out of the group",
            Duration::from_secs(90),
            Duration::from_millis(200),
            || {
                self.cluster.nodes.iter().all(|node| {
                    let routing = node
                        .shared
                        .cluster_routing
                        .as_ref()
                        .expect("cluster routing")
                        .read()
                        .unwrap_or_else(|p| p.into_inner());
                    routing.group_info(group).is_some_and(|info| {
                        !info.members.contains(&leaving)
                            && !info.learners.contains(&leaving)
                            && info.members.contains(&4)
                    })
                })
            },
        )
        .await;
    }

    /// What `node`'s own cores hold of the departed vShards' data, one line
    /// per kind. Empty once a purge finished there.
    pub async fn stale_on(&self, node: &TestClusterNode) -> Vec<String> {
        let snapshots = node.stored_tenant_snapshots(TenantId::new(TENANT)).await;
        stale_in(&snapshots, &self.names, &self.departed)
    }

    /// Whether `node` still holds the edge with an endpoint on a vShard it
    /// keeps.
    pub async fn holds_kept_edge(&self, node: &TestClusterNode) -> bool {
        let prefix = format!(
            "{}\x00{}\x00L\x00{}\x00",
            self.names.graph, self.names.src, self.names.kept_dst
        );
        node.stored_tenant_snapshots(TenantId::new(TENANT))
            .await
            .iter()
            .any(|snap| snap.edges.iter().any(|(key, _)| key.starts_with(&prefix)))
    }

    /// Every engine's rows read back through `node`, served by the vShards'
    /// owners.
    pub async fn assert_served(&self, node: &TestClusterNode, stage: &str) {
        let n = &self.names;
        for collection in n.collections() {
            let rows = first_column(node, &format!("SELECT COUNT(*) FROM {collection}")).await;
            assert_eq!(
                rows,
                vec!["1".to_string()],
                "{stage}: {collection} serves its row"
            );
        }
        let edges = first_column(
            node,
            &format!("MATCH (a)-[:L]->(b) IN '{}' RETURN b", n.graph),
        )
        .await;
        let mut want = vec![n.dst.clone(), n.kept_dst.clone()];
        want.sort();
        assert_eq!(edges, want, "{stage}: both edges serve");
        let labeled = first_column(
            node,
            &format!("MATCH (a:Departed)-[:L]->(b) IN '{}' RETURN a", n.graph),
        )
        .await;
        assert!(
            labeled.contains(&n.src),
            "{stage}: the label serves: {labeled:?}"
        );
        let cells = first_column(
            node,
            &format!(
                "SELECT v FROM ARRAY_SLICE('{}', '{{x: [0, 63], y: [0, 63]}}', ['v'], 1000)",
                n.array
            ),
        )
        .await;
        assert_eq!(
            cells.len() as u64,
            ARRAY_CELLS,
            "{stage}: every array cell serves"
        );
        assert_crdt_served(node, &n.crdt, stage).await;
    }
}

/// The departed data `snapshots` hold, one line per kind.
fn stale_in(
    snapshots: &[TenantDataSnapshot],
    names: &Names,
    departed: &BTreeSet<u32>,
) -> Vec<String> {
    let mut stale = Vec::new();
    for snap in snapshots {
        for collection in names.collections() {
            let keyed = snap
                .documents
                .iter()
                .chain(&snap.kv_tables)
                .chain(&snap.columnar_engines)
                .chain(&snap.timeseries)
                .chain(&snap.vectors)
                .any(|(key, _)| key.contains(collection))
                || snap
                    .flushed_ts_segments
                    .iter()
                    .any(|blob| blob.collection_key.contains(collection));
            if keyed {
                stale.push(format!("rows of {collection}"));
            }
        }
        let departed_edge = format!("{}\x00{}\x00L\x00{}\x00", names.graph, names.src, names.dst);
        if snap
            .edges
            .iter()
            .any(|(key, _)| key.starts_with(&departed_edge))
        {
            stale.push("the edge both of whose endpoints left".into());
        }
        if snap
            .node_labels
            .iter()
            .any(|(_, _, node, _)| node == &names.src)
            || snap
                .node_label_versions
                .iter()
                .any(|(_, _, node, _, _)| node == &names.src)
        {
            stale.push("the label of a node that left".into());
        }
        if snap
            .arrays
            .iter()
            .any(|blob| departed.contains(&blob.vshard))
        {
            stale.push("array cells of a departed vShard".into());
        }
        if holds_crdt_state(snap, &names.crdt) {
            stale.push("the CRDT row of a departed vShard".into());
        }
    }
    stale
}

/// The first column of every row `sql` returns through `node`, sorted.
pub async fn first_column(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    let mut values: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect();
    values.sort();
    values
}
