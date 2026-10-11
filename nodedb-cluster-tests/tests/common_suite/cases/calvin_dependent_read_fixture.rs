// SPDX-License-Identifier: BUSL-1.1

//! Shared fixture for the Calvin dependent-read barrier cases.
//!
//! `SELECT TRANSFER_ITEM(src, dst, 'sword', 'alice', 'bob')` between two
//! key-value collections on distinct vShards is a read-dependent Calvin
//! transaction. The source vShard reads `alice:sword` under the
//! transaction's locks and broadcasts it, through the data-group log of each
//! active vShard, to the delete at the source and the put at the
//! destination.
//!
//! Each replica's content is read from its own Data-Plane cores, so a check
//! sees what that replica installed, never a row a gateway fetched.

use std::collections::BTreeMap;
use std::time::Duration;

use nodedb::types::TenantId;

use super::calvin_multishard_fixture::Fixture;
use super::calvin_replica_content::names_on_distinct_vshards;
#[cfg(feature = "failpoints")]
use super::calvin_replica_content::vshard_of;
use crate::common::cluster_harness::shared_steps::{db_detail, leader_of};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

/// The tenant the `nodedb` pgwire user writes as.
const TENANT: u64 = 1;

/// The item's key at the source.
pub(super) const SOURCE_KEY: &[u8] = b"alice:sword";

/// The item's key at the destination.
pub(super) const DEST_KEY: &[u8] = b"bob:sword";

/// How long the replicas get to converge on the move's outcome.
const CONVERGE: Duration = Duration::from_secs(30);

/// A cluster with a source and a destination key-value collection on
/// distinct vShards, the item stored at the source.
pub(super) struct ItemMove {
    pub(super) fx: Fixture,
    pub(super) source: String,
    pub(super) dest: String,
}

impl ItemMove {
    /// Spawn the cluster, create both collections, and store the item.
    pub(super) async fn spawn(prefix: &str) -> Self {
        let source_prefix = format!("{prefix}_src");
        let dest_prefix = format!("{prefix}_dst");
        let [source, dest] =
            names_on_distinct_vshards([source_prefix.as_str(), dest_prefix.as_str()]);
        let fx = Fixture::spawn(&[kv_ddl(&source), kv_ddl(&dest)]).await;
        fx.wait_group_mounted(&source).await;
        fx.wait_group_mounted(&dest).await;
        // Collection creation installs each collection's constraint set on
        // every replica in the background. The install is one more write of
        // the collection, so the cases start their txns once it applied.
        wait_constraints_installed(&fx.cluster, &source).await;
        wait_constraints_installed(&fx.cluster, &dest).await;
        fx.coordinator()
            .client
            .simple_query(&format!(
                "INSERT INTO {source} (key, owner, note) VALUES ('alice:sword', 'alice', 'forged')"
            ))
            .await
            .unwrap_or_else(|e| panic!("store the item: {}", db_detail(&e)));
        fx.converge().await;
        Self { fx, source, dest }
    }

    /// The statement that moves the item from the source to the
    /// destination.
    pub(super) fn transfer_sql(&self) -> String {
        format!(
            "SELECT TRANSFER_ITEM('{}', '{}', 'sword', 'alice', 'bob')",
            self.source, self.dest
        )
    }

    pub(super) fn cluster(&self) -> &TestCluster {
        &self.fx.cluster
    }

    /// The vShard of the source collection.
    #[cfg(feature = "failpoints")]
    pub(super) fn source_vshard(&self) -> u32 {
        vshard_of(&self.source)
    }

    /// The vShard of the destination collection.
    #[cfg(feature = "failpoints")]
    pub(super) fn dest_vshard(&self) -> u32 {
        vshard_of(&self.dest)
    }

    /// The data group of `collection`, as the first node routes it.
    pub(super) fn group_of(&self, collection: &str) -> u64 {
        self.cluster().nodes[0]
            .group_id_for_collection(collection)
            .unwrap_or_else(|| panic!("no data group for {collection}"))
    }

    /// Wait until every replica agrees on the move's outcome. With
    /// `moved = None`, every source replica holds the item and no
    /// destination replica holds it. With `moved = Some(bytes)`, no source
    /// replica holds the item and every destination replica holds `bytes`
    /// for it: the bytes the source held, not merely some row at the key.
    pub(super) async fn wait_replicas_agree(&self, moved: Option<&[u8]>) {
        let source_group = self.group_of(&self.source);
        let dest_group = self.group_of(&self.dest);
        let deadline = std::time::Instant::now() + CONVERGE;
        loop {
            let mut agree = true;
            let mut views = Vec::new();
            for node in &self.cluster().nodes {
                let source_holds = node.replicates_data_group(source_group)
                    && local_kv_rows(node, &self.source)
                        .await
                        .contains_key(SOURCE_KEY);
                let dest_bytes = if node.replicates_data_group(dest_group) {
                    local_kv_rows(node, &self.dest).await.remove(DEST_KEY)
                } else {
                    moved.map(<[u8]>::to_vec)
                };
                if (node.replicates_data_group(source_group) && source_holds == moved.is_some())
                    || dest_bytes.as_deref() != moved
                {
                    agree = false;
                }
                views.push(format!(
                    "node {}: source holds the item = {source_holds}, destination bytes match = \
                     {}, source group applied {} (leader {}), destination group applied {} \
                     (leader {})",
                    node.node_id,
                    dest_bytes.as_deref() == moved,
                    node.shared.applied_index_watcher(source_group).current(),
                    leader_of(node, source_group),
                    node.shared.applied_index_watcher(dest_group).current(),
                    leader_of(node, dest_group),
                ));
            }
            if agree {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the replicas disagree on the move (moved = {}) after {CONVERGE:?}:\n{}",
                moved.is_some(),
                views.join("\n")
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// The bytes the source's replicas hold for the item. Panics when two
    /// replicas disagree or none holds it.
    pub(super) async fn source_bytes(&self) -> Vec<u8> {
        let source_group = self.group_of(&self.source);
        let mut seen: Option<Vec<u8>> = None;
        for node in &self.cluster().nodes {
            if !node.replicates_data_group(source_group) {
                continue;
            }
            let bytes = local_kv_rows(node, &self.source)
                .await
                .remove(SOURCE_KEY)
                .unwrap_or_else(|| panic!("node {} does not hold the item", node.node_id));
            if let Some(first) = &seen {
                assert_eq!(first, &bytes, "the source's replicas disagree");
            }
            seen = Some(bytes);
        }
        seen.expect("the source group has a replica")
    }
}

/// Assert no node halted a Calvin scheduler.
pub(super) fn assert_no_apply_halt(cluster: &TestCluster) {
    for node in &cluster.nodes {
        let halt = node.shared.sequencer_halt.apply_halt().report();
        assert!(
            halt.is_none(),
            "node {} halted a Calvin scheduler: {halt:?}",
            node.node_id
        );
    }
}

/// The txns of `vshard` whose barrier entries `node` holds and no scheduler
/// took, in memory or in their stored row.
#[cfg(feature = "failpoints")]
pub(super) fn waiting_barrier_txns(node: &TestClusterNode, vshard: u32) -> usize {
    node.shared.calvin.read_results.waiting_txns(vshard)
}

/// The barrier rows `node` stores for `vshard`: one per txn whose barrier
/// entry applied there and did not finish.
#[cfg(feature = "failpoints")]
pub(super) fn stored_barrier_rows(node: &TestClusterNode, vshard: u32) -> usize {
    node.shared
        .credentials
        .catalog()
        .load_calvin_barrier_logs(Some(&std::collections::BTreeSet::from([vshard])))
        .expect("the barrier rows load")
        .len()
}

/// The `(group, log index)` of the latest read-result entries `node`
/// applied for `vshard` in its current life.
#[cfg(feature = "failpoints")]
pub(super) fn read_entries(node: &TestClusterNode, vshard: u32) -> Vec<(u64, u64)> {
    node.shared
        .calvin
        .read_results
        .vshard(vshard)
        .read_entries()
}

/// The read-result entries `node` applied for `vshard`.
#[cfg(feature = "failpoints")]
pub(super) fn reads_applied(node: &TestClusterNode, vshard: u32) -> u64 {
    node.shared
        .calvin
        .read_results
        .vshard(vshard)
        .reads_applied()
}

/// Wait until every replica of `collection`'s data group installed the
/// collection's constraint set in its CRDT validator. The set of a key-value
/// collection names its key `NOT NULL`, so it is never empty.
async fn wait_constraints_installed(cluster: &TestCluster, collection: &str) {
    let group = cluster.nodes[0]
        .group_id_for_collection(collection)
        .unwrap_or_else(|| panic!("no data group for {collection}"));
    wait_for_async(
        &format!("every replica of {collection} installed its constraint set"),
        CONVERGE,
        Duration::from_millis(100),
        || async move {
            for node in &cluster.nodes {
                if node.replicates_data_group(group)
                    && node
                        .crdt_constraints(TenantId::new(TENANT), collection)
                        .await
                        .is_empty()
                {
                    return false;
                }
            }
            true
        },
    )
    .await;
}

fn kv_ddl(collection: &str) -> String {
    format!(
        "CREATE COLLECTION {collection} (key TEXT PRIMARY KEY, owner TEXT, note TEXT) \
         WITH (engine='kv')"
    )
}

/// Every key-value row of `collection` as `node` stores it: key to stored
/// bytes, read from each of its Data-Plane cores.
async fn local_kv_rows(node: &TestClusterNode, collection: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let table = format!("0:{TENANT}:{collection}");
    let mut rows = BTreeMap::new();
    for core in 0..node.num_cores() {
        let snapshot = node
            .tenant_snapshot_on_core(core, TenantId::new(TENANT))
            .await;
        for (key, bytes) in snapshot.kv_tables {
            if key != table {
                continue;
            }
            let entries: Vec<(Vec<u8>, Vec<u8>, u64, u32)> =
                zerompk::from_msgpack(&bytes).expect("KV table snapshot decodes");
            rows.extend(entries.into_iter().map(|(key, value, _, _)| (key, value)));
        }
    }
    rows
}
