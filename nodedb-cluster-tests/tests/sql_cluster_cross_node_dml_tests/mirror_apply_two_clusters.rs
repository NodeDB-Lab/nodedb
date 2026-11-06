// SPDX-License-Identifier: BUSL-1.1

//! Two clusters in one process: a source behind the real mTLS listener, and a
//! mirror whose applier follows it with an allowlisted client.
//!
//! The mirror's descriptor and resume positions are seeded here. Bootstrap is a
//! later step of the link, and these tests start from an empty database.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::mirror::applier::spawn_mirror_applier;
use nodedb::control::mirror::source::cut_loop::{spawn_mirror_source_loops, take_mirror_cut};
use nodedb::control::mirror::source::service::SourceService;
use nodedb::control::security::catalog::database_types::DatabaseStatus;
use nodedb_cluster::mirror::{
    CrossClusterLink, MAX_TAIL_BYTES, MirrorAllowlist, MirrorClient, MirrorLinkConfig,
    MirrorSourceClient, MirrorTailEntry, MirrorTailRequest, SendThrottle, bind_mirror_endpoint,
    make_mirror_client_config, serve_mirror_source,
};
use nodedb_cluster::transport::config::SNI_HOSTNAME;
use nodedb_cluster::{generate_node_credentials, issue_leaf_for_sans};
use nodedb_types::{
    DatabaseId, MirrorCutState, MirrorGroupPosition, MirrorMode, MirrorOrigin, MirrorStatus,
};
use tokio::sync::watch;

use super::mirror_bootstrap_fixture::use_database;
use super::mirror_source_cross_node::{assert_no_straddle, barrier_seq, calvin_places};
use crate::common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};
use nodedb::control::wal_replication::ReplicatedEntry;

const SOURCE_NAME: &str = "source";
const DATABASE: &str = "mirror_apply_db";
const LINK: u64 = 51;
/// Raft compacts a group past this many applied entries.
const COMPACTION_THRESHOLD: u64 = 64;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

struct Pair {
    source: TestCluster,
    mirror: TestCluster,
    database: DatabaseId,
    link: Arc<CrossClusterLink>,
    groups: Vec<u64>,
    /// Keeps every listener serving. Dropping it stops them.
    _stop: watch::Sender<bool>,
}

impl Pair {
    fn client(&self) -> MirrorSourceClient {
        MirrorSourceClient::new(Arc::clone(&self.link))
    }

    fn data_groups(&self) -> impl Iterator<Item = u64> + '_ {
        self.groups.iter().copied().filter(|group| *group != 0)
    }

    fn start_appliers(&self) {
        for node in &self.mirror.nodes {
            spawn_mirror_applier(&node.shared, self.database, Arc::clone(&self.link));
        }
    }

    async fn shutdown(self) {
        self.mirror.shutdown().await;
        self.source.shutdown().await;
    }
}

fn commit_of(node: &TestClusterNode, group: u64) -> u64 {
    node.shared
        .multi_raft
        .get()
        .expect("raft")
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .commit_index_for(group)
        .unwrap_or(0)
}

fn log_start_of(node: &TestClusterNode, group: u64) -> u64 {
    node.shared
        .multi_raft
        .get()
        .expect("raft")
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .first_available_index(group)
        .unwrap_or(0)
}

/// Bring up both clusters and the link. The pin is live before any position is
/// read, so nothing the seeded positions name can compact away.
async fn pair(source: TestCluster) -> Pair {
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");

    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("create database");
    let catalog = source.nodes[0].shared.credentials.catalog();
    let database = catalog
        .get_database_id_by_name(DATABASE)
        .expect("catalog")
        .expect("database");

    let (ca, server) = generate_node_credentials("source").expect("server credentials");
    let client_creds = issue_leaf_for_sans(&ca, &["dr"]).expect("client credentials");
    let (stop, stopped) = watch::channel(false);
    let mut seeds = Vec::new();
    for node in &source.nodes {
        let allowlist = Arc::new(MirrorAllowlist::new([MirrorClient {
            name: "dr".to_string(),
            spki: client_creds.spki_pin,
            databases: vec![DATABASE.to_string()],
        }]));
        let endpoint =
            bind_mirror_endpoint(loopback(), &server, Arc::clone(&allowlist)).expect("listener");
        seeds.push(endpoint.local_addr().expect("listener address"));
        let service = Arc::new(SourceService::new(
            Arc::clone(&node.shared),
            SOURCE_NAME.to_string(),
        ));
        tokio::spawn(serve_mirror_source(
            endpoint,
            allowlist,
            service,
            stopped.clone(),
        ));
    }
    let link = Arc::new(CrossClusterLink::new(
        MirrorLinkConfig {
            source_cluster: SOURCE_NAME.to_string(),
            source_database: DATABASE.to_string(),
            mirror_cluster_id: mirror.nodes[0]
                .shared
                .mirror
                .cluster_id
                .get()
                .copied()
                .expect("mirror cluster id"),
            link_id: LINK,
            mode: nodedb_types::MirrorMode::Async,
            seeds,
            server_name: SNI_HOSTNAME.to_string(),
        },
        quinn::Endpoint::client(loopback()).expect("client endpoint"),
        make_mirror_client_config(&client_creds).expect("client config"),
        SendThrottle::default_cap(),
    ));

    let ack = MirrorSourceClient::new(Arc::clone(&link))
        .hello()
        .await
        .expect("an allowlisted client gets a hello through the real listener");
    assert_eq!(ack.source_database_id, database.as_u64());
    wait_for(
        "every source node applied the pin",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            source
                .nodes
                .iter()
                .all(|node| node.shared.mirror.pins.get(LINK).is_some())
        },
    )
    .await;

    let positions = ack
        .groups
        .iter()
        .map(|group| MirrorGroupPosition {
            group_id: *group,
            log_index: commit_of(&source.nodes[0], *group),
        })
        .collect();
    seed_mirror(&mirror, &source, database, positions).await;

    Pair {
        source,
        mirror,
        database,
        link,
        groups: ack.groups,
        _stop: stop,
    }
}

/// Create the mirror's descriptor under the source's id, with its resume place.
async fn seed_mirror(
    mirror: &TestCluster,
    source: &TestCluster,
    database: DatabaseId,
    positions: Vec<MirrorGroupPosition>,
) {
    let mut descriptor = source.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_database(database)
        .expect("catalog")
        .expect("descriptor");
    descriptor.status = DatabaseStatus::Mirroring;
    descriptor.mirror_origin = Some(MirrorOrigin {
        source_cluster: SOURCE_NAME.to_string(),
        source_database: database,
        source_database_name: DATABASE.to_string(),
        mode: MirrorMode::Async,
        status: MirrorStatus::Following,
    });
    let shared = &mirror.nodes[0].shared;
    propose_catalog_entry_async(shared, &CatalogEntry::PutDatabase(Box::new(descriptor)))
        .await
        .expect("seed the descriptor");
    propose_catalog_entry_async(
        shared,
        &CatalogEntry::PutMirrorCutState {
            database_id: database.as_u64(),
            state: Box::new(MirrorCutState {
                positions,
                ..MirrorCutState::default()
            }),
        },
    )
    .await
    .expect("seed the positions");
    wait_for(
        "every mirror node applied the seed",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            mirror.nodes.iter().all(|node| {
                node.shared.mirror.fence.read_only_name(database).is_some()
                    && node
                        .shared
                        .credentials
                        .catalog()
                        .get_mirror_cut_state(database)
                        .is_ok_and(|state| state.is_some_and(|state| !state.positions.is_empty()))
            })
        },
    )
    .await;
    for node in &mirror.nodes {
        node.exec(&format!("USE DATABASE {DATABASE}"))
            .await
            .expect("use the mirror");
        node.exec("SET default_read_consistency = 'eventual'")
            .await
            .expect("a mirror serves eventual reads");
    }
}

/// Wait until every mirror node reads `rows` rows of `collection`.
async fn await_rows(mirror: &TestCluster, collection: &str, rows: usize) {
    let sql = format!("SELECT id FROM {collection}");
    wait_for_async(
        &format!("every mirror node holds {rows} rows of {collection}"),
        Duration::from_secs(90),
        Duration::from_millis(200),
        || {
            let sql = sql.clone();
            async move {
                for node in &mirror.nodes {
                    let held = node
                        .query_named_rows(&sql)
                        .await
                        .map_or(0, |found| found.len());
                    if held != rows {
                        return false;
                    }
                }
                true
            }
        },
    )
    .await;
}

/// Compaction stops at the pin until an ack moves it, and the mirror holds what
/// the source wrote, through the real service behind the real listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_source_write_reaches_the_mirror_and_an_ack_releases_compaction() {
    const ROWS: usize = 200;
    let source = TestCluster::spawn_three_with_compaction_threshold(COMPACTION_THRESHOLD)
        .await
        .expect("source cluster");
    let pair = pair(source).await;
    use_database(&pair.source, DATABASE).await;
    for node in &pair.source.nodes {
        spawn_mirror_source_loops(&node.shared);
    }
    let baselines: HashMap<(u64, u64), u64> = pair
        .source
        .nodes
        .iter()
        .enumerate()
        .flat_map(|(at, node)| {
            pair.groups
                .iter()
                .map(move |group| ((at as u64, *group), log_start_of(node, *group)))
        })
        .collect();
    let mirror_reserve = pair.mirror.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_surrogate_reserve_index()
        .expect("reserve index");

    // No applier runs yet, so no ack moves the pin.
    pair.source
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    for row in 0..ROWS {
        pair.source.nodes[0]
            .exec(&format!("INSERT INTO docs (id, v) VALUES ('row{row}', 1)"))
            .await
            .expect("insert");
    }
    let busy = pair
        .data_groups()
        .max_by_key(|group| commit_of(&pair.source.nodes[0], *group))
        .expect("a data group");
    assert!(
        commit_of(&pair.source.nodes[0], busy) > 2 * COMPACTION_THRESHOLD,
        "the busy group retains past the compaction threshold"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    for (at, node) in pair.source.nodes.iter().enumerate() {
        assert_eq!(
            log_start_of(node, busy),
            baselines[&(at as u64, busy)],
            "node {at} compacted group {busy} past a pin that holds the whole log"
        );
    }

    pair.start_appliers();
    await_rows(&pair.mirror, "docs", ROWS).await;

    // The applier's ack moves the pin, and compaction follows.
    wait_for(
        "an ack released compaction of the busy group",
        Duration::from_secs(120),
        Duration::from_millis(200),
        || log_start_of(&pair.source.nodes[0], busy) > baselines[&(0, busy)],
    )
    .await;
    let pin = pair.source.nodes[0]
        .shared
        .mirror
        .pins
        .get(LINK)
        .expect("the pin");
    assert!(
        log_start_of(&pair.source.nodes[0], busy) <= pin.ceiling(busy).saturating_add(1),
        "compaction passed the acked position {}",
        pin.ceiling(busy)
    );

    // The link keeps following past the compaction.
    pair.source.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('late', 1)")
        .await
        .expect("insert");
    await_rows(&pair.mirror, "docs", ROWS + 1).await;

    // The mirror stays read-only and draws no surrogate range of its own.
    pair.mirror.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('local', 1)")
        .await
        .expect_err("a mirror refuses a client write");
    assert_eq!(
        pair.mirror.nodes[0]
            .shared
            .credentials
            .catalog()
            .get_surrogate_reserve_index()
            .expect("reserve index"),
        mirror_reserve,
        "the applier installs carried surrogates and mints none"
    );

    pair.shutdown().await;
}

/// Every source entry of `group` from `from_index`, fetched through the link.
async fn fetch_group(
    client: &MirrorSourceClient,
    group: u64,
    mut from_index: u64,
) -> Vec<MirrorTailEntry> {
    let mut entries = Vec::new();
    loop {
        let batch = client
            .tail(&MirrorTailRequest {
                link_id: LINK,
                group,
                from_index,
                max_bytes: MAX_TAIL_BYTES,
                wait_ms: 0,
            })
            .await
            .expect("fetch");
        entries.extend(batch.entries);
        if batch.next_index == from_index {
            return entries;
        }
        from_index = batch.next_index;
    }
}

/// Each transaction's slices sit between the same two cuts in every group, and
/// the mirror holds every slice of each one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cut_never_splits_a_calvin_transaction_across_groups() {
    const COLLECTIONS: usize = 16;
    const ROUNDS: u64 = 8;
    let source = TestCluster::spawn_three().await.expect("source cluster");
    let pair = pair(source).await;
    use_database(&pair.source, DATABASE).await;
    let first_index: Vec<(u64, u64)> = pair
        .data_groups()
        .map(|group| (group, commit_of(&pair.source.nodes[0], group) + 1))
        .collect();
    for index in 0..COLLECTIONS {
        pair.source
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION t_{index} (id TEXT PRIMARY KEY, v INT)"
            ))
            .await
            .expect("create collection");
    }
    pair.start_appliers();

    let writer = &pair.source.nodes[0];
    let shared = &writer.shared;
    let mut last_seq = 0;
    for round in 0..ROUNDS {
        writer.exec("BEGIN").await.expect("begin");
        for index in 0..COLLECTIONS {
            writer
                .exec(&format!(
                    "INSERT INTO t_{index} (id, v) VALUES ('k{round}', 1)"
                ))
                .await
                .expect("insert");
        }
        last_seq = (1u64 << 40) | (round + 1);
        let (committed, cut) =
            tokio::join!(writer.exec("COMMIT"), take_mirror_cut(shared, last_seq));
        committed.expect("commit");
        cut.expect("cut");
    }
    // One more cut covers whatever the last round's cut placed behind it.
    last_seq += 1;
    take_mirror_cut(shared, last_seq).await.expect("cut");

    for index in 0..COLLECTIONS {
        await_rows(&pair.mirror, &format!("t_{index}"), ROUNDS as usize).await;
    }

    let client = pair.client();
    let mut places = Vec::new();
    for (group, from_index) in first_index {
        let entries = fetch_group(&client, group, from_index).await;
        assert!(
            entries
                .iter()
                .filter_map(|entry| ReplicatedEntry::from_bytes(&entry.data))
                .any(|entry| barrier_seq(&entry) == Some(last_seq)),
            "group {group} holds the last cut's barrier"
        );
        places.push((group, calvin_places(&entries)));
    }
    assert_no_straddle(places);

    pair.shutdown().await;
}
