// SPDX-License-Identifier: BUSL-1.1

//! A source cluster's side of a mirror link on three nodes: the cut, the
//! filter, the pin, and a Calvin transaction's place around a cut. These call
//! the service the listener calls, with the client the listener would name.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::metadata_proposer::propose_catalog_entry_async;
use nodedb::control::mirror::source::cut_loop::take_mirror_cut;
use nodedb::control::mirror::source::service::SourceService;
use nodedb::control::wal_replication::decode::{DecodedTransactionRedo, decode_transaction_redo};
use nodedb::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use nodedb_cluster::mirror::{
    MIRROR_PROTOCOL_VERSION, MirrorAck, MirrorClient, MirrorHello, MirrorRefusal,
    MirrorSourceService, MirrorTailEntry, MirrorTailRequest,
};
use nodedb_types::{MirrorGroupPosition, MirrorPinState};

use crate::common::cluster_harness::{TestCluster, wait_for};

const CLUSTER_NAME: &str = "source";
const DATABASE: &str = "mirror_source_db";
const OTHER_DATABASE: &str = "mirror_source_other";
const LINK: u64 = 41;
/// A cluster id no test cluster draws.
const MIRROR_CLUSTER: u64 = u64::MAX;

fn client() -> MirrorClient {
    MirrorClient {
        name: "dr".to_string(),
        spki: [0; 32],
        databases: vec![DATABASE.to_string()],
    }
}

fn service(cluster: &TestCluster, node: usize) -> SourceService {
    SourceService::new(
        Arc::clone(&cluster.nodes[node].shared),
        CLUSTER_NAME.to_string(),
    )
}

/// Every entry of `group` that leaves for the link, from a node that hosts it.
async fn tail_group(cluster: &TestCluster, group: u64) -> Vec<MirrorTailEntry> {
    for node in 0..cluster.nodes.len() {
        let source = service(cluster, node);
        let mut entries = Vec::new();
        let mut from_index = 1;
        loop {
            let read = source
                .tail(
                    &client(),
                    MirrorTailRequest {
                        link_id: LINK,
                        group,
                        from_index,
                        max_bytes: 1024 * 1024,
                        wait_ms: 0,
                    },
                )
                .await;
            match read {
                Ok(batch) => {
                    entries.extend(batch.entries);
                    if batch.next_index == from_index {
                        return entries;
                    }
                    from_index = batch.next_index;
                }
                Err(MirrorRefusal::NotHosted { .. }) => break,
                Err(refusal) => panic!("group {group} fetch refused: {refusal}"),
            }
        }
    }
    panic!("no node hosts group {group}");
}

fn barrier_seq(entry: &ReplicatedEntry) -> Option<u64> {
    let ReplicatedWrite::CutBarrier { mirror, .. } = &entry.write else {
        return None;
    };
    mirror.as_ref().map(|barrier| barrier.mark.seq)
}

async fn open_link(cluster: &TestCluster) -> Vec<u64> {
    let ack = service(cluster, 0)
        .hello(
            &client(),
            MirrorHello {
                source_cluster: CLUSTER_NAME.to_string(),
                source_database: DATABASE.to_string(),
                mirror_cluster_id: MIRROR_CLUSTER,
                link_id: LINK,
                protocol_version: MIRROR_PROTOCOL_VERSION,
            },
        )
        .await
        .expect("hello");
    wait_for(
        "every node applied the pin",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|node| node.shared.mirror.pins.get(LINK).is_some())
        },
    )
    .await;
    ack.groups
}

async fn seed_databases(cluster: &TestCluster) {
    for database in [OTHER_DATABASE, DATABASE] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .expect("create database");
    }
}

async fn use_database(cluster: &TestCluster, database: &str) {
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {database}"))
            .await
            .expect("use database");
    }
}

/// Wait until `group`'s log holds cut `seq`'s barrier on the replica it reads.
async fn await_barrier(cluster: &TestCluster, group: u64, seq: u64) -> Vec<MirrorTailEntry> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let entries = tail_group(cluster, group).await;
        let placed = entries
            .iter()
            .filter_map(|entry| ReplicatedEntry::from_bytes(&entry.data))
            .any(|entry| barrier_seq(&entry) == Some(seq));
        if placed {
            return entries;
        }
        assert!(
            Instant::now() < deadline,
            "group {group} holds no barrier of cut {seq}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The cut reaches every data group, only the link's database leaves, the pin
/// holds compaction until an ack moves it, and an expiry ends the link.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_tail_holds_its_database_and_a_barrier_in_every_group() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    seed_databases(&cluster).await;
    use_database(&cluster, OTHER_DATABASE).await;
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION secrets (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");
    cluster.nodes[0]
        .exec("INSERT INTO secrets (id, v) VALUES ('other', 1)")
        .await
        .expect("insert");
    use_database(&cluster, DATABASE).await;
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create collection");

    let groups = open_link(&cluster).await;
    let shared = &cluster.nodes[0].shared;
    let database = shared
        .credentials
        .catalog()
        .get_database_id_by_name(DATABASE)
        .expect("catalog")
        .expect("database")
        .as_u64();
    cluster.nodes[0]
        .exec("INSERT INTO docs (id, v) VALUES ('mine', 1)")
        .await
        .expect("insert");

    let seq = (1u64 << 40) | 1;
    let mark = take_mirror_cut(shared, seq).await.expect("cut");
    assert_eq!(mark.seq, seq);

    for group in groups.iter().copied().filter(|group| *group != 0) {
        for entry in await_barrier(&cluster, group, seq).await {
            let decoded = ReplicatedEntry::from_bytes(&entry.data).expect("an entry");
            assert!(
                barrier_seq(&decoded).is_some() || decoded.database_id == database,
                "group {group} index {} left for another database",
                entry.index
            );
            assert!(!matches!(
                decoded.write,
                ReplicatedWrite::CalvinReadResult { .. }
                    | ReplicatedWrite::CalvinReadTimeout { .. }
            ));
        }
    }

    // A new pin holds the whole log, and an ack moves the hold.
    let group = groups
        .iter()
        .copied()
        .find(|group| *group != 0)
        .expect("a data group");
    assert!(
        shared.mirror.pins.has_ceiling(group),
        "the hello bound group {group} by the pins"
    );
    let ceiling = shared.mirror.pins.ceiling_for(group);
    assert_eq!(ceiling.load(std::sync::atomic::Ordering::Acquire), 0);
    service(&cluster, 0)
        .ack(
            &client(),
            MirrorAck {
                link_id: LINK,
                cut: seq,
                positions: vec![MirrorGroupPosition {
                    group_id: group,
                    log_index: 1,
                }],
            },
        )
        .await
        .expect("ack");
    wait_for(
        "the ack moved the hold",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || ceiling.load(std::sync::atomic::Ordering::Acquire) == 1,
    )
    .await;

    // Another client cannot read this link.
    let stranger = MirrorClient {
        name: "stranger".to_string(),
        ..client()
    };
    let refused = service(&cluster, 0)
        .tail(
            &stranger,
            MirrorTailRequest {
                link_id: LINK,
                group,
                from_index: 1,
                max_bytes: 1024,
                wait_ms: 0,
            },
        )
        .await;
    assert!(matches!(refused, Err(MirrorRefusal::LinkNotOwned { .. })));

    // An expiry releases compaction and ends the link.
    let mut expired = shared.mirror.pins.get(LINK).expect("the pin");
    expired.state = MirrorPinState::Expired;
    propose_catalog_entry_async(shared, &CatalogEntry::PutMirrorPin(Box::new(expired)))
        .await
        .expect("expire");
    wait_for(
        "the expiry released the hold",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || ceiling.load(std::sync::atomic::Ordering::Acquire) == u64::MAX,
    )
    .await;
    let refused = service(&cluster, 0)
        .tail(
            &client(),
            MirrorTailRequest {
                link_id: LINK,
                group,
                from_index: 1,
                max_bytes: 1024,
                wait_ms: 0,
            },
        )
        .await;
    assert!(matches!(refused, Err(MirrorRefusal::PinExpired { .. })));

    cluster.shutdown().await;
}

/// Where each Calvin position sits among `group`'s cuts: the number of the
/// first cut after it.
fn calvin_places(entries: &[MirrorTailEntry]) -> Vec<((u64, u32), Option<u64>)> {
    let mut places: Vec<((u64, u32), Option<u64>)> = Vec::new();
    for entry in entries {
        let Some(decoded) = ReplicatedEntry::from_bytes(&entry.data) else {
            continue;
        };
        if let Some(seq) = barrier_seq(&decoded) {
            for place in places.iter_mut().filter(|place| place.1.is_none()) {
                place.1 = Some(seq);
            }
            continue;
        }
        let Ok(DecodedTransactionRedo::Inline(payload)) = decode_transaction_redo(&decoded.write)
        else {
            continue;
        };
        if let Some(stamp) = payload.redo.calvin_stamp {
            places.push(((stamp.epoch, stamp.position), None));
        }
    }
    places
}

/// Each slice of one Calvin transaction sits between the same two cuts in every
/// group, with cuts and transactions running at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn no_calvin_transaction_straddles_a_mirror_cut() {
    const COLLECTIONS: usize = 8;
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    seed_databases(&cluster).await;
    use_database(&cluster, DATABASE).await;
    for index in 0..COLLECTIONS {
        cluster
            .exec_ddl_on_any_leader(&format!(
                "CREATE COLLECTION t_{index} (id TEXT PRIMARY KEY, v INT)"
            ))
            .await
            .expect("create collection");
    }
    let groups = open_link(&cluster).await;

    let mut last_seq = 0;
    for round in 0..10u64 {
        let writer = &cluster.nodes[0];
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
        let (committed, cut) = tokio::join!(
            writer.exec("COMMIT"),
            take_mirror_cut(&cluster.nodes[0].shared, last_seq)
        );
        committed.expect("commit");
        cut.expect("cut");
    }

    let mut seen: BTreeMap<(u64, u32), Option<u64>> = BTreeMap::new();
    let mut slices = 0usize;
    for group in groups.iter().copied().filter(|group| *group != 0) {
        for (position, next_cut) in calvin_places(&await_barrier(&cluster, group, last_seq).await) {
            slices += 1;
            if let Some(held) = seen.insert(position, next_cut) {
                assert_eq!(
                    held, next_cut,
                    "Calvin position {position:?} straddles a cut in group {group}"
                );
            }
        }
    }
    assert!(slices > seen.len(), "no transaction spanned two slices");

    cluster.shutdown().await;
}
