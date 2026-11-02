// SPDX-License-Identifier: BUSL-1.1

//! Topics and consumer groups are replicated catalog state, not node-local
//! config.
//!
//! A node that did not run the statement receives the mutation as a
//! `CatalogEntry` and must end up with the same durable row and the same live
//! registration. These tests drive that follower path directly — apply, then
//! the synchronous post-apply — and assert on `EpTopicRegistry`,
//! `GroupRegistry`, and the node-local offset store.

use nodedb::control::catalog_entry::post_apply::apply_post_apply_side_effects_sync;
use nodedb::control::catalog_entry::{CatalogEntry, apply};
use nodedb::event::cdc::consumer_group::ConsumerGroupDef;
use nodedb::event::cdc::stream_def::RetentionConfig;
use nodedb::event::topic::TopicDef;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::DatabaseId;

const DB: u64 = 0;
const TENANT: u64 = 1;
const TOPIC: &str = "replicated_orders";
const GROUP: &str = "replicated_readers";
const PARTITION: u32 = 0;

fn database() -> DatabaseId {
    DatabaseId::new(DB)
}

fn canonical_stream() -> String {
    format!("topic:{TOPIC}")
}

fn topic() -> TopicDef {
    TopicDef {
        database_id: database(),
        tenant_id: TENANT,
        name: TOPIC.to_string(),
        retention: RetentionConfig {
            max_events: 10_000,
            max_age_secs: 3_600,
        },
        owner: "admin".to_string(),
        created_at: 1_000,
        last_sequence: 0,
        last_lsn: 0,
        last_epoch: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    }
}

fn group(stream: &str) -> ConsumerGroupDef {
    ConsumerGroupDef {
        database_id: database(),
        tenant_id: TENANT,
        name: GROUP.to_string(),
        stream_name: stream.to_string(),
        owner: "admin".to_string(),
        created_at: 1_000,
        modification_hlc: nodedb_types::Hlc::ZERO,
    }
}

fn create_topic_entry() -> CatalogEntry {
    CatalogEntry::CreateTopicIfAbsent(Box::new(topic()))
}

fn delete_topic_entry() -> CatalogEntry {
    CatalogEntry::DeleteTopicWithConsumerGroups {
        database_id: DB,
        tenant_id: TENANT,
        name: TOPIC.to_string(),
        target_hlc: nodedb_types::Hlc::ZERO,
    }
}

/// The names of every durable topic row.
fn stored_topics(server: &TestServer) -> Vec<String> {
    server
        .shared
        .credentials
        .catalog()
        .load_all_ep_topics()
        .expect("load topics")
        .into_iter()
        .map(|t| t.name)
        .collect()
}

/// The stream name of every durable consumer-group row named `GROUP`.
fn stored_group_streams(server: &TestServer) -> Vec<String> {
    server
        .shared
        .credentials
        .catalog()
        .load_all_consumer_groups()
        .expect("load consumer groups")
        .into_iter()
        .filter(|g| g.name == GROUP)
        .map(|g| g.stream_name)
        .collect()
}

/// Apply an entry the way the metadata applier does: durable write first,
/// then the synchronous in-memory install.
fn apply_entry(server: &TestServer, entry: &CatalogEntry) {
    apply::apply_to(entry, server.shared.credentials.catalog()).expect("apply catalog entry");
    apply_post_apply_side_effects_sync(entry, &server.shared);
}

/// A replicated `CreateTopicIfAbsent` makes the topic durable and live on a
/// node that never parsed the statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_create_installs_the_topic() {
    let server = TestServer::start().await;
    assert!(
        server
            .shared
            .ep_topic_registry
            .get(database(), TENANT, TOPIC)
            .is_none(),
        "no topic is registered before the entry applies"
    );

    apply_entry(&server, &create_topic_entry());

    let live = server
        .shared
        .ep_topic_registry
        .get(database(), TENANT, TOPIC)
        .expect("apply must install the definition in the live registry");
    assert_eq!(live.owner, "admin");
    assert_eq!(live.retention.max_age_secs, 3_600);
    assert!(
        stored_topics(&server).contains(&TOPIC.to_string()),
        "apply must write the durable row too"
    );
}

/// A re-delivered create is idempotent: it never rewinds a live topic's
/// durable high-water marks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_create_keeps_the_advanced_high_water_marks() {
    let server = TestServer::start().await;
    let mut advanced = topic();
    advanced.last_sequence = 42;
    advanced.last_lsn = 99;
    apply_entry(
        &server,
        &CatalogEntry::CreateTopicIfAbsent(Box::new(advanced)),
    );

    apply_entry(&server, &create_topic_entry());

    let live = server
        .shared
        .ep_topic_registry
        .get(database(), TENANT, TOPIC)
        .expect("the topic stays registered after the re-delivery");
    assert_eq!(live.last_sequence, 42, "the re-delivery must not rewind");
    assert_eq!(live.last_lsn, 99);
    assert_eq!(
        stored_topics(&server)
            .iter()
            .filter(|n| n.as_str() == TOPIC)
            .count(),
        1,
        "create-only writes one row"
    );
}

/// A replicated `DeleteTopicWithConsumerGroups` drops the topic, every group
/// attached to it, and the committed offsets those groups held on this node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_topic_its_groups_and_their_offsets() {
    let server = TestServer::start().await;
    let stream = canonical_stream();
    apply_entry(&server, &create_topic_entry());
    apply_entry(
        &server,
        &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(group(&stream))),
    );
    server
        .shared
        .offset_store
        .commit_offset(database(), TENANT, &stream, GROUP, PARTITION, 7_u64)
        .expect("commit an offset to clean up");

    apply_entry(&server, &delete_topic_entry());

    assert!(
        server
            .shared
            .ep_topic_registry
            .get(database(), TENANT, TOPIC)
            .is_none(),
        "the live topic registration must be gone"
    );
    assert!(
        server
            .shared
            .group_registry
            .get(database(), TENANT, &stream, GROUP)
            .is_none(),
        "the topic's groups must be unregistered on every node"
    );
    assert_eq!(
        server
            .shared
            .offset_store
            .get_offset(database(), TENANT, &stream, GROUP, PARTITION)
            .index,
        0,
        "a stale cursor would make a recreated topic skip events on this node"
    );
    assert!(!stored_topics(&server).contains(&TOPIC.to_string()));
    assert!(stored_group_streams(&server).is_empty());
}

/// A replicated `PutConsumerGroupIfAbsent` makes the group durable and live on
/// a node that never parsed the statement, and never overwrites a live one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_put_installs_the_consumer_group_once() {
    let server = TestServer::start().await;
    let stream = canonical_stream();
    apply_entry(&server, &create_topic_entry());

    apply_entry(
        &server,
        &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(group(&stream))),
    );

    let live = server
        .shared
        .group_registry
        .get(database(), TENANT, &stream, GROUP)
        .expect("apply must install the group in the live registry");
    assert_eq!(live.owner, "admin");
    assert_eq!(stored_group_streams(&server), vec![stream.clone()]);

    let reowned = ConsumerGroupDef {
        owner: "someone-else".to_string(),
        ..group(&stream)
    };
    apply_entry(
        &server,
        &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(reowned)),
    );

    let live = server
        .shared
        .group_registry
        .get(database(), TENANT, &stream, GROUP)
        .expect("the group stays registered after the re-delivery");
    assert_eq!(
        live.owner, "admin",
        "a re-delivered create must not overwrite the live definition"
    );
    assert_eq!(stored_group_streams(&server).len(), 1);
}

/// A replicated `DeleteConsumerGroup` drops the row, the registration, and the
/// node-local committed offsets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_delete_removes_the_consumer_group_and_its_offsets() {
    let server = TestServer::start().await;
    let stream = canonical_stream();
    apply_entry(&server, &create_topic_entry());
    apply_entry(
        &server,
        &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(group(&stream))),
    );
    server
        .shared
        .offset_store
        .commit_offset(database(), TENANT, &stream, GROUP, PARTITION, 11_u64)
        .expect("commit an offset to clean up");

    apply_entry(
        &server,
        &CatalogEntry::DeleteConsumerGroup {
            database_id: DB,
            tenant_id: TENANT,
            stream_name: stream.clone(),
            name: GROUP.to_string(),
            target_hlc: nodedb_types::Hlc::ZERO,
        },
    );

    assert!(
        server
            .shared
            .group_registry
            .get(database(), TENANT, &stream, GROUP)
            .is_none(),
        "the live registration must be gone"
    );
    assert_eq!(
        server
            .shared
            .offset_store
            .get_offset(database(), TENANT, &stream, GROUP, PARTITION)
            .index,
        0,
        "the replicated delete must clear committed offsets on every node"
    );
    assert!(stored_group_streams(&server).is_empty());
}

/// A replicated `DeleteTopicWithConsumerGroups` removes only the groups on the
/// topic's `topic:<name>` stream. A group on a change stream that shares the
/// bare name keeps its row, its registration, and its offsets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_topic_delete_keeps_a_same_named_stream_group() {
    let server = TestServer::start().await;
    apply_entry(&server, &create_topic_entry());
    apply_entry(
        &server,
        &CatalogEntry::PutConsumerGroupIfAbsent(Box::new(group(TOPIC))),
    );
    server
        .shared
        .offset_store
        .commit_offset(database(), TENANT, TOPIC, GROUP, PARTITION, 5_u64)
        .expect("commit a change-stream offset");

    apply_entry(&server, &delete_topic_entry());

    assert!(
        server
            .shared
            .group_registry
            .get(database(), TENANT, TOPIC, GROUP)
            .is_some(),
        "the change stream's group is not a topic group"
    );
    assert_eq!(stored_group_streams(&server), vec![TOPIC.to_string()]);
    assert_eq!(
        server
            .shared
            .offset_store
            .get_offset(database(), TENANT, TOPIC, GROUP, PARTITION)
            .index,
        5,
        "the change stream's cursor survives DROP TOPIC"
    );
}
