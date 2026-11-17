// SPDX-License-Identifier: BUSL-1.1

//! REINDEX must keep every write, and must rebuild every index it names.
//!
//! - A full-text or CSR rebuild snapshots its index, builds off the core,
//!   then swaps the result in. Rows written, updated or deleted between the
//!   snapshot and the swap must show in the swapped-in index exactly as
//!   they do in the rows. A failpoint holds the rebuild thread so the
//!   writes land inside that window.
//! - REINDEX with no index name rebuilds every index kind the collection
//!   has: HNSW, full-text and CSR. Both the concurrent and the plain form
//!   are covered.
//! - Plain REINDEX rebuilds off the core like the concurrent form and
//!   answers only after the cutover. While its rebuild is held, the same
//!   core still answers queries on other collections.
//!
//! Each rebuild reports on the `nodedb::reindex` tracing target:
//! `rebuild_started` when it starts, then `atomic_cutover` when it is
//! swapped in or `rebuild_refused` when it is discarded. The tests count
//! those events.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use nodedb_test_support::pgwire_harness::TestServer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// ── Event recorder ────────────────────────────────────────────────────────────

/// `(message, index, scope)` of every `nodedb::reindex` event. `scope` is
/// the event's `collection` field, or its `key` field for HNSW.
#[derive(Default)]
pub(super) struct EventLog(Mutex<Vec<(String, String, String)>>);

impl EventLog {
    pub(super) fn count(&self, message: &str, index: &str, collection: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, i, s)| m == message && i == index && s.contains(collection))
            .count()
    }
}

struct Recorder(Arc<EventLog>);

#[derive(Default)]
struct Fields {
    message: String,
    index: String,
    scope: String,
}

impl Fields {
    fn record_text(&mut self, field: &tracing::field::Field, text: &str) {
        match field.name() {
            "message" => self.message = text.to_string(),
            "index" => self.index = text.to_string(),
            "collection" | "key" => self.scope = text.to_string(),
            _ => {}
        }
    }
}

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        self.record_text(field, text.trim_matches('"'));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_text(field, value);
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Recorder {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "nodedb::reindex" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0
            .0
            .lock()
            .unwrap()
            .push((fields.message, fields.index, fields.scope));
    }
}

/// The process-wide event log. Installed before the first server starts.
pub(super) fn events() -> Arc<EventLog> {
    static LOG: OnceLock<Arc<EventLog>> = OnceLock::new();
    Arc::clone(LOG.get_or_init(|| {
        let log = Arc::new(EventLog::default());
        tracing_subscriber::registry()
            .with(Recorder(Arc::clone(&log)))
            .try_init()
            .expect("the reindex event recorder must be the process's tracing subscriber");
        log
    }))
}

/// Wait until every started `index` rebuild of `collection` has been swapped
/// in, and fail on any rebuild that was discarded instead.
pub(super) async fn wait_for_cutovers(log: &EventLog, index: &str, collection: &str) {
    let started = log.count("rebuild_started", index, collection);
    assert!(
        started > 0,
        "REINDEX must start a {index} rebuild of {collection}"
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let refused = log.count("rebuild_refused", index, collection);
        assert_eq!(
            refused, 0,
            "a {index} rebuild of {collection} was discarded instead of swapped in"
        );
        let cutovers = log.count("atomic_cutover", index, collection);
        if cutovers >= started {
            assert_eq!(cutovers, started, "one cutover per started {index} rebuild");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{index} rebuild of {collection} did not cut over: {cutovers} of {started}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Ids a full-text match on `body` returns, sorted.
pub(super) async fn text_ids(server: &TestServer, collection: &str, term: &str) -> Vec<String> {
    let mut ids = server
        .query_text(&format!(
            "SELECT id FROM {collection} WHERE text_match(body, '{term}')"
        ))
        .await
        .unwrap();
    ids.sort();
    ids
}

/// Create a search index on the `body` field of `collection`.
pub(super) async fn search_index(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE SEARCH INDEX ON {collection} (body)"))
        .await
        .unwrap();
}

/// Nodes one hop out of `root` along label `l`, as one text blob.
pub(super) async fn out_of_root(server: &TestServer, collection: &str) -> String {
    server
        .query_text_joined(&format!(
            "GRAPH TRAVERSE IN '{collection}' FROM 'root' DEPTH 1 LABEL 'l' DIRECTION out"
        ))
        .await
        .unwrap()
        .join("\n")
}

pub(super) async fn insert_edge(server: &TestServer, collection: &str, dst: &str) {
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'root' TO '{dst}' TYPE 'l'"
        ))
        .await
        .unwrap();
}

// ── REINDEX with no index name ────────────────────────────────────────────────

/// A collection with an HNSW index, full-text rows and graph edges.
async fn collection_with_every_index(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection} TYPE document"))
        .await
        .unwrap();
    search_index(server, collection).await;
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{collection} ON {collection} (embedding) METRIC cosine DIM 4"
        ))
        .await
        .unwrap();
    for (id, body, emb) in [
        ("r1", "alpha one", "1.0,0.0,0.0,0.0"),
        ("r2", "alpha two", "0.0,1.0,0.0,0.0"),
        ("r3", "alpha three", "0.0,0.0,1.0,0.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, body, embedding) VALUES ('{id}', '{body}', ARRAY[{emb}])"
            ))
            .await
            .unwrap();
    }
    insert_edge(server, collection, "r1").await;
    insert_edge(server, collection, "r2").await;
}

/// One numeric property of the collection's vector index status.
async fn vector_status(server: &TestServer, collection: &str, property: &str) -> u64 {
    let rows = server
        .query_rows(&format!(
            "SHOW VECTOR INDEX status ON {collection}.embedding"
        ))
        .await
        .unwrap();
    rows.iter()
        .find(|r| r[0] == property)
        .unwrap_or_else(|| panic!("SHOW VECTOR INDEX must report {property}: {rows:?}"))[1]
        .parse()
        .unwrap()
}

/// Seal the vector rows into one segment and wait for its first build, so
/// an HNSW rebuild has a sealed segment to work on.
async fn seal_vectors(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "ALTER VECTOR INDEX ON {collection}.embedding SEAL"
        ))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let sealed = vector_status(server, collection, "sealed_segments").await;
        let building = vector_status(server, collection, "building_segments").await;
        if sealed >= 1 && building == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the sealed segment did not build: sealed={sealed} building={building}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reindex_without_a_name_rebuilds_every_index_kind() {
    let log = events();
    let server = TestServer::start().await;
    collection_with_every_index(&server, "rx_all").await;
    seal_vectors(&server, "rx_all").await;

    server.exec("REINDEX CONCURRENTLY rx_all").await.unwrap();

    wait_for_cutovers(&log, "fts", "rx_all").await;
    wait_for_cutovers(&log, "csr", "rx_all").await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while log.count("atomic_cutover", "hnsw", "rx_all") == 0 {
        assert!(
            Instant::now() < deadline,
            "REINDEX with no index name must rebuild the HNSW index too"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        text_ids(&server, "rx_all", "alpha").await,
        ["r1", "r2", "r3"]
    );
    let blob = out_of_root(&server, "rx_all").await;
    assert!(blob.contains("r1") && blob.contains("r2"), "got: {blob}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_reindex_without_a_name_rebuilds_every_index_kind() {
    let log = events();
    let server = TestServer::start().await;
    collection_with_every_index(&server, "rx_plain").await;
    seal_vectors(&server, "rx_plain").await;

    // The plain form answers after every cutover.
    server.exec("REINDEX rx_plain").await.unwrap();

    for index in ["fts", "csr"] {
        let started = log.count("rebuild_started", index, "rx_plain");
        assert!(started > 0, "REINDEX must rebuild the {index} index");
        assert_eq!(
            log.count("atomic_cutover", index, "rx_plain"),
            started,
            "the plain {index} rebuild must cut over before REINDEX returns"
        );
        assert_eq!(log.count("rebuild_refused", index, "rx_plain"), 0);
    }
    assert!(
        log.count("atomic_cutover", "hnsw", "rx_plain") > 0,
        "the plain HNSW rebuild must cut over before REINDEX returns"
    );

    assert_eq!(
        text_ids(&server, "rx_plain", "alpha").await,
        ["r1", "r2", "r3"]
    );
    let blob = out_of_root(&server, "rx_plain").await;
    assert!(blob.contains("r1") && blob.contains("r2"), "got: {blob}");
}
