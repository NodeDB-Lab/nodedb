// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal CRDT history and the server-owned temporal fields.
//!
//! A CRDT collection with `bitemporal=true` keeps its history in the SQL
//! sparse projection. Loro holds only the current merged state, one entry
//! per row id, so UNIQUE judges every present row.
//!
//! `_ts_system`, `_ts_valid_from` and `_ts_valid_until` are server-owned on
//! every CRDT collection. A sync delta, a native apply or a SQL write that
//! names one is refused as a constraint violation.

use std::time::Duration;

use loro::LoroValue;

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb::engine::crdt::tenant_state::{ApplyTarget, TenantCrdtEngine, ValidatedApplyOutcome};
use nodedb::types::TenantId;
use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::violation::ViolationType;

const USERS: &str = "users";
const BITEMPORAL: &str = "bt_users";
const PLAIN: &str = "plain_users";
const RESERVED: [&str; 3] = ["_ts_system", "_ts_valid_from", "_ts_valid_until"];

/// A tenant with a strict UNIQUE email constraint on `USERS`.
fn tenant() -> TenantCrdtEngine {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", USERS, "email");
    let mut eng = TenantCrdtEngine::new(TenantId::new(1), 1, cs).unwrap();
    eng.set_collection_policy_typed(USERS, CollectionPolicy::strict());
    eng
}

/// A peer's Loro snapshot that writes one `USERS` row with `fields`.
fn engine_delta(peer: u64, row_id: &str, fields: &[(&str, LoroValue)]) -> Vec<u8> {
    let doc = loro::LoroDoc::new();
    doc.set_peer_id(peer).unwrap();
    let row = doc
        .get_map(USERS)
        .insert_container(row_id, loro::LoroMap::new())
        .unwrap();
    for (name, value) in fields {
        row.insert(name, value.clone()).unwrap();
    }
    doc.commit();
    doc.export(loro::ExportMode::Snapshot).unwrap()
}

/// Apply `delta` for `row_id` through the validated sync apply path.
fn engine_apply(
    eng: &mut TenantCrdtEngine,
    peer: u64,
    row_id: &str,
    delta: &[u8],
) -> Result<(), ViolationType> {
    let outcome = eng.apply_committed_delta_validated(
        USERS,
        delta,
        ApplyTarget::Document {
            document_id: row_id,
            surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(row_id.as_bytes()),
        },
        peer,
        &nodedb_crs::CrsContext::builtin_only(),
    );
    match outcome {
        ValidatedApplyOutcome::Clean { .. } => Ok(()),
        ValidatedApplyOutcome::Rejected(violation) => Err(violation),
        other => panic!("{row_id}: expected a clean or rejected apply, got {other:?}"),
    }
}

/// A delta naming any reserved field is refused, and the row stays absent.
#[test]
fn a_sync_delta_naming_a_reserved_field_is_refused() {
    for (peer, field) in (20..).zip(RESERVED) {
        let mut eng = tenant();
        let delta = engine_delta(
            peer,
            "u1",
            &[
                ("email", LoroValue::String("a@example.com".into())),
                (field, LoroValue::I64(1_000)),
            ],
        );
        let violation = engine_apply(&mut eng, peer, "u1", &delta)
            .expect_err("a reserved field must refuse the delta");
        match violation {
            ViolationType::ConstraintViolation { detail } => {
                assert!(detail.contains(field), "{field}: {detail}");
            }
            other => panic!("{field}: expected a constraint violation, got {other:?}"),
        }
        assert!(eng.read_row(USERS, "u1").is_none(), "{field}: row applied");
    }
}

/// Two rows holding one UNIQUE value collide. Loro holds one entry per row
/// id, so no superseded version sits beside a live row.
#[test]
fn two_rows_with_one_unique_value_collide() {
    let mut eng = tenant();
    let email = || ("email", LoroValue::String("dup@example.com".into()));
    engine_apply(&mut eng, 11, "u1", &engine_delta(11, "u1", &[email()]))
        .expect("the first claim applies");

    let violation = engine_apply(&mut eng, 12, "u2", &engine_delta(12, "u2", &[email()]))
        .expect_err("a second row with the same value must collide");
    assert!(
        matches!(&violation, ViolationType::UniqueViolation { field, .. } if field == "email"),
        "{violation:?}"
    );
    assert!(eng.read_row(USERS, "u2").is_none());
}

/// A server with a bitemporal and a plain CRDT collection.
async fn crdt_server() -> TestServer {
    let server = TestServer::start().await;
    for name in [BITEMPORAL, PLAIN] {
        let bitemporal = if name == BITEMPORAL {
            ", bitemporal=true"
        } else {
            ""
        };
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, email TEXT) \
                 WITH (crdt='true'{bitemporal})"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    server
}

/// [`crdt_server`] and a sync client connected to it.
async fn sync_fixture() -> (TestServer, SyncTestClient) {
    let server = crdt_server().await;
    let config = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) =
        ShutdownBus::new(std::sync::Arc::new(ShutdownWatch::new()));
    let state = start_sync_listener(
        config,
        Some(std::sync::Arc::clone(&server.shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    let client = SyncTestClient::connect(state.config.listen_addr)
        .await
        .expect("sync handshake");
    (server, client)
}

/// One client document. Each write exports the updates since the last one.
struct PeerDoc {
    doc: loro::LoroDoc,
    sent: loro::VersionVector,
}

impl PeerDoc {
    fn new(peer: u64) -> Self {
        let doc = loro::LoroDoc::new();
        doc.set_peer_id(peer).expect("peer id");
        let sent = doc.oplog_vv();
        Self { doc, sent }
    }

    /// The delta that sets `fields` on row `row` of `collection`.
    fn write(&mut self, collection: &str, row: &str, fields: &[(&str, LoroValue)]) -> Vec<u8> {
        let coll = self.doc.get_map(collection);
        let map = match coll.get(row) {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) => map,
            _ => coll
                .insert_container(row, loro::LoroMap::new())
                .expect("row container"),
        };
        map.insert("id", row).expect("id field");
        for (name, value) in fields {
            map.insert(name, value.clone()).expect("field");
        }
        self.doc.commit();
        let delta = self
            .doc
            .export(loro::ExportMode::updates(&self.sent))
            .expect("export updates");
        self.sent = self.doc.oplog_vv();
        delta
    }
}

fn email(value: &str) -> (&'static str, LoroValue) {
    ("email", LoroValue::String(value.into()))
}

/// N sync applies to one row leave N sparse versions. `AS OF SYSTEM TIME`
/// at the first version's stamp returns the first body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_applies_to_one_row_leave_one_sparse_version_each() {
    let (server, mut client) = sync_fixture().await;
    let mut peer = PeerDoc::new(41);
    let bodies = ["v1@example.com", "v2@example.com", "v3@example.com"];
    for (mutation, body) in (1..).zip(bodies) {
        let delta = peer.write(BITEMPORAL, "u1", &[email(body)]);
        let outcome = client
            .push_delta(BITEMPORAL, "u1", 41, mutation, delta)
            .await
            .expect("push delta");
        assert!(
            matches!(outcome, DeltaOutcome::Ack(_)),
            "{body}: {outcome:?}"
        );
        // Each version lands in its own millisecond, so an `AS OF` cut at
        // one stamp excludes the next version.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let versions = server
        .query_named_rows(&format!(
            "SELECT email, _ts_system FROM {BITEMPORAL} AS OF SYSTEM TIME NULL \
             WHERE id = 'u1'"
        ))
        .await
        .expect("audit read");
    assert_eq!(versions.len(), bodies.len(), "{versions:?}");

    let mut stamped: Vec<(i64, String)> = versions
        .iter()
        .map(|row| {
            let ts = row
                .get("_ts_system")
                .and_then(|ts| ts.parse::<i64>().ok())
                .unwrap_or_else(|| panic!("version without an integer _ts_system: {row:?}"));
            let body = row.get("email").cloned().unwrap_or_default();
            (ts, body)
        })
        .collect();
    stamped.sort();
    let bodies_in_order: Vec<&str> = stamped.iter().map(|(_, body)| body.as_str()).collect();
    assert_eq!(bodies_in_order, bodies);

    let first_ts = stamped[0].0;
    let at_first = server
        .query_text(&format!(
            "SELECT email FROM {BITEMPORAL} AS OF SYSTEM TIME {first_ts} WHERE id = 'u1'"
        ))
        .await
        .expect("as-of read");
    assert_eq!(at_first, vec![bodies[0].to_string()]);
}

async fn row_ids(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// A sync delta writing `_ts_valid_until` is refused terminally on a
/// bitemporal and on a plain CRDT collection. The row stays absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_delta_writing_valid_until_is_refused_on_every_crdt_collection() {
    let (server, mut client) = sync_fixture().await;
    for (peer, collection) in (51..).zip([BITEMPORAL, PLAIN]) {
        let mut doc = PeerDoc::new(peer);
        let delta = doc.write(
            collection,
            "r",
            &[
                email("r@example.com"),
                ("_ts_valid_until", LoroValue::I64(1_000)),
            ],
        );
        let outcome = client
            .push_delta(collection, "r", peer, 1, delta)
            .await
            .expect("push delta");
        match &outcome {
            DeltaOutcome::Reject(reject) => assert!(
                format!("{reject:?}").contains("_ts_valid_until"),
                "{collection}: the refusal names the field: {reject:?}"
            ),
            DeltaOutcome::Ack(_) => panic!("{collection}: a reserved-field delta applied"),
        }
        assert!(
            row_ids(&server, collection).await.is_empty(),
            "{collection}"
        );

        // The refusal does not wedge the stream. A fresh peer writes the
        // clean row, so its delta holds no operation the server refused.
        let clean_peer = peer + 100;
        let clean = PeerDoc::new(clean_peer).write(collection, "ok", &[email("ok@example.com")]);
        let outcome = client
            .push_delta(collection, "ok", clean_peer, 2, clean)
            .await
            .expect("push delta");
        assert!(
            matches!(outcome, DeltaOutcome::Ack(_)),
            "{collection}: {outcome:?}"
        );
    }
}

/// A SQL write naming `_ts_valid_until` is refused on a bitemporal and on a
/// plain CRDT collection. The row stays absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sql_write_of_valid_until_is_refused_on_every_crdt_collection() {
    let server = crdt_server().await;
    for collection in [BITEMPORAL, PLAIN] {
        let err = server
            .exec(&format!(
                "INSERT INTO {collection} {{ id: 's', email: 's@example.com', \
                 _ts_valid_until: 1000 }}"
            ))
            .await
            .expect_err("a reserved-field SQL write must be refused");
        assert!(err.contains("_ts_valid_until"), "{collection}: {err}");
        assert!(
            row_ids(&server, collection).await.is_empty(),
            "{collection}"
        );
    }
}
