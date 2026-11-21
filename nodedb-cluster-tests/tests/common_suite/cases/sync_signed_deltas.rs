// SPDX-License-Identifier: BUSL-1.1
//! A `SIGNED_DELTAS` collection refuses an unsigned sync delta and accepts a
//! correctly signed one, on every replica.
//!
//! ## What this guards
//!
//! `CREATE COLLECTION ... WITH (crdt=true, signed_deltas=true)` needs an
//! authenticated WAL. The cluster here shares one WAL key, so every node
//! opens an encrypted WAL. A sync session that authenticates with a bearer
//! token gets a per-user signing key at handshake. A Trust-mode session gets
//! none, because it presents no credential the key could bind to.
//!
//! - An unsigned delta pushed over sync gets a terminal `DeltaReject`. The
//!   refusal happens before any Raft proposal, so no replica holds the row.
//! - A delta signed with the handshake key, the session's producer id, epoch
//!   and stream seq is acked. Its WAL record carries the admitted signature,
//!   so every follower applies it under the same admission.

use crate::common;
use common::cluster_harness::{PitrStorage, TestCluster};

use std::time::{Duration, Instant};

use nodedb::control::security::jwks::registry::JwksRegistry;
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_test_support::jwks_fixture::{JwksFixture, loopback_egress, now_secs};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::compensation::CompensationHint;

const COLL: &str = "signed_docs";
const UNSIGNED_DOC: &str = "unsigned-doc";
const SIGNED_DOC: &str = "signed-doc";
/// The tenant the harness superuser creates the collection in.
const TENANT: u64 = 1;
const ISSUER: &str = "https://sync-signed-deltas.example/";
const AUDIENCE: &str = "nodedb-sync";
/// The `user_id` claim of the signing principal. The signature binds it.
const SIGNER_USER_ID: u64 = 4242;

/// A Loro snapshot delta that writes one row `row_id` with a `name` field.
fn row_delta(row_id: &str, name: &str) -> Vec<u8> {
    let doc = loro::LoroDoc::new();
    let coll = doc.get_map(COLL);
    let row = coll
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", name).expect("field");
    doc.commit();
    doc.export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot")
}

/// `crdt_state(COLL, doc)` on `client`. Empty when the node holds no row.
/// Retries transient catch-up errors until `timeout`.
async fn crdt_state(client: &tokio_postgres::Client, doc: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT crdt_state('{COLL}', '{doc}')"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return r.get(0).unwrap_or("").to_string();
                    }
                }
                return String::new();
            }
            // A document the node does not hold answers `02000`.
            Err(e) if e.code() == Some(&tokio_postgres::error::SqlState::NO_DATA) => {
                return String::new();
            }
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Err(e) => panic!("crdt_state('{COLL}', '{doc}') failed: {e:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_deltas_refuse_unsigned_and_accept_signed_on_every_replica() {
    let jwks = JwksFixture::spawn().await;
    let registry = JwksRegistry::init(
        jwks.auth_config(ISSUER, AUDIENCE, TENANT),
        loopback_egress(),
    )
    .await
    .expect("test JWKS registry must initialize");
    let root = tempfile::tempdir().expect("tempdir");
    let pitr = PitrStorage::create(root.path()).expect("PITR storage");
    let cluster =
        TestCluster::spawn_three_with_pitr_and_jwks(pitr, Some(std::sync::Arc::new(registry)))
            .await
            .expect("spawn three-node cluster with an encrypted WAL");
    assert!(
        cluster
            .nodes
            .iter()
            .all(|node| node.shared.wal.payloads_authenticated()),
        "every node must open an authenticated WAL"
    );

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} WITH (engine='document_schemaless', crdt=true, \
             signed_deltas=true)"
        ))
        .await
        .expect("CREATE COLLECTION with SIGNED_DELTAS");

    let cfg = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) =
        ShutdownBus::new(std::sync::Arc::new(ShutdownWatch::new()));
    let state = start_sync_listener(
        cfg,
        Some(std::sync::Arc::clone(&cluster.nodes[0].shared)),
        shutdown_bus,
    )
    .await
    .expect("start sync listener");
    let addr = state.config.listen_addr;

    // Unsigned: refused terminally before any proposal.
    let mut unsigned = SyncTestClient::connect(addr)
        .await
        .expect("unsigned client handshake");
    let outcome = unsigned
        .push_delta(COLL, UNSIGNED_DOC, 7, 1, row_delta(UNSIGNED_DOC, "nobody"))
        .await
        .expect("push unsigned delta");
    match outcome {
        DeltaOutcome::Reject(reject) => assert!(
            matches!(
                reject.compensation,
                Some(CompensationHint::PermissionDenied)
            ),
            "an unsigned delta must be refused as permission denied, got {reject:?}"
        ),
        DeltaOutcome::Ack(ack) => {
            panic!("an unsigned delta on a SIGNED_DELTAS collection was acked: {ack:?}")
        }
    }

    // A Trust-mode session presents no credential, so it gets no key.
    let trust = SyncTestClient::connect_as_lite(addr, "signed-deltas-trust-device", 1)
        .await
        .expect("trust client handshake");
    assert_eq!(
        trust.delta_signing_key(),
        [0; 32],
        "a Trust-mode session must not receive a delta signing key"
    );

    // Signed: a token-authenticated session gets a key, and the signed
    // delta is acked.
    let now = now_secs();
    let token = jwks.mint(&serde_json::json!({
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "signed-deltas-writer",
        "user_id": SIGNER_USER_ID,
        "roles": ["readwrite"],
        "iat": now,
        "exp": now + 600,
    }));
    let mut signer =
        SyncTestClient::connect_as_lite_with_token(addr, &token, "signed-deltas-device", 1)
            .await
            .expect("signing client handshake");
    assert_ne!(
        signer.delta_signing_key(),
        [0; 32],
        "a token-authenticated session on an encrypted WAL gets a delta signing key"
    );
    let outcome = signer
        .push_signed_delta(
            SIGNER_USER_ID,
            COLL,
            SIGNED_DOC,
            8,
            1,
            row_delta(SIGNED_DOC, "alice"),
        )
        .await
        .expect("push signed delta");
    assert!(
        matches!(outcome, DeltaOutcome::Ack(_)),
        "a correctly signed delta must be acked, got {outcome:?}"
    );

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Every replica holds the signed row and none holds the unsigned one.
    for node in &cluster.nodes {
        let signed_state = crdt_state(&node.client, SIGNED_DOC, Duration::from_secs(20)).await;
        assert!(
            !signed_state.is_empty(),
            "node {} does not hold the signed row",
            node.node_id
        );
        let unsigned_state = crdt_state(&node.client, UNSIGNED_DOC, Duration::from_secs(20)).await;
        assert!(
            unsigned_state.is_empty(),
            "node {} holds the refused unsigned row",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
