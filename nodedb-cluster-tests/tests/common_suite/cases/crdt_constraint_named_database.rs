// SPDX-License-Identifier: BUSL-1.1
//! A CRDT collection in a named database enforces its UNIQUE constraint.
//!
//! ## What this guards
//!
//! Outside the default database a collection's stored name is
//! `{database_id}/{name}`. The catalog derives each constraint under that
//! stored name, and every replica's validator installs and finds it there.
//! The install refuses a constraint in any other name form.
//!
//! This test creates a database and a CRDT collection with a UNIQUE index in
//! it. It reads the validator itself through `ReadConstraints` and checks
//! that each constraint names the stored form. It then drives the sync
//! WebSocket into that database. A second delta that claims the same email
//! comes back as a `UniqueViolation`.

use crate::common;
use common::cluster_harness::node::lifecycle::HARNESS_SUPERUSER;
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for_async};

use std::time::{Duration, Instant};

use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_test_support::sync_client::{DeltaOutcome, SyncTestClient};
use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::{DatabaseId, QualifiedCollection, TenantId};

const DATABASE: &str = "crdt_named";
const COLL: &str = "users";

/// A Loro snapshot delta that inserts row `row_id` with one `email` field.
fn row_delta(row_id: &str, email: &str) -> Vec<u8> {
    let doc = loro::LoroDoc::new();
    let row = doc
        .get_map(COLL)
        .insert_container(row_id, loro::LoroMap::new())
        .expect("row container");
    row.insert("email", email).expect("field");
    doc.commit();
    doc.export(loro::ExportMode::Snapshot)
        .expect("export loro snapshot")
}

/// Poll `node`'s validator until the UNIQUE(email) constraint on `COLL` of
/// `database` is installed. Returns the installed set.
async fn await_unique_email_constraint(
    node: &TestClusterNode,
    database: DatabaseId,
) -> Vec<Constraint> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let constraints = node
            .crdt_constraints_in(database, TenantId::new(1), COLL)
            .await;
        if constraints
            .iter()
            .any(|c| c.kind == ConstraintKind::Unique && c.field == "email")
        {
            return constraints;
        }
        if Instant::now() >= deadline {
            panic!(
                "UNIQUE(email) constraint on '{DATABASE}.{COLL}' did not install within 30s; \
                 observed: {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_database_enforces_a_crdt_unique_constraint() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn three-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .expect("CREATE DATABASE");
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {DATABASE}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE {DATABASE} on node {}: {e}", node.node_id));
    }
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION users");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE UNIQUE INDEX users_email_uniq ON {COLL} (email)"
        ))
        .await
        .expect("CREATE UNIQUE INDEX users_email_uniq");

    let database = cluster.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(DATABASE)
        .expect("catalog read")
        .expect("the database exists");
    assert_ne!(database, DatabaseId::DEFAULT);

    // The validator holds each constraint under the stored name.
    let stored = QualifiedCollection::new(database, COLL);
    assert_eq!(stored.as_str(), format!("{}/{COLL}", database.as_u64()));
    let installed = await_unique_email_constraint(&cluster.nodes[0], database).await;
    for constraint in &installed {
        assert_eq!(
            constraint.collection,
            stored.as_str(),
            "constraint {} names the wrong form",
            constraint.name
        );
    }

    // A trust sync session runs in its user's default database.
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER USER {HARNESS_SUPERUSER} SET DEFAULT DATABASE {DATABASE}"
        ))
        .await
        .expect("ALTER USER SET DEFAULT DATABASE");
    wait_for_async(
        "node 0 holds the superuser's default database",
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            cluster.nodes[0]
                .shared
                .credentials
                .get_user(HARNESS_SUPERUSER)
                .is_some_and(|user| user.default_database_id == database.as_u64())
        },
    )
    .await;

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
    let mut client = SyncTestClient::connect(state.config.listen_addr)
        .await
        .expect("sync handshake with node 0");

    let first = client
        .push_delta(COLL, "doc1", 7, 1, row_delta("doc1", "x@y.com"))
        .await
        .expect("push the first delta");
    assert!(
        matches!(first, DeltaOutcome::Ack(_)),
        "the first claim of x@y.com must be acked, got: {first:?}"
    );

    let second = client
        .push_delta(COLL, "doc2", 7, 2, row_delta("doc2", "x@y.com"))
        .await
        .expect("push the second delta");
    match second {
        DeltaOutcome::Reject(reject) => match reject.compensation {
            Some(CompensationHint::UniqueViolation {
                field,
                conflicting_value,
            }) => {
                assert_eq!(field, "email");
                assert_eq!(conflicting_value, "x@y.com");
            }
            other => panic!("expected CompensationHint::UniqueViolation, got: {other:?}"),
        },
        DeltaOutcome::Ack(_) => {
            panic!("the duplicate email in '{DATABASE}' must be rejected, but it was acked")
        }
    }

    cluster.shutdown().await;
}
