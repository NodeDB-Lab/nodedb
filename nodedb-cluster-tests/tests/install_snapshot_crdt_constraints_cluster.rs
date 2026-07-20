// SPDX-License-Identifier: BUSL-1.1

//! A node must reconstruct its CRDT validator's installed constraint set
//! from a restored tenant snapshot — not come up empty and retry-fence
//! every peer delta on a constrained collection forever (U6).
//!
//! ## What this guards
//!
//! `CREATE UNIQUE INDEX` bumps the collection's `constraint_version` in the
//! catalog and (once the metadata-leader reconcile loop runs) installs the
//! translated `Constraint` into the hosting node's per-core CRDT validator
//! for that collection. U6 made `MetaOp::CreateTenantSnapshot` (`create.rs`)
//! carry the validator's installed constraint set into the snapshot's
//! `crdt_constraints` field, and made `MetaOp::RestoreTenantSnapshot`
//! (`restore.rs`) reconstruct the validator from that field on restore.
//!
//! ## Why this is a direct handler round-trip, not a learner E2E
//!
//! The natural end-to-end proof would add a learner and let a real Raft
//! `InstallSnapshot` deliver the snapshot, observing the learner's validator
//! afterward. That path is currently infeasible here: a freshly-joined
//! learner never locally mounts the collection's data group (a separate,
//! known post-join hosting gap), so the learner's follower `apply_snapshot`
//! hook never fires and the test would hang on a precondition unrelated to
//! U6.
//!
//! Instead this test drives the two U6-changed handlers directly, on one
//! node, deterministically:
//!
//! 1. Install a UNIQUE(email) constraint on `users` (via one on-demand
//!    reconcile pass, with the background reconcile loop held off).
//! 2. CAPTURE a tenant snapshot (`create_tenant_snapshot` ->
//!    `MetaOp::CreateTenantSnapshot`) and assert its `crdt_constraints`
//!    field carries the installed constraint — proving `create.rs`.
//! 3. CLEAR the validator directly (`crdt_drop_constraints` ->
//!    `CrdtOp::DropConstraints`) so the constraint is definitely gone.
//! 4. RESTORE the captured snapshot (`restore_tenant_snapshot` ->
//!    `MetaOp::RestoreTenantSnapshot`) and assert the validator shows
//!    UNIQUE(email) installed again — proving `restore.rs` reconstructs the
//!    validator from the snapshot rather than merely round-tripping other
//!    engine state.
//!
//! The cluster runs one data core per node, so a single dispatch to the
//! collection's vshard core on node 0 sees all of that node's state; no
//! cross-core aggregation is needed to observe capture or restore.

use std::time::{Duration, Instant};

mod common;

use crate::common::cluster_harness::TestCluster;

use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_types::TenantId;

const COLLECTION: &str = "users";
const TENANT: u64 = 1;

/// cluster/install_snapshot_crdt_constraints
///
/// Deterministic capture -> clear -> restore round-trip proving U6's
/// tenant snapshot carries and reconstructs CRDT constraint state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_constraint_survives_snapshot_capture_and_restore() {
    // Hold the background constraint-reconcile loop off for the lifetime of
    // this test so the only constraint install is the explicit
    // `run_constraint_reconcile_once()` call below.
    unsafe {
        std::env::set_var("NODEDB_CONSTRAINT_RECONCILE_INTERVAL_MS", "3600000");
    }

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION users");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE UNIQUE INDEX users_email_uniq ON {COLLECTION} (email)"
        ))
        .await
        .expect("CREATE UNIQUE INDEX users_email_uniq");

    // Install the constraint deterministically: drive one reconcile pass on
    // every node (only the metadata leader actually proposes; the rest
    // no-op), then poll node 0's validator until UNIQUE(email) is installed.
    for node in &cluster.nodes {
        node.run_constraint_reconcile_once().await;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let constraints = cluster.nodes[0]
            .crdt_constraints(TenantId::new(TENANT), COLLECTION)
            .await;
        if constraints
            .iter()
            .any(|c: &Constraint| c.kind == ConstraintKind::Unique && c.field == "email")
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "UNIQUE(email) constraint on '{COLLECTION}' did not install within 15s of \
                 driving reconcile on demand; observed: {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // CAPTURE: build the tenant snapshot via the U6 `create.rs` handler and
    // assert it carries the installed constraint.
    let snap_bytes = cluster.nodes[0]
        .create_tenant_snapshot(TenantId::new(TENANT))
        .await;
    assert!(
        !snap_bytes.is_empty(),
        "create_tenant_snapshot returned an empty payload"
    );
    let snap: nodedb::types::TenantDataSnapshot =
        zerompk::from_msgpack(&snap_bytes).expect("decode TenantDataSnapshot");
    assert!(
        !snap.crdt_constraints.is_empty(),
        "captured snapshot's crdt_constraints is empty; U6 capture did not carry the \
         validator's installed constraint set"
    );
    assert!(
        snap.crdt_constraints.iter().any(|entry| {
            entry.tenant_id == TENANT
                && entry.collection == COLLECTION
                && entry.version == 1
                && !entry.constraints.is_empty()
        }),
        "captured snapshot did not contain an entry for (tenant={TENANT}, collection={COLLECTION}, \
         version=1) with at least one constraint blob; observed: {:?}",
        snap.crdt_constraints
    );

    // CLEAR: drop the validator's constraint set directly so restore is the
    // only possible source of reinstallation.
    assert!(
        cluster.nodes[0]
            .crdt_drop_constraints(TenantId::new(TENANT), COLLECTION, 1)
            .await,
        "crdt_drop_constraints dispatch did not return Ok"
    );
    let clear_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let constraints = cluster.nodes[0]
            .crdt_constraints(TenantId::new(TENANT), COLLECTION)
            .await;
        if constraints.is_empty() {
            break;
        }
        if Instant::now() >= clear_deadline {
            panic!(
                "validator still reports constraints after crdt_drop_constraints within 5s; \
                 observed: {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // RESTORE: apply the captured snapshot via the U6 `restore.rs` handler.
    assert!(
        cluster.nodes[0].restore_tenant_snapshot(snap_bytes).await,
        "restore_tenant_snapshot dispatch did not return Ok"
    );

    // PRIMARY ASSERTION: the validator must show UNIQUE(email) installed
    // again. Without the U6 fix, the snapshot would not carry the
    // validator's constraint set, so after the drop above the restore could
    // not reinstall it — the validator would stay empty forever.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let constraints = cluster.nodes[0]
            .crdt_constraints(TenantId::new(TENANT), COLLECTION)
            .await;
        if constraints
            .iter()
            .any(|c: &Constraint| c.kind == ConstraintKind::Unique && c.field == "email")
        {
            break;
        }
        if Instant::now() >= deadline {
            panic!(
                "validator never re-observed UNIQUE(email) on '{COLLECTION}' within 10s of \
                 restoring the captured snapshot; without the U6 fix the snapshot does not \
                 carry the validator's installed constraint set, so restore cannot reinstall \
                 it after the drop and the validator stays empty forever; observed: \
                 {constraints:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    cluster.shutdown().await;
}
