// SPDX-License-Identifier: BUSL-1.1
//! 3-node cluster test for a MATERIALIZED SUM whose SOURCE and TARGET
//! collections home to DIFFERENT vShards.
//!
//! A collection homes to one vShard, so this is the ordinary case, not the
//! exotic one: two collections named independently almost never collide. The
//! balance therefore cannot ride the source write's transaction — that
//! transaction belongs to the source's core, which owns none of the target's
//! rows. The Control Plane appends an `ApplyBalanceDelta` task homed on the
//! target instead, the pair classifies as multi-shard, and Calvin commits both
//! or neither.
//!
//! The test asserts the homing premise FIRST. Without that assertion a change
//! that happened to make the two collections co-resident would leave every
//! balance assertion below passing while testing the co-resident path.

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::Duration;

use nodedb::types::DatabaseId;

/// Source and target, chosen for readability rather than for their hashes — the
/// homing assertion below is what makes the choice meaningful.
pub(super) const SOURCE: &str = "xs_entries";
pub(super) const TARGET: &str = "xs_accounts";

pub(super) fn pg_detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{e}"),
    }
}

/// The balance column of one `TARGET` row, as read through `client`.
pub(super) async fn balance_of(client: &tokio_postgres::Client, account: &str) -> String {
    let rows = client
        .simple_query(&format!(
            "SELECT balance FROM {TARGET} WHERE id = '{account}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read balance: {}", pg_detail(&e)));
    rows.into_iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| panic!("target row {account} must exist"))
}

/// The balance of the account every single-account test uses.
async fn balance(client: &tokio_postgres::Client) -> String {
    balance_of(client, "acc-1").await
}

/// Create the two collections and declare the binding.
pub(super) async fn declare_binding(cluster: &TestCluster) {
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {TARGET} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {SOURCE} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source collection");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "ALTER COLLECTION {TARGET} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {SOURCE} \
             ON {SOURCE}.account_id = {TARGET}.id VALUE {SOURCE}.amount"
        ))
        .await
        .expect("declare materialized sum");
}

/// The premise the whole file rests on: the two collections do NOT share a
/// vShard, so every balance below travels on its own task.
#[test]
fn source_and_target_home_to_different_vshards() {
    let source = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE).vshard();
    let target = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, TARGET).vshard();
    assert_ne!(
        source, target,
        "this file tests the CROSS-SHARD path; '{SOURCE}' and '{TARGET}' must not be co-resident"
    );
}

/// A single INSERT into the source credits the target's balance, across shards.
///
/// The failure this guards is silent: before the balance travelled as its own
/// task, the derived write was applied inside the source's transaction, on the
/// source's core — a store no reader of the target collection ever consults. The
/// statement succeeded and the total never moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_insert_credits_the_target_balance() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '100')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-1', '25')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "125",
        "100 + 25 must land on the target row that lives on another vShard"
    );
}

/// Several inserts against the same account accumulate, and the column the sum
/// does not touch survives every read-modify-write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_cross_shard_inserts_accumulate() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('acc-1', 'alice', '0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    for (id, amount) in [("e1", "10"), ("e2", "20"), ("e3", "30.5")] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {SOURCE} (id, account_id, amount) VALUES \
                 ('{id}', 'acc-1', '{amount}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "60.5",
        "every cross-shard entry must be counted exactly once"
    );

    let rows = cluster.nodes[0]
        .client
        .simple_query(&format!("SELECT owner FROM {TARGET} WHERE id = 'acc-1'"))
        .await
        .unwrap_or_else(|e| panic!("read owner: {}", pg_detail(&e)));
    let owner = rows.into_iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_string),
        _ => None,
    });
    assert_eq!(
        owner.as_deref(),
        Some("alice"),
        "columns the sum does not touch must survive the write-back"
    );
}

/// Seed `account` with `balance` and one entry per `(id, amount)`, then wait for
/// the whole cluster to catch up.
///
/// The seeding entries go in through the INSERT path, which already credits the
/// target across shards; every test below then exercises a NON-insert shape
/// against that settled starting total.
pub(super) async fn seed(
    cluster: &TestCluster,
    account: &str,
    start: &str,
    entries: &[(&str, &str)],
) {
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {TARGET} (id, owner, balance) VALUES ('{account}', 'alice', '{start}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed account {account}: {}", pg_detail(&e)));
    for (id, amount) in entries {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {SOURCE} (id, account_id, amount) VALUES \
                 ('{id}', '{account}', '{amount}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed entry {id}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
}

/// A cross-shard UPDATE moves only the DIFFERENCE.
///
/// The delta an UPDATE owes is `new − old`, and the plan carries only `new`.
/// Before the pre-image was folded on the Control Plane and shipped, this shape
/// deferred nothing: the source core folded the real difference into its OWN
/// store, which owns none of the target's rows, and the statement succeeded with
/// the total unmoved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_update_moves_only_the_difference() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    seed(&cluster, "acc-1", "100", &[("e1", "25")]).await;
    assert_eq!(balance(&cluster.nodes[0].client).await, "125");

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "UPDATE {SOURCE} SET amount = '40' WHERE id = 'e1'"
        ))
        .await
        .unwrap_or_else(|e| panic!("update entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "140",
        "only the 25 -> 40 difference may move; re-adding the whole new value \
         would read 165 and re-adding nothing would read 125"
    );
}

/// A cross-shard DELETE takes the removed row's contribution back off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_delete_takes_the_row_back_off() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    seed(&cluster, "acc-1", "100", &[("e1", "25"), ("e2", "30")]).await;
    assert_eq!(balance(&cluster.nodes[0].client).await, "155");

    cluster.nodes[0]
        .client
        .simple_query(&format!("DELETE FROM {SOURCE} WHERE id = 'e1'"))
        .await
        .unwrap_or_else(|e| panic!("delete entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "130",
        "a deleted row's contribution must come back off the cross-shard total"
    );
}

/// A cross-shard join-key MOVE debits the account the row leaves and credits
/// the one it joins — two sibling balance tasks, one statement.
///
/// This is the shape with the most ways to go wrong: crediting only the new
/// account leaves the old one permanently overstated, and debiting only the old
/// one leaves the new one short. The two accounts hash independently of each
/// other and of the source, so each side is its own task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_join_key_move_debits_one_account_and_credits_the_other() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    seed(&cluster, "acc-1", "100", &[("e1", "25")]).await;
    seed(&cluster, "acc-2", "0", &[]).await;
    assert_eq!(balance_of(&cluster.nodes[0].client, "acc-1").await, "125");
    assert_eq!(balance_of(&cluster.nodes[0].client, "acc-2").await, "0");

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "UPDATE {SOURCE} SET account_id = 'acc-2' WHERE id = 'e1'"
        ))
        .await
        .unwrap_or_else(|e| panic!("move entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance_of(&cluster.nodes[0].client, "acc-1").await,
        "100",
        "the account the row LEFT must lose the row's whole value"
    );
    assert_eq!(
        balance_of(&cluster.nodes[0].client, "acc-2").await,
        "25",
        "the account the row JOINED must gain it"
    );
}

/// A cross-shard UPSERT onto an EXISTING row folds as an update, not an insert.
///
/// The conflict branch rewrites a row that already contributed, so the delta is
/// the difference between the stored row and the merged one. Counting the whole
/// merged value would double the entry's contribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_upsert_onto_an_existing_row_moves_only_the_difference() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    seed(&cluster, "acc-1", "100", &[("e1", "25")]).await;
    assert_eq!(balance(&cluster.nodes[0].client).await, "125");

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {SOURCE} (id, account_id, amount) VALUES ('e1', 'acc-1', '60') \
             ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount"
        ))
        .await
        .unwrap_or_else(|e| panic!("upsert entry: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "160",
        "the conflict branch owes 60 - 25, not 60"
    );
}

/// A cross-shard PREDICATE update completes instead of retrying forever.
///
/// A predicate write carries a resolution the leader verifies against the rows
/// it actually matched, and the settlement deliberately REMOVES the cross-shard
/// join values from that resolution. A coverage check that still demanded them
/// would report every such statement as diverged, and the coordinator would
/// re-recon, resolve, remove them again and resubmit — spinning until the retry
/// budget ran out rather than writing a wrong total. The statement finishing
/// with the right total is what says the check now asks only about the bindings
/// the source core actually applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_shard_predicate_update_completes_instead_of_retrying_forever() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    declare_binding(&cluster).await;
    seed(
        &cluster,
        "acc-1",
        "0",
        &[("e1", "10"), ("e2", "20"), ("e3", "30")],
    )
    .await;
    assert_eq!(balance(&cluster.nodes[0].client).await, "60");

    // No primary key in the WHERE, so this is a predicate-driven bulk update:
    // three rows, each contributing its own difference.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "UPDATE {SOURCE} SET amount = '5' WHERE account_id = 'acc-1'"
        ))
        .await
        .unwrap_or_else(|e| panic!("bulk update: {}", pg_detail(&e)));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    assert_eq!(
        balance(&cluster.nodes[0].client).await,
        "15",
        "three rows at 5 each; the statement must settle, not spin"
    );
}
