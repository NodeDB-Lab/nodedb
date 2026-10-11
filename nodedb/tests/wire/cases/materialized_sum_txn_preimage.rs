// SPDX-License-Identifier: BUSL-1.1

//! A write to a materialized-sum source in a transaction block settles its
//! balance from the row as the transaction sees it.
//!
//! An UPDATE or DELETE of a source row owes the target the difference between
//! the row's pre-image and its post-image. When an earlier statement of the
//! same transaction inserted or rewrote that row, the pre-image is the staged
//! row, not the committed one. A pre-image read from committed base alone
//! misses a staged insert and returns the base value of a staged rewrite. The
//! balance is then wrong by the staged row's contribution.
//!
//! Each test runs on both pairs. On the cross-shard pair the balance travels
//! on its own task, settled from the pre-image at statement time. On the
//! co-resident pair the source write folds it at commit, from the target rows
//! the statement resolved off the same pre-image.

use nodedb_types::id::DatabaseId;

use crate::harness::TestServer;

/// A target and source on one vShard: the balance rides the source write.
const CO_RESIDENT: (&str, &str) = ("uo_accounts", "uo_entries");
/// A target and source on two vShards: the balance travels on its own task.
const CROSS_SHARD: (&str, &str) = ("ptv_acct", "ptv_post");
/// Data Plane cores the server runs.
const CORES: usize = 2;

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// The premise both paths rest on: the names hash as the constants say, and
/// the cross-shard pair lands on two cores.
#[test]
fn the_pairs_hash_to_the_paths_they_name() {
    assert_eq!(vshard(CO_RESIDENT.0), vshard(CO_RESIDENT.1));
    assert_ne!(
        vshard(CROSS_SHARD.0),
        vshard(CROSS_SHARD.1),
        "the cross-shard pair must plan a separate balance task"
    );
    assert_ne!(
        vshard(CROSS_SHARD.0) as usize % CORES,
        vshard(CROSS_SHARD.1) as usize % CORES,
        "a balance folded on the source's core must find no target row"
    );
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// Start a server and create both sum pairs. Each pair has accounts `acc-0`
/// and `acc-1`, and posting `e0` puts 10 on `acc-0`.
async fn fixture() -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    for (target, source) in [CO_RESIDENT, CROSS_SHARD] {
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, owner TEXT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
        exec(
            &server,
            &format!(
                "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
                 MATERIALIZED_SUM SOURCE {source} \
                 ON {source}.account_id = {target}.id VALUE {source}.amount"
            ),
        )
        .await;
        for account in ["acc-0", "acc-1"] {
            exec(
                &server,
                &format!(
                    "INSERT INTO {target} (id, owner, balance) VALUES ('{account}', 'alice', '0')"
                ),
            )
            .await;
        }
        exec(
            &server,
            &format!("INSERT INTO {source} (id, account_id, amount) VALUES ('e0', 'acc-0', '10')"),
        )
        .await;
        assert_eq!(
            balance(&server, target, "acc-0").await,
            "10",
            "the seed posting lands on {target}"
        );
    }
    server
}

/// The stored balance of `account` in `target`.
async fn balance(server: &TestServer, target: &str, account: &str) -> String {
    let rows = server
        .query_text(&format!(
            "SELECT balance FROM {target} WHERE id = '{account}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read the balance of {target}.{account}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one balance row for {target}.{account}, got {other:?}"),
    }
}

/// Run `statements` in one transaction block on every pair, then check each
/// pair's balances of `acc-0` and `acc-1`.
async fn run_block(statements: &[&str], expected: (&str, &str)) {
    let server = fixture().await;
    for (target, source) in [CO_RESIDENT, CROSS_SHARD] {
        let body: Vec<String> = statements
            .iter()
            .map(|statement| statement.replace("{source}", source))
            .collect();
        let sql = format!("BEGIN; {}; COMMIT", body.join("; "));
        exec(&server, &sql).await;
        assert_eq!(
            balance(&server, target, "acc-0").await,
            expected.0,
            "acc-0 of {target} after {sql}"
        );
        assert_eq!(
            balance(&server, target, "acc-1").await,
            expected.1,
            "acc-1 of {target} after {sql}"
        );
    }
}

/// A row inserted and deleted in one block leaves the balance unchanged. The
/// delete's pre-image is the staged row, so it debits the amount the insert
/// credited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_insert_then_delete_leaves_the_balance_unchanged() {
    run_block(
        &[
            "INSERT INTO {source} (id, account_id, amount) VALUES ('e1', 'acc-0', '5')",
            "DELETE FROM {source} WHERE id = 'e1'",
        ],
        ("10", "0"),
    )
    .await;
}

/// A row inserted and then updated in one block credits the updated amount.
/// The update's pre-image is the staged row, so it moves the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_insert_then_update_credits_the_updated_amount() {
    run_block(
        &[
            "INSERT INTO {source} (id, account_id, amount) VALUES ('e1', 'acc-0', '5')",
            "UPDATE {source} SET amount = '7' WHERE id = 'e1'",
        ],
        ("17", "0"),
    )
    .await;
}

/// A row moved to another account and then updated in one block: the old
/// account loses the original amount and the new account holds the updated
/// one. The second update's pre-image names the account the first moved the
/// row to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_move_then_update_settles_on_the_new_account() {
    run_block(
        &[
            "UPDATE {source} SET account_id = 'acc-1' WHERE id = 'e0'",
            "UPDATE {source} SET amount = '9' WHERE id = 'e0'",
        ],
        ("0", "9"),
    )
    .await;
}

/// A row inserted and then moved to another account in one block credits
/// only the account it ends on. The move's pre-image is the staged row, so
/// the statement resolves and debits the account the insert credited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_insert_then_move_credits_only_the_final_account() {
    run_block(
        &[
            "INSERT INTO {source} (id, account_id, amount) VALUES ('e1', 'acc-0', '5')",
            "UPDATE {source} SET account_id = 'acc-1' WHERE id = 'e1'",
        ],
        ("10", "5"),
    )
    .await;
}
