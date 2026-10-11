// SPDX-License-Identifier: BUSL-1.1

//! Concurrent cross-shard Calvin transactions commit exactly once while the
//! data-group leadership of their vShards moves between nodes.
//!
//! 1. A 3-node cluster holds `accts`, whose `balance` is the materialized
//!    sum of `entries.amount`, and `entries`, on distinct vShards. Each
//!    account starts at 0.
//! 2. Two sessions on two nodes run transactions at once. Each inserts one
//!    entry, which moves its account's balance on the other vShard.
//! 3. Meanwhile the test moves the leadership of each participant data
//!    group to another replica, several times. A moved leader drops what it
//!    staged, and the new leader stages its held transactions again.
//! 4. After quiesce, every entry a COMMIT answered for is stored once,
//!    every replica holds the same content, and each balance equals both
//!    the sum of its entries and the sum the client committed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::calvin_multishard_fixture::Fixture;
use super::calvin_replica_content::{
    first_column, names_on_distinct_vshards, run_retrying, strict_session, sum_ddl,
    wait_replicas_agree,
};
use crate::common::cluster_harness::shared_steps::{group_members, group_of};
use crate::common::cluster_harness::{GroupLeader, TestCluster};

/// Transactions each session runs.
const ROUNDS: usize = 12;

/// Sessions that run transactions at once, one per node.
const SESSIONS: usize = 2;

/// Accounts in `accts`, `acc-0` to `acc-{ACCOUNTS - 1}`.
const ACCOUNTS: usize = 3;

/// Leadership moves of each participant data group.
const MOVES: usize = 6;

/// Pause between two rounds of moves, so transactions run under each
/// leader.
const MOVE_PAUSE: Duration = Duration::from_millis(250);

/// How long a group gets to show a leader before a move.
const LEADER_WAIT: Duration = Duration::from_secs(30);

/// The account transaction `k` of session `session` books to.
fn account(session: usize, k: usize) -> usize {
    (session + k) % ACCOUNTS
}

/// The amount transaction `k` of session `session` books. Every amount is
/// distinct, so a sum names the entries it counts.
fn amount(session: usize, k: usize) -> usize {
    session * 100 + k + 1
}

/// Run session `session`'s transactions through `client`, one at a time.
/// Each returns only once it committed.
async fn book_entries(client: &tokio_postgres::Client, entries: &str, session: usize) {
    for k in 0..ROUNDS {
        let txn = format!(
            "BEGIN; \
             INSERT INTO {entries} (id, account_id, amount) \
             VALUES ('s{session}e{k}', 'acc-{}', '{}'); \
             COMMIT",
            account(session, k),
            amount(session, k),
        );
        run_retrying(client, &format!("session {session} txn {k}"), &txn).await;
    }
}

/// Move the leadership of each of `groups` to another of its replicas,
/// [`MOVES`] times, cycling through the replicas.
async fn move_leaders(cluster: &TestCluster, groups: &[u64]) {
    for round in 0..MOVES {
        for &group in groups {
            let current = cluster
                .wait_for_group_leader(group, GroupLeader::Any, LEADER_WAIT)
                .await;
            let others: Vec<u64> = group_members(&cluster.nodes[0], group)
                .into_iter()
                .filter(|member| *member != current)
                .collect();
            assert!(
                !others.is_empty(),
                "group {group} has a replica besides its leader node {current}"
            );
            let target = others[round % others.len()];
            cluster.transfer_leadership(group, target).await;
        }
        tokio::time::sleep(MOVE_PAUSE).await;
    }
}

/// A served number column as an integer.
fn number(text: &str, what: &str) -> usize {
    text.parse::<f64>()
        .map(|value| value as usize)
        .unwrap_or_else(|e| panic!("{what} `{text}` is not a number: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn calvin_txns_commit_once_while_data_group_leaders_move() {
    let [accts, entries] = names_on_distinct_vshards(["split_accts", "split_entries"]);
    let [accts_ddl, entries_ddl, sum] = sum_ddl(&entries, &accts);
    let fx = Fixture::spawn(&[accts_ddl, entries_ddl]).await;
    fx.cluster
        .exec_ddl_on_any_leader(&sum)
        .await
        .unwrap_or_else(|e| panic!("{sum}: {e}"));
    for collection in [&accts, &entries] {
        fx.wait_group_mounted(collection).await;
    }
    for account in 0..ACCOUNTS {
        run_retrying(
            &fx.cluster.nodes[0].client,
            "seed an account",
            &format!(
                "INSERT INTO {accts} (id, owner, balance) VALUES ('acc-{account}', 'seed', '0')"
            ),
        )
        .await;
    }
    fx.converge().await;

    let groups: Vec<u64> = [&accts, &entries]
        .into_iter()
        .map(|collection| group_of(&fx.cluster.nodes[0], collection))
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .collect();
    let sessions = [
        strict_session(&fx.cluster.nodes[1]).await,
        strict_session(&fx.cluster.nodes[2]).await,
    ];
    tokio::join!(
        book_entries(&sessions[0], &entries, 0),
        book_entries(&sessions[1], &entries, 1),
        move_leaders(&fx.cluster, &groups),
    );

    fx.converge().await;
    let collections = [accts.as_str(), entries.as_str()];
    wait_replicas_agree(&fx.cluster, &collections).await;

    let client = &fx.coordinator().client;
    let mut stored = first_column(client, &format!("SELECT id FROM {entries}")).await;
    stored.sort();
    let mut expected_ids: Vec<String> = (0..SESSIONS)
        .flat_map(|session| (0..ROUNDS).map(move |k| format!("s{session}e{k}")))
        .collect();
    expected_ids.sort();
    assert_eq!(
        stored, expected_ids,
        "each committed transaction's entry is stored once"
    );

    let mut committed: BTreeMap<usize, usize> = BTreeMap::new();
    for session in 0..SESSIONS {
        for k in 0..ROUNDS {
            *committed.entry(account(session, k)).or_default() += amount(session, k);
        }
    }
    for account in 0..ACCOUNTS {
        let balances = first_column(
            client,
            &format!("SELECT balance FROM {accts} WHERE id = 'acc-{account}'"),
        )
        .await;
        let [balance] = balances.as_slice() else {
            panic!("acc-{account} has one row, found balances {balances:?}");
        };
        let amounts = first_column(
            client,
            &format!("SELECT amount FROM {entries} WHERE account_id = 'acc-{account}'"),
        )
        .await;
        let summed: usize = amounts.iter().map(|a| number(a, "amount")).sum();
        let balance = number(balance, "balance");
        assert_eq!(
            balance, summed,
            "acc-{account}'s balance is the sum of its entries {amounts:?}"
        );
        assert_eq!(
            balance,
            committed.get(&account).copied().unwrap_or_default(),
            "acc-{account}'s balance counts each committed transaction once"
        );
    }

    fx.cluster.shutdown().await;
}
