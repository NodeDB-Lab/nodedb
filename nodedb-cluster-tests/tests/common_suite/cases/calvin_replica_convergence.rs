// SPDX-License-Identifier: BUSL-1.1

//! Concurrent fast-path writes and cross-shard Calvin transactions on the
//! same rows leave every replica of every vShard with the same content.
//!
//! A 3-node cluster holds four collections, each on its own vShard:
//! - `accts`: strict, its `balance` is the materialized sum of
//!   `entries.amount` per account,
//! - `entries`: strict, the sum's source,
//! - `notes` and `scratch`: keyed.
//!
//! Three sessions on three nodes run at once:
//! - fast path: single-shard autocommit writes to `accts`, `notes` and
//!   `scratch`,
//! - Calvin: transactions that insert and delete `entries` rows and update
//!   the same `notes` rows,
//! - Calvin with TRUNCATE: transactions that truncate `scratch`, and once
//!   `entries`, next to a `notes` insert.
//!
//! After quiesce, each vShard's documents and index entries are byte-equal
//! on every replica, read from each node's own cores. Each account's served
//! balance equals the sum of its served entries.

use super::calvin_multishard_fixture::{Fixture, keyed_ddl};
use super::calvin_replica_content::{
    first_column, names_on_distinct_vshards, run_retrying, strict_session, sum_ddl, vshard_of,
    wait_replicas_agree,
};

/// Rounds each session runs.
const ROUNDS: usize = 20;

/// Accounts in `accts`, `acc-0` to `acc-{ACCOUNTS - 1}`.
const ACCOUNTS: usize = 3;

/// Seeded `notes` rows, `n0` to `n{NOTES - 1}`.
const NOTES: usize = 5;

/// The round whose TRUNCATE names `entries`. Every other round truncates
/// `scratch`.
const ENTRIES_TRUNCATE_ROUND: usize = ROUNDS / 2;

/// The four collections of the case.
struct Names {
    accts: String,
    entries: String,
    notes: String,
    scratch: String,
}

impl Names {
    fn all(&self) -> [&str; 4] {
        [
            self.accts.as_str(),
            self.entries.as_str(),
            self.notes.as_str(),
            self.scratch.as_str(),
        ]
    }
}

/// Single-shard autocommit writes: an `accts` column the sum leaves alone,
/// a `notes` row the Calvin session also updates, and a new `scratch` row.
async fn fast_path_writes(client: &tokio_postgres::Client, names: &Names) {
    for k in 0..ROUNDS {
        let account = k % ACCOUNTS;
        let note = k % NOTES;
        let statements = [
            format!(
                "UPDATE {} SET owner = 'fp{k}' WHERE id = 'acc-{account}'",
                names.accts
            ),
            format!(
                "UPDATE {} SET v = 'fp{k}' WHERE id = 'n{note}'",
                names.notes
            ),
            format!(
                "INSERT INTO {} (id, v) VALUES ('fp{k}', 'fp')",
                names.scratch
            ),
        ];
        for sql in &statements {
            run_retrying(client, &format!("fast path round {k}"), sql).await;
        }
    }
}

/// Cross-shard transactions: each inserts an entry, which moves its
/// account's balance on another vShard, and updates a `notes` row. Every
/// third round also deletes an earlier entry.
async fn calvin_writes(client: &tokio_postgres::Client, names: &Names) {
    for k in 0..ROUNDS {
        let account = k % ACCOUNTS;
        let note = k % NOTES;
        let amount = k + 1;
        let delete = if k >= 3 && k % 3 == 0 {
            format!("DELETE FROM {} WHERE id = 'e{}'; ", names.entries, k - 3)
        } else {
            String::new()
        };
        let txn = format!(
            "BEGIN; \
             INSERT INTO {entries} (id, account_id, amount) \
             VALUES ('e{k}', 'acc-{account}', '{amount}'); \
             UPDATE {notes} SET v = 'cv{k}' WHERE id = 'n{note}'; \
             {delete}\
             COMMIT",
            entries = names.entries,
            notes = names.notes,
        );
        run_retrying(client, &format!("calvin round {k}"), &txn).await;
    }
}

/// Cross-shard transactions that truncate a collection and insert a
/// `notes` row. Round [`ENTRIES_TRUNCATE_ROUND`] truncates `entries`, which
/// takes every truncated entry back off its account's balance.
async fn truncating_writes(client: &tokio_postgres::Client, names: &Names) {
    for k in 0..ROUNDS {
        let truncated = if k == ENTRIES_TRUNCATE_ROUND {
            &names.entries
        } else {
            &names.scratch
        };
        let txn = format!(
            "BEGIN; \
             TRUNCATE {truncated}; \
             INSERT INTO {notes} (id, v) VALUES ('t{k}', 'tr'); \
             COMMIT",
            notes = names.notes,
        );
        run_retrying(client, &format!("truncate round {k}"), &txn).await;
    }
}

/// A served number column as `f64`.
fn number(text: &str, what: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|e| panic!("{what} `{text}` is not a number: {e}"))
}

/// Each account's served balance equals the sum of its served entries.
async fn assert_balances_match_entries(client: &tokio_postgres::Client, names: &Names) {
    for account in 0..ACCOUNTS {
        let balances = first_column(
            client,
            &format!(
                "SELECT balance FROM {} WHERE id = 'acc-{account}'",
                names.accts
            ),
        )
        .await;
        let [balance] = balances.as_slice() else {
            panic!("acc-{account} has one row, found balances {balances:?}");
        };
        let amounts = first_column(
            client,
            &format!(
                "SELECT amount FROM {} WHERE account_id = 'acc-{account}'",
                names.entries
            ),
        )
        .await;
        let expected: f64 = amounts.iter().map(|a| number(a, "amount")).sum();
        assert_eq!(
            number(balance, "balance"),
            expected,
            "acc-{account}'s balance is the sum of its entries {amounts:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn replicas_converge_under_concurrent_fast_path_and_calvin_writes() {
    let [accts, entries, notes, scratch] =
        names_on_distinct_vshards(["conv_accts", "conv_entries", "conv_notes", "conv_scratch"]);
    let names = Names {
        accts,
        entries,
        notes,
        scratch,
    };
    let [accts_ddl, entries_ddl, sum] = sum_ddl(&names.entries, &names.accts);
    let fx = Fixture::spawn(&[
        accts_ddl,
        entries_ddl,
        keyed_ddl(&names.notes),
        keyed_ddl(&names.scratch),
    ])
    .await;
    fx.cluster
        .exec_ddl_on_any_leader(&sum)
        .await
        .unwrap_or_else(|e| panic!("{sum}: {e}"));
    for collection in names.all() {
        fx.wait_group_mounted(collection).await;
    }

    let seeder = &fx.cluster.nodes[0].client;
    for account in 0..ACCOUNTS {
        run_retrying(
            seeder,
            "seed an account",
            &format!(
                "INSERT INTO {} (id, owner, balance) VALUES ('acc-{account}', 'seed', '0')",
                names.accts
            ),
        )
        .await;
    }
    for note in 0..NOTES {
        run_retrying(
            seeder,
            "seed a note",
            &format!(
                "INSERT INTO {} (id, v) VALUES ('n{note}', 'seed')",
                names.notes
            ),
        )
        .await;
    }
    fx.converge().await;

    let calvin = strict_session(&fx.cluster.nodes[1]).await;
    let truncating = strict_session(&fx.cluster.nodes[2]).await;
    tokio::join!(
        fast_path_writes(&fx.cluster.nodes[0].client, &names),
        calvin_writes(&calvin, &names),
        truncating_writes(&truncating, &names),
    );

    fx.converge().await;
    let content = wait_replicas_agree(&fx.cluster, &names.all()).await;
    // The `entries` truncate can run after the last insert, so only the
    // seeded collections are sure to hold rows.
    for collection in [&names.accts, &names.notes] {
        assert!(
            content
                .get(&vshard_of(collection))
                .is_some_and(|stored| !stored.is_empty()),
            "{collection} holds rows after the workload; equal empty replicas prove nothing"
        );
    }
    assert_balances_match_entries(&fx.coordinator().client, &names).await;

    fx.cluster.shutdown().await;
}
