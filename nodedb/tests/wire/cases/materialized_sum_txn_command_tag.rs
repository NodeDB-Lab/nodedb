// SPDX-License-Identifier: BUSL-1.1

//! A write to a materialized-sum source in a transaction block answers with
//! the command tag of its own rows only.
//!
//! A sum-source write whose target lives on another vShard plans a second
//! task: the balance move on the target row. That task is a derived write.
//! Its verb and its count never reach the statement's tag. An `INSERT`
//! answers `INSERT 0 1`, never `INSERT 0 2`. A `DELETE` answers `DELETE 1`,
//! never an error for mixing `DELETE` with the balance move's `UPDATE`.

use nodedb_types::id::DatabaseId;
use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

/// A target and source that share one vShard: the balance rides the source
/// write's own transaction.
const CO_RESIDENT: (&str, &str) = ("uo_accounts", "uo_entries");

/// A target and source on two vShards: the balance travels on its own task.
const CROSS_SHARD: (&str, &str) = ("ptv_acct", "ptv_post");

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// The premise both paths rest on: the names hash as the constants say.
#[test]
fn the_pairs_hash_to_the_paths_they_name() {
    assert_eq!(vshard(CO_RESIDENT.0), vshard(CO_RESIDENT.1));
    assert_ne!(
        vshard(CROSS_SHARD.0),
        vshard(CROSS_SHARD.1),
        "the cross-shard pair must plan a separate balance task"
    );
}

/// Every `CommandComplete` count in `sql`'s simple-query response, in wire
/// order.
async fn command_counts(server: &TestServer, sql: &str) -> Vec<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .collect()
}

/// Create `target` with a `balance` summed from `source.amount`, and seed
/// one account and one entry.
async fn sum_pair(server: &TestServer, target: &str, source: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {target} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {source} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER COLLECTION {target} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {source} \
             ON {source}.account_id = {target}.id VALUE {source}.amount"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {target} (id, owner, balance) VALUES ('acc', 'alice', '0')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {source} (id, account_id, amount) VALUES ('e0', 'acc', '10')"
        ))
        .await
        .unwrap();
}

/// `BEGIN; INSERT; DELETE; COMMIT` on a sum source tags each write with its
/// own row count, whether the target shares the source's vShard or not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sum_source_write_in_a_block_tags_only_its_own_rows() {
    let server = TestServer::start().await;
    for (target, source) in [CO_RESIDENT, CROSS_SHARD] {
        sum_pair(&server, target, source).await;

        let sql = format!(
            "BEGIN; \
             INSERT INTO {source} (id, account_id, amount) VALUES ('e1', 'acc', '4'); \
             DELETE FROM {source} WHERE id = 'e0'; \
             COMMIT"
        );
        assert_eq!(
            command_counts(&server, &sql).await,
            vec![0, 1, 1, 0],
            "BEGIN, INSERT 0 1, DELETE 1, COMMIT for {source}"
        );

        let balance = server
            .query_text(&format!("SELECT balance FROM {target} WHERE id = 'acc'"))
            .await
            .unwrap();
        assert_eq!(
            balance,
            vec!["4".to_string()],
            "{target} holds the inserted entry and not the deleted one"
        );
    }
}
