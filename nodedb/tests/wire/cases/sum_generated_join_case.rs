// SPDX-License-Identifier: BUSL-1.1

//! The generated join column refusal reads the column name without regard to
//! letter case. A sum that names the generated column in another case is
//! still refused with SQLSTATE `0A000`.

use crate::harness::TestServer;

const ACCOUNTS: &str = "gjc_acct";
const POSTINGS: &str = "gjc_post";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_generated_join_column_named_in_another_case_is_refused() {
    let server = TestServer::start_multicores(2).await;
    server
        .exec(&format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the target");
    server
        .exec(&format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, acct TEXT, amount FLOAT, \
             account_id TEXT GENERATED ALWAYS AS ('a-' || acct)) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("create the source");

    let bind = format!(
        "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance FLOAT \
         MATERIALIZED_SUM SOURCE {POSTINGS} \
         ON {POSTINGS}.ACCOUNT_ID = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
    );
    match server.exec(&bind).await {
        Err(message) => assert!(
            message.contains("0A000") || message.contains("generated column"),
            "expected the generated join column refusal, got: {message}"
        ),
        Ok(()) => panic!("a sum bound on the generated join column named as ACCOUNT_ID"),
    }
}
