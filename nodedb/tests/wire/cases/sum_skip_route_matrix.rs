// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... ON CONFLICT DO NOTHING` on the source of a cross-shard
//! materialized sum, over every transport.
//!
//! The fixture starts with `e0` = 10 and `e1` = 5 on `acc`, balance 15, and
//! `acc2` empty. Each transport sends a skip of a stored key, then a page that
//! names one new key twice. A shape builds its new keys from the number of its
//! pair, so no two pairs share a new key.
//!
//! After each statement the balance of every account must equal the sum of its
//! source rows. The cell lists all findings, and realigns the balances so the
//! next pair stands alone.

use super::sum_cell_support::{Drift, SumPair, Transport, sum_fixture};
use crate::harness::TestServer;

const INSERT: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES";
const ON_CONFLICT_NOTHING: &str = "ON CONFLICT (id) DO NOTHING";

/// The route a statement takes to the server.
#[derive(Clone, Copy)]
enum Wire {
    PgSimple,
    PgExtended,
    Request(Transport),
}

const WIRES: [(&str, Wire); 6] = [
    ("pgwire simple", Wire::PgSimple),
    ("pgwire extended", Wire::PgExtended),
    ("native", Wire::Request(Transport::Native)),
    ("http", Wire::Request(Transport::Http)),
    ("ndjson", Wire::Request(Transport::Ndjson)),
    ("ws", Wire::Request(Transport::Ws)),
];

/// A statement shape. `sql` builds the statement for the pair numbered `n`.
struct Shape {
    name: &'static str,
    sql: fn(usize) -> String,
}

const SHAPES: [Shape; 2] = [
    Shape {
        name: "insert on conflict do nothing of a stored key",
        sql: |n| format!("{INSERT} ('e0', 'acc', '{n}') {ON_CONFLICT_NOTHING}"),
    },
    Shape {
        name: "repeated new key on conflict do nothing",
        sql: |n| {
            format!("{INSERT} ('d{n}', 'acc', '2'), ('d{n}', 'acc', '7') {ON_CONFLICT_NOTHING}")
        },
    },
];

/// Send `sql` over `wire`. `Err` carries the text of a refusal.
async fn send(server: &TestServer, wire: Wire, sql: &str) -> Result<(), String> {
    match wire {
        Wire::PgSimple => server.exec(sql).await,
        Wire::PgExtended => server
            .client
            .execute(sql, &[])
            .await
            .map(drop)
            .map_err(|e| e.to_string()),
        Wire::Request(transport) => {
            let sent = transport.send(server, sql).await;
            if sent.errored { Err(sent.text) } else { Ok(()) }
        }
    }
}

/// Write each drifting balance back to its source sum.
async fn realign(server: &TestServer, drift: &[Drift]) {
    for row in drift {
        server
            .exec(&format!(
                "UPDATE ptv_acct SET balance = '{}' WHERE id = '{}'",
                row.sum, row.id
            ))
            .await
            .expect("write the balance back to the source sum");
    }
}

/// The post-condition reports a balance written apart from its source rows.
/// Without this control a blind check passes every pair.
async fn detector_finds_a_corrupt_balance(server: &TestServer) -> bool {
    server
        .exec("UPDATE ptv_acct SET balance = '999' WHERE id = 'acc2'")
        .await
        .expect("corrupt the balance of acc2");
    let drift = server
        .sum_drift(SumPair::POSTINGS)
        .await
        .expect("read both sides of the sum");
    realign(server, &drift).await;
    drift.iter().any(|row| row.id == "acc2")
}

/// Every transport applies each skip shape, and each balance equals its source
/// sum afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_transport_settles_an_insert_that_skips_a_conflict() {
    let server = sum_fixture().await;
    let mut findings = Vec::new();
    if !detector_finds_a_corrupt_balance(&server).await {
        findings.push("the post-condition missed a corrupt balance".to_string());
    }

    let mut n = 0;
    for (wire_name, wire) in WIRES {
        for shape in &SHAPES {
            n += 1;
            let sql = (shape.sql)(n);
            let outcome = send(&server, wire, &sql).await;
            if let Err(refusal) = &outcome {
                findings.push(format!(
                    "{wire_name}, {}: refused: {refusal} (statement {sql})",
                    shape.name
                ));
            }
            let drift = server
                .sum_drift(SumPair::POSTINGS)
                .await
                .expect("read both sides of the sum");
            if drift.is_empty() {
                continue;
            }
            let rows: Vec<String> = drift.iter().map(ToString::to_string).collect();
            findings.push(format!(
                "{wire_name}, {}: {} (statement {sql})",
                shape.name,
                rows.join("; ")
            ));
            realign(&server, &drift).await;
        }
    }

    assert!(
        findings.is_empty(),
        "{} findings over {n} pairs:\n{}",
        findings.len(),
        findings.join("\n")
    );
}
