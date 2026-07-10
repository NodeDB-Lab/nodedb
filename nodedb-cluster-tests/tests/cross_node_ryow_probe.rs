// SPDX-License-Identifier: BUSL-1.1

//! DEFERRED PROBE (design unit U4/U5) — CROSS-NODE in-transaction
//! read-your-own-writes. `#[ignore]`d: cross-node in-txn RYOW is out of scope
//! for U3, whose validation surface is single-node only (LOCKED design §8:
//! "single-node-verifiable: U1, U2, U3 (RYOW), U7"; cross-node is "cluster-bound
//! (stage-2): U4, U5, U6, U8"). Kept in-tree, ignored, as the executable
//! specification for the U4/U5 cross-shard-transaction work.
//!
//! SINGLE-NODE in-txn RYOW works today (U3): an in-block read takes the
//! NON-streaming dispatch path (`maybe_stream_select` bails on `InBlock`), and
//! `dispatch_local` threads the session `txn_id` to the local leaseholder so the
//! Data Plane merges this transaction's staging overlay. The native twin
//! (`native_sql_ryow_probe.rs`) and the graph twin
//! (`single_node_calvin_graph_txn.rs`) both cover that and stay GREEN.
//!
//! CONFIRMED ROOT CAUSE of the cross-node gap (diagnosed 2026-07, live trace):
//! the whole per-transaction machinery — staging gate, COMMIT buffer, and the
//! COMMIT/ROLLBACK overlay flush — is LOCAL-ONLY. On a NON-owner node the pgwire
//! dispatcher takes the remote-leader forward short-circuit
//! (`routing/execute.rs`: `should_forward_via_gateway` → `dispatch_tasks_via_gateway`)
//! and RETURNS before `dispatch_task_loop`, so `route_in_tx_write` never runs.
//! The in-txn write is therefore shipped to the owner as a PLAIN replicable
//! write (Raft-proposed = committed at statement time, NOT staged into
//! `txn_overlays[txn_id]`), and `dispatch_tasks_via_gateway` hardcodes
//! `QueryContext.txn_id = None` (`routing/gateway_dispatch.rs`) so the in-txn
//! read reaches the owner with no txn id and cannot merge the overlay. Net: a
//! cross-node in-txn write bypasses the transaction entirely — it neither
//! reads-its-own-write NOR honours ROLLBACK atomicity.
//!
//! The `ExecuteRequest.txn_id` wire field (+ `WIRE_VERSION` 2→3) and the
//! globally-unique `TxnId::from_origin` packing landed on this branch are the
//! necessary GROUNDWORK for the fix, but are NOT sufficient on their own: making
//! this probe pass requires routing the txn staging / read / COMMIT / ROLLBACK /
//! overlay-drop through the gateway to the owning node — i.e. the U4/U5
//! cross-shard-transaction unit.
//!
//! This probe drives BEGIN / INSERT / SELECT(own write) / ROLLBACK on EVERY node
//! of a 3-node cluster against a single-vShard-homed collection. Exactly one node
//! leads that vShard (local RYOW — the GREEN control); the other two route the
//! whole transaction to it over QUIC and so exercise the cross-node path, which
//! MISSES today. Remove `#[ignore]` when U4/U5 lands to turn it back into a gate.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

/// `n` for `id` read on `client` in its current session. `None` when absent, the
/// column did not parse as an integer, or the query itself errored (a cross-node
/// failure is recorded by the caller as a RYOW miss, not a panic, so every node
/// is exercised).
async fn n_for(client: &tokio_postgres::Client, id: &str) -> Option<i64> {
    let msgs = client
        .simple_query(&format!("SELECT n FROM t WHERE id = '{id}'"))
        .await
        .ok()?;
    msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => {
            r.get("n").and_then(|s| s.parse::<i64>().ok())
        }
        _ => None,
    })
}

/// A transaction reads its OWN staged write even when the collection's vShard is
/// led by a DIFFERENT node (cross-node read-your-own-writes).
///
/// `#[ignore]`d: DEFERRED to U4/U5 (cross-shard transactions). U3's validation
/// surface is single-node only (LOCKED design §8). See the module docstring for
/// the confirmed root cause. Run explicitly with `--ignored` to reproduce the
/// cross-node gap while iterating on U4/U5.
#[ignore = "cross-node in-txn RYOW is U4/U5 (cross-shard txn); U3 is single-node only — see module docstring"]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_in_txn_select_reads_own_write() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION t \
             (id TEXT PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION t");

    wait_for(
        "all 3 nodes see collection t",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;

    // Drive the whole transaction on each node in turn. The collection is
    // single-vShard-homed, so every key lives on the ONE owning vShard: the node
    // leading it does local RYOW (the control that passes today), the other two
    // route the transaction to it over QUIC and exercise the cross-node path.
    let mut failures: Vec<String> = Vec::new();
    for idx in 0..cluster.nodes.len() {
        let client = &cluster.nodes[idx].client;
        let id = format!("k{idx}");

        client.simple_query("BEGIN").await.expect("BEGIN");

        // Single-shard write; on a non-owning node it stages to the remote
        // leaseholder's overlay. An error here is itself a cross-node gap
        // symptom — record it and move on rather than aborting the whole probe.
        if let Err(e) = client
            .simple_query(&format!("INSERT INTO t (id, n) VALUES ('{id}', 42)"))
            .await
        {
            let _ = client.simple_query("ROLLBACK").await;
            failures.push(format!("node {idx}: in-txn INSERT errored: {e}"));
            continue;
        }

        // THE PROBE: the transaction's OWN read must observe its staged write.
        let seen = n_for(client, &id).await;

        // ROLLBACK before asserting so a miss never leaves the txn open and every
        // node's staging overlay is torn down regardless of outcome.
        client.simple_query("ROLLBACK").await.expect("ROLLBACK");

        if seen != Some(42) {
            failures.push(format!(
                "node {idx}: RYOW MISS — in-txn SELECT saw {seen:?}, expected Some(42)"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "CROSS-NODE in-txn read-your-own-writes is broken (ExecuteRequest carries \
         no txn_id, so the owning node stages/reads with txn_id=None): {failures:#?}"
    );

    cluster.shutdown().await;
}
