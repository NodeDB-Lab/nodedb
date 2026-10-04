// SPDX-License-Identifier: BUSL-1.1

//! A `CREATE MATERIALIZED VIEW ... STREAMING` created over SQL must wire the
//! Event-Plane incremental aggregation: the DDL handler has to register a
//! streaming MV definition into `mv_registry` so that writes to the source
//! collection — fanned out through the MV's source change stream — drive an
//! O(1)-per-event incremental aggregate update.
//!
//! Read surface: streaming-MV aggregate state lives in the in-memory
//! `mv_registry` (per-group partial aggregate `MvState`), not in a batch
//! target collection like `REFRESH MATERIALIZED VIEW`. The harness exposes
//! `TestServer::shared`, so this test observes the aggregation directly on the
//! registry the Event Plane maintains. The default harness connection runs as
//! tenant id 1.

use std::time::Duration;

use nodedb::event::streaming_mv::state::AggRow;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::Value;

/// The default harness superuser (`nodedb`) is provisioned under tenant id 1.
const TENANT_ID: u64 = 1;

/// Streaming materialized view fed by a change stream must incrementally
/// aggregate source writes: two `active` orders and one `pending` order must
/// surface as per-group COUNT/SUM in the MV's live aggregate state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_incrementally_aggregates_source_writes() {
    let server = TestServer::start().await;

    // Base collection whose writes feed the change stream.
    server.exec("CREATE COLLECTION smv_orders").await.unwrap();

    // Change stream over the base collection. Streaming MVs source from a
    // change stream (registered in `stream_registry`), not from the base
    // collection directly. Registered via catalog post-apply on CREATE.
    server
        .exec("CREATE CHANGE STREAM smv_order_changes ON smv_orders")
        .await
        .unwrap();

    // Streaming MV: aggregate per `status` with COUNT(*) and SUM(amount),
    // sourced from the change stream via the FROM clause. The `ON smv_orders`
    // clause names the lineage collection so the handler's source-existence
    // check passes; `STREAMING` selects incremental refresh mode.
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_order_stats ON smv_orders STREAMING AS \
             SELECT status, COUNT(*) AS cnt, SUM(amount) AS total \
             FROM smv_order_changes GROUP BY status",
        )
        .await
        .unwrap();

    // Writes to the base collection produce WriteEvents that the Event Plane
    // fans out to the change stream and, from there, into every streaming MV
    // sourced from that stream.
    server
        .exec("INSERT INTO smv_orders { id: 'o1', status: 'active', amount: 10 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o2', status: 'active', amount: 20 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o3', status: 'pending', amount: 5 }")
        .await
        .unwrap();

    // The Event Plane consumes WriteEvents asynchronously. Poll the registry
    // until the MV state materializes both group keys (or time out). This is a
    // deterministic convergence poll, not a blind fixed sleep.
    let results = await_groups(&server, "smv_order_stats", 2).await;

    // A correctly-wired streaming MV registers its definition on CREATE and
    // then incrementally aggregates the three source writes into two groups.
    assert_eq!(
        results.len(),
        2,
        "streaming MV must aggregate source writes into two groups (active, pending); got {results:?}"
    );

    let active = results
        .iter()
        .find(|(k, _)| k == "active")
        .expect("`active` group must be present in streaming MV state");
    // Aggregate order matches the SELECT list: index 0 = COUNT(*), 1 = SUM(amount).
    assert_eq!(
        active.1[0].1,
        Value::Integer(2),
        "active COUNT(*) must be 2; got {active:?}"
    );
    assert_eq!(
        active.1[1].1,
        Value::Integer(30),
        "active SUM(amount) must be 10 + 20 = 30; got {active:?}"
    );

    let pending = results
        .iter()
        .find(|(k, _)| k == "pending")
        .expect("`pending` group must be present in streaming MV state");
    assert_eq!(
        pending.1[0].1,
        Value::Integer(1),
        "pending COUNT(*) must be 1; got {pending:?}"
    );
    assert_eq!(
        pending.1[1].1,
        Value::Integer(5),
        "pending SUM(amount) must be 5; got {pending:?}"
    );
}

/// Poll `view`'s state until it holds at least `groups` group keys, or time
/// out. The Event Plane consumes WriteEvents asynchronously, so this is a
/// convergence poll, not a blind fixed sleep.
async fn await_groups(server: &TestServer, view: &str, groups: usize) -> Vec<(String, AggRow)> {
    let mut results = Vec::new();
    for _ in 0..80 {
        if let Some(state) =
            server
                .shared
                .mv_registry
                .get_state(nodedb::types::DatabaseId::DEFAULT, TENANT_ID, view)
        {
            results = state.read_results().expect("view totals stay in range");
            if results.len() >= groups {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    results
}

/// Poll `view` until its `group` row's COUNT (aggregate 0) reaches `count`.
async fn await_count(server: &TestServer, view: &str, group: &str, count: i64) -> AggRow {
    let mut row = Vec::new();
    for _ in 0..80 {
        if let Some((_, found)) = await_groups(server, view, 1)
            .await
            .into_iter()
            .find(|(k, _)| k == group)
        {
            row = found;
            if row.first().map(|(_, v)| v) == Some(&Value::Integer(count)) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    row
}

/// The cells of the ad-hoc `sql` result's one row.
async fn ad_hoc_row(server: &TestServer, sql: &str) -> Vec<String> {
    let rows = server.query_rows(sql).await.unwrap();
    assert_eq!(rows.len(), 1, "{sql}: one row, got {rows:?}");
    rows[0].clone()
}

/// A view value as the text the ad-hoc query returns for it.
fn text(v: &Value) -> String {
    v.to_string()
}

/// A streaming view over integers above 2^53 and nanosecond timestamps holds
/// the same COUNT / SUM / MIN / MAX / AVG the ad-hoc query computes over the
/// source rows, including a SUM past `i64::MAX`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_integer_aggregates_equal_ad_hoc() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION smv_exact").await.unwrap();
    server
        .exec("CREATE CHANGE STREAM smv_exact_changes ON smv_exact")
        .await
        .unwrap();
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_exact_stats ON smv_exact STREAMING AS \
             SELECT g, COUNT(*) AS cnt, SUM(v) AS total, MIN(v) AS lo, MAX(v) AS hi, \
             AVG(v) AS mean FROM smv_exact_changes GROUP BY g",
        )
        .await
        .unwrap();

    let groups: [(&str, &[i64]); 2] = [
        (
            "big",
            &[9_007_199_254_740_993, 9_007_199_254_740_992, i64::MAX],
        ),
        (
            "nanos",
            &[
                1_700_000_000_000_000_002,
                1_700_000_000_000_000_001,
                1_700_000_000_000_000_003,
            ],
        ),
    ];
    for (g, values) in groups {
        for (i, v) in values.iter().enumerate() {
            server
                .exec(&format!(
                    "INSERT INTO smv_exact {{ id: '{g}{i}', g: '{g}', v: {v} }}"
                ))
                .await
                .unwrap();
        }
    }

    for (g, values) in groups {
        let view = await_count(&server, "smv_exact_stats", g, values.len() as i64).await;
        let ad_hoc = ad_hoc_row(
            &server,
            &format!(
                "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM smv_exact WHERE g = '{g}'"
            ),
        )
        .await;
        let cells: Vec<&Value> = view.iter().map(|(_, v)| v).collect();
        assert_eq!(cells.len(), 5, "{g}: view row {view:?}");
        for (i, name) in ["COUNT", "SUM", "MIN", "MAX"].iter().enumerate() {
            assert_eq!(
                text(cells[i]),
                ad_hoc[i],
                "{g}: view {name} must equal the ad-hoc {name}"
            );
        }
        let view_avg = cells[4].as_f64().expect("AVG is a float");
        let ad_hoc_avg: f64 = ad_hoc[4].parse().expect("ad-hoc AVG is numeric");
        assert_eq!(
            view_avg, ad_hoc_avg,
            "{g}: view AVG must equal the ad-hoc AVG"
        );
    }

    // The SUM past `i64::MAX` is exact, not rounded or wrapped.
    let big = await_count(&server, "smv_exact_stats", "big", 3).await;
    let exact: i128 = 9_007_199_254_740_993 + 9_007_199_254_740_992 + i128::from(i64::MAX);
    assert_eq!(text(&big[1].1), exact.to_string());
}

/// Install a mask on `collection`.`field` for a single role, exactly as the
/// metadata applier does when a policy replicates.
fn install_mask(server: &TestServer, collection: &str, field: &str) {
    let stored = nodedb::control::security::catalog::redaction::StoredRedactionPolicy {
        tenant_id: TENANT_ID,
        collection: collection.to_string(),
        display_collection: collection.to_string(),
        for_role: "support".to_string(),
        name: format!("mask_{collection}_{field}"),
        rules_json: format!(r#"[{{"field":"{field}","mode":{{"Mask":"***"}}}}]"#),
    };
    server
        .shared
        .redaction
        .install_replicated_policy(stored.to_runtime().expect("policy parses"));
}

/// A streaming MV keeps its group key and its aggregate state in storage the
/// result-path mask never reaches, so a definition over a protected column
/// would put that column's cleartext at rest — keyed by it, or accumulated from
/// it. The DDL must be refused before the definition is proposed, and the
/// refusal must stay per column: a rule elsewhere on the same collection cannot
/// block a view that never reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_over_a_redacted_column_is_refused() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION smv_pii").await.unwrap();
    server
        .exec("CREATE CHANGE STREAM smv_pii_changes ON smv_pii")
        .await
        .unwrap();
    install_mask(&server, "smv_pii", "amount");

    let grouped = server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_by_amount ON smv_pii STREAMING AS \
             SELECT amount, COUNT(*) AS cnt FROM smv_pii_changes GROUP BY amount",
        )
        .await
        .expect_err("grouping by a redacted column must be refused");
    assert!(
        grouped.contains("amount") && grouped.contains("smv_pii"),
        "the refusal must name the column and the collection; got {grouped}"
    );

    let aggregated = server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_total ON smv_pii STREAMING AS \
             SELECT status, SUM(amount) AS total FROM smv_pii_changes GROUP BY status",
        )
        .await
        .expect_err("aggregating a redacted column must be refused");
    assert!(
        aggregated.contains("amount"),
        "the refusal must name the aggregated column; got {aggregated}"
    );

    // A rule on `amount` must not block a view that groups by `status` and
    // counts events: nothing it persists comes from the protected column.
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_pii_by_status ON smv_pii STREAMING AS \
             SELECT status, COUNT(*) AS cnt FROM smv_pii_changes GROUP BY status",
        )
        .await
        .expect("a view reading no redacted column must still be created");
    assert!(
        server
            .shared
            .mv_registry
            .get_def(
                nodedb::types::DatabaseId::DEFAULT,
                TENANT_ID,
                "smv_pii_by_status"
            )
            .is_some(),
        "the allowed view must be registered"
    );
    assert!(
        server
            .shared
            .mv_registry
            .get_def(
                nodedb::types::DatabaseId::DEFAULT,
                TENANT_ID,
                "smv_pii_by_amount"
            )
            .is_none(),
        "a refused view must never be proposed to the catalog"
    );
}
