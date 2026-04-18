//! Planner visibility of secondary indexes.
//!
//! Two regression modes are guarded here:
//!
//! 1. **Planner doesn't pick the index** — silent full-scan regression
//!    (original reporter's bug). EXPLAIN must reference an index
//!    variant (`IndexedFetch`/`IndexLookup`) for equality on an
//!    indexed field.
//! 2. **Planner emits an `IndexedFetch` node with empty `filters` /
//!    empty `projection`** — the handler today ignores both
//!    (`_filters`, `_projection` in `execute_document_indexed_fetch`).
//!    The symptom-hiding Control-Plane compensators (post-filters /
//!    column trim after the handler returns) mask the missing handler
//!    work, but the right guard is plan-shape at the planner/Bridge
//!    boundary: the `IndexedFetch` node MUST carry the residual
//!    filters and the column projection so the handler can honor
//!    them without a Control-Plane round trip. EXPLAIN renders the
//!    `PhysicalPlan` via `{:?}`, so `filters: [..]` and
//!    `projection: [..]` appear verbatim in the plan text and are
//!    directly assertable.

use super::common::pgwire_harness::TestServer;
use super::helpers::{explain_lower, explain_raw};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_on_strict_document_used_by_planner() {
    let server = TestServer::start().await;

    // Exact scenario from the reporter's bug repro.
    server
        .exec(
            "CREATE COLLECTION events TYPE DOCUMENT STRICT (\
               id STRING PRIMARY KEY, \
               tenant_id STRING NOT NULL, \
               user_id STRING NOT NULL, \
               created_at TIMESTAMP)",
        )
        .await
        .unwrap();
    server
        .exec("CREATE INDEX idx_events_lookup ON events(tenant_id)")
        .await
        .unwrap();

    let plan = explain_lower(&server, "SELECT id FROM events WHERE tenant_id = 'acme'").await;

    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "plan for WHERE on indexed column must reference an index lookup, \
         got: {plan}"
    );
    assert!(
        !plan.contains("full scan") && !plan.contains("fulltablescan"),
        "plan must not full-scan when indexed column has a CREATE INDEX, \
         got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_index_on_schemaless_document_used_by_planner() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION idx_schemaless")
        .await
        .unwrap();
    server
        .exec("INSERT INTO idx_schemaless { id: 'a', role: 'admin' }")
        .await
        .unwrap();
    server
        .exec("CREATE INDEX ON idx_schemaless(role)")
        .await
        .unwrap();

    let plan = explain_lower(
        &server,
        "SELECT id FROM idx_schemaless WHERE role = 'admin'",
    )
    .await;

    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "schemaless CREATE INDEX must wire into the planner the same way as \
         a strict column index; got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_index_removes_planner_awareness() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_drop").await.unwrap();
    server
        .exec("CREATE INDEX idx_drop_role ON idx_drop(role)")
        .await
        .unwrap();
    server.exec("DROP INDEX idx_drop_role").await.unwrap();

    let plan = explain_lower(&server, "SELECT id FROM idx_drop WHERE role = 'admin'").await;

    assert!(
        !plan.contains("indexedfetch")
            && !plan.contains("indexlookup")
            && !plan.contains("index lookup")
            && !plan.contains("index_lookup"),
        "after DROP INDEX the planner must not pick an index path, got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_on_indexed_equality_uses_index() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_distinct").await.unwrap();
    server
        .exec("CREATE INDEX ON idx_distinct(email)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO idx_distinct { id: 'a', email: 'x@y.z' }")
        .await
        .unwrap();

    let plan = explain_lower(
        &server,
        "SELECT DISTINCT id FROM idx_distinct WHERE email = 'x@y.z'",
    )
    .await;
    assert!(
        plan.contains("indexedfetch")
            || plan.contains("indexlookup")
            || plan.contains("index lookup")
            || plan.contains("index_lookup"),
        "DISTINCT over an equality-on-indexed-field must still use the \
         index path; got plan: {plan}"
    );
}

// ───────────────────────── Issue #72: plan-shape for IndexedFetch ─────────────────────────
//
// The tests below assert the STRUCTURE of the emitted `IndexedFetch`
// node, not just its presence. EXPLAIN renders the physical plan via
// `{:?}`, so the fields on `DocumentOp::IndexedFetch` — `filters`,
// `projection`, `limit`, `offset` — appear verbatim in the plan text.
//
// Today the `execute_document_indexed_fetch` handler binds the
// `filters` and `projection` fields with a leading `_` prefix (i.e.
// accepts them but ignores them). The Control Plane compensates by
// applying filters and column trimming on the response payload after
// the Data Plane returns. That post-handler compensation is a
// classic symptom-hiding fallback: the plan reports an optimised
// index path but the actual work is a no-op index-only fetch
// followed by a full-payload scan-style post-filter. The plan-shape
// test pins the Bridge-level contract: filters and projection MUST
// travel with the `IndexedFetch` node so the handler can honor them.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_fetch_plan_carries_projection() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_proj").await.unwrap();
    server
        .exec("CREATE INDEX ON idx_proj(email)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO idx_proj { id: 'a', email: 'x@y.z', role: 'admin' }")
        .await
        .unwrap();

    // `SELECT id ...` is a single-column projection. The emitted
    // `IndexedFetch` plan node MUST carry `projection: ["id"]` so
    // the handler can skip materializing the full document body.
    //
    // Today's planner populates `proj_names` via
    // `extract_projection_names`, but the handler binds `_projection`
    // — a plan-shape test is the cheapest durable guard that the
    // projection field remains populated end-to-end. If a future
    // refactor drops the projection from the plan variant (e.g.
    // "the handler ignores it anyway, drop it"), this test catches
    // the regression before the handler fix lands.
    let plan = explain_raw(&server, "SELECT id FROM idx_proj WHERE email = 'x@y.z'").await;
    let lower = plan.to_lowercase();
    assert!(
        lower.contains("indexedfetch"),
        "expected IndexedFetch in plan, got: {plan}"
    );
    // Debug format of `Vec<String>` for `["id"]` is `["id"]`. An empty
    // projection vector renders as `[]` — the bug signature.
    assert!(
        plan.contains("projection: [\"id\"]"),
        "IndexedFetch plan must carry projection: [\"id\"], got: {plan}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_fetch_plan_carries_residual_filters() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION idx_resid").await.unwrap();
    server
        .exec("CREATE INDEX ON idx_resid(email)")
        .await
        .unwrap();
    server
        .exec(
            "INSERT INTO idx_resid \
             { id: 'a', email: 'x@y.z', created_at: 100 }",
        )
        .await
        .unwrap();

    // `WHERE email = '...' AND created_at > 50` — the equality lands
    // on the indexed field (drives the IndexedFetch), the `created_at`
    // clause is a RESIDUAL filter that must travel WITH the
    // IndexedFetch node (as `filters: [<non-empty bytes>]`), NOT as
    // a separate Filter plan node wrapping a bare IndexedFetch.
    //
    // The regression mode: planner lowers the residual into an
    // outer Filter, leaving `filters: []` on the IndexedFetch. The
    // handler-level post-filter plumbing is then irrelevant (empty
    // residual) and the Control Plane does the filtering instead.
    // That reproduces the issue #72 anti-pattern (filtering at the
    // SQL layer rather than in the Data Plane handler that already
    // has the row) and wastes a full-payload wire roundtrip.
    let plan = explain_raw(
        &server,
        "SELECT id FROM idx_resid \
         WHERE email = 'x@y.z' AND created_at > 50",
    )
    .await;
    let lower = plan.to_lowercase();
    assert!(
        lower.contains("indexedfetch"),
        "expected IndexedFetch in plan, got: {plan}"
    );
    // `filters: [` followed by `]` with bytes in between. Empty bytes
    // serialize to `filters: []`; non-empty to `filters: [N, M, ..]`.
    // Extract the filters slice and require it to be non-empty.
    let marker = "filters: [";
    let idx = plan
        .find(marker)
        .unwrap_or_else(|| panic!("no `filters:` field in plan: {plan}"));
    let tail = &plan[idx + marker.len()..];
    let close = tail
        .find(']')
        .unwrap_or_else(|| panic!("unterminated filters array in plan: {plan}"));
    let filters_body = tail[..close].trim();
    assert!(
        !filters_body.is_empty(),
        "IndexedFetch must carry residual `created_at > 50` in `filters`, \
         not delegate to an outer Filter node — the Control-Plane post-filter \
         compensation is the anti-pattern issue #72 is about. Got plan: {plan}"
    );
}
