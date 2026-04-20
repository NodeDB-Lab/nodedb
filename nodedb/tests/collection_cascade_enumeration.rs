//! Integration-level coverage for the collection hard-delete cascade
//! enumerator. Sits on top of the `SystemCatalog` redb fixture from
//! `catalog_integrity_helpers` and exercises `collect_dependents`
//! end-to-end against every supported dependent kind.

mod catalog_integrity_helpers;

use std::collections::HashSet;

use nodedb::control::cascade::{
    DependentKind, change_streams::find_change_streams_on, collect_dependents,
    materialized_views::find_mvs_sourcing, orchestrator::dependents_error,
    rls::find_rls_policies_on, schedules::find_schedules_referencing,
    sequences::find_implicit_sequences, triggers::find_triggers_on,
};
use nodedb::control::security::catalog::rls::StoredRlsPolicy;
use nodedb::event::cdc::stream_def::ChangeStreamDef;

use catalog_integrity_helpers::{
    TENANT, make_catalog, make_mv_sourced, make_schedule, make_sequence, make_stream, make_trigger,
};

fn put_rls(catalog: &nodedb::control::security::catalog::SystemCatalog, name: &str, coll: &str) {
    let p = StoredRlsPolicy {
        tenant_id: TENANT,
        collection: coll.into(),
        name: name.into(),
        policy_type_tag: 0,
        legacy_predicate: Vec::new(),
        compiled_predicate_json: String::new(),
        mode_tag: 0,
        on_deny_json: String::new(),
        enabled: true,
        created_by: "admin".into(),
        created_at: 0,
    };
    catalog.put_rls_policy(&p).unwrap();
}

fn put_targeted_schedule(
    catalog: &nodedb::control::security::catalog::SystemCatalog,
    name: &str,
    target: &str,
) {
    let mut s = make_schedule(name);
    s.target_collection = Some(target.to_string());
    catalog.put_schedule(&s).unwrap();
}

fn put_targeted_stream(
    catalog: &nodedb::control::security::catalog::SystemCatalog,
    name: &str,
    target: &str,
) {
    let mut s: ChangeStreamDef = make_stream(name);
    s.collection = target.to_string();
    catalog.put_change_stream(&s).unwrap();
}

/// Build a realistic catalog with dependents spanning every kind and
/// assert `collect_dependents` returns them all — sorted, deduped,
/// tenant-scoped.
#[test]
fn orchestrator_enumerates_every_kind() {
    let (_tmp, catalog) = make_catalog();

    // Implicit sequences for `users.id`, `users.created_at`, plus one
    // unrelated sequence for a sibling collection.
    catalog
        .put_sequence(&make_sequence("users_id_seq"))
        .unwrap();
    catalog
        .put_sequence(&make_sequence("users_created_at_seq"))
        .unwrap();
    catalog
        .put_sequence(&make_sequence("orders_id_seq"))
        .unwrap();

    // Triggers — two on users, one on orders (ignored).
    catalog
        .put_trigger(&make_trigger("t_users_ins", "users"))
        .unwrap();
    catalog
        .put_trigger(&make_trigger("t_users_upd", "users"))
        .unwrap();
    catalog
        .put_trigger(&make_trigger("t_orders_ins", "orders"))
        .unwrap();

    // RLS — one on users, one on orders.
    put_rls(&catalog, "p_users_select", "users");
    put_rls(&catalog, "p_orders_select", "orders");

    // MV — a direct MV sourcing `users` and a chained MV sourcing the
    // first MV (tests the transitive walk).
    catalog
        .put_materialized_view(&make_mv_sourced("mv_users", "users"))
        .unwrap();
    catalog
        .put_materialized_view(&make_mv_sourced("mv_users_agg", "mv_users"))
        .unwrap();
    catalog
        .put_materialized_view(&make_mv_sourced("mv_unrelated", "orders"))
        .unwrap();

    // Change streams targeting users + an unrelated one on orders.
    put_targeted_stream(&catalog, "cs_users", "users");
    put_targeted_stream(&catalog, "cs_orders", "orders");

    // Schedules targeting users + an opaque-body one + an unrelated.
    put_targeted_schedule(&catalog, "sch_users_daily", "users");
    catalog.put_schedule(&make_schedule("sch_opaque")).unwrap();
    put_targeted_schedule(&catalog, "sch_orders_hourly", "orders");

    let mut visited = HashSet::new();
    let deps = collect_dependents(&catalog, TENANT, "users", &mut visited).unwrap();

    // Expect exactly the 8 user-scoped dependents, sorted by
    // (kind, name).
    let got: Vec<(DependentKind, &str)> = deps.iter().map(|d| (d.kind, d.name.as_str())).collect();
    assert_eq!(
        got,
        vec![
            (DependentKind::Sequence, "users_created_at_seq"),
            (DependentKind::Sequence, "users_id_seq"),
            (DependentKind::Trigger, "t_users_ins"),
            (DependentKind::Trigger, "t_users_upd"),
            (DependentKind::RlsPolicy, "p_users_select"),
            (DependentKind::MaterializedView, "mv_users"),
            (DependentKind::MaterializedView, "mv_users_agg"),
            (DependentKind::ChangeStream, "cs_users"),
            (DependentKind::Schedule, "sch_users_daily"),
        ],
        "cascade enumerator must return every user-scoped dependent, sorted"
    );
}

#[test]
fn per_kind_finders_match_orchestrator() {
    let (_tmp, catalog) = make_catalog();
    catalog
        .put_sequence(&make_sequence("books_id_seq"))
        .unwrap();
    catalog
        .put_trigger(&make_trigger("t_books_ins", "books"))
        .unwrap();
    put_rls(&catalog, "p_books", "books");
    catalog
        .put_materialized_view(&make_mv_sourced("mv_books", "books"))
        .unwrap();
    put_targeted_stream(&catalog, "cs_books", "books");
    put_targeted_schedule(&catalog, "sch_books", "books");

    assert_eq!(
        find_implicit_sequences(&catalog, TENANT, "books").unwrap(),
        vec!["books_id_seq"]
    );
    assert_eq!(
        find_triggers_on(&catalog, TENANT, "books").unwrap(),
        vec!["t_books_ins"]
    );
    assert_eq!(
        find_rls_policies_on(&catalog, TENANT, "books").unwrap(),
        vec!["p_books"]
    );
    assert_eq!(
        find_mvs_sourcing(&catalog, TENANT, "books").unwrap(),
        vec!["mv_books"]
    );
    assert_eq!(
        find_change_streams_on(&catalog, TENANT, "books").unwrap(),
        vec!["cs_books"]
    );
    assert_eq!(
        find_schedules_referencing(&catalog, TENANT, "books").unwrap(),
        vec!["sch_books"]
    );
}

#[test]
fn empty_catalog_has_no_dependents() {
    let (_tmp, catalog) = make_catalog();
    let mut visited = HashSet::new();
    let deps = collect_dependents(&catalog, TENANT, "anything", &mut visited).unwrap();
    assert!(deps.is_empty());
}

#[test]
fn visited_set_suppresses_duplicates_across_calls() {
    let (_tmp, catalog) = make_catalog();
    catalog
        .put_sequence(&make_sequence("users_id_seq"))
        .unwrap();

    let mut visited = HashSet::new();
    let first = collect_dependents(&catalog, TENANT, "users", &mut visited).unwrap();
    assert_eq!(first.len(), 1);

    // Second call with the same visited set emits nothing — the
    // sequence has already been reported to the caller.
    let second = collect_dependents(&catalog, TENANT, "users", &mut visited).unwrap();
    assert!(
        second.is_empty(),
        "orchestrator must not re-emit a dependent already in visited set"
    );
}

#[test]
fn cross_tenant_dependents_are_ignored() {
    let (_tmp, catalog) = make_catalog();

    // Tenant 1 (TENANT): one of each kind on "users".
    catalog
        .put_sequence(&make_sequence("users_id_seq"))
        .unwrap();
    catalog
        .put_trigger(&make_trigger("t_users", "users"))
        .unwrap();

    // Tenant 2 fixtures — identical names. These must not leak
    // into the cascade report for tenant 1.
    let mut other_seq = make_sequence("users_id_seq");
    other_seq.tenant_id = 2;
    catalog.put_sequence(&other_seq).unwrap();
    let mut other_trig = make_trigger("t_users", "users");
    other_trig.tenant_id = 2;
    catalog.put_trigger(&other_trig).unwrap();

    let mut visited = HashSet::new();
    let deps = collect_dependents(&catalog, TENANT, "users", &mut visited).unwrap();
    // Exactly the two tenant-1 dependents — not duplicates from tenant 2.
    assert_eq!(deps.len(), 2);
}

#[test]
fn mv_cycle_detection_returns_cascade_cycle_error() {
    // A simple 2-cycle converges via the `found` dedup set — the
    // real CascadeCycle trigger is an MV chain longer than the BFS
    // depth cap (`materialized_views::MAX_DEPTH = 32`). Synthesise
    // one by creating a fan-out where each level has a UNIQUE MV
    // sourcing the previous level's MV, so `found` never short-
    // circuits the walk.
    let (_tmp, catalog) = make_catalog();
    // mv_0 sources "root"; mv_1 sources mv_0; ... mv_40 sources mv_39.
    // Depth at which the walker looks for MVs sourcing mv_k is k+1,
    // so 40 links cleanly exceeds MAX_DEPTH (32).
    for i in 0..40 {
        let source = if i == 0 {
            "root".to_string()
        } else {
            format!("mv_{}", i - 1)
        };
        catalog
            .put_materialized_view(&make_mv_sourced(&format!("mv_{i}"), &source))
            .unwrap();
    }

    let res = find_mvs_sourcing(&catalog, TENANT, "root");
    match res {
        Ok(mvs) => panic!(
            "expected CascadeCycle, got Ok with {} mvs (depth cap should have tripped)",
            mvs.len()
        ),
        Err(nodedb::Error::CascadeCycle { root, depth, .. }) => {
            assert_eq!(root, "root");
            assert_eq!(depth, 32);
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn dependents_error_variant_carries_structured_payload() {
    let (_tmp, catalog) = make_catalog();
    catalog.put_sequence(&make_sequence("logs_id_seq")).unwrap();
    catalog
        .put_trigger(&make_trigger("t_logs", "logs"))
        .unwrap();

    let mut visited = HashSet::new();
    let deps = collect_dependents(&catalog, TENANT, "logs", &mut visited).unwrap();
    let err = dependents_error(TENANT, "logs", &deps);
    match err {
        nodedb::Error::DependentObjectsExist {
            tenant_id,
            root_kind,
            root_name,
            dependent_count,
            dependents,
        } => {
            assert_eq!(tenant_id, TENANT);
            assert_eq!(root_kind, "collection");
            assert_eq!(root_name, "logs");
            assert_eq!(dependent_count, 2);
            assert!(
                dependents
                    .iter()
                    .any(|(k, n)| k == "trigger" && n == "t_logs")
            );
            assert!(
                dependents
                    .iter()
                    .any(|(k, n)| k == "sequence" && n == "logs_id_seq")
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
