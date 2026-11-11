// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the router claims and the arm prefix table.

use super::*;

/// The prefixes [`guarded_arm_claims`] reads with an argument check.
const GUARDED_PREFIXES: &[&str] = &["SELECT LAST_VALUES(", "SELECT LAST_VALUE(", "SEARCH "];

/// The source of every string arm cluster, by file name.
const ARM_SOURCES: &[(&str, &str)] = &[
    ("string_admin.rs", include_str!("string_admin.rs")),
    ("string_engine_ops.rs", include_str!("string_engine_ops.rs")),
    (
        "string_introspection.rs",
        include_str!("string_introspection.rs"),
    ),
    ("string_schema.rs", include_str!("string_schema.rs")),
    ("string_streaming.rs", include_str!("string_streaming.rs")),
    ("string_versioning.rs", include_str!("string_versioning.rs")),
];

/// Every literal an arm compares `upper` against, before the test module.
fn arm_literals(source: &str) -> Vec<String> {
    let body = source.split("#[cfg(test)]").next().unwrap_or(source);
    let mut literals = Vec::new();
    for marker in ["upper.starts_with(\"", "upper == \""] {
        let mut rest = body;
        while let Some(at) = rest.find(marker) {
            rest = &rest[at + marker.len()..];
            let Some(end) = rest.find('"') else {
                break;
            };
            literals.push(rest[..end].replace("\\\\", "\\"));
            rest = &rest[end..];
        }
    }
    literals
}

#[test]
fn the_table_covers_every_string_arm_prefix() {
    let mut seen = 0;
    for (file, source) in ARM_SOURCES {
        for literal in arm_literals(source) {
            seen += 1;
            let covered = ARM_PREFIXES
                .iter()
                .any(|prefix| literal.starts_with(prefix))
                || GUARDED_PREFIXES.contains(&literal.as_str());
            assert!(
                covered,
                "{file}: the arm prefix {literal:?} is missing from ARM_PREFIXES"
            );
        }
    }
    // A renamed `upper` binding hides every arm from the scan.
    assert!(seen > 200, "the scan found only {seen} arm prefixes");
}

/// The router reads the shared parts, and keeps no recognizer of its own.
#[test]
fn the_router_reads_the_shared_predicate() {
    let router = include_str!("dispatch.rs");
    let router = router.split("#[cfg(test)]").next().unwrap_or(router);
    for call in ["string_arm_claims(sql, &upper)", "untyped_claim(sql)"] {
        assert!(router.contains(call), "the router must call {call}");
    }
    for copy in [
        "fn is_copy_from",
        "fn is_object_literal_write",
        "fn is_upsert_values",
        "starts_with(\"",
    ] {
        assert!(
            !router.contains(copy),
            "the router keeps its own recognizer: {copy}"
        );
    }
    let planner = include_str!("../../../../../planner/context/query/planning_request.rs");
    assert!(
        planner.contains("router::claims(statement)"),
        "the request planner must read the router's predicate"
    );
}

#[test]
fn claims_matches_the_router_decision() {
    for statement in [
        "SELECT KV_INCR('counter', 1)",
        "select kv_incr ('counter', 1)",
        "  \n\tSELECT TOPK('scores', 3)",
        "SELECT * FROM TOPK('scores', 3)",
        "SELECT TRANSFER('wallets', 'a', 'b', 'balance', 10)",
        "SELECT RATE_CHECK('login', 'u1', 5, 60)",
        "REFRESH MATERIALIZED VIEW totals",
        "SELECT LAST_VALUE('metrics', 7)",
        "SELECT * FROM RANGE('scores', 1, 9)",
        "SELECT * FROM docs AT VERSION 'c1'",
        "-- a note\nSELECT * FROM docs AT VERSION 'c1'",
        "UPSERT INTO docs (v) VALUES ('a')",
        "upsert into docs { id: 'a' }",
        "INSERT INTO docs { v: 'a' }",
        "/* note */ INSERT INTO docs { v: 'a' }",
        "COPY docs FROM STDIN",
        "CREATE COLLECTION extra",
        "CONVERT COLLECTION events TO kv",
    ] {
        assert!(claims(statement), "the router runs {statement:?}");
    }
    for statement in [
        "SELECT id FROM docs",
        "INSERT INTO docs (id, v) VALUES ('a', 'b')",
        "INSERT INTO docs (body) VALUES ('{ }')",
        "UPSERT INTO docs SELECT 'VALUES' AS id",
        "UPDATE docs SET v = 'b' WHERE id = 'a'",
        "DELETE FROM docs WHERE id = 'a'",
        "SELECT 'KV_INCR(' FROM docs",
        "SELECT LAST_VALUE(v) OVER (ORDER BY ts) FROM metrics",
        "SELECT * FROM docs WHERE note = 'AT VERSION 3'",
        // The string arms read the raw prefix, so a comment hides it.
        "-- a note\nSELECT KV_INCR('counter', 1)",
        "TRUNCATE docs",
    ] {
        assert!(!claims(statement), "the router plans {statement:?}");
    }
}

#[test]
fn a_plain_insert_and_select_pair_is_not_claimed() {
    for statement in [
        "INSERT INTO docs (id, v) VALUES ('a', 'b')",
        "SELECT id, v FROM docs WHERE id = 'a'",
    ] {
        assert!(!claims(statement), "{statement}");
        assert_eq!(untyped_claim(statement), None, "{statement}");
    }
}

#[test]
fn the_untyped_arm_follows_the_router_order() {
    for (statement, arm) in [
        ("COPY orders FROM STDIN", UntypedClaim::CopyFrom),
        (
            "INSERT INTO docs { id: 'a' }",
            UntypedClaim::ObjectLiteralInsert,
        ),
        (
            "UPSERT INTO docs (id, n) VALUES ('a', 1)",
            UntypedClaim::Upsert,
        ),
        ("upsert into docs {id: 'a'}", UntypedClaim::Upsert),
    ] {
        assert_eq!(untyped_claim(statement), Some(arm), "{statement}");
    }
}

#[test]
fn only_a_possible_write_or_copy_is_tokenized() {
    assert!(may_lead_with_write_or_copy("INSERT INTO T { ID: 1 }"));
    assert!(may_lead_with_write_or_copy(
        "  UPSERT INTO T (A) VALUES (1)"
    ));
    assert!(may_lead_with_write_or_copy("COPY T FROM STDIN"));
    assert!(may_lead_with_write_or_copy("copy t from stdin"));
    assert!(may_lead_with_write_or_copy(
        "-- NOTE\nINSERT INTO T { ID: 1 }"
    ));
    assert!(may_lead_with_write_or_copy("/* NOTE */ COPY T FROM STDIN"));
    assert!(!may_lead_with_write_or_copy("SELECT * FROM T"));
    assert!(!may_lead_with_write_or_copy("INSERTED"));
    assert!(!may_lead_with_write_or_copy("COPYRIGHT"));
    assert!(!may_lead_with_write_or_copy(""));
}

#[test]
fn copy_from_is_routed_on_the_clause_after_the_collection() {
    assert!(is_copy_from(&RouteTokens::new("COPY orders FROM STDIN")));
    assert!(!is_copy_from(&RouteTokens::new(
        "COPY (SELECT 'x FROM y') TO STDOUT"
    )));
}

#[test]
fn object_literal_writes_are_routed_on_the_brace_after_the_collection() {
    let insert = RouteTokens::new("INSERT INTO docs { id: 'a' }");
    assert!(is_object_literal_write(&insert, "INSERT"));
    assert!(!is_object_literal_write(&insert, "UPSERT"));
    // A brace inside a literal is data.
    assert!(!is_object_literal_write(
        &RouteTokens::new("INSERT INTO docs (body) VALUES ('{ }')"),
        "INSERT"
    ));
    assert!(is_object_literal_write(
        &RouteTokens::new("upsert into docs {id: 'a'}"),
        "UPSERT"
    ));
}

#[test]
fn upsert_values_is_routed_on_a_top_level_values_clause() {
    assert!(is_upsert_values(&RouteTokens::new(
        "UPSERT INTO docs (id, n) VALUES ('a', 1)"
    )));
    // `VALUES` inside a string literal is data.
    assert!(!is_upsert_values(&RouteTokens::new(
        "UPSERT INTO docs SELECT 'VALUES' AS id"
    )));
}
