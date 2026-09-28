// SPDX-License-Identifier: BUSL-1.1

//! A row bound the planner cannot resolve must be rejected, never dropped.
//!
//! `LIMIT` and `OFFSET` values reach the planner as `sqlparser::ast::Expr`.
//! `crate::coerce::checked_row_bound` resolves each one to a
//! `crate::coerce::RowBound`: `Rows(n)` for a literal in
//! `[0, usize::MAX]`, `Unbounded` for `NULL` or `ALL` (PostgreSQL reads
//! both as no bound at all), or `Err` for anything else — negative,
//! fractional, wider than `usize`, or non-numeric text. `Err` fails the
//! statement with SQLSTATE `2201W` (`invalid_limit_value`), matching
//! PostgreSQL.
//!
//! Each rejection test asserts the failure AND that no plan came back
//! carrying a permissive default, so a regression to silent widening
//! fails here rather than returning a plan that quietly reads everything.
//! The unbounded tests pin the `NULL` / `ALL` reading against the same
//! regression from the other direction: rejecting them outright breaks
//! a prepared `LIMIT $1`, whose Parse-time schema inference plans
//! `LIMIT NULL` to derive the row description (see
//! `fn prepared_limit_placeholder_null_plans_cleanly` below).

use nodedb_sql::types::{CollectionInfo, CtePlan, DocumentIndexLookupPlan, EngineType, SqlPlan};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::DatabaseId;

/// A declared TEXT column, for the stub catalog below.
fn text_column(name: &str, is_primary_key: bool) -> nodedb_sql::types::ColumnInfo {
    nodedb_sql::types::ColumnInfo {
        name: name.into(),
        data_type: nodedb_sql::types::SqlDataType::String,
        nullable: !is_primary_key,
        is_primary_key,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "articles" | "authors" => Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentStrict,
                columns: vec![text_column("id", true), text_column("name", false)],
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
            }),
            _ => None,
        };
        Ok(info)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

/// The row bound a plan ended up carrying, for the failure message when a
/// query that must be rejected is planned anyway.
fn bound_of(plan: &SqlPlan) -> String {
    match plan {
        SqlPlan::Scan { limit, offset, .. }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { limit, offset, .. })
        | SqlPlan::Subquery { limit, offset, .. } => format!("limit={limit:?} offset={offset}"),
        SqlPlan::Cte(CtePlan { outer, .. }) => format!("cte outer -> {}", bound_of(outer)),
        other => format!("{other:?}"),
    }
}

/// Assert the statement is rejected at plan time.
///
/// The guard is the point: an accepted plan is reported with the bound it
/// carries, so the silent-widening failure mode (`limit=None`, `offset=0`)
/// is named in the failure output instead of showing up as a missing error.
fn expect_rejected(sql: &str) {
    match plan_sql(sql, &Catalog) {
        Err(_) => {}
        Ok(plans) => {
            let bounds: Vec<String> = plans.iter().map(bound_of).collect();
            panic!("must reject an out-of-domain row bound, planned instead: {sql} -> {bounds:?}");
        }
    }
}

/// The single plan for a statement that must plan cleanly.
fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).expect("planning must succeed");
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().expect("one plan")
}

// ---------------------------------------------------------------------------
// LIMIT
// ---------------------------------------------------------------------------

/// The reported symptom: a negative LIMIT literal.
#[test]
fn negative_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT -1");
}

/// The untyped-parameter spelling of the same bound. A pgwire driver that
/// sends `Type::UNKNOWN` binds the value as a single-quoted string, so
/// `LIMIT $1` with `-1` reaches the planner as `LIMIT '-1'`.
#[test]
fn negative_limit_as_unknown_param_text_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT '-1'");
}

/// Non-numeric text in a LIMIT is not a bound at all.
#[test]
fn non_numeric_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 'abc'");
}

/// A fractional LIMIT has no `usize` reading.
#[test]
fn fractional_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 1.5");
}

/// A LIMIT wider than `usize` cannot be applied and must not be dropped.
#[test]
fn overflowing_limit_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 99999999999999999999999999");
}

// ---------------------------------------------------------------------------
// OFFSET
// ---------------------------------------------------------------------------

/// A negative OFFSET is out of the `usize` domain and fails the statement.
#[test]
fn negative_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET -2");
}

/// The untyped-parameter spelling of a negative OFFSET.
#[test]
fn negative_offset_as_unknown_param_text_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET '-2'");
}

/// Non-numeric text in an OFFSET is not a bound at all.
#[test]
fn non_numeric_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET 'abc'");
}

/// An OFFSET wider than `usize` cannot be applied and must not be dropped.
#[test]
fn overflowing_offset_is_rejected() {
    expect_rejected("SELECT * FROM articles OFFSET 99999999999999999999999999");
}

// ---------------------------------------------------------------------------
// Nested query shapes — the same extractor, reached through other planners
// ---------------------------------------------------------------------------

/// A derived table carries its own LIMIT clause through the same extractor.
#[test]
fn negative_limit_in_derived_table_is_rejected() {
    expect_rejected("SELECT * FROM (SELECT * FROM articles LIMIT -1) s");
}

/// A CTE body carries its own LIMIT clause through the same extractor.
#[test]
fn negative_limit_in_cte_body_is_rejected() {
    expect_rejected("WITH c AS (SELECT * FROM articles LIMIT -1) SELECT * FROM c");
}

/// `plan_lateral` reads the inner LIMIT with its own copy of the extractor
/// (`limit_from_query`), so it needs its own coverage.
#[test]
fn negative_limit_in_lateral_subquery_is_rejected() {
    expect_rejected(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id LIMIT -1) e ON true",
    );
}

/// `SqlPlan::LateralTopK` carries no offset field, so a nonzero inner
/// OFFSET has nowhere to plan to. Reject rather than drop it.
#[test]
fn nonzero_offset_in_lateral_subquery_with_limit_is_rejected() {
    expect_rejected(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id \
         ORDER BY id LIMIT 3 OFFSET 2) e ON true",
    );
}

/// An inner OFFSET with no LIMIT takes the equi-hash-join branch of
/// `plan_lateral_join`, not the top-k branch. The same rejection applies.
#[test]
fn nonzero_offset_in_lateral_subquery_without_limit_is_rejected() {
    expect_rejected(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id OFFSET 2) e ON true",
    );
}

/// A negative inner OFFSET fails inside `checked_row_bound`, the same as a
/// negative OFFSET anywhere else, before the LATERAL-specific check runs.
#[test]
fn negative_offset_in_lateral_subquery_is_rejected() {
    expect_rejected(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id \
         ORDER BY id LIMIT 3 OFFSET -2) e ON true",
    );
}

/// `OFFSET 0` skips nothing. It is satisfiable and must still plan — the
/// control proving the check reads the resolved value, not the clause's
/// presence.
#[test]
fn zero_offset_in_lateral_subquery_plans_cleanly() {
    let _ = plan_one(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id \
         ORDER BY id LIMIT 3 OFFSET 0) e ON true",
    );
}

/// A LATERAL subquery with LIMIT and no OFFSET plans cleanly — the control
/// showing the rejection targets OFFSET, not LIMIT.
#[test]
fn lateral_subquery_with_limit_and_no_offset_plans_cleanly() {
    let _ = plan_one(
        "SELECT a.id, e.id FROM authors a \
         JOIN LATERAL (SELECT id FROM articles WHERE articles.id = a.id \
         ORDER BY id LIMIT 3) e ON true",
    );
}

// ---------------------------------------------------------------------------
// Controls — valid bounds keep working
// ---------------------------------------------------------------------------

/// `LIMIT 0` is a valid bound meaning "no rows", not a missing bound.
#[test]
fn zero_limit_is_honored() {
    match plan_one("SELECT * FROM articles LIMIT 0") {
        SqlPlan::Scan { limit, .. } => assert_eq!(limit, Some(0)),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// A plain positive bound still plans.
#[test]
fn positive_limit_and_offset_are_honored() {
    match plan_one("SELECT * FROM articles LIMIT 5 OFFSET 2") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, Some(5));
            assert_eq!(offset, 2);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// No clause at all stays unbounded — the one input for which `None` is the
/// correct answer.
#[test]
fn absent_limit_stays_unbounded() {
    match plan_one("SELECT * FROM articles") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, None);
            assert_eq!(offset, 0);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// `LIMIT NULL` is PostgreSQL's other spelling of "no LIMIT clause" and
/// must plan the same as an absent clause, not fail.
#[test]
fn null_limit_stays_unbounded() {
    match plan_one("SELECT * FROM articles LIMIT NULL") {
        SqlPlan::Scan { limit, .. } => assert_eq!(limit, None),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// `OFFSET NULL` is the same as omitting OFFSET — skip nothing.
#[test]
fn null_offset_stays_unbounded() {
    match plan_one("SELECT * FROM articles OFFSET NULL") {
        SqlPlan::Scan { offset, .. } => assert_eq!(offset, 0),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// `LIMIT NULL` combined with a real OFFSET applies the OFFSET and leaves
/// LIMIT unbounded — the two clauses resolve independently.
#[test]
fn null_limit_with_offset_is_honored() {
    match plan_one("SELECT * FROM articles LIMIT NULL OFFSET 2") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, None);
            assert_eq!(offset, 2);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// The exact shape `substitute_placeholders_with_null` produces for a
/// prepared `SELECT id FROM t ORDER BY id LIMIT $1`: the pgwire Parse-time
/// schema inference path rewrites `$1` to the literal `NULL` before
/// planning, to derive the row description ahead of Bind/Execute. Rejecting
/// `LIMIT NULL` breaks every prepared statement with a LIMIT placeholder —
/// describe fails, the client gets an empty column list, and Execute then
/// dies with a field-count mismatch. This test is the only coverage of
/// that path in `nodedb-sql`; deleting it removes the only signal that a
/// future change to `checked_row_bound` regresses prepared LIMIT again.
#[test]
fn prepared_limit_placeholder_null_plans_cleanly() {
    let _ = plan_one("SELECT id FROM articles ORDER BY id LIMIT NULL");
}

// ---------------------------------------------------------------------------
// FETCH FIRST / NEXT — a LIMIT synonym, honored the same way
// ---------------------------------------------------------------------------

/// `FETCH FIRST n ROWS ONLY` is a LIMIT synonym and must bound the scan.
#[test]
fn fetch_first_n_rows_only_is_honored() {
    match plan_one("SELECT * FROM articles FETCH FIRST 2 ROWS ONLY") {
        SqlPlan::Scan { limit, .. } => assert_eq!(limit, Some(2)),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// `FETCH FIRST ROW ONLY` with no count means one row (standard SQL).
#[test]
fn fetch_first_row_only_with_no_count_is_one_row() {
    match plan_one("SELECT * FROM articles FETCH FIRST ROW ONLY") {
        SqlPlan::Scan { limit, .. } => assert_eq!(limit, Some(1)),
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// `OFFSET ... FETCH FIRST ... ROWS ONLY` applies both bounds.
#[test]
fn offset_and_fetch_first_are_both_honored() {
    match plan_one("SELECT * FROM articles OFFSET 2 FETCH FIRST 2 ROWS ONLY") {
        SqlPlan::Scan { limit, offset, .. } => {
            assert_eq!(limit, Some(2));
            assert_eq!(offset, 2);
        }
        other => panic!("expected a Scan, got: {other:?}"),
    }
}

/// Non-numeric text in a FETCH FIRST count is not a bound at all.
#[test]
fn non_numeric_fetch_first_is_rejected() {
    expect_rejected("SELECT * FROM articles FETCH FIRST 'abc' ROWS ONLY");
}

/// `WITH TIES` needs a plan primitive `SqlPlan` does not have — reject
/// rather than silently drop the modifier and apply a plain LIMIT.
#[test]
fn fetch_first_with_ties_is_rejected() {
    expect_rejected("SELECT * FROM articles ORDER BY id FETCH FIRST 2 ROWS WITH TIES");
}

/// PostgreSQL accepts only one spelling of the row-bound clause; a query
/// with both is rejected rather than silently picking one.
#[test]
fn limit_combined_with_fetch_first_is_rejected() {
    expect_rejected("SELECT * FROM articles LIMIT 2 FETCH FIRST 2 ROWS ONLY");
}

// ---------------------------------------------------------------------------
// UPDATE / DELETE ... LIMIT — a MySQL extension PostgreSQL does not have
// ---------------------------------------------------------------------------

/// `UPDATE ... LIMIT` is a MySQL extension with no PostgreSQL equivalent.
/// Honoring it needs a new cross-engine capability; reject instead.
#[test]
fn update_limit_is_rejected() {
    expect_rejected("UPDATE articles SET name = 'x' WHERE id = '1' LIMIT 1");
}

/// `DELETE ... LIMIT` is the same MySQL extension on the DELETE side.
#[test]
fn delete_limit_is_rejected() {
    expect_rejected("DELETE FROM articles WHERE id = '1' LIMIT 1");
}

/// Control: `UPDATE` without a LIMIT still plans — the guard rejects only
/// the LIMIT clause, not every UPDATE.
#[test]
fn update_without_limit_plans() {
    let _ = plan_one("UPDATE articles SET name = 'x' WHERE id = '1'");
}

/// Control: `DELETE` without a LIMIT still plans.
#[test]
fn delete_without_limit_plans() {
    let _ = plan_one("DELETE FROM articles WHERE id = '1'");
}
