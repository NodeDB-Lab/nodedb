// SPDX-License-Identifier: Apache-2.0

//! Integration tests: SQL constraint keywords in CREATE TABLE / CREATE COLLECTION
//! are rejected with `SqlError::UnsupportedConstraint` (SQLSTATE 0A000).
//!
//! Tests cover:
//! - Table-level constraint clauses (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY, CONSTRAINT)
//! - Inline column constraints (id INT PRIMARY KEY, id INT UNIQUE, etc.)
//! - Valid cases that must succeed (NOT NULL, DEFAULT, plain columns, engine override)
//! - CREATE COLLECTION must still succeed (regression pin)
//! - CREATE TABLE with no columns must fail with 42601

use nodedb_sql::SqlError;
use nodedb_sql::ddl_ast::parse as ddl_parse;

fn parse_ok(sql: &str) -> nodedb_sql::ddl_ast::NodedbStatement {
    ddl_parse(sql)
        .unwrap_or_else(|| panic!("expected Some from parse, got None for: {sql}"))
        .unwrap_or_else(|e| panic!("expected Ok from parse, got Err({e}) for: {sql}"))
}

fn parse_constraint_err(sql: &str) -> (String, String) {
    match ddl_parse(sql) {
        Some(Err(SqlError::UnsupportedConstraint { feature, hint })) => (feature, hint),
        Some(Err(other)) => panic!("expected UnsupportedConstraint, got {other:?} for: {sql}"),
        Some(Ok(stmt)) => {
            panic!("expected Err(UnsupportedConstraint), got Ok({stmt:?}) for: {sql}")
        }
        None => panic!("expected Some, got None for: {sql}"),
    }
}

// ── Table-level PRIMARY KEY ───────────────────────────────────────────────────

#[test]
fn table_level_primary_key_rejected() {
    let (feature, hint) = parse_constraint_err("CREATE TABLE x (id INT, PRIMARY KEY (id))");
    assert!(
        feature.contains("PRIMARY KEY"),
        "feature should name PRIMARY KEY, got: {feature}"
    );
    // Table-level form hints at the inline form, not a UNIQUE index.
    assert!(
        hint.contains("inline") || hint.contains("PRIMARY KEY"),
        "hint should point at inline form, got: {hint}"
    );
}

// ── Inline PRIMARY KEY ────────────────────────────────────────────────────────
// The inline form (`id INT PRIMARY KEY`) is wired through parse_column_type_str_full
// which sets is_pk=true on the column. It must parse successfully.

#[test]
fn inline_primary_key_accepted() {
    use nodedb_sql::ddl_ast::NodedbStatement;
    let stmt = parse_ok("CREATE TABLE x (id INT PRIMARY KEY)");
    use nodedb_sql::ddl_ast::CollectionStmt;
    if let NodedbStatement::Collection(CollectionStmt::CreateTable { columns, .. }) = stmt {
        assert_eq!(columns.len(), 1, "expected 1 column");
        let (name, type_str) = &columns[0];
        assert_eq!(name, "id", "column name should be 'id'");
        // The type string must carry the PRIMARY KEY marker so downstream
        // parse_column_type_str_full can extract is_pk = true.
        let upper = type_str.to_uppercase();
        assert!(
            upper.contains("PRIMARY"),
            "type_str should retain PRIMARY KEY marker, got: {type_str}"
        );
    } else {
        panic!("expected CreateTable, got: {stmt:?}");
    }
}

// ── Inline UNIQUE ─────────────────────────────────────────────────────────────

#[test]
fn inline_unique_rejected() {
    let (feature, hint) = parse_constraint_err("CREATE TABLE x (id INT UNIQUE)");
    assert!(
        feature.contains("UNIQUE"),
        "feature should name UNIQUE, got: {feature}"
    );
    assert!(
        hint.contains("UNIQUE"),
        "hint should mention UNIQUE index, got: {hint}"
    );
}

// ── Table-level UNIQUE ────────────────────────────────────────────────────────

#[test]
fn table_level_unique_rejected() {
    let (feature, hint) = parse_constraint_err("CREATE TABLE x (id INT, UNIQUE (id))");
    assert!(
        feature.contains("UNIQUE"),
        "feature should name UNIQUE, got: {feature}"
    );
    assert!(
        hint.contains("UNIQUE"),
        "hint should mention UNIQUE index, got: {hint}"
    );
}

// ── Inline CHECK ──────────────────────────────────────────────────────────────

#[test]
fn inline_check_rejected() {
    let (feature, hint) = parse_constraint_err("CREATE TABLE x (id INT CHECK (id > 0))");
    assert!(
        feature.contains("CHECK"),
        "feature should name CHECK, got: {feature}"
    );
    assert!(
        hint.contains("application"),
        "hint should mention application code, got: {hint}"
    );
}

// ── Inline REFERENCES (FK shorthand) ─────────────────────────────────────────

#[test]
fn inline_references_rejected() {
    let (feature, hint) = parse_constraint_err("CREATE TABLE x (id INT REFERENCES other(id))");
    assert!(
        feature.to_uppercase().contains("REFERENCES") || feature.contains("FOREIGN"),
        "feature should name REFERENCES or FOREIGN KEY, got: {feature}"
    );
    assert!(
        hint.contains("application"),
        "hint should mention application code, got: {hint}"
    );
}

// ── FOREIGN KEY table-level ───────────────────────────────────────────────────

#[test]
fn table_level_foreign_key_rejected() {
    let (feature, hint) =
        parse_constraint_err("CREATE TABLE x (id INT, FOREIGN KEY (id) REFERENCES other(id))");
    assert!(
        feature.contains("FOREIGN"),
        "feature should name FOREIGN KEY, got: {feature}"
    );
    assert!(
        hint.contains("application"),
        "hint should mention application code, got: {hint}"
    );
}

// ── Named CONSTRAINT (PRIMARY KEY form) ──────────────────────────────────────

#[test]
fn named_constraint_primary_key_rejected() {
    let (feature, hint) =
        parse_constraint_err("CREATE TABLE x (id INT, CONSTRAINT pk PRIMARY KEY (id))");
    assert!(
        feature.contains("PRIMARY") || feature.contains("CONSTRAINT"),
        "feature should name PRIMARY KEY or CONSTRAINT, got: {feature}"
    );
    assert!(
        hint.contains("inline") || hint.contains("PRIMARY KEY") || hint.contains("NodeDB"),
        "hint should point at inline form or NodeDB enforcement, got: {hint}"
    );
}

// ── Valid: plain column, succeeds ─────────────────────────────────────────────

#[test]
fn plain_columns_succeed() {
    let stmt = parse_ok("CREATE TABLE x (id INT)");
    assert!(
        matches!(
            stmt,
            nodedb_sql::ddl_ast::NodedbStatement::Collection(
                nodedb_sql::ddl_ast::CollectionStmt::CreateTable { .. }
            )
        ),
        "expected CreateTable statement"
    );
}

// ── Valid: engine override, succeeds ─────────────────────────────────────────

#[test]
fn engine_override_succeeds() {
    let stmt = parse_ok("CREATE TABLE x (id INT) WITH (engine='kv')");
    assert!(
        matches!(
            stmt,
            nodedb_sql::ddl_ast::NodedbStatement::Collection(
                nodedb_sql::ddl_ast::CollectionStmt::CreateTable { .. }
            )
        ),
        "expected CreateTable statement"
    );
}

// ── Valid: NOT NULL and DEFAULT accepted ──────────────────────────────────────

#[test]
fn not_null_and_default_accepted() {
    let stmt = parse_ok("CREATE TABLE x (id INT NOT NULL DEFAULT 0)");
    if let nodedb_sql::ddl_ast::NodedbStatement::Collection(
        nodedb_sql::ddl_ast::CollectionStmt::CreateTable { columns, .. },
    ) = stmt
    {
        assert_eq!(columns.len(), 1, "expected 1 column");
        let (name, type_str) = &columns[0];
        assert_eq!(name, "id");
        // The type string should not contain the constraint keywords.
        let upper = type_str.to_uppercase();
        assert!(
            !upper.contains("PRIMARY"),
            "type_str should not contain PRIMARY, got: {type_str}"
        );
        assert!(
            !upper.contains("UNIQUE"),
            "type_str should not contain UNIQUE, got: {type_str}"
        );
    } else {
        panic!("expected CreateTable");
    }
}

// ── ENGINE = <name> trailing suffix (E1 regression) ──────────────────────────
//
// A MySQL-style trailing `ENGINE = <name>` (or `ENGINE=<name>`) after the
// column list must feed the same `engine` field as `WITH (engine='<name>')`,
// so both forms resolve to byte-identical `CollectionStmt::CreateCollection`
// / `CreateTable` `engine` values.

fn engine_field(sql: &str) -> Option<String> {
    match parse_ok(sql) {
        nodedb_sql::ddl_ast::NodedbStatement::Collection(
            nodedb_sql::ddl_ast::CollectionStmt::CreateTable { engine, .. },
        ) => engine,
        nodedb_sql::ddl_ast::NodedbStatement::Collection(
            nodedb_sql::ddl_ast::CollectionStmt::CreateCollection { engine, .. },
        ) => engine,
        other => panic!("expected CreateTable/CreateCollection, got {other:?} for: {sql}"),
    }
}

#[test]
fn engine_suffix_matches_with_clause() {
    let suffix = engine_field("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE = timeseries");
    let with_clause =
        engine_field("CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='timeseries')");
    assert_eq!(suffix, Some("timeseries".to_string()));
    assert_eq!(suffix, with_clause);
}

#[test]
fn engine_suffix_no_spaces_around_equals() {
    let engine = engine_field("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE=columnar");
    assert_eq!(engine, Some("columnar".to_string()));
}

#[test]
fn engine_suffix_survives_nested_paren_column_types() {
    let stmt = parse_ok(
        "CREATE COLLECTION t (id INT PRIMARY KEY, v VECTOR(3), amt NUMERIC(10,2)) ENGINE = kv",
    );
    if let nodedb_sql::ddl_ast::NodedbStatement::Collection(
        nodedb_sql::ddl_ast::CollectionStmt::CreateCollection {
            engine, columns, ..
        },
    ) = stmt
    {
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns.len(), 3, "columns: {columns:?}");
    } else {
        panic!("expected CreateCollection");
    }
}

#[test]
fn conflicting_with_and_suffix_engine_rejected() {
    match ddl_parse(
        "CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = timeseries",
    ) {
        Some(Err(SqlError::ConflictingEngineClause {
            with_engine,
            suffix_engine,
        })) => {
            assert_eq!(with_engine, "kv");
            assert_eq!(suffix_engine, "timeseries");
        }
        other => panic!("expected ConflictingEngineClause, got {other:?}"),
    }
}

#[test]
fn agreeing_with_and_suffix_engine_accepted() {
    let engine =
        engine_field("CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = kv");
    assert_eq!(engine, Some("kv".to_string()));
}

// ── CREATE COLLECTION regression pin ─────────────────────────────────────────

#[test]
fn create_collection_succeeds() {
    let stmt = parse_ok("CREATE COLLECTION x");
    assert!(
        matches!(
            stmt,
            nodedb_sql::ddl_ast::NodedbStatement::Collection(
                nodedb_sql::ddl_ast::CollectionStmt::CreateCollection { .. }
            )
        ),
        "expected CreateCollection statement"
    );
}

// ── CREATE TABLE no columns: handled downstream (not a parse error) ──────────
//
// No-column CREATE TABLE parses successfully at the AST layer (returns empty
// columns vec). The "column list required" 42601 error is enforced in the
// create_table handler, not in the parser. This test pins that expectation.

#[test]
fn create_table_no_columns_parse_succeeds_with_empty_columns() {
    let stmt = parse_ok("CREATE TABLE x");
    if let nodedb_sql::ddl_ast::NodedbStatement::Collection(
        nodedb_sql::ddl_ast::CollectionStmt::CreateTable { columns, .. },
    ) = stmt
    {
        assert!(
            columns.is_empty(),
            "expected empty columns for no-column CREATE TABLE"
        );
    } else {
        panic!("expected CreateTable");
    }
}
