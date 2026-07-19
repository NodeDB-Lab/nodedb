// SPDX-License-Identifier: BUSL-1.1

//! PostgreSQL wire-compatibility reflection surface on virtual catalog tables.
//!
//! libpq-shaped clients (psql `\d`, the standard PostgreSQL drivers, and ORM
//! connection bootstraps) probe `pg_catalog` with four expression shapes that
//! the in-process virtual-table evaluator must resolve the same way the
//! PostgreSQL planner does:
//!
//!   1. `::regclass` / `::regtype` casts — resolve a relation/type name string
//!      to its catalog OID.
//!   2. extended catalog columns (`pg_type.typelem`, `typarray`,
//!      `pg_class.relhasindex`, `pg_attribute.attisdropped`, …) projected by
//!      driver type caches and `\d`.
//!   3. `ANY(<array>)` predicates, including `ANY(current_schemas(...))` where
//!      `current_schemas` is a catalog function returning `TEXT[]`.
//!   4. cross-vtable JOINs that project and filter columns from every relation
//!      in the FROM clause.
//!
//! Each test asserts the correct spec: the shape resolves and returns the
//! PostgreSQL-equivalent result. They fail today because the evaluator rejects
//! these shapes (leaking AST internals back to the client) instead of
//! resolving them.

mod common;
use common::pgwire_harness::TestServer;

// ───────────────────────── ::regclass / ::regtype casts ─────────────────────

/// `WHERE oid = '<relation>'::regclass` must resolve the relation name to its
/// catalog OID and match the corresponding `pg_class` row.
#[tokio::test]
async fn regclass_cast_resolves_relname_to_oid() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_regclass_target (id INTEGER PRIMARY KEY)")
        .await
        .expect("create collection");

    let rows = srv
        .query_text("SELECT relname FROM pg_class WHERE oid = 'reflect_regclass_target'::regclass")
        .await
        .expect("regclass cast resolves to the collection OID");
    assert_eq!(
        rows,
        vec!["reflect_regclass_target".to_string()],
        "'<relname>'::regclass must resolve to the row's OID and select exactly that relation"
    );
}

/// `WHERE oid = '<typename>'::regtype` must resolve the type name to its
/// `pg_type` OID — the type-cache analog of `::regclass` that every driver
/// uses to look up type OIDs by name.
#[tokio::test]
async fn regtype_cast_resolves_typename_to_oid() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT typname FROM pg_type WHERE oid = 'int4'::regtype")
        .await
        .expect("regtype cast resolves to the type OID");
    assert_eq!(
        rows,
        vec!["int4".to_string()],
        "'int4'::regtype must resolve to OID 23 and select the int4 row"
    );
}

// ───────────────────────── extended catalog columns ─────────────────────────

/// `pg_type.typelem` must project the element-type OID: zero for a scalar
/// type, and the element type's OID for an array type.
#[tokio::test]
async fn pg_type_typelem_column_projects_element_oid() {
    let srv = TestServer::start().await;

    let scalar = srv
        .query_text("SELECT typelem FROM pg_type WHERE typname = 'int4'")
        .await
        .expect("typelem column resolves");
    assert_eq!(
        scalar,
        vec!["0".to_string()],
        "int4 is a scalar type — typelem must be 0"
    );

    let array = srv
        .query_text("SELECT typelem FROM pg_type WHERE typname = '_float4'")
        .await
        .expect("typelem column resolves for array type");
    assert_eq!(
        array,
        vec!["700".to_string()],
        "_float4 is the array of float4 — typelem must be 700 (float4 OID)"
    );
}

/// `pg_type.typarray` must project the OID of the array type whose element is
/// this type. Driver type caches load `typarray` to recognize `_int4` etc.
#[tokio::test]
async fn pg_type_typarray_column_projects_array_oid() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT typarray FROM pg_type WHERE typname = 'int4'")
        .await
        .expect("typarray column resolves");
    assert_eq!(
        rows,
        vec!["1007".to_string()],
        "int4's array type is _int4 (OID 1007) — typarray must be 1007"
    );
}

/// `pg_class.relhasindex` must be true for a collection with a secondary
/// index and false for one without — `\d` and drivers read it to decide
/// whether to fetch index metadata.
#[tokio::test]
async fn pg_class_relhasindex_reflects_index_presence() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_idx_yes (id INTEGER PRIMARY KEY, email TEXT)")
        .await
        .expect("create indexed collection");
    srv.exec("CREATE UNIQUE INDEX reflect_idx_yes_email ON reflect_idx_yes (email)")
        .await
        .expect("create index");
    srv.exec("CREATE COLLECTION reflect_idx_no (id INTEGER PRIMARY KEY)")
        .await
        .expect("create non-indexed collection");

    let yes = srv
        .query_text("SELECT relhasindex FROM pg_class WHERE relname = 'reflect_idx_yes'")
        .await
        .expect("relhasindex column resolves");
    assert_eq!(
        yes,
        vec!["t".to_string()],
        "a collection with a secondary index must report relhasindex = true"
    );

    let no = srv
        .query_text("SELECT relhasindex FROM pg_class WHERE relname = 'reflect_idx_no'")
        .await
        .expect("relhasindex column resolves");
    assert_eq!(
        no,
        vec!["f".to_string()],
        "a collection with no secondary index must report relhasindex = false"
    );
}

/// `pg_attribute.attisdropped` must project (false for every live column).
/// Column-introspection queries filter `WHERE attisdropped = false` to skip
/// tombstoned columns.
#[tokio::test]
async fn pg_attribute_attisdropped_column_projects() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_attr (id INTEGER PRIMARY KEY, reflect_live_field TEXT)")
        .await
        .expect("create collection");

    let rows = srv
        .query_text("SELECT attisdropped FROM pg_attribute WHERE attname = 'reflect_live_field'")
        .await
        .expect("attisdropped column resolves");
    assert_eq!(
        rows,
        vec!["f".to_string()],
        "a live column must report attisdropped = false"
    );
}

/// Multi-word SQL type declarations must retain their PostgreSQL OIDs, and
/// collatable text columns must reference the default `pg_collation` row.
#[tokio::test]
async fn pg_attribute_preserves_multiword_types_and_collation() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION reflect_type_names (\
             id INTEGER PRIMARY KEY, \
             happened_at TIMESTAMP WITH TIME ZONE, \
             label CHARACTER VARYING)",
    )
    .await
    .expect("create collection with multi-word types");

    assert_eq!(
        srv.query_text(
            "SELECT atttypid FROM pg_attribute \
             WHERE attrelid = 'reflect_type_names'::regclass \
               AND attname = 'happened_at'",
        )
        .await
        .expect("timestamp type OID projects"),
        vec!["1184".to_string()]
    );
    assert_eq!(
        srv.query_text(
            "SELECT atttypid FROM pg_attribute \
             WHERE attrelid = 'reflect_type_names'::regclass \
               AND attname = 'label'",
        )
        .await
        .expect("varchar type OID projects"),
        vec!["1043".to_string()]
    );
    assert_eq!(
        srv.query_text(
            "SELECT c.collname FROM pg_attribute a \
             JOIN pg_collation c ON a.attcollation = c.oid \
             WHERE a.attrelid = 'reflect_type_names'::regclass \
               AND a.attname = 'label'",
        )
        .await
        .expect("default collation joins"),
        vec!["default".to_string()]
    );
}

// ──────────────────────────── ANY(<array>) predicates ───────────────────────

#[tokio::test]
async fn current_schemas_scalar_returns_text_array() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT current_schemas(false)")
        .await
        .expect("current_schemas(false) scalar select evaluates");
    assert_eq!(
        rows,
        vec!["{public}".to_string()],
        "current_schemas(false) must return a PostgreSQL TEXT[] value"
    );

    let with_implicit = srv
        .query_text("SELECT current_schemas(true)")
        .await
        .expect("current_schemas(true) scalar select evaluates");
    assert_eq!(
        with_implicit,
        vec!["{pg_catalog,public}".to_string()],
        "current_schemas(true) must include the implicit pg_catalog schema"
    );
}

/// `ANY(current_schemas(true))` must treat `current_schemas(true)` as a
/// `TEXT[]` including implicit schemas (`pg_catalog`) and evaluate the
/// membership predicate against it.
#[tokio::test]
async fn any_current_schemas_true_includes_implicit() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT nspname FROM pg_namespace WHERE nspname = ANY (current_schemas(true))")
        .await
        .expect("ANY(current_schemas(true)) evaluates");
    assert!(
        rows.iter().any(|s| s == "public"),
        "current_schemas(true) must include 'public': {rows:?}"
    );
    assert!(
        rows.iter().any(|s| s == "pg_catalog"),
        "current_schemas(true) must include the implicit 'pg_catalog': {rows:?}"
    );
}

/// `current_schemas(false)` excludes implicit schemas — the boolean argument
/// must be honored, not ignored.
#[tokio::test]
async fn any_current_schemas_false_excludes_implicit() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_text("SELECT nspname FROM pg_namespace WHERE nspname = ANY (current_schemas(false))")
        .await
        .expect("ANY(current_schemas(false)) evaluates");
    assert!(
        rows.iter().any(|s| s == "public"),
        "current_schemas(false) must include 'public': {rows:?}"
    );
    assert!(
        !rows.iter().any(|s| s == "pg_catalog"),
        "current_schemas(false) must NOT include the implicit 'pg_catalog': {rows:?}"
    );
}

/// `= ANY(ARRAY[...])` over an array literal must evaluate membership against
/// every element. Tests the `ANY` + array-literal path independent of the
/// catalog-function path.
#[tokio::test]
async fn any_over_array_literal_matches_each_element() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_set_a (id INTEGER PRIMARY KEY)")
        .await
        .expect("create a");
    srv.exec("CREATE COLLECTION reflect_set_b (id INTEGER PRIMARY KEY)")
        .await
        .expect("create b");

    let rows = srv
        .query_text(
            "SELECT relname FROM pg_class \
             WHERE relname = ANY (ARRAY['reflect_set_a', 'reflect_set_b'])",
        )
        .await
        .expect("ANY over array literal evaluates");
    assert!(
        rows.iter().any(|s| s == "reflect_set_a"),
        "ANY(ARRAY[...]) must match the first element: {rows:?}"
    );
    assert!(
        rows.iter().any(|s| s == "reflect_set_b"),
        "ANY(ARRAY[...]) must match the second element: {rows:?}"
    );
}

// ───────────────────── ActiveRecord catalog bootstrap queries ─────────────────────

/// ActiveRecord's type-map bootstrap always LEFT JOINs `pg_range`, even when
/// none of the requested types are ranges. The real query must resolve every
/// projected catalog column and preserve the requested `pg_type` rows.
#[tokio::test]
async fn active_record_type_map_query_joins_pg_range() {
    let srv = TestServer::start().await;

    let rows = srv
        .query_rows(
            "SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, \
                    r.rngsubtype, t.typtype, t.typbasetype \
             FROM pg_type AS t \
             LEFT JOIN pg_range AS r ON t.oid = r.rngtypid \
             WHERE t.oid IN (23, 25) \
             ORDER BY t.oid",
        )
        .await
        .expect("ActiveRecord type-map catalog query evaluates");

    assert_eq!(rows.len(), 2, "expected int4 and text type rows: {rows:?}");
    assert!(
        rows.iter().all(|row| row.len() == 8),
        "ActiveRecord expects all eight type-map columns: {rows:?}"
    );
    assert_eq!(rows[0][0], "23");
    assert_eq!(rows[0][1], "int4");
    assert_eq!(rows[1][0], "25");
    assert_eq!(rows[1][1], "text");
}

/// ActiveRecord's column introspection query requires `pg_attrdef` and
/// `pg_collation` to participate in LEFT JOINs, plus the PostgreSQL metadata
/// scalar functions used in its projection.
#[tokio::test]
async fn active_record_column_definitions_query_resolves() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION reflect_ar_columns (\
             id INTEGER PRIMARY KEY, \
             title TEXT DEFAULT 'untitled' NOT NULL, \
             enabled BOOLEAN NOT NULL)",
    )
    .await
    .expect("create collection with introspectable columns");

    let base_rows = srv
        .query_rows(
            "SELECT a.attname FROM pg_attribute a \
             WHERE a.attrelid = 'reflect_ar_columns'::regclass \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .await
        .expect("base pg_attribute query evaluates");
    assert_eq!(
        base_rows.len(),
        3,
        "the collection must expose three live pg_attribute rows: {base_rows:?}"
    );

    let defaults = srv
        .query_rows("SELECT adbin FROM pg_attrdef")
        .await
        .expect("pg_attrdef defaults project");
    assert_eq!(
        defaults,
        vec![vec!["'untitled'".to_string()]],
        "pg_attrdef must expose the declared default expression"
    );

    let attrdef_rows = srv
        .query_rows(
            "SELECT a.attname, d.adbin FROM pg_attribute a \
             LEFT JOIN pg_attrdef d \
               ON a.attrelid = d.adrelid AND a.attnum = d.adnum",
        )
        .await
        .expect("pg_attrdef LEFT JOIN evaluates");
    assert_eq!(
        attrdef_rows.len(),
        3,
        "pg_attrdef LEFT JOIN must preserve columns without defaults: {attrdef_rows:?}"
    );
    let title_default = attrdef_rows
        .iter()
        .find(|row| row.first().is_some_and(|name| name == "title"))
        .expect("title attribute row");
    assert_eq!(title_default[1], "'untitled'");

    let typed_rows = srv
        .query_rows(
            "SELECT a.attname FROM pg_attribute a \
             LEFT JOIN pg_attrdef d \
               ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             LEFT JOIN pg_type t ON a.atttypid = t.oid",
        )
        .await
        .expect("pg_type LEFT JOIN evaluates after pg_attrdef");
    assert_eq!(
        typed_rows.len(),
        3,
        "pg_type LEFT JOIN must preserve every attribute: {typed_rows:?}"
    );

    let joined_rows = srv
        .query_rows(
            "SELECT a.attname FROM pg_attribute a \
             LEFT JOIN pg_attrdef d \
               ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             LEFT JOIN pg_type t ON a.atttypid = t.oid \
             LEFT JOIN pg_collation c \
               ON a.attcollation = c.oid AND a.attcollation <> t.typcollation",
        )
        .await
        .expect("all ActiveRecord LEFT JOINs evaluate");
    assert_eq!(
        joined_rows.len(),
        3,
        "ActiveRecord LEFT JOIN chain must preserve all attributes: {joined_rows:?}"
    );

    let filtered_rows = srv
        .query_rows(
            "SELECT a.attname FROM pg_attribute a \
             LEFT JOIN pg_attrdef d \
               ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             LEFT JOIN pg_type t ON a.atttypid = t.oid \
             LEFT JOIN pg_collation c \
               ON a.attcollation = c.oid AND a.attcollation <> t.typcollation \
             WHERE a.attrelid = 'reflect_ar_columns'::regclass \
               AND a.attnum > 0 \
             ORDER BY a.attnum",
        )
        .await
        .expect("ActiveRecord predicates evaluate after LEFT JOINs");
    assert_eq!(
        filtered_rows.len(),
        3,
        "ActiveRecord predicates must retain all live columns: {filtered_rows:?}"
    );

    let rows = srv
        .query_rows(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), \
                    pg_get_expr(d.adbin, d.adrelid), a.attnotnull, \
                    a.atttypid, a.atttypmod, c.collname, \
                    col_description(a.attrelid, a.attnum) AS comment, \
                    a.attidentity, a.attgenerated \
             FROM pg_attribute a \
             LEFT JOIN pg_attrdef d \
               ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
             LEFT JOIN pg_type t ON a.atttypid = t.oid \
             LEFT JOIN pg_collation c \
               ON a.attcollation = c.oid AND a.attcollation <> t.typcollation \
             WHERE a.attrelid = 'reflect_ar_columns'::regclass \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .await
        .expect("ActiveRecord column-definitions catalog query evaluates");

    assert_eq!(rows.len(), 3, "expected one row per live column: {rows:?}");
    assert!(
        rows.iter().all(|row| row.len() == 10),
        "ActiveRecord expects all ten column-definition fields: {rows:?}"
    );
    assert_eq!(rows[0][0], "id");
    assert_eq!(rows[0][1], "integer");
    assert_eq!(rows[0][3], "t");
    assert_eq!(rows[1][0], "title");
    assert_eq!(rows[1][1], "text");
    assert_eq!(rows[1][2], "'untitled'");
    assert_eq!(rows[1][3], "t");
    assert_eq!(rows[2][0], "enabled");
    assert_eq!(rows[2][1], "boolean");
    assert_eq!(rows[2][3], "t");
}

// ───────────────────────────── cross-vtable JOINs ───────────────────────────

/// A JOIN across two virtual tables must resolve table-qualified projected
/// columns from both sides of the join.
#[tokio::test]
async fn cross_vtable_join_projects_columns_from_both_sides() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_join_basic (id INTEGER PRIMARY KEY)")
        .await
        .expect("create collection");

    let rows = srv
        .query_rows(
            "SELECT c.relname, n.nspname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace LIMIT 1",
        )
        .await
        .expect("cross-vtable join projects both sides");
    assert_eq!(rows.len(), 1, "expected exactly one joined row (LIMIT 1)");
    let row = &rows[0];
    assert_eq!(row.len(), 2, "expected two projected columns: {row:?}");
    assert!(
        !row[0].is_empty(),
        "c.relname (from the joined pg_class row) must be projected: {row:?}"
    );
    assert_eq!(
        row[1], "public",
        "n.nspname must resolve via the join key (relnamespace 2200 = public): {row:?}"
    );
}

/// A JOIN must apply a WHERE predicate on the *joined* table's column — the
/// canonical `\d` shape that filters by namespace.
#[tokio::test]
async fn cross_vtable_join_filters_on_joined_column() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_join_filter (id INTEGER PRIMARY KEY)")
        .await
        .expect("create collection");

    let rows = srv
        .query_text(
            "SELECT c.relname FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public'",
        )
        .await
        .expect("join with WHERE on joined column evaluates");
    assert!(
        rows.iter().any(|s| s == "reflect_join_filter"),
        "the public-schema collection must survive the joined-column filter: {rows:?}"
    );
}

/// The three-way `pg_class ⋈ pg_attribute ⋈ pg_type` join is the literal
/// shape `\d <table>` emits to describe a relation's columns and their types.
#[tokio::test]
async fn three_way_join_resolves_column_types() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_threeway (id INTEGER PRIMARY KEY)")
        .await
        .expect("create collection");

    let rows = srv
        .query_rows(
            "SELECT c.relname, a.attname, t.typname FROM pg_class c \
             JOIN pg_attribute a ON a.attrelid = c.oid \
             JOIN pg_type t ON t.oid = a.atttypid \
             WHERE c.relname = 'reflect_threeway'",
        )
        .await
        .expect("three-way join resolves across all relations");
    let id_row = rows.iter().find(|r| r.len() == 3 && r[1] == "id");
    let id_row =
        id_row.unwrap_or_else(|| panic!("expected a joined row for column 'id', got {rows:?}"));
    assert_eq!(
        id_row[0], "reflect_threeway",
        "c.relname must resolve in the three-way join: {id_row:?}"
    );
    // The `atttypid → pg_type.oid` leg of the join must land on a real type
    // row (the exact type depends on whether the engine records it; what this
    // proves is that all three relations resolved and joined).
    assert!(
        !id_row[2].is_empty(),
        "t.typname must resolve via atttypid → pg_type.oid in the three-way join: {id_row:?}"
    );
}

// ───────────────────────── AST-leak regression guard ────────────────────────

/// The specific silent/leaky failure mode: rejected catalog expressions
/// echoed the debug-formatted sqlparser AST back to the wire
/// (`expression Cast { kind: DoubleColon, … }`), both confusing clients and
/// enumerating evaluator capabilities. Whether these shapes resolve (post-fix)
/// or are rejected, the client must never see AST internals.
#[tokio::test]
async fn catalog_eval_errors_never_leak_ast_internals() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION reflect_leak_guard (id INTEGER PRIMARY KEY)")
        .await
        .expect("create collection");

    let shapes = [
        "SELECT 'pg_class'::regclass::oid",
        "SELECT typname, typelem FROM pg_type WHERE typname = 'int4'",
        "SELECT n.nspname FROM pg_namespace n WHERE n.nspname = ANY (current_schemas(true))",
        "SELECT c.relname, n.nspname FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace LIMIT 1",
    ];
    // Tokens that only appear in a `format!("{:?}")` of the sqlparser AST.
    let ast_markers = [
        "Cast {",
        "AnyOp {",
        "BinaryOp {",
        "Function {",
        "DoubleColon",
        "data_type: Regclass",
        "CompoundIdentifier",
    ];

    for sql in shapes {
        if let Err(msg) = srv.query_text(sql).await {
            for marker in ast_markers {
                assert!(
                    !msg.contains(marker),
                    "catalog eval error leaked AST internals ({marker:?}) for `{sql}`: {msg}"
                );
            }
        }
    }
}
