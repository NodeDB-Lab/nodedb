// SPDX-License-Identifier: BUSL-1.1

//! SQL-text utilities for `NodeDbQueryParser`.
//!
//! `parser.rs` calls these during Parse-message handling to classify a
//! statement and to count and neutralise its `$N` placeholders. Result-column
//! types come from the planner's `OutputSchema`, never from here.

/// Return true if `sql` starts with a DSL or DDL keyword that `plan_sql`
/// cannot parse and must be routed through `execute_sql` at Execute time.
///
/// Mirrors the prefix checks in the protocol-neutral DDL router so the
/// extended-query Parse handler can mark such statements as DSL passthroughs
/// and route them through the DSL dispatcher at Execute time.
///
/// NodeDB-specific DDL (`CREATE COLLECTION`, `DROP COLLECTION`, etc.) is also
/// included here because `execute_planned_sql_with_params` uses the standard
/// SQL planner (sqlparser) which does not recognise NodeDB extensions.
pub(super) fn is_dsl_statement(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    // `SEARCH ... USING VECTOR(...)` is preprocessor-rewritten into canonical
    // SELECT and goes through plan_sql like any other SELECT. Only the FUSION
    // form (and other SEARCH variants without a SELECT lowering) is a DSL
    // passthrough.
    if upper.starts_with("SEARCH ") && upper.contains("USING VECTOR") {
        return false;
    }
    // NodeDB DDL: `ddl_ast::parse` recognises these but `plan_sql` does not.
    // Route through `execute_sql` so the DDL router handles them. The full
    // parser tokenises and tries ~20 family dispatchers, so gate on the
    // first keyword first — most Parse messages carry plain SELECT/INSERT.
    let first_token = upper.split_whitespace().next().unwrap_or("");
    let may_be_ddl = matches!(
        first_token,
        "CREATE"
            | "DROP"
            | "ALTER"
            | "SHOW"
            | "DESCRIBE"
            | "GRANT"
            | "REVOKE"
            | "ANALYZE"
            | "COPY"
            | "BACKUP"
            | "RESTORE"
            | "UNDROP"
            | "REINDEX"
            | "REMOVE"
            | "REBALANCE"
            | "COMPACT"
    );
    if may_be_ddl && nodedb_sql::ddl_ast::parse(sql).is_some() {
        return true;
    }
    // Function, procedure, and aggregate DDL handled by the text-based DDL
    // router but not recognised by nodedb_sql::ddl_ast::parse.
    // Route through execute_sql so the DDL router intercepts them.
    if may_be_ddl
        && (upper.starts_with("CREATE OR REPLACE FUNCTION ")
            || upper.starts_with("CREATE FUNCTION ")
            || upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
            || upper.starts_with("CREATE AGGREGATE FUNCTION ")
            || upper.starts_with("CREATE OR REPLACE PROCEDURE ")
            || upper.starts_with("CREATE PROCEDURE ")
            || upper.starts_with("DROP FUNCTION ")
            || upper.starts_with("DROP PROCEDURE ")
            || upper.starts_with("ALTER FUNCTION ")
            || upper.starts_with("CALL "))
    {
        return true;
    }
    upper.starts_with("SEARCH ")
        || upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH ")
        || upper.starts_with("CRDT MERGE ")
        || upper.starts_with("UPSERT INTO ")
        || upper.starts_with("CREATE VECTOR INDEX ")
        || upper.starts_with("CREATE FULLTEXT INDEX ")
        || upper.starts_with("CREATE SEARCH INDEX ")
        || upper.starts_with("CREATE SPARSE INDEX ")
        // Kind-qualified index drops: recognized by the DDL router, rejected by
        // the SQL parser, so they must bypass Parse-time schema inference the
        // same way their CREATE counterparts do.
        || upper.starts_with("DROP VECTOR INDEX ")
        || upper.starts_with("DROP FULLTEXT INDEX ")
        || upper.starts_with("DROP SEARCH INDEX ")
        || upper.starts_with("DROP SPATIAL INDEX ")
        || upper.starts_with("DROP SPARSE INDEX ")
}

/// Replace each `$N` placeholder in `sql` with the literal `NULL`.
/// Used only for Parse-time schema inference — the real bound values
/// are substituted at Execute time.
pub(super) fn substitute_placeholders_with_null(sql: &str) -> String {
    let ranges = crate::control::server::shared::sql::placeholder::placeholder_ranges(sql);
    if ranges.is_empty() {
        return sql.to_owned();
    }
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for (start, end, _idx) in ranges {
        out.push_str(&sql[cursor..start]);
        out.push_str("NULL");
        cursor = end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Count $1, $2, ... placeholders in SQL text.
pub(super) fn count_placeholders(sql: &str) -> usize {
    let mut max_idx = 0usize;
    for (_, _, idx) in crate::control::server::shared::sql::placeholder::placeholder_ranges(sql) {
        if idx > max_idx {
            max_idx = max_idx.max(idx);
        }
    }
    max_idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_placeholders_basic() {
        assert_eq!(count_placeholders("SELECT $1, $2, $3"), 3);
        assert_eq!(count_placeholders("SELECT 1"), 0);
        assert_eq!(count_placeholders("WHERE id = $1 AND name = $1"), 1);
    }

    #[test]
    fn count_placeholders_malformed_body_unaffected() {
        assert_eq!(count_placeholders("SELECT $"), 0);
        assert_eq!(count_placeholders("SELECT $abc"), 0);
    }

    #[test]
    fn count_placeholders_bounded_against_absurd_index() {
        // Must not attempt a `Vec` sized off an attacker-controlled index
        // downstream — the shared scanner refuses to track it at all.
        assert_eq!(count_placeholders("SELECT $99999999999999"), 0);
        assert_eq!(count_placeholders("SELECT $65536"), 0);
        assert_eq!(count_placeholders("SELECT $65535"), 65535);
    }
}
